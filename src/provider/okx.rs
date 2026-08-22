//! Okx Spot and Perpetual REST history and raw WebSocket transport.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use reqwest::{StatusCode, Url, header::RETRY_AFTER};
use serde::Deserialize;
use serde_json::Value;
use time::{Date, Month, OffsetDateTime};
#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    cli::canonicalize_instrument,
    clock::{Clock, checked_deadline},
    error::{ErrorContext, ErrorOperation, PayloadError, ProviderError, SanitizedMessage},
    model::{
        Candle, HistoryRequest, HistoryRequestKind, Instrument, InstrumentSpec, Market, ProviderId,
        Timeframe,
    },
    provider::{
        LiveFeed, LiveRequest, MarketDataProvider, ProviderCapabilities, ProviderFuture,
        RateGateSnapshot,
        runtime::{
            http::{HttpRuntime, RateLimitDecision},
            live::LiveSupervisorConfig as SharedLiveSupervisorConfig,
        },
    },
};

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
use crate::{
    error::TimeoutKind,
    provider::runtime::{
        live::{
            ConnectionRotation, LiveAdapter, LiveConfig, LiveRateGate, LiveSocket, LiveSocketEvent,
            ProcessBlockPolicy, ReconciliationLimits,
        },
        websocket::{
            DecodedFrame, ReadinessInput, WsCodec, WsConfig, connect_websocket_url,
            contextualize_websocket_configuration, validate_websocket_base,
        },
    },
};

const CANDLES_PATH: &str = "/api/v5/market/candles";
const HISTORY_CANDLES_PATH: &str = "/api/v5/market/history-candles";
pub const REST_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const REST_BODY_LIMIT: usize = 2 * 1024 * 1024;
pub const RATE_LIMIT_FALLBACK: Duration = Duration::from_secs(30);
pub const OKX_MAX_RESPONSE_ROWS: usize = 300;
const UNSUPPORTED_TIMEFRAME: &str = "OKX does not support the 8h timeframe";

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_REST_BASE: &str = "https://openapi.okx.com";
#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_WS_BASE: &str = "wss://ws.okx.com:8443/ws/v5/business";

pub const SUBSCRIBE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
pub const APPLICATION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
pub const MESSAGE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_FUTURE_CANDLE_SKEW: Duration = Duration::from_secs(5 * 60);
const MAX_GAP_RECONCILIATION_CANDLES: usize = 64_000;
const MAX_GAP_RECONCILIATION_PAGES: usize = 214;
const MAX_PRE_ACK_CANDLES: usize = 16;
const SUBSCRIPTION_ID: &str = "fccli1";

#[derive(Clone, Debug)]
pub struct OkxLiveConfig {
    pub supervisor: SharedLiveSupervisorConfig,
    pub subscribe_ack_timeout: Duration,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub application_heartbeat_interval_for_test: Duration,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub heartbeat_test_hook: Option<HeartbeatTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub subscribe_flush_test_hook: Option<SubscribeFlushTestHook>,
}
impl Default for OkxLiveConfig {
    fn default() -> Self {
        let mut supervisor = SharedLiveSupervisorConfig::default();
        supervisor.ws_config.message_inactivity_timeout = MESSAGE_INACTIVITY_TIMEOUT;
        Self {
            supervisor,
            subscribe_ack_timeout: SUBSCRIBE_ACK_TIMEOUT,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            application_heartbeat_interval_for_test: APPLICATION_HEARTBEAT_INTERVAL,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            heartbeat_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            subscribe_flush_test_hook: None,
        }
    }
}
impl OkxLiveConfig {
    pub fn validate(&self) -> Result<(), ProviderError> {
        self.supervisor.validate()?;
        if !(Duration::from_millis(1)..=Duration::from_secs(60))
            .contains(&self.subscribe_ack_timeout)
        {
            return Err(ProviderError::Configuration(
                "subscribe acknowledgement timeout is outside 1ms..=60s",
            ));
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if !(Duration::from_millis(1)..=Duration::from_secs(60))
            .contains(&self.application_heartbeat_interval_for_test)
        {
            return Err(ProviderError::Configuration(
                "application heartbeat interval is outside 1ms..=60s",
            ));
        }
        Ok(())
    }
}
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
use crate::provider::runtime::websocket::{HeartbeatTestHook, SubscribeFlushTestHook};

#[derive(Deserialize)]
struct OkxEnvelope {
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Vec<Vec<Value>>,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug, Default)]
pub struct OkxWsCodec {
    subscribed: bool,
    pre_ack: VecDeque<Candle>,
    now_ms: Option<i64>,
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
#[derive(Clone, Debug, Default)]
pub(crate) struct OkxWsCodec {
    subscribed: bool,
    pre_ack: VecDeque<Candle>,
    now_ms: Option<i64>,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug, PartialEq)]
pub enum OkxDecoded {
    Candle(Candle),
    SubscribeAccepted { buffered: Vec<Candle> },
    ApplicationPong,
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OkxDecoded {
    Candle(Candle),
    SubscribeAccepted { buffered: Vec<Candle> },
    ApplicationPong,
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl OkxWsCodec {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            subscribed: false,
            pre_ack: VecDeque::new(),
            now_ms: None,
        }
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    #[must_use]
    pub const fn with_now_ms(now_ms: i64) -> Self {
        Self {
            subscribed: false,
            pre_ack: VecDeque::new(),
            now_ms: Some(now_ms),
        }
    }
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl WsCodec for OkxWsCodec {
    type Outcome = OkxDecoded;

    fn decode(
        &mut self,
        message: Message,
        instrument: &Instrument,
        timeframe: Timeframe,
        config: &WsConfig,
        output: &mut VecDeque<DecodedFrame<Self::Outcome>>,
    ) {
        decode_ws_frame(self, message, instrument, timeframe, config, output);
    }

    fn readiness_priority(outcome: &Self::Outcome) -> u8 {
        match outcome {
            OkxDecoded::Candle(_) => 1,
            OkxDecoded::SubscribeAccepted { .. } => 2,
            OkxDecoded::ApplicationPong => u8::MAX,
        }
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    fn is_subscribe_accepted(outcome: &Self::Outcome) -> bool {
        matches!(outcome, OkxDecoded::SubscribeAccepted { .. })
    }
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub fn websocket_url(instrument: &Instrument, timeframe: Timeframe) -> Result<Url, ProviderError> {
    websocket_url_from_base(production_ws_base(), timeframe, false)
        .map_err(|error| contextualize_websocket_configuration(error, instrument, timeframe))
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn test_websocket_url(
    base_url: &str,
    instrument: &Instrument,
    timeframe: Timeframe,
) -> Result<Url, ProviderError> {
    websocket_url_from_base(base_url, timeframe, true)
        .map_err(|error| contextualize_websocket_configuration(error, instrument, timeframe))
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn production_rest_base() -> &'static str {
    PRODUCTION_REST_BASE
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn production_ws_base() -> &'static str {
    PRODUCTION_WS_BASE
}
#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
fn websocket_url_from_base(
    base_url: &str,
    timeframe: Timeframe,
    loopback_only: bool,
) -> Result<Url, ProviderError> {
    reject_unsupported_timeframe(timeframe)?;
    let url = validate_websocket_base(base_url, loopback_only)?;
    if !loopback_only && url.path() != "/ws/v5/business" {
        return Err(ProviderError::Configuration(
            "invalid OKX business WebSocket URL",
        ));
    }
    Ok(url)
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn decode_ws_frame(
    codec: &mut OkxWsCodec,
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<OkxDecoded>>,
) {
    decode_ws_frame_impl(codec, message, instrument, timeframe, config, outcomes);
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn decode_ws_frame(
    codec: &mut OkxWsCodec,
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<OkxDecoded>>,
) {
    decode_ws_frame_impl(codec, message, instrument, timeframe, config, outcomes);
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
fn decode_ws_frame_impl(
    codec: &mut OkxWsCodec,
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<OkxDecoded>>,
) {
    if let Err(error) = config.validate() {
        outcomes.push_back(DecodedFrame::ProviderError(error));
        return;
    }
    match message {
        Message::Text(text) => {
            decode_ws_payload(
                codec,
                text.as_bytes(),
                instrument,
                timeframe,
                config,
                outcomes,
            );
        }
        Message::Binary(bytes) => {
            decode_ws_payload(codec, &bytes, instrument, timeframe, config, outcomes);
        }
        Message::Close(frame) => {
            outcomes.push_back(DecodedFrame::Close(frame.map(|frame| frame.code)));
        }
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
            outcomes.push_back(DecodedFrame::Ignored);
        }
    }
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
fn decode_ws_payload(
    codec: &mut OkxWsCodec,
    bytes: &[u8],
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<OkxDecoded>>,
) {
    let context =
        ErrorContext::operation(ErrorOperation::WebSocket).with_market(instrument, timeframe);
    if bytes.len() > config.max_message_size {
        outcomes.push_back(DecodedFrame::ProviderError(payload(
            &context,
            PayloadError::OverBudget {
                limit_bytes: config.max_message_size,
            },
        )));
        return;
    }
    if bytes == b"pong" {
        outcomes.push_back(DecodedFrame::Provider(OkxDecoded::ApplicationPong));
        return;
    }
    let value: Value = match serde_json::from_slice(bytes) {
        Ok(value) => value,
        Err(_) => {
            outcomes.push_back(DecodedFrame::ProviderError(payload(
                &context,
                PayloadError::MalformedProtocol,
            )));
            return;
        }
    };
    let event = value.get("event").and_then(Value::as_str);
    if event == Some("notice") {
        if value.get("code").and_then(Value::as_str) == Some("64008") {
            outcomes.push_back(DecodedFrame::ReconnectRequested);
        } else {
            outcomes.push_back(DecodedFrame::Ignored);
        }
        return;
    }
    if event == Some("error") {
        outcomes.push_back(DecodedFrame::ProviderError(ProviderError::Protocol {
            context,
            detail: "provider reported a WebSocket error",
        }));
        return;
    }
    let expected_channel = match channel(timeframe) {
        Ok(channel) => channel,
        Err(error) => {
            outcomes.push_back(DecodedFrame::ProviderError(error));
            return;
        }
    };
    let arg_matches = |arg: &Value| {
        arg.get("channel").and_then(Value::as_str) == Some(expected_channel)
            && arg.get("instId").and_then(Value::as_str) == Some(instrument.provider_symbol())
    };
    if event == Some("subscribe") {
        let exact_ack = value.get("id").and_then(Value::as_str) == Some(SUBSCRIPTION_ID)
            && value.get("arg").is_some_and(arg_matches);
        if codec.subscribed || !exact_ack {
            outcomes.push_back(DecodedFrame::ProviderError(payload(
                &context,
                PayloadError::MalformedProtocol,
            )));
            return;
        }
        codec.subscribed = true;
        let buffered = codec.pre_ack.drain(..).collect();
        outcomes.push_back(DecodedFrame::Provider(OkxDecoded::SubscribeAccepted {
            buffered,
        }));
        return;
    }
    let Some(arg) = value.get("arg") else {
        outcomes.push_back(DecodedFrame::Ignored);
        return;
    };
    if !arg_matches(arg) {
        outcomes.push_back(DecodedFrame::ProviderError(ProviderError::Protocol {
            context,
            detail: "WebSocket candle market does not match subscription",
        }));
        return;
    }
    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        outcomes.push_back(DecodedFrame::ProviderError(payload(
            &context,
            PayloadError::MalformedProtocol,
        )));
        return;
    };
    if rows.len() > OKX_MAX_RESPONSE_ROWS {
        outcomes.push_back(DecodedFrame::ProviderError(payload(
            &context,
            PayloadError::MalformedProtocol,
        )));
        return;
    }
    let now_ms = match codec.now_ms.map_or_else(unix_now_ms, Ok) {
        Ok(now_ms) => now_ms,
        Err(error) => {
            outcomes.push_back(DecodedFrame::ProviderError(error));
            return;
        }
    };
    for row in rows {
        let Some(fields) = row.as_array() else {
            outcomes.push_back(DecodedFrame::ProviderError(payload(
                &context,
                PayloadError::WrongArity {
                    expected: 9,
                    actual: 0,
                },
            )));
            return;
        };
        let candle = match decode_okx_row(
            fields,
            instrument.market(),
            timeframe,
            &context,
            true,
            now_ms,
        ) {
            Ok(candle) => candle,
            Err(error) => {
                outcomes.push_back(DecodedFrame::ProviderError(error));
                return;
            }
        };
        if codec.subscribed {
            outcomes.push_back(DecodedFrame::Provider(OkxDecoded::Candle(candle)));
        } else if let Some(existing) = codec
            .pre_ack
            .iter_mut()
            .find(|item| item.open_time() == candle.open_time())
        {
            *existing = candle;
        } else {
            if codec.pre_ack.len() == MAX_PRE_ACK_CANDLES {
                outcomes.push_back(DecodedFrame::ProviderError(ProviderError::Protocol {
                    context: context.clone(),
                    detail: "Okx pre-ack candle buffer exceeded 16 distinct candles",
                }));
                return;
            }
            codec.pre_ack.push_back(candle);
        }
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub type RawWebSocket = crate::provider::runtime::websocket::RawWebSocket<OkxWsCodec>;

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub(crate) type RawWebSocket = crate::provider::runtime::websocket::RawWebSocket<OkxWsCodec>;

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub(crate) async fn connect_websocket(
    instrument: &Instrument,
    timeframe: Timeframe,
    config: WsConfig,
) -> Result<RawWebSocket, ProviderError> {
    let url = websocket_url(instrument, timeframe)?;
    connect_websocket_url(
        &url,
        instrument,
        timeframe,
        config,
        OkxWsCodec::new(),
        Some(Message::Text("ping".into())),
    )
    .await
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub async fn connect_test_websocket(
    base_url: &str,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: WsConfig,
) -> Result<RawWebSocket, ProviderError> {
    let url = test_websocket_url(base_url, instrument, timeframe)?;
    connect_websocket_url(
        &url,
        instrument,
        timeframe,
        config,
        OkxWsCodec::new(),
        Some(Message::Text("ping".into())),
    )
    .await
}

#[derive(Clone)]
pub struct OkxProvider {
    http: HttpRuntime,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    base_url: Url,
    clock: Arc<dyn Clock>,
    rate_limit_fallback: Duration,
    live: OkxLiveConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    ws_base_url: Option<String>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    now_ms: Option<i64>,
}
struct OkxBuildConfig {
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    base_url: Url,
    request_timeout: Duration,
    body_limit: usize,
    rate_limit_fallback: Duration,
    live: OkxLiveConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    ws_base_url: Option<String>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    now_ms: Option<i64>,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct OkxTestConfig {
    pub base_url: String,
    pub request_timeout: Duration,
    pub body_limit: usize,
    pub rate_limit_fallback: Duration,
    pub now_ms: Option<i64>,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
impl OkxTestConfig {
    #[must_use]
    pub fn loopback(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            request_timeout: REST_REQUEST_TIMEOUT,
            body_limit: REST_BODY_LIMIT,
            rate_limit_fallback: RATE_LIMIT_FALLBACK,
            now_ms: None,
        }
    }

    #[must_use]
    pub fn with_websocket_base(self, base_url: impl Into<String>) -> OkxLiveTestConfig {
        OkxLiveTestConfig {
            rest: self,
            ws_base_url: base_url.into(),
            live: OkxLiveConfig::default(),
        }
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct OkxLiveTestConfig {
    pub rest: OkxTestConfig,
    pub ws_base_url: String,
    pub live: OkxLiveConfig,
}

impl OkxProvider {
    #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
    pub fn new(clock: Arc<dyn Clock>) -> Result<Self, ProviderError> {
        Self::build(
            clock,
            OkxBuildConfig {
                request_timeout: REST_REQUEST_TIMEOUT,
                body_limit: REST_BODY_LIMIT,
                rate_limit_fallback: RATE_LIMIT_FALLBACK,
                live: OkxLiveConfig::default(),
            },
        )
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test(
        base_url: impl AsRef<str>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        Self::new_test_with_config_and_clock(OkxTestConfig::loopback(base_url.as_ref()), clock)
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test_with_config_and_clock(
        config: OkxTestConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        let base_url = validate_loopback_base(&config.base_url)?;
        Self::build(
            clock,
            OkxBuildConfig {
                base_url,
                request_timeout: config.request_timeout,
                body_limit: config.body_limit,
                rate_limit_fallback: config.rate_limit_fallback,
                live: OkxLiveConfig::default(),
                ws_base_url: None,
                now_ms: config.now_ms,
            },
        )
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test_live(
        config: OkxLiveTestConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        let base_url = validate_loopback_base(&config.rest.base_url)?;
        validate_loopback_ws_base(&config.ws_base_url)?;
        Self::build(
            clock,
            OkxBuildConfig {
                base_url,
                request_timeout: config.rest.request_timeout,
                body_limit: config.rest.body_limit,
                rate_limit_fallback: config.rest.rate_limit_fallback,
                live: config.live,
                ws_base_url: Some(config.ws_base_url),
                now_ms: config.rest.now_ms,
            },
        )
    }

    fn build(clock: Arc<dyn Clock>, config: OkxBuildConfig) -> Result<Self, ProviderError> {
        if config.request_timeout.is_zero()
            || config.body_limit == 0
            || config.rate_limit_fallback.is_zero()
        {
            return Err(ProviderError::Configuration(
                "REST timeout, body limit, and fallback must be positive",
            ));
        }
        config.live.validate()?;
        let http = HttpRuntime::new(
            Arc::clone(&clock),
            config.request_timeout,
            config.body_limit,
        )?;
        Ok(Self {
            http,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            base_url: config.base_url,
            clock,
            rate_limit_fallback: config.rate_limit_fallback,
            live: config.live,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            ws_base_url: config.ws_base_url,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            now_ms: config.now_ms,
        })
    }

    pub async fn history(
        &self,
        instrument: &Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> Result<Vec<Candle>, ProviderError> {
        reject_unsupported_timeframe(timeframe)?;
        if request.limit() > OKX_MAX_RESPONSE_ROWS as u16 {
            return Err(ProviderError::Configuration(
                "OKX history request limit exceeds 300",
            ));
        }
        let context =
            ErrorContext::operation(ErrorOperation::History).with_market(instrument, timeframe);
        self.await_gate(&cancellation, &context).await?;
        #[cfg(any(
            all(feature = "production-transport", not(feature = "test-transport")),
            all(feature = "test-transport", not(feature = "production-transport"))
        ))]
        {
            #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
            let mut url = Url::parse(production_rest_base())
                .map_err(|_| ProviderError::Configuration("invalid production REST base URL"))?;
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            let mut url = self.base_url.clone();
            url.set_path(match request.kind() {
                HistoryRequestKind::Latest => CANDLES_PATH,
                HistoryRequestKind::Older | HistoryRequestKind::Gap => HISTORY_CANDLES_PATH,
            });
            url.set_query(None);
            let mut query = vec![
                ("instId", instrument.provider_symbol().to_owned()),
                ("bar", bar(timeframe)?.to_owned()),
                ("limit", request.limit().to_string()),
            ];
            match request.kind() {
                HistoryRequestKind::Latest => {}
                HistoryRequestKind::Older => query.push((
                    "after",
                    request
                        .end_time()
                        .ok_or(ProviderError::Invariant(
                            "older history request is missing endTime",
                        ))?
                        .to_string(),
                )),
                HistoryRequestKind::Gap => {
                    let start = request.start_time().ok_or(ProviderError::Invariant(
                        "gap history request is missing startTime",
                    ))?;
                    let end = request.end_time().ok_or(ProviderError::Invariant(
                        "gap history request is missing endTime",
                    ))?;
                    let page_upper = page_upper_bound(timeframe, start, request.limit())?.min(end);
                    query.push(("before", start.saturating_sub(1).to_string()));
                    query.push(("after", page_upper.saturating_add(1).to_string()));
                }
            }
            let response = self
                .http
                .send(
                    self.http.client().get(url).query(&query),
                    &cancellation,
                    &context,
                )
                .await?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                return self.handle_rate_limit(&response, context);
            }
            let bytes = self
                .http
                .read_response(response, &cancellation, context.clone(), map_http_error)
                .await?;
            if serde_json::from_slice::<OkxEnvelope>(&bytes)
                .ok()
                .is_some_and(|envelope| envelope.code == "50011")
            {
                let deadline = checked_deadline(self.clock.now(), self.rate_limit_fallback)
                    .map_err(|_| ProviderError::Invariant("rate-limit deadline overflow"))?;
                self.http.apply_rate_limit(
                    RateLimitDecision::TimedUntil(deadline),
                    context.clone(),
                    StatusCode::OK,
                )?;
            }
            decode_candles(
                &bytes,
                instrument.market(),
                timeframe,
                request,
                context,
                self.unix_now_ms()?,
            )
        }
        #[cfg(all(feature = "production-transport", feature = "test-transport"))]
        {
            let _ = request;
            unreachable!("mutually exclusive transport features are rejected by src/lib.rs")
        }
    }

    async fn await_gate(
        &self,
        cancellation: &CancellationToken,
        context: &ErrorContext,
    ) -> Result<(), ProviderError> {
        self.http
            .await_gate(cancellation, context, |_| {
                ProviderError::Invariant("Okx rate gate cannot be process-blocked")
            })
            .await
    }

    fn handle_rate_limit(
        &self,
        response: &reqwest::Response,
        context: ErrorContext,
    ) -> Result<Vec<Candle>, ProviderError> {
        let deadline = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|seconds| {
                checked_deadline(self.clock.now(), Duration::from_secs(seconds)).ok()
            })
            .or_else(|| checked_deadline(self.clock.now(), self.rate_limit_fallback).ok())
            .ok_or(ProviderError::Invariant("rate-limit deadline overflow"))?;
        self.http.apply_rate_limit(
            RateLimitDecision::TimedUntil(deadline),
            context,
            response.status(),
        )?;
        unreachable!("rate-limit application always returns an error")
    }
    pub fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        if spec.provider().as_str() != "okx" || spec.venue().is_some() {
            return Err(ProviderError::Configuration(
                "instrument is not valid for OKX",
            ));
        }
        let local = canonicalize_instrument(spec)
            .map_err(|_| ProviderError::Configuration("instrument is not valid for OKX"))?;
        let suffix = if spec.market() == Market::Perpetual {
            "-SWAP"
        } else {
            ""
        };
        Instrument::new(
            spec.provider().clone(),
            spec.market(),
            local.base(),
            local.quote(),
            format!("{}-{}{}", local.base(), local.quote(), suffix),
        )
        .map_err(|_| ProviderError::Configuration("instrument is not valid for OKX"))
    }

    fn unix_now_ms(&self) -> Result<i64, ProviderError> {
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if let Some(now_ms) = self.now_ms {
            return Ok(now_ms);
        }
        unix_now_ms()
    }

    #[must_use]
    pub fn rate_gate(&self) -> RateGateSnapshot {
        self.http.gate_snapshot()
    }

    fn history_page_limit(&self) -> u16 {
        300
    }

    #[cfg(any(
        all(feature = "production-transport", not(feature = "test-transport")),
        all(feature = "test-transport", not(feature = "production-transport"))
    ))]
    async fn connect_live_socket(
        &self,
        instrument: &Instrument,
        timeframe: Timeframe,
    ) -> Result<RawWebSocket, ProviderError> {
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        {
            connect_websocket(instrument, timeframe, self.live.supervisor.ws_config).await
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            let base = self
                .ws_base_url
                .as_deref()
                .ok_or(ProviderError::Configuration(
                    "test WebSocket base URL is required for live feeds",
                ))?;
            connect_test_websocket(base, instrument, timeframe, self.live.supervisor.ws_config)
                .await
        }
        #[cfg(all(feature = "production-transport", feature = "test-transport"))]
        {
            let _ = (instrument, timeframe);
            unreachable!("mutually exclusive transport features are rejected by src/lib.rs")
        }
    }
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
pub(crate) struct OkxLiveAdapter {
    provider: OkxProvider,
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl OkxLiveAdapter {
    fn new(provider: OkxProvider) -> Self {
        Self { provider }
    }

    fn subscribe_ack_timeout(
        &self,
        instrument: &Instrument,
        timeframe: Timeframe,
    ) -> ProviderError {
        ProviderError::Timeout {
            context: ErrorContext::operation(ErrorOperation::WebSocket)
                .with_market(instrument, timeframe),
            kind: TimeoutKind::SubscribeAck,
        }
    }
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
pub(crate) struct OkxLiveSocket {
    raw: RawWebSocket,
    buffered: VecDeque<Candle>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    stalled_write_probe_frames: usize,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    stalled_write_probe_payload_size: usize,
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl LiveSocket for OkxLiveSocket {
    async fn read(&mut self) -> Result<LiveSocketEvent, ProviderError> {
        if let Some(candle) = self.buffered.pop_front() {
            return Ok(LiveSocketEvent::Candle(candle));
        }
        match self.raw.read().await? {
            DecodedFrame::Provider(OkxDecoded::Candle(candle)) => {
                Ok(LiveSocketEvent::Candle(candle))
            }
            DecodedFrame::Ignored | DecodedFrame::Provider(OkxDecoded::ApplicationPong) => {
                Ok(LiveSocketEvent::Ignored)
            }
            DecodedFrame::ProviderError(error) => Ok(LiveSocketEvent::DecodedError(error)),
            DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested => {
                self.raw.finalize_peer_close().await?;
                Ok(LiveSocketEvent::ReconnectRequested)
            }
            DecodedFrame::Provider(OkxDecoded::SubscribeAccepted { .. }) => Ok(
                LiveSocketEvent::ProtocolViolation("duplicate Okx subscribe acknowledgement"),
            ),
        }
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    async fn after_gap_sync_test_probe(&mut self) -> Result<(), ProviderError> {
        if self.stalled_write_probe_frames == 0 {
            return Ok(());
        }
        let payload = Message::Binary(vec![0; self.stalled_write_probe_payload_size].into());
        for _ in 0..self.stalled_write_probe_frames {
            self.raw.send(payload.clone()).await?;
        }
        Ok(())
    }
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl LiveAdapter for OkxLiveAdapter {
    type Socket = OkxLiveSocket;

    fn validate_request(
        &self,
        _instrument: &Instrument,
        timeframe: Timeframe,
    ) -> Result<(), ProviderError> {
        reject_unsupported_timeframe(timeframe)
    }

    async fn connect_ready_socket(
        &self,
        instrument: Instrument,
        timeframe: Timeframe,
    ) -> Result<Self::Socket, ProviderError> {
        let mut raw = self
            .provider
            .connect_live_socket(&instrument, timeframe)
            .await?;
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if self.provider.live.supervisor.stalled_write_probe_frames != 0 {
            let payload_size = self
                .provider
                .live
                .supervisor
                .ws_config
                .write_buffer_size
                .min(self.provider.live.supervisor.ws_config.max_frame_size)
                .min(self.provider.live.supervisor.ws_config.max_message_size)
                .max(1);
            let payload = Message::Binary(vec![0; payload_size].into());
            for _ in 0..self.provider.live.supervisor.stalled_write_probe_frames {
                raw.send(payload.clone()).await?;
            }
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            raw.subscribe_flush_test_hook = self.provider.live.subscribe_flush_test_hook.clone();
        }
        raw.send(Message::Text(
            subscribe_message(&instrument, timeframe).into(),
        ))
        .await?;
        let ack_deadline = checked_deadline(
            self.provider.clock.now(),
            self.provider.live.subscribe_ack_timeout,
        )
        .map_err(|_| ProviderError::Invariant("subscribe-ack deadline overflow"))?;
        let buffered = loop {
            let inactivity_deadline = raw.inactivity_deadline();
            tokio::select! {
                biased;
                input = raw.read_readiness() => match input {
                    ReadinessInput::Error(error) => return Err(error),
                    ReadinessInput::Frame(DecodedFrame::Provider(OkxDecoded::SubscribeAccepted { buffered })) => break buffered,
                    ReadinessInput::Frame(DecodedFrame::Ignored | DecodedFrame::Provider(OkxDecoded::ApplicationPong)) => {
                        if self.provider.clock.now() >= ack_deadline {
                            return Err(self.subscribe_ack_timeout(&instrument, timeframe));
                        }
                    }
                    ReadinessInput::Frame(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => {
                        raw.finalize_peer_close().await?;
                        return Err(ProviderError::Protocol { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&instrument, timeframe), detail: "WebSocket peer requested reconnect" });
                    }
                    ReadinessInput::Frame(DecodedFrame::ProviderError(error)) => return Err(error),
                    ReadinessInput::Frame(DecodedFrame::Provider(OkxDecoded::Candle(_))) => return Err(ProviderError::Protocol { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&instrument, timeframe), detail: "Okx codec emitted a candle before subscribe acknowledgement" }),
                },
                () = self.provider.clock.sleep_until(ack_deadline) => return Err(self.subscribe_ack_timeout(&instrument, timeframe)),
                () = tokio::time::sleep_until(inactivity_deadline) => return Err(ProviderError::Timeout { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&instrument, timeframe), kind: TimeoutKind::WebSocketInactivity }),
            }
        };
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            raw.heartbeat_test_hook = self.provider.live.heartbeat_test_hook.clone();
        }
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        raw.start_application_heartbeat(APPLICATION_HEARTBEAT_INTERVAL);
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        raw.start_application_heartbeat(self.provider.live.application_heartbeat_interval_for_test);
        Ok(OkxLiveSocket {
            raw,
            buffered: buffered.into(),
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            stalled_write_probe_frames: self.provider.live.supervisor.stalled_write_probe_frames,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            stalled_write_probe_payload_size: self
                .provider
                .live
                .supervisor
                .ws_config
                .write_buffer_size
                .min(self.provider.live.supervisor.ws_config.max_frame_size)
                .min(self.provider.live.supervisor.ws_config.max_message_size)
                .max(1),
        })
    }

    async fn history(
        &self,
        instrument: Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> Result<Vec<Candle>, ProviderError> {
        self.provider
            .history(&instrument, timeframe, request, cancellation)
            .await
    }

    fn rate_gate(&self) -> LiveRateGate {
        LiveRateGate {
            snapshot: self.provider.rate_gate(),
            process_block: ProcessBlockPolicy::Forbidden("Okx rate gate cannot be process-blocked"),
        }
    }

    fn live_config(&self) -> LiveConfig<'_> {
        LiveConfig {
            supervisor: &self.provider.live.supervisor,
            reconciliation: ReconciliationLimits {
                max_successors: MAX_GAP_RECONCILIATION_CANDLES,
                max_pages: MAX_GAP_RECONCILIATION_PAGES,
                span_exceeded: "Okx gap reconciliation target exceeds the per-generation span limit",
                page_exceeded: "Okx gap reconciliation exceeded the per-generation page limit",
                distinct_exceeded: "Okx gap reconciliation exceeded the distinct buffered-candle limit",
            },
        }
    }

    fn connection_rotation(&self) -> ConnectionRotation {
        ConnectionRotation::Never
    }
}

impl MarketDataProvider for OkxProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("okx").expect("static provider id")
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            markets: &[Market::Spot, Market::Perpetual],
            timeframes: &[
                Timeframe::Second1,
                Timeframe::Minute1,
                Timeframe::Minute3,
                Timeframe::Minute5,
                Timeframe::Minute15,
                Timeframe::Minute30,
                Timeframe::Hour1,
                Timeframe::Hour2,
                Timeframe::Hour4,
                Timeframe::Hour6,
                Timeframe::Hour12,
                Timeframe::Day1,
                Timeframe::Day3,
                Timeframe::Week1,
                Timeframe::Month1,
            ],
            history_page_limit: self.history_page_limit(),
        }
    }
    fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        OkxProvider::canonicalize(self, spec)
    }
    fn history<'a>(
        &'a self,
        instrument: &'a Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>> {
        Box::pin(OkxProvider::history(
            self,
            instrument,
            timeframe,
            request,
            cancellation,
        ))
    }
    fn open_live<'a>(&'a self, request: LiveRequest) -> ProviderFuture<'a, LiveFeed> {
        #[cfg(any(
            all(feature = "production-transport", not(feature = "test-transport")),
            all(feature = "test-transport", not(feature = "production-transport"))
        ))]
        {
            let adapter = OkxLiveAdapter::new(self.clone());
            let clock = Arc::clone(&self.clock);
            let capabilities = MarketDataProvider::capabilities(self);
            Box::pin(crate::provider::runtime::live::open_live(
                adapter,
                clock,
                capabilities,
                request,
            ))
        }
        #[cfg(all(feature = "production-transport", feature = "test-transport"))]
        {
            let _ = request;
            Box::pin(async {
                unreachable!("mutually exclusive transport features are rejected by src/lib.rs")
            })
        }
    }

    fn rate_gate(&self) -> RateGateSnapshot {
        OkxProvider::rate_gate(self)
    }
}
fn unix_now_ms() -> Result<i64, ProviderError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .ok_or(ProviderError::Invariant("unix epoch clock is unavailable"))
}

fn decode_candles(
    bytes: &[u8],
    market: Market,
    timeframe: Timeframe,
    request: HistoryRequest,
    context: ErrorContext,
    now_ms: i64,
) -> Result<Vec<Candle>, ProviderError> {
    let envelope: OkxEnvelope = serde_json::from_slice(bytes)
        .map_err(|_| payload(&context, PayloadError::MalformedJson))?;
    if envelope.code != "0" {
        let code = envelope.code.parse::<i64>().ok();
        let message = (!envelope.msg.is_empty()).then(|| SanitizedMessage::new(&envelope.msg));
        if code == Some(51001) {
            return Err(ProviderError::InvalidSymbol {
                context,
                code: 51001,
                message: message.unwrap_or(SanitizedMessage::InvalidSymbol),
            });
        }
        return Err(ProviderError::ClientStatus {
            context,
            status: 200,
            code,
            message,
        });
    }
    let limit = usize::from(request.limit());
    if envelope.data.len() > limit {
        return Err(payload(&context, PayloadError::MalformedProtocol));
    }
    let (window_start, window_end) = match request.kind() {
        HistoryRequestKind::Latest => (None, None),
        HistoryRequestKind::Older => (None, request.end_time()),
        HistoryRequestKind::Gap => {
            let start = request.start_time().ok_or(ProviderError::Invariant(
                "gap history request is missing startTime",
            ))?;
            let end = request.end_time().ok_or(ProviderError::Invariant(
                "gap history request is missing endTime",
            ))?;
            (
                Some(start),
                Some(page_upper_bound(timeframe, start, request.limit())?.min(end)),
            )
        }
    };
    let mut candles = Vec::with_capacity(envelope.data.len());
    let mut previous = None;
    for row in &envelope.data {
        let candle = decode_okx_row(row, market, timeframe, &context, false, now_ms)?;
        let outside_window = match request.kind() {
            HistoryRequestKind::Latest => false,
            HistoryRequestKind::Older => window_end.is_some_and(|end| candle.open_time() >= end),
            HistoryRequestKind::Gap => {
                window_start.is_some_and(|start| candle.open_time() < start)
                    || window_end.is_some_and(|end| candle.open_time() > end)
            }
        };
        if previous.is_some_and(|open| candle.open_time() >= open) || outside_window {
            return Err(payload(&context, PayloadError::MalformedProtocol));
        }
        previous = Some(candle.open_time());
        candles.push(candle);
    }
    candles.reverse();
    Ok(candles)
}

fn decode_okx_row(
    fields: &[Value],
    market: Market,
    timeframe: Timeframe,
    context: &ErrorContext,
    from_websocket: bool,
    now_ms: i64,
) -> Result<Candle, ProviderError> {
    if fields.len() != 9 {
        return Err(payload(
            context,
            PayloadError::WrongArity {
                expected: 9,
                actual: fields.len(),
            },
        ));
    }
    let text = |index: usize, field: &'static str| {
        fields[index]
            .as_str()
            .ok_or_else(|| payload(context, PayloadError::InvalidField { field }))
    };
    let number = |index: usize, field: &'static str| {
        text(index, field)?
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .ok_or_else(|| payload(context, PayloadError::InvalidField { field }))
    };
    let volume = |index: usize, field: &'static str| {
        number(index, field).and_then(|value| {
            (value >= 0.0)
                .then_some(value)
                .ok_or_else(|| payload(context, PayloadError::InvalidField { field }))
        })
    };
    let open_time = text(0, "timestamp")?
        .parse::<i64>()
        .map_err(|_| payload(context, PayloadError::InvalidField { field: "timestamp" }))?;
    let close_time = timeframe_successor_open(timeframe, open_time)
        .and_then(|next| next.checked_sub(1))
        .ok_or_else(|| payload(context, PayloadError::MalformedProtocol))?;
    let future_limit = i64::try_from(MAX_FUTURE_CANDLE_SKEW.as_millis())
        .ok()
        .and_then(|skew| now_ms.checked_add(skew))
        .ok_or(ProviderError::Invariant("future candle bound overflow"))?;
    if open_time > future_limit {
        return Err(payload(context, PayloadError::MalformedProtocol));
    }
    let closed = match text(8, "confirm")? {
        "0" => false,
        "1" => true,
        _ => {
            return Err(payload(
                context,
                PayloadError::InvalidField { field: "confirm" },
            ));
        }
    };
    let (open, high, low, close) = (
        number(1, "open")?,
        number(2, "high")?,
        number(3, "low")?,
        number(4, "close")?,
    );
    let spot_volume = volume(5, "volume")?;
    let swap_volume = volume(6, "volume_currency")?;
    volume(7, "quote_volume")?;
    let base_volume = match market {
        Market::Spot => spot_volume,
        Market::Perpetual => swap_volume,
    };
    if from_websocket {
        Candle::from_ws(
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            base_volume,
            closed,
        )
    } else {
        Candle::from_rest(open_time, close_time, open, high, low, close, base_volume)
    }
    .map_err(|source| ProviderError::Domain {
        context: context.clone(),
        source,
    })
}

fn page_upper_bound(timeframe: Timeframe, start: i64, limit: u16) -> Result<i64, ProviderError> {
    let mut upper = start;
    for _ in 1..limit {
        upper = timeframe_successor_open(timeframe, upper).ok_or(ProviderError::Configuration(
            "OKX history gap start is off grid",
        ))?;
    }
    timeframe_successor_open(timeframe, upper).ok_or(ProviderError::Configuration(
        "OKX history gap start is off grid",
    ))?;
    Ok(upper)
}

fn timeframe_successor_open(timeframe: Timeframe, open_time: i64) -> Option<i64> {
    let fixed_grid = match timeframe {
        Timeframe::Second1 => Some((1_000, 0)),
        Timeframe::Minute1 => Some((60_000, 0)),
        Timeframe::Minute3 => Some((180_000, 0)),
        Timeframe::Minute5 => Some((300_000, 0)),
        Timeframe::Minute15 => Some((900_000, 0)),
        Timeframe::Minute30 => Some((1_800_000, 0)),
        Timeframe::Hour1 => Some((3_600_000, 0)),
        Timeframe::Hour2 => Some((7_200_000, 0)),
        Timeframe::Hour4 => Some((14_400_000, 0)),
        Timeframe::Hour6 => Some((21_600_000, 0)),
        Timeframe::Hour8 => return None,
        Timeframe::Hour12 => Some((43_200_000, 0)),
        Timeframe::Day1 => Some((86_400_000, 0)),
        Timeframe::Day3 => Some((259_200_000, 0)),
        Timeframe::Week1 => Some((604_800_000, 4 * 86_400_000)),
        Timeframe::Month1 => None,
    };
    if let Some((period_ms, grid_offset_ms)) = fixed_grid {
        let on_grid = open_time.checked_sub(grid_offset_ms)?.rem_euclid(period_ms) == 0;
        return on_grid.then(|| open_time.checked_add(period_ms)).flatten();
    }
    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(open_time) * 1_000_000).ok()?;
    if timestamp.time() != time::Time::MIDNIGHT || timestamp.day() != 1 {
        return None;
    }
    let (year, month) = if timestamp.month() == Month::December {
        (timestamp.year().checked_add(1)?, Month::January)
    } else {
        (timestamp.year(), timestamp.month().next())
    };
    i64::try_from(
        Date::from_calendar_date(year, month, 1)
            .ok()?
            .midnight()
            .assume_utc()
            .unix_timestamp_nanos()
            / 1_000_000,
    )
    .ok()
}
fn map_http_error(status: StatusCode, _bytes: &[u8], context: ErrorContext) -> ProviderError {
    if status.is_client_error() {
        ProviderError::ClientStatus {
            context,
            status: status.as_u16(),
            code: None,
            message: None,
        }
    } else {
        ProviderError::ServerStatus {
            context,
            status: status.as_u16(),
        }
    }
}

fn payload(context: &ErrorContext, source: PayloadError) -> ProviderError {
    ProviderError::Payload {
        context: context.clone(),
        source,
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
fn validate_loopback_base(value: &str) -> Result<Url, ProviderError> {
    let url = Url::parse(value)
        .map_err(|_| ProviderError::Configuration("test REST base URL is invalid"))?;
    let host = url.host_str().ok_or(ProviderError::Configuration(
        "test REST base URL requires a host",
    ))?;
    let address: std::net::IpAddr = host.parse().map_err(|_| {
        ProviderError::Configuration("test REST base URL must use a literal loopback address")
    })?;
    if !address.is_loopback()
        || url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ProviderError::Configuration(
            "test REST base URL must be plain HTTP on a literal loopback address",
        ));
    }
    Ok(url)
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
fn validate_loopback_ws_base(value: &str) -> Result<Url, ProviderError> {
    let url = Url::parse(value)
        .map_err(|_| ProviderError::Configuration("test WebSocket base URL is invalid"))?;
    let host = url.host_str().ok_or(ProviderError::Configuration(
        "test WebSocket base URL requires a host",
    ))?;
    let address: std::net::IpAddr = host.parse().map_err(|_| {
        ProviderError::Configuration("test WebSocket base URL must use a literal loopback address")
    })?;
    if !address.is_loopback()
        || !matches!(url.scheme(), "ws" | "wss")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ProviderError::Configuration(
            "test WebSocket base URL must use WS on a literal loopback address",
        ));
    }
    Ok(url)
}

fn reject_unsupported_timeframe(timeframe: Timeframe) -> Result<(), ProviderError> {
    if timeframe == Timeframe::Hour8 {
        return Err(ProviderError::Configuration(UNSUPPORTED_TIMEFRAME));
    }
    Ok(())
}

fn bar(timeframe: Timeframe) -> Result<&'static str, ProviderError> {
    Ok(match timeframe {
        Timeframe::Second1 => "1s",
        Timeframe::Minute1 => "1m",
        Timeframe::Minute3 => "3m",
        Timeframe::Minute5 => "5m",
        Timeframe::Minute15 => "15m",
        Timeframe::Minute30 => "30m",
        Timeframe::Hour1 => "1H",
        Timeframe::Hour2 => "2H",
        Timeframe::Hour4 => "4H",
        Timeframe::Hour6 => "6Hutc",
        Timeframe::Hour12 => "12Hutc",
        Timeframe::Day1 => "1Dutc",
        Timeframe::Day3 => "3Dutc",
        Timeframe::Week1 => "1Wutc",
        Timeframe::Month1 => "1Mutc",
        Timeframe::Hour8 => return Err(ProviderError::Configuration(UNSUPPORTED_TIMEFRAME)),
    })
}

fn channel(timeframe: Timeframe) -> Result<&'static str, ProviderError> {
    Ok(match bar(timeframe)? {
        "1Dutc" => "candle1Dutc",
        "3Dutc" => "candle3Dutc",
        "1Wutc" => "candle1Wutc",
        "1Mutc" => "candle1Mutc",
        "1s" => "candle1s",
        "1m" => "candle1m",
        "3m" => "candle3m",
        "5m" => "candle5m",
        "15m" => "candle15m",
        "30m" => "candle30m",
        "1H" => "candle1H",
        "2H" => "candle2H",
        "4H" => "candle4H",
        "6Hutc" => "candle6Hutc",
        "12Hutc" => "candle12Hutc",
        _ => return Err(ProviderError::Configuration(UNSUPPORTED_TIMEFRAME)),
    })
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn test_subscribe_message(instrument: &Instrument, timeframe: Timeframe) -> String {
    subscribe_message(instrument, timeframe)
}

fn subscribe_message(instrument: &Instrument, timeframe: Timeframe) -> String {
    serde_json::json!({
        "id": SUBSCRIPTION_ID,
        "op": "subscribe",
        "args": [{ "channel": channel(timeframe).expect("validated timeframe"), "instId": instrument.provider_symbol() }]
    }).to_string()
}
