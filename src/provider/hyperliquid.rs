//! Hyperliquid Spot and Perpetual REST history and raw WebSocket transport.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use reqwest::{StatusCode, Url, header::RETRY_AFTER};
use serde::{
    Deserialize,
    de::{IgnoredAny, SeqAccess, Visitor},
};
use serde_json::Value;
use time::{Date, Month, OffsetDateTime};
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
use tokio::sync::Notify;
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
        Timeframe, is_spot_index_token,
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

const INFO_PATH: &str = "/info";
pub const REST_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const REST_BODY_LIMIT: usize = 2 * 1024 * 1024;
pub const RATE_LIMIT_FALLBACK: Duration = Duration::from_secs(30);
pub const HYPERLIQUID_MAX_RESPONSE_ROWS: usize = 1001;
const UNSUPPORTED_TIMEFRAME: &str = "Hyperliquid does not support the 1s or 6h timeframes; use 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d, 3d, 1w, or 1M";
/// Locked mainnet `spotMeta` universe index for UBTC/USDC (UI-mapped BTC spot).
const SPOT_UBTC_INDEX: &str = "@142";
/// Locked mainnet `spotMeta` universe index for HYPE/USDC.
const SPOT_HYPE_INDEX: &str = "@107";

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_REST_BASE: &str = "https://api.hyperliquid.xyz";
#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_WS_BASE: &str = "wss://api.hyperliquid.xyz/ws";

pub const SUBSCRIBE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
pub const APPLICATION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(50);
const MAX_FUTURE_CANDLE_SKEW: Duration = Duration::from_secs(5 * 60);
const MAX_GAP_RECONCILIATION_CANDLES: usize = 64_000;
const MAX_GAP_RECONCILIATION_PAGES: usize = 64;

#[derive(Clone, Debug)]
pub struct HyperliquidLiveConfig {
    pub supervisor: SharedLiveSupervisorConfig,
    pub subscribe_ack_timeout: Duration,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub application_heartbeat_interval_for_test: Duration,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub heartbeat_test_hook: Option<HeartbeatTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub readiness_inactivity_test_hook: Option<Arc<Notify>>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub readiness_decoded_ack_test_hook: Option<ReadinessDecodedAckTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub readiness_drain_budget_test_hook: Option<ReadinessDrainBudgetTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub subscribe_flush_test_hook: Option<SubscribeFlushTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub close_flush_test_hook: Option<CloseFlushTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub force_stalled_write_after_readiness_frame: bool,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub max_gap_reconciliation_candles_for_test: usize,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub advertised_history_page_limit: u16,
}
impl Default for HyperliquidLiveConfig {
    fn default() -> Self {
        Self {
            supervisor: SharedLiveSupervisorConfig::default(),
            subscribe_ack_timeout: SUBSCRIBE_ACK_TIMEOUT,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            application_heartbeat_interval_for_test: APPLICATION_HEARTBEAT_INTERVAL,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            heartbeat_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            readiness_inactivity_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            readiness_decoded_ack_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            readiness_drain_budget_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            subscribe_flush_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            close_flush_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            force_stalled_write_after_readiness_frame: false,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            max_gap_reconciliation_candles_for_test: MAX_GAP_RECONCILIATION_CANDLES,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            advertised_history_page_limit: 1000,
        }
    }
}
impl HyperliquidLiveConfig {
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
        {
            if !(Duration::from_millis(1)..=Duration::from_secs(60))
                .contains(&self.application_heartbeat_interval_for_test)
            {
                return Err(ProviderError::Configuration(
                    "application heartbeat interval is outside 1ms..=60s",
                ));
            }
            if !(1..=MAX_GAP_RECONCILIATION_CANDLES)
                .contains(&self.max_gap_reconciliation_candles_for_test)
            {
                return Err(ProviderError::Configuration(
                    "live reconciliation candle bound is outside 1..=64000",
                ));
            }
        }
        Ok(())
    }
}
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
use crate::provider::runtime::websocket::HeartbeatTestHook;

#[derive(Deserialize)]
struct HlCandle {
    #[serde(rename = "t")]
    open_time: i64,
    #[serde(rename = "T")]
    close_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: Value,
    #[serde(rename = "c")]
    close: Value,
    #[serde(rename = "h")]
    high: Value,
    #[serde(rename = "l")]
    low: Value,
    #[serde(rename = "v")]
    volume: Value,
    #[serde(rename = "n")]
    _trade_count: u64,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug, Default)]
pub struct HyperliquidWsCodec {
    retained_candle: Option<Candle>,
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
#[derive(Clone, Debug, Default)]
pub(crate) struct HyperliquidWsCodec {
    retained_candle: Option<Candle>,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug, PartialEq)]
pub enum HyperliquidDecoded {
    Candle(Candle),
    SubscribeAccepted,
    ApplicationPong,
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HyperliquidDecoded {
    Candle(Candle),
    SubscribeAccepted,
    ApplicationPong,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
use crate::provider::runtime::websocket::{
    CloseFlushTestHook, ReadinessDecodedAckTestHook, ReadinessDrainBudgetTestHook,
    SubscribeFlushTestHook,
};

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl HyperliquidWsCodec {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            retained_candle: None,
        }
    }
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl WsCodec for HyperliquidWsCodec {
    type Outcome = HyperliquidDecoded;

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
            HyperliquidDecoded::Candle(_) => 1,
            HyperliquidDecoded::SubscribeAccepted => 2,
            HyperliquidDecoded::ApplicationPong => u8::MAX,
        }
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    fn is_subscribe_accepted(outcome: &Self::Outcome) -> bool {
        matches!(outcome, HyperliquidDecoded::SubscribeAccepted)
    }
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub fn websocket_url(instrument: &Instrument, timeframe: Timeframe) -> Result<Url, ProviderError> {
    websocket_url_from_base(production_ws_base(), instrument, timeframe, false)
        .map_err(|error| contextualize_websocket_configuration(error, instrument, timeframe))
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn test_websocket_url(
    base_url: &str,
    instrument: &Instrument,
    timeframe: Timeframe,
) -> Result<Url, ProviderError> {
    websocket_url_from_base(base_url, instrument, timeframe, true)
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
    instrument: &Instrument,
    timeframe: Timeframe,
    loopback_only: bool,
) -> Result<Url, ProviderError> {
    let url = validate_websocket_base(base_url, loopback_only)?;
    let _ = (instrument, timeframe);
    if url.path() != "/" && !url.path().is_empty() && url.path() != "/ws" {
        return Err(ProviderError::Configuration("invalid WebSocket base URL"));
    }
    Ok(url)
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn decode_ws_frame(
    codec: &mut HyperliquidWsCodec,
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<HyperliquidDecoded>>,
) {
    decode_ws_frame_impl(codec, message, instrument, timeframe, config, outcomes);
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn decode_ws_frame(
    codec: &mut HyperliquidWsCodec,
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<HyperliquidDecoded>>,
) {
    decode_ws_frame_impl(codec, message, instrument, timeframe, config, outcomes);
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
fn decode_ws_frame_impl(
    codec: &mut HyperliquidWsCodec,
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<HyperliquidDecoded>>,
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
    codec: &mut HyperliquidWsCodec,
    bytes: &[u8],
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
    outcomes: &mut VecDeque<DecodedFrame<HyperliquidDecoded>>,
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
    match value.get("channel").and_then(Value::as_str) {
        Some("subscriptionResponse") => {
            let valid = value.get("data").is_some_and(|data| {
                data.get("method").and_then(Value::as_str) == Some("subscribe")
                    && data.get("subscription").is_some_and(|subscription| {
                        subscription.get("type").and_then(Value::as_str) == Some("candle")
                            && subscription.get("coin").and_then(Value::as_str)
                                == Some(instrument.provider_symbol())
                            && subscription.get("interval").and_then(Value::as_str)
                                == Some(timeframe.as_str())
                    })
            });
            outcomes.push_back(if valid {
                DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted)
            } else {
                DecodedFrame::ProviderError(payload(&context, PayloadError::MalformedProtocol))
            });
        }
        Some("pong") => {
            outcomes.push_back(DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong))
        }
        Some("candle") => {
            decode_candle_payload(codec, &value, instrument, timeframe, &context, outcomes)
        }
        _ => outcomes.push_back(DecodedFrame::Ignored),
    }
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
fn decode_candle_payload(
    codec: &mut HyperliquidWsCodec,
    value: &Value,
    instrument: &Instrument,
    timeframe: Timeframe,
    context: &ErrorContext,
    outcomes: &mut VecDeque<DecodedFrame<HyperliquidDecoded>>,
) {
    let Some(data) = value.get("data") else {
        outcomes.push_back(DecodedFrame::ProviderError(payload(
            context,
            PayloadError::MalformedProtocol,
        )));
        return;
    };
    let kline: HlCandle = match serde_json::from_value(data.clone()) {
        Ok(kline) => kline,
        Err(_) => {
            outcomes.push_back(DecodedFrame::ProviderError(payload(
                context,
                PayloadError::MalformedProtocol,
            )));
            return;
        }
    };
    if let Err(error) = candle_market_matches(&kline, instrument, timeframe, context, "WebSocket") {
        outcomes.push_back(DecodedFrame::ProviderError(error));
        return;
    }
    if let Err(error) = validate_candle_time_window(&kline, timeframe, context) {
        outcomes.push_back(DecodedFrame::ProviderError(error));
        return;
    }
    let (open, high, low, close, volume) = match candle_ohlcv(&kline, context) {
        Ok(ohlcv) => ohlcv,
        Err(error) => {
            outcomes.push_back(DecodedFrame::ProviderError(error));
            return;
        }
    };
    let candidate = match Candle::from_ws(
        kline.open_time,
        kline.close_time,
        open,
        high,
        low,
        close,
        volume,
        false,
    ) {
        Ok(candle) => candle,
        Err(source) => {
            outcomes.push_back(DecodedFrame::ProviderError(ProviderError::Domain {
                context: context.clone(),
                source,
            }));
            return;
        }
    };
    let Some(retained) = codec.retained_candle.as_ref() else {
        codec.retained_candle = Some(candidate.clone());
        outcomes.push_back(DecodedFrame::Provider(HyperliquidDecoded::Candle(
            candidate,
        )));
        return;
    };
    if candidate.open_time() < retained.open_time() || candidate == *retained {
        return;
    }
    if candidate.open_time() == retained.open_time() {
        codec.retained_candle = Some(candidate.clone());
        outcomes.push_back(DecodedFrame::Provider(HyperliquidDecoded::Candle(
            candidate,
        )));
        return;
    }
    let closed = Candle::from_ws(
        retained.open_time(),
        retained.close_time(),
        retained.open(),
        retained.high(),
        retained.low(),
        retained.close(),
        retained.base_volume(),
        true,
    )
    .expect("retained validated candle remains valid when closed");
    codec.retained_candle = Some(candidate.clone());
    outcomes.push_back(DecodedFrame::Provider(HyperliquidDecoded::Candle(closed)));
    outcomes.push_back(DecodedFrame::Provider(HyperliquidDecoded::Candle(
        candidate,
    )));
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub type RawWebSocket = crate::provider::runtime::websocket::RawWebSocket<HyperliquidWsCodec>;

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub(crate) type RawWebSocket =
    crate::provider::runtime::websocket::RawWebSocket<HyperliquidWsCodec>;

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
        HyperliquidWsCodec::new(),
        Some(Message::Text(r#"{"method":"ping"}"#.into())),
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
        HyperliquidWsCodec::new(),
        Some(Message::Text(r#"{"method":"ping"}"#.into())),
    )
    .await
}

#[derive(Clone)]
pub struct HyperliquidProvider {
    http: HttpRuntime,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    base_url: Url,
    clock: Arc<dyn Clock>,
    rate_limit_fallback: Duration,
    live: HyperliquidLiveConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    ws_base_url: Option<String>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    now_ms: Option<i64>,
}
struct HyperliquidBuildConfig {
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    base_url: Url,
    request_timeout: Duration,
    body_limit: usize,
    rate_limit_fallback: Duration,
    live: HyperliquidLiveConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    ws_base_url: Option<String>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    now_ms: Option<i64>,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct HyperliquidTestConfig {
    pub base_url: String,
    pub request_timeout: Duration,
    pub body_limit: usize,
    pub rate_limit_fallback: Duration,
    pub now_ms: Option<i64>,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
impl HyperliquidTestConfig {
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
    pub fn with_websocket_base(self, base_url: impl Into<String>) -> HyperliquidLiveTestConfig {
        HyperliquidLiveTestConfig {
            rest: self,
            ws_base_url: base_url.into(),
            live: HyperliquidLiveConfig::default(),
        }
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct HyperliquidLiveTestConfig {
    pub rest: HyperliquidTestConfig,
    pub ws_base_url: String,
    pub live: HyperliquidLiveConfig,
}

impl HyperliquidProvider {
    #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
    pub fn new(clock: Arc<dyn Clock>) -> Result<Self, ProviderError> {
        Self::build(
            clock,
            HyperliquidBuildConfig {
                request_timeout: REST_REQUEST_TIMEOUT,
                body_limit: REST_BODY_LIMIT,
                rate_limit_fallback: RATE_LIMIT_FALLBACK,
                live: HyperliquidLiveConfig::default(),
            },
        )
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test(
        base_url: impl AsRef<str>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        Self::new_test_with_config_and_clock(
            HyperliquidTestConfig::loopback(base_url.as_ref()),
            clock,
        )
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test_with_config_and_clock(
        config: HyperliquidTestConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        let base_url = validate_loopback_base(&config.base_url)?;
        Self::build(
            clock,
            HyperliquidBuildConfig {
                base_url,
                request_timeout: config.request_timeout,
                body_limit: config.body_limit,
                rate_limit_fallback: config.rate_limit_fallback,
                live: HyperliquidLiveConfig::default(),
                ws_base_url: None,
                now_ms: config.now_ms,
            },
        )
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test_live(
        config: HyperliquidLiveTestConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        let base_url = validate_loopback_base(&config.rest.base_url)?;
        validate_loopback_ws_base(&config.ws_base_url)?;
        Self::build(
            clock,
            HyperliquidBuildConfig {
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

    fn build(clock: Arc<dyn Clock>, config: HyperliquidBuildConfig) -> Result<Self, ProviderError> {
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
            url.set_path(INFO_PATH);
            url.set_query(None);
            let (start_time, end_time) = self.candle_window(timeframe, request)?;
            let body = serde_json::json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": instrument.provider_symbol(),
                    "interval": timeframe.as_str(),
                    "startTime": start_time,
                    "endTime": end_time,
                }
            });

            let response = self
                .http
                .send(
                    self.http.client().post(url).json(&body),
                    &cancellation,
                    &context,
                )
                .await?;

            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
                return self.handle_rate_limit(&response, context);
            }
            let bytes = self
                .http
                .read_response(response, &cancellation, context.clone(), map_http_error)
                .await?;
            let (window_start, window_end) = response_validation_window(
                timeframe,
                request.kind(),
                request.limit(),
                start_time,
                end_time,
            )?;
            decode_candles(
                &bytes,
                CandleDecodeContext {
                    kind: request.kind(),
                    requested_limit: request.limit(),
                    instrument,
                    timeframe,
                    window_start,
                    window_end,
                    error: context,
                },
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
                ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked")
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
        if spec.provider().as_str() != "hyperliquid" {
            return Err(ProviderError::Configuration(
                "instrument is not valid for Hyperliquid",
            ));
        }
        if spec.venue().is_some() && spec.market() != Market::Perpetual {
            return Err(ProviderError::Configuration(
                "HIP-3 builder DEX markets are perpetual-only; use `hyperliquid:<dex>:<coin>.p`",
            ));
        }
        remap_instrument(spec)
    }

    fn candle_window(
        &self,
        timeframe: Timeframe,
        request: HistoryRequest,
    ) -> Result<(i64, i64), ProviderError> {
        let interval = interval_ms(timeframe)?;
        let span = i64::from(request.limit()).saturating_mul(interval);
        match request.kind() {
            HistoryRequestKind::Latest => {
                let end = self.unix_now_ms()?;
                let start = end.saturating_sub(span).saturating_add(1);
                Ok((start, end))
            }
            HistoryRequestKind::Older => {
                let end = request.end_time().ok_or(ProviderError::Invariant(
                    "older history request is missing endTime",
                ))?;
                Ok((end.saturating_sub(span).saturating_add(1), end))
            }
            HistoryRequestKind::Gap => {
                let start = request.start_time().ok_or(ProviderError::Invariant(
                    "gap history request is missing startTime",
                ))?;
                let end = request.end_time().ok_or(ProviderError::Invariant(
                    "gap history request is missing endTime",
                ))?;
                let page_end = start.saturating_add(span).saturating_sub(1);
                Ok((start, end.min(page_end)))
            }
        }
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
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            self.live.advertised_history_page_limit
        }
        #[cfg(any(
            all(feature = "production-transport", not(feature = "test-transport")),
            all(feature = "production-transport", feature = "test-transport")
        ))]
        {
            1000
        }
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
pub(crate) struct HyperliquidLiveAdapter {
    provider: HyperliquidProvider,
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl HyperliquidLiveAdapter {
    fn new(provider: HyperliquidProvider) -> Self {
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
pub(crate) struct HyperliquidLiveSocket {
    raw: RawWebSocket,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    stalled_write_probe_frames: usize,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    stalled_write_probe_payload_size: usize,
}

#[cfg(any(
    all(feature = "production-transport", not(feature = "test-transport")),
    all(feature = "test-transport", not(feature = "production-transport"))
))]
impl LiveSocket for HyperliquidLiveSocket {
    async fn read(&mut self) -> Result<LiveSocketEvent, ProviderError> {
        match self.raw.read().await? {
            DecodedFrame::Provider(HyperliquidDecoded::Candle(candle)) => {
                Ok(LiveSocketEvent::Candle(candle))
            }
            DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong) => {
                Ok(LiveSocketEvent::Ignored)
            }
            DecodedFrame::ProviderError(error) => Ok(LiveSocketEvent::DecodedError(error)),
            DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested => {
                self.raw.finalize_peer_close().await?;
                Ok(LiveSocketEvent::ReconnectRequested)
            }
            DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted) => {
                Ok(LiveSocketEvent::ProtocolViolation(
                    "duplicate Hyperliquid subscribe acknowledgement",
                ))
            }
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
impl LiveAdapter for HyperliquidLiveAdapter {
    type Socket = HyperliquidLiveSocket;

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
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            raw.readiness_inactivity_test_hook =
                self.provider.live.readiness_inactivity_test_hook.clone();
            raw.readiness_decoded_ack_test_hook =
                self.provider.live.readiness_decoded_ack_test_hook.clone();
            raw.readiness_drain_budget_test_hook =
                self.provider.live.readiness_drain_budget_test_hook.clone();
            raw.close_flush_test_hook = self.provider.live.close_flush_test_hook.clone();
            raw.force_stalled_write_after_readiness_frame =
                self.provider.live.force_stalled_write_after_readiness_frame;
        }
        loop {
            let inactivity_deadline = raw.inactivity_deadline();
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            let readiness_inactivity_test_hook = raw.readiness_inactivity_test_hook.clone();
            tokio::select! {
                biased;
                input = raw.read_readiness() => match input {
                    ReadinessInput::Error(error) => return Err(error),
                    ReadinessInput::Frame(DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted)) => break,
                    ReadinessInput::Frame(DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong)) => {
                        if self.provider.clock.now() >= ack_deadline {
                            return Err(self.subscribe_ack_timeout(&instrument, timeframe));
                        }
                    }
                    ReadinessInput::Frame(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => {
                        raw.finalize_peer_close().await?;
                        return Err(ProviderError::Protocol { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&instrument, timeframe), detail: "WebSocket peer requested reconnect" });
                    }
                    ReadinessInput::Frame(DecodedFrame::ProviderError(error)) => return Err(error),
                    ReadinessInput::Frame(DecodedFrame::Provider(HyperliquidDecoded::Candle(_))) => return Err(ProviderError::Protocol { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&instrument, timeframe), detail: "Hyperliquid candle arrived before subscribe acknowledgement" }),
                },
                () = self.provider.clock.sleep_until(ack_deadline) => return Err(self.subscribe_ack_timeout(&instrument, timeframe)),
                () = async move {
                    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
                    if let Some(hook) = readiness_inactivity_test_hook { hook.notified().await; return; }
                    tokio::time::sleep_until(inactivity_deadline).await;
                } => return Err(ProviderError::Timeout { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&instrument, timeframe), kind: TimeoutKind::WebSocketInactivity }),
            }
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            raw.heartbeat_test_hook = self.provider.live.heartbeat_test_hook.clone();
        }
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        raw.start_application_heartbeat(APPLICATION_HEARTBEAT_INTERVAL);
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        raw.start_application_heartbeat(self.provider.live.application_heartbeat_interval_for_test);
        Ok(HyperliquidLiveSocket {
            raw,
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
            process_block: ProcessBlockPolicy::Forbidden(
                "Hyperliquid rate gate cannot be process-blocked",
            ),
        }
    }

    fn live_config(&self) -> LiveConfig<'_> {
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        let max_successors = self.provider.live.max_gap_reconciliation_candles_for_test;
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        let max_successors = MAX_GAP_RECONCILIATION_CANDLES;
        LiveConfig {
            supervisor: &self.provider.live.supervisor,
            reconciliation: ReconciliationLimits {
                max_successors,
                max_pages: MAX_GAP_RECONCILIATION_PAGES,
                span_exceeded: "Hyperliquid gap reconciliation target exceeds the per-generation span limit",
                page_exceeded: "Hyperliquid gap reconciliation exceeded the per-generation page limit",
                distinct_exceeded: "Hyperliquid gap reconciliation exceeded the distinct buffered-candle limit",
            },
        }
    }

    fn connection_rotation(&self) -> ConnectionRotation {
        ConnectionRotation::Never
    }
}

impl MarketDataProvider for HyperliquidProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("hyperliquid").expect("static provider id")
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            markets: &[Market::Spot, Market::Perpetual],
            timeframes: &[
                Timeframe::Minute1,
                Timeframe::Minute3,
                Timeframe::Minute5,
                Timeframe::Minute15,
                Timeframe::Minute30,
                Timeframe::Hour1,
                Timeframe::Hour2,
                Timeframe::Hour4,
                Timeframe::Hour8,
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
        HyperliquidProvider::canonicalize(self, spec)
    }
    fn history<'a>(
        &'a self,
        instrument: &'a Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>> {
        Box::pin(HyperliquidProvider::history(
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
            let adapter = HyperliquidLiveAdapter::new(self.clone());
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
        HyperliquidProvider::rate_gate(self)
    }
}

const OVERSIZED_CANDLE_ARRAY: &str = "Hyperliquid candle row limit exceeded";
struct CandleDecodeContext<'a> {
    kind: HistoryRequestKind,
    requested_limit: u16,
    instrument: &'a Instrument,
    timeframe: Timeframe,
    window_start: i64,
    window_end: i64,
    error: ErrorContext,
}

struct BoundedCandleVisitor<'decode, 'instrument> {
    decode: &'decode CandleDecodeContext<'instrument>,
    requested_limit: usize,
}

impl<'de> Visitor<'de> for BoundedCandleVisitor<'_, '_> {
    type Value = Vec<Candle>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded array of Hyperliquid candle rows")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut candles = VecDeque::with_capacity(self.requested_limit);
        let mut row_count = 0_usize;
        let mut previous_open = None;
        while row_count < HYPERLIQUID_MAX_RESPONSE_ROWS {
            let Some(raw) = sequence.next_element::<HlCandle>()? else {
                return Ok(candles.into_iter().collect());
            };
            row_count += 1;
            candle_market_matches(
                &raw,
                self.decode.instrument,
                self.decode.timeframe,
                &self.decode.error,
                "REST",
            )
            .map_err(serde::de::Error::custom)?;
            validate_rest_candle_time_window(
                &raw,
                self.decode.timeframe,
                self.decode.window_start,
                self.decode.window_end,
                previous_open,
                &self.decode.error,
            )
            .map_err(serde::de::Error::custom)?;
            previous_open = Some(raw.open_time);
            let (open, high, low, close, volume) =
                candle_ohlcv(&raw, &self.decode.error).map_err(serde::de::Error::custom)?;
            let candle = Candle::from_rest(
                raw.open_time,
                raw.close_time,
                open,
                high,
                low,
                close,
                volume,
            )
            .map_err(serde::de::Error::custom)?;
            match self.decode.kind {
                HistoryRequestKind::Latest | HistoryRequestKind::Older => {
                    if candles.len() == self.requested_limit {
                        candles.pop_front();
                    }
                    candles.push_back(candle);
                }
                HistoryRequestKind::Gap => {
                    if candles.len() < self.requested_limit {
                        candles.push_back(candle);
                    }
                }
            }
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::custom(OVERSIZED_CANDLE_ARRAY));
        }
        Ok(candles.into_iter().collect())
    }
}

fn decode_candles(
    bytes: &[u8],
    decode: CandleDecodeContext<'_>,
) -> Result<Vec<Candle>, ProviderError> {
    if bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        != Some(b'[')
    {
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|_| payload(&decode.error, PayloadError::MalformedJson))?;
        if let Some(error) = info_error_text(&value) {
            return Err(invalid_hyperliquid_symbol(decode.error, error));
        }
        return Err(payload(&decode.error, PayloadError::ExpectedArray));
    }
    let requested_limit = usize::from(decode.requested_limit);
    if !(1..=1000).contains(&requested_limit) {
        return Err(payload(&decode.error, PayloadError::MalformedProtocol));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let candles = serde::de::Deserializer::deserialize_seq(
        &mut deserializer,
        BoundedCandleVisitor {
            decode: &decode,
            requested_limit,
        },
    )
    .map_err(|_| payload(&decode.error, PayloadError::MalformedProtocol))?;
    deserializer
        .end()
        .map_err(|_| payload(&decode.error, PayloadError::MalformedJson))?;
    Ok(candles)
}
fn response_validation_window(
    timeframe: Timeframe,
    kind: HistoryRequestKind,
    requested_limit: u16,
    request_start: i64,
    request_end: i64,
) -> Result<(i64, i64), ProviderError> {
    let overlap_rows = HYPERLIQUID_MAX_RESPONSE_ROWS.saturating_sub(usize::from(requested_limit));
    let overlap_rows = i64::try_from(overlap_rows)
        .map_err(|_| ProviderError::Invariant("Hyperliquid overlap row count is invalid"))?;
    let interval = interval_ms(timeframe)?;
    let overlap_span = interval.saturating_mul(overlap_rows);
    Ok(match kind {
        HistoryRequestKind::Latest | HistoryRequestKind::Older => (
            request_start.saturating_sub(overlap_span).saturating_sub(1),
            request_end,
        ),
        HistoryRequestKind::Gap => (request_start, request_end.saturating_add(overlap_span)),
    })
}

fn validate_rest_candle_time_window(
    candle: &HlCandle,
    timeframe: Timeframe,
    window_start: i64,
    window_end: i64,
    previous_open: Option<i64>,
    context: &ErrorContext,
) -> Result<(), ProviderError> {
    let expected_close = timeframe_successor_open(timeframe, candle.open_time)
        .and_then(|successor| successor.checked_sub(1));
    if expected_close != Some(candle.close_time)
        || candle.open_time < window_start
        || candle.open_time > window_end
        || previous_open.is_some_and(|previous| candle.open_time <= previous)
    {
        return Err(payload(context, PayloadError::MalformedProtocol));
    }
    Ok(())
}

fn validate_candle_time_window(
    candle: &HlCandle,
    timeframe: Timeframe,
    context: &ErrorContext,
) -> Result<(), ProviderError> {
    let expected_close = timeframe_successor_open(timeframe, candle.open_time)
        .and_then(|successor| successor.checked_sub(1));
    let future_ceiling = unix_now_ms().ok().and_then(|now| {
        i64::try_from(MAX_FUTURE_CANDLE_SKEW.as_millis())
            .ok()
            .and_then(|skew| now.checked_add(skew))
    });
    if expected_close != Some(candle.close_time)
        || future_ceiling.is_none_or(|ceiling| candle.open_time > ceiling)
    {
        return Err(payload(context, PayloadError::MalformedProtocol));
    }
    Ok(())
}

fn timeframe_successor_open(timeframe: Timeframe, open_time: i64) -> Option<i64> {
    let fixed_milliseconds = match timeframe {
        Timeframe::Second1 => Some(1_000),
        Timeframe::Minute1 => Some(60_000),
        Timeframe::Minute3 => Some(180_000),
        Timeframe::Minute5 => Some(300_000),
        Timeframe::Minute15 => Some(900_000),
        Timeframe::Minute30 => Some(1_800_000),
        Timeframe::Hour1 => Some(3_600_000),
        Timeframe::Hour2 => Some(7_200_000),
        Timeframe::Hour4 => Some(14_400_000),
        Timeframe::Hour6 => Some(21_600_000),
        Timeframe::Hour8 => Some(28_800_000),
        Timeframe::Hour12 => Some(43_200_000),
        Timeframe::Day1 => Some(86_400_000),

        Timeframe::Day3 => Some(259_200_000),
        Timeframe::Week1 => Some(604_800_000),
        Timeframe::Month1 => None,
    };
    if let Some(milliseconds) = fixed_milliseconds {
        if open_time.rem_euclid(milliseconds) != 0 {
            return None;
        }
        return open_time.checked_add(milliseconds);
    }
    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(open_time) * 1_000_000).ok()?;
    if timestamp.time() != time::Time::MIDNIGHT || timestamp.day() != 1 {
        return None;
    }
    let (year, month) = match timestamp.month() {
        Month::December => (timestamp.year().checked_add(1)?, Month::January),
        month => (timestamp.year(), month.next()),
    };
    let successor = Date::from_calendar_date(year, month, 1)
        .ok()?
        .midnight()
        .assume_utc()
        .unix_timestamp_nanos()
        / 1_000_000;
    i64::try_from(successor).ok()
}

fn candle_market_matches(
    candle: &HlCandle,
    instrument: &Instrument,
    timeframe: Timeframe,
    context: &ErrorContext,
    transport: &'static str,
) -> Result<(), ProviderError> {
    if candle.symbol != instrument.provider_symbol() || candle.interval != timeframe.as_str() {
        return Err(ProviderError::Protocol {
            context: context.clone(),
            detail: match transport {
                "REST" => "REST candle market does not match request",
                _ => "WebSocket candle market does not match subscription",
            },
        });
    }
    Ok(())
}

fn candle_ohlcv(
    candle: &HlCandle,
    context: &ErrorContext,
) -> Result<(f64, f64, f64, f64, f64), ProviderError> {
    json_decimal(&candle.open)
        .zip(json_decimal(&candle.high))
        .zip(json_decimal(&candle.low))
        .zip(json_decimal(&candle.close))
        .zip(json_decimal(&candle.volume))
        .map(|((((open, high), low), close), volume)| (open, high, low, close, volume))
        .ok_or_else(|| {
            payload(
                context,
                PayloadError::InvalidField {
                    field: "candle numeric field",
                },
            )
        })
}

fn info_error_text(value: &Value) -> Option<&str> {
    value
        .get("error")
        .and_then(Value::as_str)
        .or_else(|| value.get("msg").and_then(Value::as_str))
}

fn invalid_hyperliquid_symbol(context: ErrorContext, message: &str) -> ProviderError {
    ProviderError::InvalidSymbol {
        context,
        code: 0,
        message: SanitizedMessage::new(message),
    }
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

#[cfg(feature = "test-transport")]
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

#[cfg(feature = "test-transport")]
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
    if matches!(timeframe, Timeframe::Second1 | Timeframe::Hour6) {
        return Err(ProviderError::Configuration(UNSUPPORTED_TIMEFRAME));
    }
    Ok(())
}

fn interval_ms(timeframe: Timeframe) -> Result<i64, ProviderError> {
    reject_unsupported_timeframe(timeframe)?;
    Ok(match timeframe {
        Timeframe::Minute1 => 60_000,
        Timeframe::Minute3 => 180_000,
        Timeframe::Minute5 => 300_000,
        Timeframe::Minute15 => 900_000,
        Timeframe::Minute30 => 1_800_000,
        Timeframe::Hour1 => 3_600_000,
        Timeframe::Hour2 => 7_200_000,
        Timeframe::Hour4 => 14_400_000,
        Timeframe::Hour8 => 28_800_000,
        Timeframe::Hour12 => 43_200_000,
        Timeframe::Day1 => 86_400_000,
        Timeframe::Day3 => 259_200_000,
        Timeframe::Week1 => 604_800_000,
        Timeframe::Month1 => 31 * 86_400_000,
        Timeframe::Second1 | Timeframe::Hour6 => {
            return Err(ProviderError::Configuration(UNSUPPORTED_TIMEFRAME));
        }
    })
}

fn subscribe_message(instrument: &Instrument, timeframe: Timeframe) -> String {
    serde_json::json!({
        "method": "subscribe",
        "subscription": {
            "type": "candle",
            "coin": instrument.provider_symbol(),
            "interval": timeframe.as_str(),
        }
    })
    .to_string()
}

fn unix_now_ms() -> Result<i64, ProviderError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .ok_or(ProviderError::Invariant("unix epoch clock is unavailable"))
}

fn json_decimal(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().filter(|value| value.is_finite()),
        Value::String(text) => text.parse::<f64>().ok().filter(|value| value.is_finite()),
        _ => None,
    }
}

fn remap_instrument(spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
    let local = canonicalize_instrument(spec)
        .map_err(|_| ProviderError::Configuration("instrument is not valid for Hyperliquid"))?;
    if let Some(dex) = spec.venue() {
        return Instrument::new(
            spec.provider().clone(),
            Market::Perpetual,
            local.base(),
            local.quote(),
            format!("{dex}:{}", local.base()),
        )
        .map_err(|_| ProviderError::Configuration("instrument is not valid for Hyperliquid"));
    }
    if spec.market() == Market::Perpetual {
        return Instrument::new(
            spec.provider().clone(),
            Market::Perpetual,
            local.base(),
            local.quote(),
            local.base(),
        )
        .map_err(|_| ProviderError::Configuration("instrument is not valid for Hyperliquid"));
    }
    if is_spot_index_token(local.base()) {
        return Instrument::new(
            spec.provider().clone(),
            Market::Spot,
            local.base(),
            local.quote(),
            local.base(),
        )
        .map_err(|_| ProviderError::Configuration("instrument is not valid for Hyperliquid"));
    }
    let (display_base, wire) = match (local.base(), local.quote()) {
        ("BTC" | "UBTC", "USDC") => ("UBTC", SPOT_UBTC_INDEX.to_owned()),
        ("HYPE", "USDC") => ("HYPE", SPOT_HYPE_INDEX.to_owned()),
        ("PURR", "USDC") => ("PURR", "PURR/USDC".to_owned()),
        (base, quote) => (base, format!("{base}/{quote}")),
    };
    Instrument::new(
        spec.provider().clone(),
        Market::Spot,
        display_base,
        local.quote(),
        wire,
    )
    .map_err(|_| ProviderError::Configuration("instrument is not valid for Hyperliquid"))
}
