//! Binance Spot and USD-M Perpetual REST history and raw WebSocket transport.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use futures_util::{FutureExt, stream};
use reqwest::{Client, StatusCode, Url, header::RETRY_AFTER};
use serde::{
    Deserialize,
    de::{IgnoredAny, SeqAccess, Visitor},
};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    cli::canonicalize_instrument,
    clock::{Clock, checked_deadline},
    error::{
        ErrorContext, ErrorOperation, PayloadError, ProviderError, SanitizedCause,
        SanitizedMessage, TimeoutKind,
    },
    model::{
        Candle, ConnectionStatus, FinalityAuthority, GapGeneration, HistoryRequest, Instrument,
        InstrumentSpec, Market, MarketEvent, MonoInstant, ProcessBlocker, ProviderId,
        RateGateState, ReplayRevision, Timeframe,
    },
    provider::{
        LiveFeed, LiveRequest, MarketDataProvider, ProviderFuture, RateGateSender,
        RateGateSnapshot, ReconcileAck, ReconcileExpectation, ReconcileExpectationError,
        rate_gate_channel,
        runtime::{
            emitter::{EventEmitter, live_channel_closed},
            websocket::{
                DecodedFrame, WsCodec, WsConfig, connect_websocket_url,
                contextualize_websocket_configuration, validate_websocket_base,
            },
        },
    },
};

const SPOT_KLINES_PATH: &str = "/api/v3/klines";
const PERPETUAL_KLINES_PATH: &str = "/fapi/v1/klines";
pub const REST_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const REST_BODY_LIMIT: usize = 2 * 1024 * 1024;
pub const RATE_LIMIT_FALLBACK: Duration = Duration::from_secs(30);

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_SPOT_REST_BASE: &str = "https://data-api.binance.vision";
#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_PERPETUAL_REST_BASE: &str = "https://fapi.binance.com";

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_SPOT_WS_BASE: &str = "wss://data-stream.binance.vision";
#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
const PRODUCTION_PERPETUAL_WS_BASE: &str = "wss://fstream.binance.com";

pub const KEYED_CANDLE_CAPACITY: usize = 1024;
pub const CONTROL_CAPACITY: usize = 64;
pub const EMERGENCY_CONTROL_CAPACITY: usize = 2;
pub const MARKET_EVENT_CHANNEL_CAPACITY: usize = 256;
pub const FIRST_KLINE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
pub const RECONCILE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_CONNECTION_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_SUPERVISOR_CAPACITY: usize = 65_536;
const GAP_PAGE_LIMIT: u16 = 1000;

#[derive(Clone, Debug)]
pub struct LiveSupervisorConfig {
    pub keyed_candle_capacity: usize,
    pub control_capacity: usize,
    pub market_event_capacity: usize,
    pub first_kline_timeout: Duration,
    pub reconcile_ack_timeout: Duration,
    pub max_connection_age: Duration,
    pub ws_config: WsConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub stalled_write_probe_frames: usize,
}

impl Default for LiveSupervisorConfig {
    fn default() -> Self {
        Self {
            keyed_candle_capacity: KEYED_CANDLE_CAPACITY,
            control_capacity: CONTROL_CAPACITY,
            market_event_capacity: MARKET_EVENT_CHANNEL_CAPACITY,
            first_kline_timeout: FIRST_KLINE_HANDSHAKE_TIMEOUT,
            reconcile_ack_timeout: RECONCILE_ACK_TIMEOUT,
            max_connection_age: MAX_CONNECTION_AGE,
            ws_config: WsConfig::default(),
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            stalled_write_probe_frames: 0,
        }
    }
}

impl LiveSupervisorConfig {
    pub fn validate(&self) -> Result<(), ProviderError> {
        for capacity in [
            self.keyed_candle_capacity,
            self.control_capacity,
            self.market_event_capacity,
        ] {
            if !(1..=MAX_SUPERVISOR_CAPACITY).contains(&capacity) {
                return Err(ProviderError::Configuration(
                    "live supervisor capacity is outside 1..=65536",
                ));
            }
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if self.stalled_write_probe_frames > MAX_SUPERVISOR_CAPACITY {
            return Err(ProviderError::Configuration(
                "live supervisor stalled-write probe is outside 0..=65536",
            ));
        }
        for timeout in [self.first_kline_timeout, self.reconcile_ack_timeout] {
            if !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&timeout) {
                return Err(ProviderError::Configuration(
                    "live supervisor timeout is outside 1ms..=60s",
                ));
            }
        }
        if self.max_connection_age.is_zero() {
            return Err(ProviderError::Configuration(
                "live connection max age must be positive",
            ));
        }
        self.ws_config.validate()?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct WsEnvelope {
    #[serde(rename = "e")]
    event: Option<String>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    k: Option<WsKline>,
}

#[derive(Deserialize)]
struct WsKline {
    #[serde(rename = "t")]
    open_time: i64,
    #[serde(rename = "T")]
    close_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "x")]
    closed: bool,
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub fn websocket_url(instrument: &Instrument, timeframe: Timeframe) -> Result<Url, ProviderError> {
    websocket_url_from_base(
        production_ws_base(instrument.market()),
        instrument,
        timeframe,
        false,
    )
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

fn klines_path(market: Market) -> &'static str {
    match market {
        Market::Spot => SPOT_KLINES_PATH,
        Market::Perpetual => PERPETUAL_KLINES_PATH,
    }
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn production_rest_base(market: Market) -> &'static str {
    match market {
        Market::Spot => PRODUCTION_SPOT_REST_BASE,
        Market::Perpetual => PRODUCTION_PERPETUAL_REST_BASE,
    }
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn production_ws_base(market: Market) -> &'static str {
    match market {
        Market::Spot => PRODUCTION_SPOT_WS_BASE,
        Market::Perpetual => PRODUCTION_PERPETUAL_WS_BASE,
    }
}
fn websocket_url_from_base(
    base_url: &str,
    instrument: &Instrument,
    timeframe: Timeframe,
    loopback_only: bool,
) -> Result<Url, ProviderError> {
    let mut url = validate_websocket_base(base_url, loopback_only)?;
    let stream = format!(
        "{}@kline_{}",
        instrument.provider_symbol().to_ascii_lowercase(),
        timeframe.as_str()
    );
    url.set_path(&format!("/ws/{stream}"));
    Ok(url)
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug, PartialEq)]
pub enum BinanceDecoded {
    Candle(Candle),
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BinanceDecoded {
    Candle(Candle),
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn decode_ws_frame(
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
) -> DecodedFrame<BinanceDecoded> {
    decode_ws_frame_impl(message, instrument, timeframe, config)
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn decode_ws_frame(
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
) -> DecodedFrame<BinanceDecoded> {
    decode_ws_frame_impl(message, instrument, timeframe, config)
}

fn decode_ws_frame_impl(
    message: Message,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
) -> DecodedFrame<BinanceDecoded> {
    if let Err(error) = config.validate() {
        return DecodedFrame::ProviderError(error);
    }
    match message {
        Message::Text(text) => decode_ws_payload(text.as_bytes(), instrument, timeframe, config),
        Message::Binary(bytes) => decode_ws_payload(&bytes, instrument, timeframe, config),
        Message::Close(frame) => DecodedFrame::Close(frame.map(|frame| frame.code)),
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => DecodedFrame::Ignored,
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Copy, Debug, Default)]
pub struct BinanceWsCodec;

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BinanceWsCodec;

impl WsCodec for BinanceWsCodec {
    type Outcome = BinanceDecoded;

    fn decode(
        &mut self,
        message: Message,
        instrument: &Instrument,
        timeframe: Timeframe,
        config: &WsConfig,
        output: &mut VecDeque<DecodedFrame<Self::Outcome>>,
    ) {
        output.push_back(decode_ws_frame(message, instrument, timeframe, config));
    }

    fn readiness_priority(outcome: &Self::Outcome) -> u8 {
        match outcome {
            BinanceDecoded::Candle(_) => 1,
        }
    }
}

fn decode_ws_payload(
    bytes: &[u8],
    instrument: &Instrument,
    timeframe: Timeframe,
    config: &WsConfig,
) -> DecodedFrame<BinanceDecoded> {
    let context =
        ErrorContext::operation(ErrorOperation::WebSocket).with_market(instrument, timeframe);
    if bytes.len() > config.max_message_size {
        return DecodedFrame::ProviderError(payload(
            &context,
            PayloadError::OverBudget {
                limit_bytes: config.max_message_size,
            },
        ));
    }
    let envelope: WsEnvelope = match serde_json::from_slice(bytes) {
        Ok(value) => value,
        Err(_) => {
            return DecodedFrame::ProviderError(payload(&context, PayloadError::MalformedProtocol));
        }
    };
    if envelope.event.as_deref() == Some("serverShutdown") {
        return DecodedFrame::ReconnectRequested;
    }
    if envelope.code == Some(-1121) {
        return DecodedFrame::ProviderError(ProviderError::InvalidSymbol {
            context,
            code: -1121,
            message: envelope
                .msg
                .as_deref()
                .map(SanitizedMessage::new)
                .unwrap_or(SanitizedMessage::InvalidSymbol),
        });
    }
    if envelope.code.is_some() || envelope.msg.is_some() {
        return DecodedFrame::ProviderError(ProviderError::Protocol {
            context,
            detail: "provider reported a WebSocket error",
        });
    }
    if envelope.event.as_deref() != Some("kline") {
        return DecodedFrame::Ignored;
    }
    let Some(kline) = envelope.k else {
        return DecodedFrame::ProviderError(payload(&context, PayloadError::MalformedProtocol));
    };
    if envelope.symbol.as_deref() != Some(instrument.provider_symbol())
        || kline.symbol != instrument.provider_symbol()
        || kline.interval != timeframe.as_str()
    {
        return DecodedFrame::ProviderError(ProviderError::Protocol {
            context,
            detail: "WebSocket kline market does not match subscription",
        });
    }
    let parse = |value: &str| {
        value
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
    };
    let Some((open, high, low, close, volume)) = parse(&kline.open)
        .zip(parse(&kline.high))
        .zip(parse(&kline.low))
        .zip(parse(&kline.close))
        .zip(parse(&kline.volume))
        .map(|((((open, high), low), close), volume)| (open, high, low, close, volume))
    else {
        return DecodedFrame::ProviderError(payload(
            &context,
            PayloadError::InvalidField {
                field: "kline numeric field",
            },
        ));
    };
    match Candle::from_ws(
        kline.open_time,
        kline.close_time,
        open,
        high,
        low,
        close,
        volume,
        kline.closed,
    ) {
        Ok(candle) => DecodedFrame::Provider(BinanceDecoded::Candle(candle)),
        Err(source) => DecodedFrame::ProviderError(ProviderError::Domain { context, source }),
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub type RawWebSocket = crate::provider::runtime::websocket::RawWebSocket<BinanceWsCodec>;

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub(crate) type RawWebSocket = crate::provider::runtime::websocket::RawWebSocket<BinanceWsCodec>;

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub(crate) async fn connect_websocket(
    instrument: &Instrument,
    timeframe: Timeframe,
    config: WsConfig,
) -> Result<RawWebSocket, ProviderError> {
    let url = websocket_url(instrument, timeframe)?;
    connect_websocket_url(&url, instrument, timeframe, config, BinanceWsCodec, None).await
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub async fn connect_test_websocket(
    base_url: &str,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: WsConfig,
) -> Result<RawWebSocket, ProviderError> {
    let url = test_websocket_url(base_url, instrument, timeframe)?;
    connect_websocket_url(&url, instrument, timeframe, config, BinanceWsCodec, None).await
}

#[derive(Clone)]
pub struct BinanceProvider {
    client: Client,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    base_url: Url,
    clock: Arc<dyn Clock>,
    gate_sender: RateGateSender,
    gate_snapshot: RateGateSnapshot,
    body_limit: usize,
    rate_limit_fallback: Duration,
    live: LiveSupervisorConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    ws_base_url: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BinanceTestConfig {
    pub base_url: String,
    pub request_timeout: Duration,
    pub body_limit: usize,
    pub rate_limit_fallback: Duration,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
impl BinanceTestConfig {
    #[must_use]
    pub fn loopback(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            request_timeout: REST_REQUEST_TIMEOUT,
            body_limit: REST_BODY_LIMIT,
            rate_limit_fallback: RATE_LIMIT_FALLBACK,
        }
    }

    #[must_use]
    pub fn with_websocket_base(self, base_url: impl Into<String>) -> BinanceLiveTestConfig {
        BinanceLiveTestConfig {
            rest: self,
            ws_base_url: base_url.into(),
            live: LiveSupervisorConfig::default(),
        }
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct BinanceLiveTestConfig {
    pub rest: BinanceTestConfig,
    pub ws_base_url: String,
    pub live: LiveSupervisorConfig,
}

impl BinanceProvider {
    #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
    pub fn new(clock: Arc<dyn Clock>) -> Result<Self, ProviderError> {
        Self::build(
            clock,
            REST_REQUEST_TIMEOUT,
            REST_BODY_LIMIT,
            RATE_LIMIT_FALLBACK,
            LiveSupervisorConfig::default(),
        )
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test(
        base_url: impl AsRef<str>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        Self::new_test_with_config_and_clock(BinanceTestConfig::loopback(base_url.as_ref()), clock)
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test_with_config_and_clock(
        config: BinanceTestConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        let base_url = validate_loopback_base(&config.base_url)?;
        Self::build(
            base_url,
            clock,
            config.request_timeout,
            config.body_limit,
            config.rate_limit_fallback,
            LiveSupervisorConfig::default(),
            None,
        )
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn new_test_live(
        config: BinanceLiveTestConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        let base_url = validate_loopback_base(&config.rest.base_url)?;
        validate_loopback_ws_base(&config.ws_base_url)?;
        Self::build(
            base_url,
            clock,
            config.rest.request_timeout,
            config.rest.body_limit,
            config.rest.rate_limit_fallback,
            config.live,
            Some(config.ws_base_url),
        )
    }

    fn build(
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        base_url: Url,
        clock: Arc<dyn Clock>,
        request_timeout: Duration,
        body_limit: usize,
        rate_limit_fallback: Duration,
        live: LiveSupervisorConfig,
        #[cfg(all(
            feature = "test-transport",
            not(feature = "production-transport")
        ))]
        ws_base_url: Option<String>,
    ) -> Result<Self, ProviderError> {
        if request_timeout.is_zero() || body_limit == 0 || rate_limit_fallback.is_zero() {
            return Err(ProviderError::Configuration(
                "REST timeout, body limit, and fallback must be positive",
            ));
        }
        live.validate()?;
        let client = Client::builder()
            .no_proxy()
            .timeout(request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("fccli/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| ProviderError::Configuration("failed to build REST client"))?;
        let (gate_sender, gate_snapshot) = rate_gate_channel(RateGateState::Open);
        Ok(Self {
            client,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            base_url,
            clock,
            gate_sender,
            gate_snapshot,
            body_limit,
            rate_limit_fallback,
            live,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            ws_base_url,
        })
    }

    pub async fn history(
        &self,
        instrument: &Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> Result<Vec<Candle>, ProviderError> {
        let context =
            ErrorContext::operation(ErrorOperation::History).with_market(instrument, timeframe);
        self.await_gate(&cancellation, &context).await?;

        #[cfg(any(
            all(feature = "production-transport", not(feature = "test-transport")),
            all(feature = "test-transport", not(feature = "production-transport"))
        ))]
        {
            #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
            let mut url = Url::parse(production_rest_base(instrument.market()))
                .map_err(|_| ProviderError::Configuration("invalid production REST base URL"))?;
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            let mut url = self.base_url.clone();
            url.set_path(klines_path(instrument.market()));
            url.set_query(None);
            let limit = request.limit().to_string();
            let mut query = vec![
                ("symbol", instrument.provider_symbol().to_owned()),
                ("interval", timeframe.as_str().to_owned()),
                ("limit", limit),
            ];
            if let Some(start_time) = request.start_time() {
                query.push(("startTime", start_time.to_string()));
            }
            if let Some(end_time) = request.end_time() {
                query.push(("endTime", end_time.to_string()));
            }

            let response = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(cancelled(context.clone())),
                result = self.client.get(url).query(&query).send() => result.map_err(|error| {
                    if error.is_timeout() {
                        ProviderError::Timeout { context: context.clone(), kind: TimeoutKind::Request }
                    } else {
                        ProviderError::Transport { context: context.clone(), cause: SanitizedCause::Connection }
                    }
                })?,
            };

            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT {
                return self.handle_rate_limit(&response, context);
            }
            if status.is_server_error() {
                return Err(ProviderError::ServerStatus {
                    context,
                    status: status.as_u16(),
                });
            }
            if status.is_redirection() {
                return Err(ProviderError::ClientStatus {
                    context,
                    status: status.as_u16(),
                    code: None,
                    message: None,
                });
            }
            if status.is_client_error() {
                let bytes =
                    match read_capped(response, self.body_limit, &cancellation, &context).await {
                        Ok(bytes) => bytes,
                        Err(error) if is_cancelled(&error) => return Err(error),
                        Err(_) => Vec::new(),
                    };
                return Err(map_http_error(status, &bytes, context));
            }
            let bytes = read_capped(response, self.body_limit, &cancellation, &context).await?;
            decode_klines(&bytes, request.limit(), context)
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
        let mut snapshot = self.gate_snapshot.clone();
        loop {
            match snapshot
                .current()
                .map_err(|_| ProviderError::Invariant("rate gate closed"))?
            {
                RateGateState::Open => return Ok(()),
                RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry) => {
                    return Err(ProviderError::InvalidBanExpiry);
                }
                RateGateState::TimedUntil(deadline) if deadline <= self.clock.now() => {
                    return Ok(());
                }
                RateGateState::TimedUntil(deadline) => {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Err(cancelled(context.clone())),
                        changed = snapshot.changed() => {
                            changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?;
                        }
                        () = self.clock.sleep_until(deadline) => {}
                    }
                }
            }
        }
    }

    fn handle_rate_limit(
        &self,
        response: &reqwest::Response,
        context: ErrorContext,
    ) -> Result<Vec<Candle>, ProviderError> {
        let status = response.status();
        let parsed = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|seconds| {
                checked_deadline(self.clock.now(), Duration::from_secs(seconds)).ok()
            });

        let deadline = if status == StatusCode::TOO_MANY_REQUESTS {
            parsed.or_else(|| checked_deadline(self.clock.now(), self.rate_limit_fallback).ok())
        } else {
            parsed
        };
        if let Some(deadline) = deadline {
            self.gate_sender
                .publish(RateGateState::TimedUntil(deadline))
                .map_err(|_| ProviderError::Invariant("rate gate closed"))?;
        } else {
            self.gate_sender
                .publish(RateGateState::ProcessBlocked(
                    ProcessBlocker::InvalidBanExpiry,
                ))
                .map_err(|_| ProviderError::Invariant("rate gate closed"))?;
        }
        let effective = self
            .gate_snapshot
            .current()
            .map_err(|_| ProviderError::Invariant("rate gate closed"))?;
        if matches!(
            effective,
            RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry)
        ) {
            return Err(ProviderError::InvalidBanExpiry);
        }
        Err(ProviderError::RateLimited {
            context,
            status: status.as_u16(),
        })
    }

    async fn connect_live_socket(
        &self,
        instrument: &Instrument,
        timeframe: Timeframe,
    ) -> Result<RawWebSocket, ProviderError> {
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        {
            connect_websocket(instrument, timeframe, self.live.ws_config).await
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            let base = self
                .ws_base_url
                .as_deref()
                .ok_or(ProviderError::Configuration(
                    "test WebSocket base URL is required for live feeds",
                ))?;
            connect_test_websocket(base, instrument, timeframe, self.live.ws_config).await
        }
        #[cfg(all(feature = "production-transport", feature = "test-transport"))]
        {
            let _ = (instrument, timeframe);
            unreachable!("mutually exclusive transport features are rejected by src/lib.rs")
        }
    }

    async fn supervise_live(
        self,
        mut request: LiveRequest,
        sender: EventEmitter,
    ) -> Result<(), ProviderError> {
        let mut generation_number = 0_u64;
        let mut backoff_index = 0_usize;
        loop {
            if request.cancellation.is_cancelled() {
                sender.shutdown().await;
                return Ok(());
            }
            if matches!(
                self.gate_snapshot.current(),
                Ok(RateGateState::ProcessBlocked(_))
            ) {
                self.send_invalid_ban_and_stop(&sender).await;
                return Err(ProviderError::InvalidBanExpiry);
            }
            generation_number = generation_number
                .checked_add(1)
                .ok_or(ProviderError::Invariant("gap generation overflow"))?;
            let generation = GapGeneration(generation_number);
            send_market(
                &sender,
                &request.cancellation,
                MarketEvent::Status {
                    generation: Some(generation),
                    status: ConnectionStatus::Connecting,
                },
            )
            .await?;
            let connect_result = {
                let connect_instrument = request.instrument.clone();
                let connect_timeframe = request.timeframe;
                let connect = self.connect_live_socket(&connect_instrument, connect_timeframe);
                tokio::pin!(connect);
                let mut gate = self.gate_snapshot.clone();
                loop {
                    tokio::select! {
                        biased;
                        () = request.cancellation.cancelled() => {
                            sender.shutdown().await;
                            return Ok(());
                        }
                        changed = request.accepted_watermark_rx.changed() => {
                            if changed.is_err() {
                                break Err(control_channel_closed(&request.instrument, request.timeframe));
                            }
                        }
                        ack = request.reconcile_ack_rx.changed() => {
                            if ack.is_err() {
                                break Err(control_channel_closed(&request.instrument, request.timeframe));
                            }
                        }
                        changed = gate.changed() => match changed {
                            Err(_) => break Err(ProviderError::Invariant("rate gate closed")),
                            Ok(RateGateState::ProcessBlocked(_)) => {
                                self.send_invalid_ban_and_stop(&sender).await;
                                return Err(ProviderError::InvalidBanExpiry);
                            }
                            Ok(RateGateState::Open | RateGateState::TimedUntil(_)) => {}
                        },
                        result = &mut connect => break result,
                    }
                }
            };
            let mut socket = match connect_result {
                Ok(socket) => socket,
                Err(error) if is_terminal_live_error(&error) => {
                    sender.invalidate_generation(generation);
                    send_market(
                        &sender,
                        &request.cancellation,
                        MarketEvent::TerminalError(error.clone()),
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => {
                    sender.invalidate_generation(generation);
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?;
                    continue;
                }
            };
            let age_deadline = checked_deadline(self.clock.now(), self.live.max_connection_age)
                .map_err(|_| ProviderError::Invariant("live connection age deadline overflow"))?;
            let outcome = self
                .run_generation(&mut request, &sender, &mut socket, generation, age_deadline)
                .await;
            drop(socket);
            if !matches!(&outcome, Ok(GenerationOutcome::Cancelled)) {
                sender.invalidate_generation(generation);
            }
            match outcome {
                Ok(GenerationOutcome::Cancelled) => {
                    sender.shutdown().await;
                    return Ok(());
                }
                Ok(GenerationOutcome::AcknowledgedReconnect(error)) => {
                    if sender.connected_delivered(generation) {
                        backoff_index = 0;
                    }
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?;
                }
                Ok(GenerationOutcome::Reconnect(error)) => {
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?;
                }
                Err(error) if matches!(error, ProviderError::InvalidBanExpiry) => {
                    self.send_invalid_ban_and_stop(&sender).await;
                    return Err(error);
                }
                Err(error) if is_terminal_live_error(&error) => {
                    send_market(
                        &sender,
                        &request.cancellation,
                        MarketEvent::TerminalError(error.clone()),
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => {
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?
                }
            }
        }
    }

    async fn run_generation(
        &self,
        request: &mut LiveRequest,
        sender: &EventEmitter,
        socket: &mut RawWebSocket,
        generation: GapGeneration,
        age_deadline: MonoInstant,
    ) -> Result<GenerationOutcome, ProviderError> {
        send_market(
            sender,
            &request.cancellation,
            MarketEvent::Status {
                generation: Some(generation),
                status: ConnectionStatus::GapSync,
            },
        )
        .await?;
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if self.live.stalled_write_probe_frames != 0 {
            let payload_size = self
                .live
                .ws_config
                .write_buffer_size
                .min(self.live.ws_config.max_frame_size)
                .min(self.live.ws_config.max_message_size)
                .max(1);
            let payload = Message::Binary(vec![0; payload_size].into());
            for _ in 0..self.live.stalled_write_probe_frames {
                tokio::select! {
                    biased;
                    () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                    result = socket.send(payload.clone()) => result?,
                }
            }
        }
        let mut gate = self.gate_snapshot.clone();
        let first_deadline = checked_deadline(self.clock.now(), self.live.first_kline_timeout)
            .map_err(|_| ProviderError::Invariant("first-kline deadline overflow"))?;
        let first = loop {
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                changed = request.accepted_watermark_rx.changed() => { changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                ack = request.reconcile_ack_rx.changed() => { ack.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                changed = gate.changed() => if matches!(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?, RateGateState::ProcessBlocked(_)) { return Err(ProviderError::InvalidBanExpiry); },
                frame = socket.read() => match frame? {
                    DecodedFrame::Provider(BinanceDecoded::Candle(candle)) => break candle,
                    DecodedFrame::Ignored => {
                        let now = self.clock.now();
                        if now >= first_deadline {
                            return Ok(GenerationOutcome::Reconnect(ProviderError::Timeout { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&request.instrument, request.timeframe), kind: TimeoutKind::FirstKline }));
                        }
                        if now >= age_deadline {
                            return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached")));
                        }
                    }
                    DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested => return Ok(GenerationOutcome::Reconnect(ProviderError::Protocol { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&request.instrument, request.timeframe), detail: "WebSocket peer requested reconnect" })),
                    DecodedFrame::ProviderError(error) if is_terminal_live_error(&error) => return Err(error),
                    DecodedFrame::ProviderError(error) => return Ok(GenerationOutcome::Reconnect(error)),
                },
                () = self.clock.sleep_until(age_deadline) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached"))),
                () = self.clock.sleep_until(first_deadline) => {
                    return Ok(GenerationOutcome::Reconnect(ProviderError::Timeout { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&request.instrument, request.timeframe), kind: TimeoutKind::FirstKline }));
                }
            }
        };
        let confirmed = request
            .accepted_watermark_rx
            .current()
            .map_err(|_| ProviderError::ChannelClosed {
                context: ErrorContext::operation(ErrorOperation::Reconciliation)
                    .with_market(&request.instrument, request.timeframe),
            })?
            .max(request.startup_watermark);
        let start = confirmed.unwrap_or_else(|| first.open_time());
        let mut target_open_time = first.open_time().max(start);
        let mut revision = ReplayRevision(1);
        let mut buffered = BTreeMap::new();
        if first.open_time() >= start {
            coalesce_candle(&mut buffered, first);
        }
        let mut deferred_reconnect: Option<ProviderError> = None;
        let mut rest_synced_through = None;

        loop {
            let mut cursor = match rest_synced_through {
                Some(last) => next_gap_cursor(request.timeframe, last)?,
                None => start,
            };
            while cursor <= target_open_time {
                let request_target = target_open_time;
                let history_request =
                    HistoryRequest::gap(cursor, request_target, GAP_PAGE_LIMIT)
                        .map_err(|_| ProviderError::Invariant("invalid gap history request"))?;
                let page = {
                    let history_instrument = request.instrument.clone();
                    let history_timeframe = request.timeframe;
                    let history_cancel = request.cancellation.child_token();
                    let history = self.history(
                        &history_instrument,
                        history_timeframe,
                        history_request,
                        history_cancel,
                    );
                    tokio::pin!(history);
                    enum ReconcileWake {
                        Cancelled,
                        AcceptedWatermark(Result<Option<i64>, ProviderError>),
                        Ack(Result<(), ProviderError>),
                        ConnectionAged,
                        Gate(Result<RateGateState, ProviderError>),
                        Socket(Result<DecodedFrame<BinanceDecoded>, ProviderError>),
                        Page(Result<Vec<Candle>, ProviderError>),
                    }

                    loop {
                        if request.cancellation.is_cancelled() {
                            return Ok(GenerationOutcome::Cancelled);
                        }
                        let wake = tokio::select! {
                            () = request.cancellation.cancelled() => ReconcileWake::Cancelled,
                            changed = request.accepted_watermark_rx.changed() => ReconcileWake::AcceptedWatermark(changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))),
                            ack = request.reconcile_ack_rx.changed() => ReconcileWake::Ack(ack.map(|_| ()).map_err(|_| control_channel_closed(&request.instrument, request.timeframe))),
                            () = self.clock.sleep_until(age_deadline) => ReconcileWake::ConnectionAged,
                            changed = gate.changed() => ReconcileWake::Gate(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))),
                            frame = socket.read() => ReconcileWake::Socket(frame),
                            page = &mut history => ReconcileWake::Page(page),
                        };
                        if request.cancellation.is_cancelled() {
                            return Ok(GenerationOutcome::Cancelled);
                        }
                        match wake {
                            ReconcileWake::Cancelled => return Ok(GenerationOutcome::Cancelled),
                            ReconcileWake::AcceptedWatermark(changed) => {
                                if let Some(watermark) = changed? {
                                    target_open_time = target_open_time.max(watermark);
                                }
                            }
                            ReconcileWake::Ack(changed) => changed?,
                            ReconcileWake::ConnectionAged => {
                                return Ok(GenerationOutcome::Reconnect(live_protocol_error(
                                    request,
                                    "24-hour WebSocket connection age reached",
                                )));
                            }
                            ReconcileWake::Gate(changed) => {
                                if matches!(changed?, RateGateState::ProcessBlocked(_)) {
                                    return Err(ProviderError::InvalidBanExpiry);
                                }
                            }
                            ReconcileWake::Socket(frame) => match frame {
                                Ok(DecodedFrame::Provider(BinanceDecoded::Candle(candle))) => {
                                    apply_reconciliation_candle(
                                        &mut buffered,
                                        candle,
                                        &mut revision,
                                        &mut target_open_time,
                                    )?;
                                }
                                Ok(DecodedFrame::Ignored) => {}
                                Ok(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => {
                                    return Ok(GenerationOutcome::Reconnect(live_protocol_error(
                                        request,
                                        "WebSocket peer requested reconnect",
                                    )));
                                }
                                Ok(DecodedFrame::ProviderError(error))
                                    if is_terminal_live_error(&error) =>
                                {
                                    return Err(error);
                                }
                                Err(error) if is_terminal_live_error(&error) => return Err(error),
                                Ok(DecodedFrame::ProviderError(error)) | Err(error) => {
                                    return Ok(GenerationOutcome::Reconnect(error));
                                }
                            },
                            ReconcileWake::Page(page) => {
                                if request.cancellation.is_cancelled() {
                                    return Ok(GenerationOutcome::Cancelled);
                                }
                                let terminal = async {
                                tokio::select! {
                                    biased;
                                    () = request.cancellation.cancelled() => Ok((Some(GenerationOutcome::Cancelled), false)),
                                    changed = request.accepted_watermark_rx.changed() => changed
                                        .map(|watermark| {
                                            if let Some(watermark) = watermark {
                                                target_open_time = target_open_time.max(watermark);
                                            }
                                            (None, true)
                                        })
                                        .map_err(|_| control_channel_closed(&request.instrument, request.timeframe)),
                                    ack = request.reconcile_ack_rx.changed() => ack
                                        .map(|_| (None, false))
                                        .map_err(|_| control_channel_closed(&request.instrument, request.timeframe)),
                                    () = self.clock.sleep_until(age_deadline) => Ok((Some(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached"))), false)),
                                    changed = gate.changed() => match changed {
                                        Ok(RateGateState::ProcessBlocked(_)) => Err(ProviderError::InvalidBanExpiry),
                                        Ok(_) => Ok((None, false)),
                                        Err(_) => Err(ProviderError::Invariant("rate gate closed")),
                                    },
                                    frame = socket.read() => match frame {
                                        Ok(DecodedFrame::Provider(BinanceDecoded::Candle(candle))) => {
                                            apply_reconciliation_candle(&mut buffered, candle, &mut revision, &mut target_open_time)?;
                                            Ok((None, false))
                                        }
                                        Ok(DecodedFrame::Ignored) => Ok((None, false)),
                                        Ok(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => Ok((Some(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect"))), false)),
                                        Ok(DecodedFrame::ProviderError(error)) | Err(error) if is_terminal_live_error(&error) => Err(error),
                                        Ok(DecodedFrame::ProviderError(error)) | Err(error) => Ok((Some(GenerationOutcome::Reconnect(error)), false)),
                                    },
                                }
                            }
                            .now_or_never()
                            .transpose()?;
                                if request.cancellation.is_cancelled() {
                                    return Ok(GenerationOutcome::Cancelled);
                                }
                                let watermark_consumed = match terminal {
                                    Some((Some(outcome), _)) => return Ok(outcome),
                                    Some((None, true)) => true,
                                    _ => false,
                                };
                                if watermark_consumed {
                                    let follow_up = async {
                                    tokio::select! {
                                        biased;
                                        () = request.cancellation.cancelled() => Ok(Some(GenerationOutcome::Cancelled)),
                                        frame = socket.read() => match frame {
                                            Ok(DecodedFrame::Provider(BinanceDecoded::Candle(candle))) => {
                                                apply_reconciliation_candle(&mut buffered, candle, &mut revision, &mut target_open_time)?;
                                                Ok(None)
                                            }
                                            Ok(DecodedFrame::Ignored) => Ok(None),
                                            Ok(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect")))),
                                            Ok(DecodedFrame::ProviderError(error)) | Err(error) if is_terminal_live_error(&error) => Err(error),
                                            Ok(DecodedFrame::ProviderError(error)) | Err(error) => Ok(Some(GenerationOutcome::Reconnect(error))),
                                        },
                                        () = self.clock.sleep_until(age_deadline) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached")))),
                                        changed = gate.changed() => match changed {
                                            Ok(RateGateState::ProcessBlocked(_)) => Err(ProviderError::InvalidBanExpiry),
                                            Ok(_) => Ok(None),
                                            Err(_) => Err(ProviderError::Invariant("rate gate closed")),
                                        },
                                        ack = request.reconcile_ack_rx.changed() => ack
                                            .map(|_| None)
                                            .map_err(|_| control_channel_closed(&request.instrument, request.timeframe)),
                                    }
                                }
                                .now_or_never()
                                .transpose()?;
                                    if request.cancellation.is_cancelled() {
                                        return Ok(GenerationOutcome::Cancelled);
                                    }
                                    if let Some(Some(outcome)) = follow_up {
                                        return Ok(outcome);
                                    }
                                }
                                break page;
                            }
                        }
                    }
                };
                let page = page?;
                let last = page.last().map(Candle::open_time);
                if page.is_empty() {
                    if confirmed.is_none() && cursor == start {
                        break;
                    }
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: None,
                        },
                    ));
                }
                if last.is_some_and(|value| value < cursor) {
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: last,
                        },
                    ));
                }
                let page_len = page.len();
                let mut accepted_any = false;
                for candle in page {
                    accepted_any |= coalesce_candle(&mut buffered, candle);
                }
                let Some(last) = last else { unreachable!() };
                rest_synced_through = Some(last);
                if last >= target_open_time {
                    break;
                }
                if last < request_target && page_len < usize::from(GAP_PAGE_LIMIT) {
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: Some(last),
                        },
                    ));
                }
                if !accepted_any && last < request_target {
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: Some(last),
                        },
                    ));
                }
                cursor = next_gap_cursor(request.timeframe, last)?;
            }
            let page_candles = buffered.values().cloned().collect();
            let expected = ReconcileExpectation {
                generation,
                revision,
                target_open_time,
            };
            request
                .reconcile_ack_rx
                .register_expectation(expected)
                .map_err(|error| match error {
                    ReconcileExpectationError::Closed => {
                        control_channel_closed(&request.instrument, request.timeframe)
                    }
                    ReconcileExpectationError::Regression | ReconcileExpectationError::Conflict => {
                        ProviderError::Invariant("reconciliation expectation invariant violated")
                    }
                })?;
            send_market(
                sender,
                &request.cancellation,
                MarketEvent::ReconcileBatch {
                    generation,
                    revision,
                    target_open_time,
                    candles: page_candles,
                },
            )
            .await?;
            let ack_deadline = checked_deadline(self.clock.now(), self.live.reconcile_ack_timeout)
                .map_err(|_| ProviderError::Invariant("ack deadline overflow"))?;
            loop {
                tokio::select! {
                    biased;
                    () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                    changed = request.accepted_watermark_rx.changed() => { changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                    () = self.clock.sleep_until(age_deadline) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached"))),
                    changed = gate.changed() => if matches!(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?, RateGateState::ProcessBlocked(_)) { return Err(ProviderError::InvalidBanExpiry); },
                    frame = socket.read(), if deferred_reconnect.is_none() => match frame? {
                        DecodedFrame::Provider(BinanceDecoded::Candle(candle)) => {
                            apply_reconciliation_candle(&mut buffered, candle, &mut revision, &mut target_open_time)?;
                            break;
                        }
                        DecodedFrame::Ignored => {
                            if let Some(ack) = request
                                .reconcile_ack_rx
                                .current()
                                .map_err(|_| {
                                    control_channel_closed(&request.instrument, request.timeframe)
                                })?
                                && ack.generation == generation
                                && ack.revision == revision
                                && ack.through >= target_open_time
                            {
                                return if let Some(error) = deferred_reconnect.take() {
                                    Ok(GenerationOutcome::Reconnect(error))
                                } else {
                                    self.connected_loop(request, sender, socket, generation, age_deadline).await
                                };
                            }
                            if self.clock.now() >= ack_deadline {
                                return Ok(GenerationOutcome::Reconnect(ProviderError::ReconcileAckTimeout { generation, revision, target_open_time }));
                            }
                        }
                        DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect"))),
                        DecodedFrame::ProviderError(error) if is_terminal_live_error(&error) => return Err(error),
                        DecodedFrame::ProviderError(error) => deferred_reconnect = Some(error),
                    },
                    ack = request.reconcile_ack_rx.changed() => {
                        let ReconcileAck { generation: ack_generation, revision: ack_revision, through } = ack.map_err(|_| ProviderError::ChannelClosed { context: ErrorContext::operation(ErrorOperation::Reconciliation).with_market(&request.instrument, request.timeframe) })?;
                        if ack_generation == generation && ack_revision == revision && through >= target_open_time {
                            return if let Some(error) = deferred_reconnect.take() {
                                Ok(GenerationOutcome::Reconnect(error))
                            } else {
                                self.connected_loop(request, sender, socket, generation, age_deadline).await
                            };
                        }
                    },
                    () = self.clock.sleep_until(ack_deadline) => {
                        if let Ok(Some(ack)) = request.reconcile_ack_rx.current()
                            && ack.generation == generation
                            && ack.revision == revision
                            && ack.through >= target_open_time
                        {
                            return if let Some(error) = deferred_reconnect.take() {
                                Ok(GenerationOutcome::Reconnect(error))
                            } else {
                                self.connected_loop(request, sender, socket, generation, age_deadline).await
                            };
                        }
                        return Ok(GenerationOutcome::Reconnect(ProviderError::ReconcileAckTimeout { generation, revision, target_open_time }));
                    }
                }
            }
        }
    }

    async fn connected_loop(
        &self,
        request: &mut LiveRequest,
        sender: &EventEmitter,
        socket: &mut RawWebSocket,
        generation: GapGeneration,
        age_deadline: MonoInstant,
    ) -> Result<GenerationOutcome, ProviderError> {
        let mut connected_queued = false;
        let mut pending = BTreeMap::<i64, Candle>::new();
        let mut gate = self.gate_snapshot.clone();
        loop {
            if matches!(gate.current(), Ok(RateGateState::ProcessBlocked(_))) {
                return Err(ProviderError::InvalidBanExpiry);
            }
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                changed = gate.changed() => match changed.map_err(|_| ProviderError::Invariant("rate gate closed"))? {
                    RateGateState::ProcessBlocked(_) => return Err(ProviderError::InvalidBanExpiry),
                    RateGateState::Open | RateGateState::TimedUntil(_) => {}
                },
                changed = request.accepted_watermark_rx.changed() => { changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                ack = request.reconcile_ack_rx.changed() => { ack.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                result = send_market(sender, &request.cancellation, MarketEvent::Status { generation: Some(generation), status: ConnectionStatus::Connected }), if !connected_queued => {
                    result?;
                    connected_queued = true;
                },
                permit = sender.reserve_regular(), if connected_queued && !pending.is_empty() => {
                    let permit = permit?;
                    let key = *pending.first_key_value().expect("pending is nonempty").0;
                    let candle = pending.remove(&key).expect("key came from pending");
                    sender.send_reserved(permit, MarketEvent::Candle { generation, candle })?;
                },
                frame = socket.read() => {
                    if self.clock.now() >= age_deadline {
                        let error = live_protocol_error(request, "24-hour WebSocket connection age reached");
                        return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                    }
                    match frame {
                        Ok(DecodedFrame::Provider(BinanceDecoded::Candle(candle))) => {
                            let is_new_key = !pending.contains_key(&candle.open_time());
                            if is_new_key && pending.len() == self.live.keyed_candle_capacity {
                                let outcome = ProviderError::QueueSaturated;
                                return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(outcome) } else { GenerationOutcome::Reconnect(outcome) });
                            }
                            coalesce_candle(&mut pending, candle);
                        }
                        Ok(DecodedFrame::Ignored) => {}
                        Ok(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => {
                            let error = live_protocol_error(request, "WebSocket peer requested reconnect");
                            return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                        }
                        Ok(DecodedFrame::ProviderError(error)) if is_terminal_live_error(&error) => return Err(error),
                        Err(error) if is_terminal_live_error(&error) => return Err(error),
                        Ok(DecodedFrame::ProviderError(error)) | Err(error) => {
                            return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                        }
                    }
                },
                () = self.clock.sleep_until(age_deadline) => {
                    let error = live_protocol_error(request, "24-hour WebSocket connection age reached");
                    return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                },
            }
        }
    }

    async fn recover_and_backoff(
        &self,
        sender: &EventEmitter,
        request: &mut LiveRequest,
        generation: Option<GapGeneration>,
        error: ProviderError,
        backoff_index: &mut usize,
    ) -> Result<(), ProviderError> {
        let cancellation = request.cancellation.clone();
        let mut gate = self.gate_snapshot.clone();
        let initial_gate = match gate.current() {
            Ok(state) => state,
            Err(_) => {
                let error = ProviderError::Invariant("rate gate closed");
                send_market(
                    sender,
                    &cancellation,
                    MarketEvent::TerminalError(error.clone()),
                )
                .await?;
                return Err(error);
            }
        };
        if matches!(initial_gate, RateGateState::ProcessBlocked(_)) {
            self.send_invalid_ban_and_stop(sender).await;
            return Err(ProviderError::InvalidBanExpiry);
        }
        let gate_deadline = match initial_gate {
            RateGateState::TimedUntil(deadline) => Some(deadline),
            RateGateState::Open | RateGateState::ProcessBlocked(_) => None,
        };
        let seconds = [1_u64, 2, 4, 8, 16, 30]
            .get(*backoff_index)
            .copied()
            .unwrap_or(30);
        *backoff_index = backoff_index.saturating_add(1);
        let backoff = checked_deadline(self.clock.now(), Duration::from_secs(seconds))
            .map_err(|_| ProviderError::Invariant("backoff deadline overflow"))?;
        let mut deadline = gate_deadline.map_or(backoff, |value| value.max(backoff));
        let queue_saturated = matches!(&error, ProviderError::QueueSaturated);
        let control_generation = if queue_saturated { None } else { generation };
        let recoverable = MarketEvent::RecoverableError {
            generation: control_generation,
            error,
            rate_gate_deadline: gate_deadline,
        };
        let backoff_status = MarketEvent::Status {
            generation: control_generation,
            status: ConnectionStatus::Backoff,
        };
        let emergency_barrier = if queue_saturated {
            if let Some(generation) = generation {
                sender.invalidate_generation(generation);
            }
            Some(sender.queue_emergency_pair(recoverable, backoff_status)?)
        } else {
            send_market(sender, &cancellation, recoverable).await?;
            send_market(sender, &cancellation, backoff_status).await?;
            None
        };
        let mut deadline_elapsed = false;
        loop {
            let barrier_elapsed = emergency_barrier
                .as_ref()
                .is_none_or(|barrier| barrier.is_dequeued());
            if deadline_elapsed && barrier_elapsed {
                return Ok(());
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    sender.shutdown().await;
                    return Ok(());
                },
                changed = request.accepted_watermark_rx.changed() => {
                    if changed.is_err() {
                        let error = control_channel_closed(&request.instrument, request.timeframe);
                        send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                        return Err(error);
                    }
                },
                ack = request.reconcile_ack_rx.changed() => {
                    if ack.is_err() {
                        let error = control_channel_closed(&request.instrument, request.timeframe);
                        send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                        return Err(error);
                    }
                },
                () = sender.wait_closed() => return Err(live_channel_closed()),
                changed = gate.changed() => match changed {
                    Err(_) => {
                        let error = ProviderError::Invariant("rate gate closed");
                        send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                        return Err(error);
                    }
                    Ok(RateGateState::ProcessBlocked(_)) => {
                        self.send_invalid_ban_and_stop(sender).await;
                        return Err(ProviderError::InvalidBanExpiry);
                    }
                    Ok(RateGateState::TimedUntil(value)) => {
                        deadline = deadline.max(value);
                        deadline_elapsed = self.clock.now() >= deadline;
                    }
                    Ok(RateGateState::Open) => {}
                },
                () = self.clock.sleep_until(deadline), if !deadline_elapsed => {
                    match gate.current() {
                        Err(_) => {
                            let error = ProviderError::Invariant("rate gate closed");
                            send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                            return Err(error);
                        }
                        Ok(RateGateState::ProcessBlocked(_)) => {
                            self.send_invalid_ban_and_stop(sender).await;
                            return Err(ProviderError::InvalidBanExpiry);
                        }
                        Ok(RateGateState::TimedUntil(value)) if value > deadline => deadline = value,
                        Ok(RateGateState::Open | RateGateState::TimedUntil(_)) => deadline_elapsed = true,
                    }
                },
                () = async { if let Some(barrier) = &emergency_barrier { barrier.wait_dequeued().await } }, if !barrier_elapsed => {}
            }
        }
    }

    async fn send_invalid_ban_and_stop(&self, sender: &EventEmitter) {
        let _ = sender
            .queue_terminal_pair(
                MarketEvent::RecoverableError {
                    generation: None,
                    error: ProviderError::InvalidBanExpiry,
                    rate_gate_deadline: None,
                },
                MarketEvent::Status {
                    generation: None,
                    status: ConnectionStatus::Stopped,
                },
            )
            .await;
    }

    pub fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        if spec.provider().as_str() != "binance" || spec.venue().is_some() {
            return Err(ProviderError::Configuration(
                "instrument is not valid for Binance",
            ));
        }
        canonicalize_instrument(spec)
            .map_err(|_| ProviderError::Configuration("instrument is not valid for Binance"))
    }

    #[must_use]
    pub fn rate_gate(&self) -> RateGateSnapshot {
        self.gate_snapshot.clone()
    }
}

enum GenerationOutcome {
    Cancelled,
    AcknowledgedReconnect(ProviderError),
    Reconnect(ProviderError),
}

async fn send_market(
    sender: &EventEmitter,
    cancellation: &CancellationToken,
    event: MarketEvent,
) -> Result<(), ProviderError> {
    tokio::select! { biased; () = cancellation.cancelled() => Ok(()), result = sender.send_regular(event) => result }
}
fn apply_reconciliation_candle(
    pending: &mut BTreeMap<i64, Candle>,
    candidate: Candle,
    revision: &mut ReplayRevision,
    target_open_time: &mut i64,
) -> Result<(), ProviderError> {
    let open_time = candidate.open_time();
    let _ = coalesce_candle(pending, candidate);
    revision.0 = revision
        .0
        .checked_add(1)
        .ok_or(ProviderError::Invariant("replay revision overflow"))?;
    *target_open_time = (*target_open_time).max(open_time);
    Ok(())
}

fn coalesce_candle(pending: &mut BTreeMap<i64, Candle>, candidate: Candle) -> bool {
    use FinalityAuthority::{
        RestProvisionalClosed, RestProvisionalOpen, WsAuthoritativeClosed, WsAuthoritativeOpen,
    };
    let key = candidate.open_time();
    match pending.get(&key) {
        None => {
            pending.insert(key, candidate);
            true
        }
        Some(current) => {
            let replace = match (current.authority(), candidate.authority()) {
                (_, WsAuthoritativeClosed) => true,
                (WsAuthoritativeClosed, _) => false,
                (WsAuthoritativeOpen, RestProvisionalOpen | RestProvisionalClosed) => false,
                (RestProvisionalOpen | RestProvisionalClosed, WsAuthoritativeOpen) => true,
                (WsAuthoritativeOpen, WsAuthoritativeOpen) => true,
                (RestProvisionalClosed, RestProvisionalOpen) => false,
                (RestProvisionalOpen, RestProvisionalClosed)
                | (RestProvisionalOpen, RestProvisionalOpen)
                | (RestProvisionalClosed, RestProvisionalClosed) => true,
            };
            if replace {
                pending.insert(key, candidate);
            }
            replace
        }
    }
}

fn control_channel_closed(instrument: &Instrument, timeframe: Timeframe) -> ProviderError {
    ProviderError::ChannelClosed {
        context: ErrorContext::operation(ErrorOperation::Reconciliation)
            .with_market(instrument, timeframe),
    }
}

fn live_protocol_error(request: &LiveRequest, detail: &'static str) -> ProviderError {
    ProviderError::Protocol {
        context: ErrorContext::operation(ErrorOperation::WebSocket)
            .with_market(&request.instrument, request.timeframe),
        detail,
    }
}

fn next_gap_cursor(_timeframe: Timeframe, value: i64) -> Result<i64, ProviderError> {
    value
        .checked_add(1)
        .ok_or(ProviderError::Invariant("gap cursor overflow"))
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveErrorDisposition {
    Recoverable,
    Terminal,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveInBandEventDisposition {
    RecoverableInBand,
    TerminalInBand,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveCompletionDisposition {
    Running,
    FinishedErr,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveErrorClassification {
    pub disposition: LiveErrorDisposition,
    pub event: LiveInBandEventDisposition,
    pub completion: LiveCompletionDisposition,
    pub retries: bool,
}

#[cfg(feature = "test-transport")]
#[must_use]
pub fn classify_live_error_for_test(error: &ProviderError) -> LiveErrorClassification {
    if is_terminal_live_error(error) {
        LiveErrorClassification {
            disposition: LiveErrorDisposition::Terminal,
            event: LiveInBandEventDisposition::TerminalInBand,
            completion: LiveCompletionDisposition::FinishedErr,
            retries: false,
        }
    } else {
        LiveErrorClassification {
            disposition: LiveErrorDisposition::Recoverable,
            event: LiveInBandEventDisposition::RecoverableInBand,
            completion: LiveCompletionDisposition::Running,
            retries: true,
        }
    }
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Debug, PartialEq)]
pub enum LiveInputClassification {
    Continue,
    Error {
        error: ProviderError,
        policy: LiveErrorClassification,
    },
}

#[cfg(feature = "test-transport")]
#[must_use]
pub fn classify_live_input_for_test(
    input: Result<DecodedFrame<BinanceDecoded>, ProviderError>,
    instrument: &Instrument,
    timeframe: Timeframe,
) -> LiveInputClassification {
    let error = match input {
        Ok(DecodedFrame::Ignored | DecodedFrame::Provider(BinanceDecoded::Candle(_))) => {
            return LiveInputClassification::Continue;
        }
        Ok(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => ProviderError::Protocol {
            context: ErrorContext::operation(ErrorOperation::WebSocket)
                .with_market(instrument, timeframe),
            detail: "WebSocket peer requested reconnect",
        },
        Ok(DecodedFrame::ProviderError(error)) | Err(error) => error,
    };
    LiveInputClassification::Error {
        policy: classify_live_error_for_test(&error),
        error,
    }
}

fn is_terminal_live_error(error: &ProviderError) -> bool {
    matches!(
        error,
        ProviderError::Configuration(_)
            | ProviderError::WebSocketConfiguration { .. }
            | ProviderError::Invariant(_)
            | ProviderError::ClientStatus { .. }
            | ProviderError::InvalidSymbol { .. }
            | ProviderError::ChannelClosed { .. }
    )
}

impl MarketDataProvider for BinanceProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("binance").expect("static provider id")
    }
    fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        BinanceProvider::canonicalize(self, spec)
    }
    fn history<'a>(
        &'a self,
        instrument: &'a Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>> {
        Box::pin(BinanceProvider::history(
            self,
            instrument,
            timeframe,
            request,
            cancellation,
        ))
    }
    fn open_live<'a>(&'a self, request: LiveRequest) -> ProviderFuture<'a, LiveFeed> {
        Box::pin(async move {
            self.live.validate()?;
            let physical_capacity = self
                .live
                .market_event_capacity
                .checked_add(2)
                .ok_or(ProviderError::Invariant("market event capacity overflow"))?;
            let (sender, receiver) = mpsc::channel(physical_capacity);
            let sender = EventEmitter::new(
                sender,
                self.live.market_event_capacity,
                self.live.control_capacity,
            );
            let invalidated_through = Arc::clone(&sender.invalidated_through);
            let emergency_barrier = Arc::clone(&sender.emergency_barrier);
            let cancellation = request.cancellation.clone();
            let stream_cancellation = cancellation.clone();
            let producer = self.clone();
            let events = stream::unfold(receiver, move |mut receiver| {
                let invalidated_through = Arc::clone(&invalidated_through);
                let emergency_barrier = Arc::clone(&emergency_barrier);
                let cancellation = stream_cancellation.clone();
                async move {
                    loop {
                        let cancelled = cancellation.is_cancelled();
                        let envelope = if cancelled {
                            receiver.recv().await?
                        } else {
                            tokio::select! {
                                biased;
                                () = cancellation.cancelled() => continue,
                                envelope = receiver.recv() => envelope?,
                            }
                        };
                        if cancellation.is_cancelled() && !envelope.is_stopped() {
                            drop(envelope);
                            continue;
                        }
                        let invalidated = envelope.purge_on_invalidate
                            && envelope.generation.is_some_and(|generation| {
                                generation.0 <= invalidated_through.load(Ordering::Acquire)
                            });
                        let suppressed = envelope
                            .emergency_slot
                            .is_some_and(|slot| emergency_barrier.is_suppressed(slot));
                        if invalidated || suppressed {
                            drop(envelope);
                            continue;
                        }
                        return Some((envelope.into_item(), receiver));
                    }
                }
            });
            Ok(LiveFeed::spawn(
                Box::pin(events),
                cancellation,
                Arc::clone(&self.clock),
                async move { producer.supervise_live(request, sender).await },
            ))
        })
    }
    fn rate_gate(&self) -> RateGateSnapshot {
        BinanceProvider::rate_gate(self)
    }
}

async fn read_capped(
    mut response: reqwest::Response,
    limit: usize,
    cancellation: &CancellationToken,
    context: &ErrorContext,
) -> Result<Vec<u8>, ProviderError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(payload(
            context,
            PayloadError::OverBudget { limit_bytes: limit },
        ));
    }
    let mut body =
        Vec::with_capacity(response.content_length().unwrap_or(0).min(limit as u64) as usize);
    loop {
        let chunk = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(cancelled(context.clone())),
            chunk = response.chunk() => chunk.map_err(|error| {
                if error.is_timeout() {
                    ProviderError::Timeout {
                        context: context.clone(),
                        kind: TimeoutKind::Request,
                    }
                } else {
                    ProviderError::Transport {
                        context: context.clone(),
                        cause: SanitizedCause::Connection,
                    }
                }
            })?,
        };
        let Some(chunk) = chunk else { break };
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|size| size > limit)
        {
            return Err(payload(
                context,
                PayloadError::OverBudget { limit_bytes: limit },
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

struct RawKline(
    Value,
    Value,
    Value,
    Value,
    Value,
    Value,
    Value,
    Value,
    Value,
    Value,
    Value,
    Value,
);
struct BoundedRowsVisitor {
    limit: usize,
}

impl<'de> Visitor<'de> for BoundedRowsVisitor {
    type Value = Vec<Value>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded array of Binance kline rows")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut rows = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(self.limit));
        while rows.len() < self.limit {
            let Some(row) = sequence.next_element::<Value>()? else {
                return Ok(rows);
            };
            rows.push(row);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::custom("kline row limit exceeded"));
        }
        Ok(rows)
    }
}
fn decode_klines(
    bytes: &[u8],
    requested_limit: u16,
    context: ErrorContext,
) -> Result<Vec<Candle>, ProviderError> {
    if bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        != Some(b'[')
    {
        return if serde_json::from_slice::<Value>(bytes).is_ok() {
            Err(payload(&context, PayloadError::ExpectedArray))
        } else {
            Err(payload(&context, PayloadError::MalformedJson))
        };
    }
    let limit = usize::from(requested_limit).min(1000);
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let rows =
        serde::de::Deserializer::deserialize_seq(&mut deserializer, BoundedRowsVisitor { limit })
            .map_err(|_| payload(&context, PayloadError::MalformedJson))?;
    deserializer
        .end()
        .map_err(|_| payload(&context, PayloadError::MalformedJson))?;
    let mut candles = Vec::with_capacity(rows.len());
    for row in rows {
        let fields = match row {
            Value::Array(fields) => fields,
            _ => {
                return Err(payload(
                    &context,
                    PayloadError::WrongArity {
                        expected: 12,
                        actual: 0,
                    },
                ));
            }
        };
        let actual = fields.len();
        let [f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11]: [Value; 12] =
            fields.try_into().map_err(|_| {
                payload(
                    &context,
                    PayloadError::WrongArity {
                        expected: 12,
                        actual,
                    },
                )
            })?;
        let raw = RawKline(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11);
        let open_time = integer_field(&raw.0, "open_time", &context)?;
        let open = decimal_field(&raw.1, "open", &context)?;
        let high = decimal_field(&raw.2, "high", &context)?;
        let low = decimal_field(&raw.3, "low", &context)?;
        let close = decimal_field(&raw.4, "close", &context)?;
        let volume = decimal_field(&raw.5, "base_volume", &context)?;
        let close_time = integer_field(&raw.6, "close_time", &context)?;
        validate_ignored_fields(&raw, &context)?;
        candles.push(
            Candle::from_rest(open_time, close_time, open, high, low, close, volume).map_err(
                |source| ProviderError::Domain {
                    context: context.clone(),
                    source,
                },
            )?,
        );
    }
    Ok(candles)
}

fn validate_ignored_fields(raw: &RawKline, context: &ErrorContext) -> Result<(), ProviderError> {
    nonnegative_decimal_field(&raw.7, "quote_volume", context)?;
    nonnegative_integer_field(&raw.8, "trade_count", context)?;
    nonnegative_decimal_field(&raw.9, "taker_buy_base_volume", context)?;
    nonnegative_decimal_field(&raw.10, "taker_buy_quote_volume", context)?;
    if !raw.11.is_string() {
        return Err(payload(
            context,
            PayloadError::InvalidField { field: "ignore" },
        ));
    }
    Ok(())
}

fn integer_field(
    value: &Value,
    field: &'static str,
    context: &ErrorContext,
) -> Result<i64, ProviderError> {
    value
        .as_i64()
        .ok_or_else(|| payload(context, PayloadError::InvalidField { field }))
}

fn decimal_field(
    value: &Value,
    field: &'static str,
    context: &ErrorContext,
) -> Result<f64, ProviderError> {
    value
        .as_str()
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|number| number.is_finite())
        .ok_or_else(|| payload(context, PayloadError::InvalidField { field }))
}

fn nonnegative_decimal_field(
    value: &Value,
    field: &'static str,
    context: &ErrorContext,
) -> Result<f64, ProviderError> {
    decimal_field(value, field, context).and_then(|number| {
        (number >= 0.0)
            .then_some(number)
            .ok_or_else(|| payload(context, PayloadError::InvalidField { field }))
    })
}

fn nonnegative_integer_field(
    value: &Value,
    field: &'static str,
    context: &ErrorContext,
) -> Result<i64, ProviderError> {
    integer_field(value, field, context).and_then(|number| {
        (number >= 0)
            .then_some(number)
            .ok_or_else(|| payload(context, PayloadError::InvalidField { field }))
    })
}

fn map_http_error(status: StatusCode, bytes: &[u8], context: ErrorContext) -> ProviderError {
    let provider_error = serde_json::from_slice::<Value>(bytes).ok();
    let code = provider_error
        .as_ref()
        .and_then(|value| value.get("code"))
        .and_then(Value::as_i64);
    let message = provider_error
        .as_ref()
        .and_then(|value| value.get("msg"))
        .and_then(Value::as_str)
        .map(SanitizedMessage::new);
    if status == StatusCode::BAD_REQUEST && code == Some(-1121) {
        return ProviderError::InvalidSymbol {
            context,
            code: -1121,
            message: message.unwrap_or(SanitizedMessage::InvalidSymbol),
        };
    }
    if status.is_client_error() {
        ProviderError::ClientStatus {
            context,
            status: status.as_u16(),
            code,
            message,
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

fn cancelled(context: ErrorContext) -> ProviderError {
    ProviderError::Transport {
        context,
        cause: SanitizedCause::Cancelled,
    }
}

fn is_cancelled(error: &ProviderError) -> bool {
    matches!(
        error,
        ProviderError::Transport {
            cause: SanitizedCause::Cancelled,
            ..
        }
    )
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
