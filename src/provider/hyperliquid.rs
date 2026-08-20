//! Hyperliquid Spot and Perpetual REST history and raw WebSocket transport.

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
use time::{Date, Month, OffsetDateTime};
use tokio::sync::{Notify, mpsc};
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
        Candle, ConnectionStatus, FinalityAuthority, GapGeneration, HistoryRequest,
        HistoryRequestKind, Instrument, InstrumentSpec, Market, MarketEvent, MonoInstant,
        ProviderId, RateGateState, ReplayRevision, Timeframe, is_spot_index_token,
    },
    provider::{
        LiveFeed, LiveRequest, MarketDataProvider, ProviderFuture, RateGateSender,
        RateGateSnapshot, ReconcileAck, ReconcileExpectation, ReconcileExpectationError,
        rate_gate_channel,
        runtime::{
            emitter::{EventEmitter, live_channel_closed},
            websocket::{
                DecodedFrame, ReadinessInput, WsCodec, WsConfig, connect_websocket_url,
                contextualize_websocket_configuration, validate_websocket_base,
            },
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

pub const KEYED_CANDLE_CAPACITY: usize = 1024;
pub const CONTROL_CAPACITY: usize = 64;
pub const EMERGENCY_CONTROL_CAPACITY: usize = 2;
pub const MARKET_EVENT_CHANNEL_CAPACITY: usize = 256;
pub const FIRST_KLINE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
pub const RECONCILE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
pub const SUBSCRIBE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
pub const APPLICATION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(50);
pub const MAX_CONNECTION_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_SUPERVISOR_CAPACITY: usize = 65_536;
const GAP_PAGE_LIMIT: u16 = 1000;
const MAX_FUTURE_CANDLE_SKEW: Duration = Duration::from_secs(5 * 60);
const MAX_GAP_RECONCILIATION_CANDLES: usize = 64_000;
const MAX_GAP_RECONCILIATION_PAGES: usize = 64;

#[derive(Clone, Debug)]
pub struct LiveSupervisorConfig {
    pub keyed_candle_capacity: usize,
    pub control_capacity: usize,
    pub market_event_capacity: usize,
    pub first_kline_timeout: Duration,
    pub subscribe_ack_timeout: Duration,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub application_heartbeat_interval_for_test: Duration,
    pub reconcile_ack_timeout: Duration,
    pub max_connection_age: Duration,
    pub ws_config: WsConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub stalled_write_probe_frames: usize,
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
}

impl Default for LiveSupervisorConfig {
    fn default() -> Self {
        Self {
            keyed_candle_capacity: KEYED_CANDLE_CAPACITY,
            control_capacity: CONTROL_CAPACITY,
            market_event_capacity: MARKET_EVENT_CHANNEL_CAPACITY,
            first_kline_timeout: FIRST_KLINE_HANDSHAKE_TIMEOUT,
            subscribe_ack_timeout: SUBSCRIBE_ACK_TIMEOUT,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            application_heartbeat_interval_for_test: APPLICATION_HEARTBEAT_INTERVAL,
            reconcile_ack_timeout: RECONCILE_ACK_TIMEOUT,
            max_connection_age: MAX_CONNECTION_AGE,
            ws_config: WsConfig::default(),
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            stalled_write_probe_frames: 0,
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
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if !(1..=MAX_GAP_RECONCILIATION_CANDLES)
            .contains(&self.max_gap_reconciliation_candles_for_test)
        {
            return Err(ProviderError::Configuration(
                "live reconciliation candle bound is outside 1..=64000",
            ));
        }
        for timeout in [
            self.first_kline_timeout,
            self.reconcile_ack_timeout,
            self.subscribe_ack_timeout,
        ] {
            if !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&timeout) {
                return Err(ProviderError::Configuration(
                    "live supervisor timeout is outside 1ms..=60s",
                ));
            }
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if !(Duration::from_millis(1)..=Duration::from_secs(60))
            .contains(&self.application_heartbeat_interval_for_test)
        {
            return Err(ProviderError::Configuration(
                "application heartbeat interval is outside 1ms..=60s",
            ));
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

#[derive(Clone, Debug, Default)]
pub struct HyperliquidWsCodec {
    retained_candle: Option<Candle>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HyperliquidDecoded {
    Candle(Candle),
    SubscribeAccepted,
    ApplicationPong,
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
use crate::provider::runtime::websocket::{
    CloseFlushTestHook, ReadinessDecodedAckTestHook, ReadinessDrainBudgetTestHook,
    SubscribeFlushTestHook,
};

impl HyperliquidWsCodec {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            retained_candle: None,
        }
    }
}

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

pub fn decode_ws_frame(
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

pub type RawWebSocket = crate::provider::runtime::websocket::RawWebSocket<HyperliquidWsCodec>;

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
pub async fn connect_websocket(
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
            live: LiveSupervisorConfig::default(),
        }
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct HyperliquidLiveTestConfig {
    pub rest: HyperliquidTestConfig,
    pub ws_base_url: String,
    pub live: LiveSupervisorConfig,
}

impl HyperliquidProvider {
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
            base_url,
            clock,
            config.request_timeout,
            config.body_limit,
            config.rate_limit_fallback,
            LiveSupervisorConfig::default(),
            None,
            config.now_ms,
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
            base_url,
            clock,
            config.rest.request_timeout,
            config.rest.body_limit,
            config.rest.rate_limit_fallback,
            config.live,
            Some(config.ws_base_url),
            config.rest.now_ms,
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
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        now_ms: Option<i64>,
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
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            now_ms,
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

            let response = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(cancelled(context.clone())),
                result = self.client.post(url).json(&body).send() => result.map_err(|error| {
                    if error.is_timeout() {
                        ProviderError::Timeout { context: context.clone(), kind: TimeoutKind::Request }
                    } else {
                        ProviderError::Transport { context: context.clone(), cause: SanitizedCause::Connection }
                    }
                })?,
            };

            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
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
            let (window_start, window_end) = response_validation_window(
                timeframe,
                request.kind(),
                request.limit(),
                start_time,
                end_time,
            )?;
            decode_candles(
                &bytes,
                request.kind(),
                request.limit(),
                instrument,
                timeframe,
                window_start,
                window_end,
                context,
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
        let mut snapshot = self.gate_snapshot.clone();
        loop {
            match snapshot
                .current()
                .map_err(|_| ProviderError::Invariant("rate gate closed"))?
            {
                RateGateState::Open => return Ok(()),
                RateGateState::ProcessBlocked(_) => {
                    return Err(ProviderError::Invariant(
                        "Hyperliquid rate gate cannot be process-blocked",
                    ));
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
        self.gate_sender
            .publish(RateGateState::TimedUntil(deadline))
            .map_err(|_| ProviderError::Invariant("rate gate closed"))?;
        Err(ProviderError::RateLimited {
            context,
            status: response.status().as_u16(),
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
                return Err(ProviderError::Invariant(
                    "Hyperliquid rate gate cannot be process-blocked",
                ));
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
                                return Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked"));
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
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            socket.subscribe_flush_test_hook = self.live.subscribe_flush_test_hook.clone();
        }
        let subscribe = subscribe_message(&request.instrument, request.timeframe);
        tokio::select! {
            biased;
            () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
            result = socket.send(Message::Text(subscribe.into())) => result?,
        }
        let ack_deadline = checked_deadline(self.clock.now(), self.live.subscribe_ack_timeout)
            .map_err(|_| ProviderError::Invariant("subscribe-ack deadline overflow"))?;
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            socket.readiness_inactivity_test_hook =
                self.live.readiness_inactivity_test_hook.clone();
            socket.readiness_decoded_ack_test_hook =
                self.live.readiness_decoded_ack_test_hook.clone();
            socket.readiness_drain_budget_test_hook =
                self.live.readiness_drain_budget_test_hook.clone();
            socket.close_flush_test_hook = self.live.close_flush_test_hook.clone();
            socket.force_stalled_write_after_readiness_frame =
                self.live.force_stalled_write_after_readiness_frame;
        }
        loop {
            let inactivity_deadline = socket.inactivity_deadline();
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            let readiness_inactivity_test_hook = socket.readiness_inactivity_test_hook.clone();
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                input = socket.read_readiness() => match input {
                    ReadinessInput::Error(error) => return Ok(GenerationOutcome::Reconnect(error)),
                    ReadinessInput::Frame(DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted)) => break,
                    ReadinessInput::Frame(DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong)) => {
                        if self.clock.now() >= ack_deadline {
                            return Ok(GenerationOutcome::Reconnect(Self::subscribe_ack_timeout(request)));
                        }
                    }
                    ReadinessInput::Frame(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => {
                        let reconnect = live_protocol_error(request, "WebSocket peer requested reconnect");
                        if let Err(error) = socket.finalize_peer_close().await {
                            return Ok(GenerationOutcome::Reconnect(error));
                        }
                        return Ok(GenerationOutcome::Reconnect(reconnect));
                    }
                    ReadinessInput::Frame(DecodedFrame::ProviderError(error)) => return Ok(GenerationOutcome::Reconnect(error)),
                    ReadinessInput::Frame(DecodedFrame::Provider(HyperliquidDecoded::Candle(_))) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "Hyperliquid candle arrived before subscribe acknowledgement"))),
                },
                () = self.clock.sleep_until(ack_deadline) => {
                    return Ok(GenerationOutcome::Reconnect(Self::subscribe_ack_timeout(request)));
                },
                () = async move {
                    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
                    if let Some(hook) = readiness_inactivity_test_hook {
                        hook.notified().await;
                        return;
                    }
                    tokio::time::sleep_until(inactivity_deadline).await;
                } => {
                    return Ok(GenerationOutcome::Reconnect(ProviderError::Timeout {
                        context: ErrorContext::operation(ErrorOperation::WebSocket)
                            .with_market(&request.instrument, request.timeframe),
                        kind: TimeoutKind::WebSocketInactivity,
                    }));
                }
            }
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        {
            socket.heartbeat_test_hook = self.live.heartbeat_test_hook.clone();
        }
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        socket.start_application_heartbeat(APPLICATION_HEARTBEAT_INTERVAL);
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        socket.start_application_heartbeat(self.live.application_heartbeat_interval_for_test);
        send_market(
            sender,
            &request.cancellation,
            MarketEvent::Status {
                generation: Some(generation),
                status: ConnectionStatus::GapSync,
            },
        )
        .await?;
        let mut gate = self.gate_snapshot.clone();
        let first_deadline = checked_deadline(self.clock.now(), self.live.first_kline_timeout)
            .map_err(|_| ProviderError::Invariant("first-kline deadline overflow"))?;
        let first = loop {
            tokio::select! {
                    biased;
                    () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                    changed = request.accepted_watermark_rx.changed() => { changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                    ack = request.reconcile_ack_rx.changed() => { ack.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                    changed = gate.changed() => if matches!(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?, RateGateState::ProcessBlocked(_)) { return Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked")); },
                    frame = socket.read() => match frame? {
                        DecodedFrame::Provider(HyperliquidDecoded::Candle(candle)) => break candle,
                        DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong) => {
                            let now = self.clock.now();
                            if now >= first_deadline {
                                return Ok(GenerationOutcome::Reconnect(ProviderError::Timeout { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&request.instrument, request.timeframe), kind: TimeoutKind::FirstKline }));
                            }
                            if now >= age_deadline {
                                return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached")));
                            }
                        }
                        DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "duplicate Hyperliquid subscribe acknowledgement"))),
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
        let mut reconciliation_pages = 0_usize;
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        let max_reconciliation_successors = self.live.max_gap_reconciliation_candles_for_test;
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        let max_reconciliation_successors = MAX_GAP_RECONCILIATION_CANDLES;

        loop {
            let mut cursor = match rest_synced_through {
                Some(last) => next_gap_cursor(request.timeframe, last)?,
                None => start,
            };
            while cursor <= target_open_time {
                if !gap_target_within_generation_span(
                    request.timeframe,
                    start,
                    target_open_time,
                    max_reconciliation_successors,
                ) {
                    return Ok(GenerationOutcome::Reconnect(live_protocol_error(
                        request,
                        "Hyperliquid gap reconciliation target exceeds the per-generation span limit",
                    )));
                }
                if let Err(error) = advance_reconciliation_page(
                    &mut reconciliation_pages,
                    ErrorContext::operation(ErrorOperation::Reconciliation)
                        .with_market(&request.instrument, request.timeframe),
                ) {
                    return Ok(GenerationOutcome::Reconnect(error));
                }
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
                        Socket(Result<DecodedFrame<HyperliquidDecoded>, ProviderError>),
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
                                    if let Err(error) = advance_reconciliation_target(
                                        &mut target_open_time,
                                        watermark,
                                        start,
                                        request.timeframe,
                                        max_reconciliation_successors,
                                    ) {
                                        return Ok(GenerationOutcome::Reconnect(error));
                                    }
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
                                    return Err(ProviderError::Invariant(
                                        "Hyperliquid rate gate cannot be process-blocked",
                                    ));
                                }
                            }
                            ReconcileWake::Socket(frame) => match frame {
                                Ok(DecodedFrame::Provider(HyperliquidDecoded::Candle(candle))) => {
                                    if let Err(error) = apply_reconciliation_candle(
                                        &mut buffered,
                                        candle,
                                        &mut revision,
                                        &mut target_open_time,
                                        start,
                                        request.timeframe,
                                        max_reconciliation_successors,
                                    ) {
                                        return Ok(GenerationOutcome::Reconnect(error));
                                    }
                                }
                                Ok(
                                    DecodedFrame::Ignored
                                    | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong),
                                ) => {}
                                Ok(DecodedFrame::Provider(
                                    HyperliquidDecoded::SubscribeAccepted,
                                )) => {
                                    return Ok(GenerationOutcome::Reconnect(live_protocol_error(
                                        request,
                                        "duplicate Hyperliquid subscribe acknowledgement",
                                    )));
                                }
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
                                        .map_err(|_| control_channel_closed(&request.instrument, request.timeframe))
                                        .map(|watermark| {
                                            let outcome = watermark.and_then(|watermark| {
                                                advance_reconciliation_target(
                                                    &mut target_open_time,
                                                    watermark,
                                                    start,
                                                    request.timeframe,
                                                    max_reconciliation_successors,
                                                )
                                                .err()
                                                .map(GenerationOutcome::Reconnect)
                                            });
                                            (outcome, true)
                                        }),
                                    ack = request.reconcile_ack_rx.changed() => ack
                                        .map(|_| (None, false))
                                        .map_err(|_| control_channel_closed(&request.instrument, request.timeframe)),
                                    () = self.clock.sleep_until(age_deadline) => Ok((Some(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached"))), false)),
                                    changed = gate.changed() => match changed {
                                        Ok(RateGateState::ProcessBlocked(_)) => Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked")),
                                        Ok(_) => Ok((None, false)),
                                        Err(_) => Err(ProviderError::Invariant("rate gate closed")),
                                    },
                                    frame = socket.read() => match frame {
                                        Ok(DecodedFrame::Provider(HyperliquidDecoded::Candle(candle))) => match apply_reconciliation_candle(
                                            &mut buffered,
                                            candle,
                                            &mut revision,
                                            &mut target_open_time,
                                            start,
                                            request.timeframe,
                                            max_reconciliation_successors,
                                        ) {
                                            Ok(()) => Ok((None, false)),
                                            Err(error) => Ok((Some(GenerationOutcome::Reconnect(error)), false)),
                                        },
                                        Ok(DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong)) => Ok((None, false)),
                                        Ok(DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted)) => Ok((Some(GenerationOutcome::Reconnect(live_protocol_error(request, "duplicate Hyperliquid subscribe acknowledgement"))), false)),
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
                                            Ok(DecodedFrame::Provider(HyperliquidDecoded::Candle(candle))) => match apply_reconciliation_candle(
                                                &mut buffered,
                                                candle,
                                                &mut revision,
                                                &mut target_open_time,
                                                start,
                                                request.timeframe,
                                                max_reconciliation_successors,
                                            ) {
                                                Ok(()) => Ok(None),
                                                Err(error) => Ok(Some(GenerationOutcome::Reconnect(error))),
                                            },
                                            Ok(DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong)) => Ok(None),
                                            Ok(DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted)) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, "duplicate Hyperliquid subscribe acknowledgement")))),
                                            Ok(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect")))),
                                            Ok(DecodedFrame::ProviderError(error)) | Err(error) if is_terminal_live_error(&error) => Err(error),
                                            Ok(DecodedFrame::ProviderError(error)) | Err(error) => Ok(Some(GenerationOutcome::Reconnect(error))),
                                        },
                                        () = self.clock.sleep_until(age_deadline) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, "24-hour WebSocket connection age reached")))),
                                        changed = gate.changed() => match changed {
                                            Ok(RateGateState::ProcessBlocked(_)) => Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked")),
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
                    changed = gate.changed() => if matches!(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?, RateGateState::ProcessBlocked(_)) { return Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked")); },
                    frame = socket.read(), if deferred_reconnect.is_none() => match frame? {
                        DecodedFrame::Provider(HyperliquidDecoded::Candle(candle)) => {
                            if let Err(error) = apply_reconciliation_candle(
                                &mut buffered,
                                candle,
                                &mut revision,
                                &mut target_open_time,
                                start,
                                request.timeframe,
                                max_reconciliation_successors,
                            ) {
                                return Ok(GenerationOutcome::Reconnect(error));
                            }
                            break;
                        }
                        DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong) => {
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
                        DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "duplicate Hyperliquid subscribe acknowledgement"))),
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
                return Err(ProviderError::Invariant(
                    "Hyperliquid rate gate cannot be process-blocked",
                ));
            }
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                changed = gate.changed() => match changed.map_err(|_| ProviderError::Invariant("rate gate closed"))? {
                    RateGateState::ProcessBlocked(_) => return Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked")),
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
                        Ok(DecodedFrame::Provider(HyperliquidDecoded::Candle(candle))) => {
                            let is_new_key = !pending.contains_key(&candle.open_time());
                            if is_new_key && pending.len() == self.live.keyed_candle_capacity {
                                let outcome = ProviderError::QueueSaturated;
                                return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(outcome) } else { GenerationOutcome::Reconnect(outcome) });
                            }
                            coalesce_candle(&mut pending, candle);
                        }
                        Ok(DecodedFrame::Ignored | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong)) => {}
                        Ok(DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted)) => {
                            let error = live_protocol_error(request, "duplicate Hyperliquid subscribe acknowledgement");
                            return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                        }
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
            return Err(ProviderError::Invariant(
                "Hyperliquid rate gate cannot be process-blocked",
            ));
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
                        return Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked"));
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
                            return Err(ProviderError::Invariant("Hyperliquid rate gate cannot be process-blocked"));
                        }
                        Ok(RateGateState::TimedUntil(value)) if value > deadline => deadline = value,
                        Ok(RateGateState::Open | RateGateState::TimedUntil(_)) => deadline_elapsed = true,
                    }
                },
                () = async { if let Some(barrier) = &emergency_barrier { barrier.wait_dequeued().await } }, if !barrier_elapsed => {}
            }
        }
    }

    fn subscribe_ack_timeout(request: &LiveRequest) -> ProviderError {
        ProviderError::Timeout {
            context: ErrorContext::operation(ErrorOperation::WebSocket)
                .with_market(&request.instrument, request.timeframe),
            kind: TimeoutKind::SubscribeAck,
        }
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
fn advance_reconciliation_target(
    target_open_time: &mut i64,
    candidate: i64,
    generation_start: i64,
    timeframe: Timeframe,
    maximum_successors: usize,
) -> Result<(), ProviderError> {
    let candidate_target = (*target_open_time).max(candidate);
    if !gap_target_within_generation_span(
        timeframe,
        generation_start,
        candidate_target,
        maximum_successors,
    ) {
        return Err(ProviderError::Protocol {
            context: ErrorContext::operation(ErrorOperation::Reconciliation),
            detail: "Hyperliquid gap reconciliation target exceeds the per-generation span limit",
        });
    }
    *target_open_time = candidate_target;
    Ok(())
}

fn apply_reconciliation_candle(
    pending: &mut BTreeMap<i64, Candle>,
    candidate: Candle,
    revision: &mut ReplayRevision,
    target_open_time: &mut i64,
    generation_start: i64,
    timeframe: Timeframe,
    maximum_successors: usize,
) -> Result<(), ProviderError> {
    let open_time = candidate.open_time();
    let distinct_key_limit = maximum_successors
        .checked_add(1)
        .ok_or(ProviderError::Invariant(
            "reconciliation buffer bound overflow",
        ))?;
    ensure_reconciliation_buffer_capacity(pending, open_time, distinct_key_limit)?;
    advance_reconciliation_target(
        target_open_time,
        open_time,
        generation_start,
        timeframe,
        maximum_successors,
    )?;
    let _ = coalesce_candle(pending, candidate);
    revision.0 = revision
        .0
        .checked_add(1)
        .ok_or(ProviderError::Invariant("replay revision overflow"))?;
    Ok(())
}

fn ensure_reconciliation_buffer_capacity(
    pending: &BTreeMap<i64, Candle>,
    open_time: i64,
    distinct_key_limit: usize,
) -> Result<(), ProviderError> {
    if !reconciliation_distinct_key_allowed(
        pending.len(),
        pending.contains_key(&open_time),
        distinct_key_limit,
    ) {
        return Err(ProviderError::Protocol {
            context: ErrorContext::operation(ErrorOperation::Reconciliation),
            detail: "Hyperliquid gap reconciliation exceeded the distinct buffered-candle limit",
        });
    }
    Ok(())
}

fn reconciliation_distinct_key_allowed(
    existing_len: usize,
    key_exists: bool,
    distinct_key_limit: usize,
) -> bool {
    key_exists || existing_len < distinct_key_limit
}

fn advance_reconciliation_page(
    pages: &mut usize,
    context: ErrorContext,
) -> Result<(), ProviderError> {
    *pages = pages.checked_add(1).ok_or(ProviderError::Invariant(
        "reconciliation page count overflow",
    ))?;
    if *pages > MAX_GAP_RECONCILIATION_PAGES {
        return Err(ProviderError::Protocol {
            context,
            detail: "Hyperliquid gap reconciliation exceeded the per-generation page limit",
        });
    }
    Ok(())
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn reconciliation_page_guard_for_test(pages: usize) -> Result<(), ProviderError> {
    let mut observed = 0;
    for _ in 0..pages {
        advance_reconciliation_page(
            &mut observed,
            ErrorContext::operation(ErrorOperation::Reconciliation),
        )?;
    }
    Ok(())
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[must_use]
pub fn reconciliation_distinct_key_allowed_for_test(existing_len: usize, key_exists: bool) -> bool {
    reconciliation_distinct_key_allowed(
        existing_len,
        key_exists,
        MAX_GAP_RECONCILIATION_CANDLES + 1,
    )
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
    input: Result<DecodedFrame<HyperliquidDecoded>, ProviderError>,
    instrument: &Instrument,
    timeframe: Timeframe,
) -> LiveInputClassification {
    let error = match input {
        Ok(
            DecodedFrame::Provider(HyperliquidDecoded::Candle(_))
            | DecodedFrame::Ignored
            | DecodedFrame::Provider(HyperliquidDecoded::ApplicationPong),
        ) => {
            return LiveInputClassification::Continue;
        }
        Ok(DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested) => ProviderError::Protocol {
            context: ErrorContext::operation(ErrorOperation::WebSocket)
                .with_market(instrument, timeframe),
            detail: "WebSocket peer requested reconnect",
        },
        Ok(DecodedFrame::Provider(HyperliquidDecoded::SubscribeAccepted)) => {
            ProviderError::Protocol {
                context: ErrorContext::operation(ErrorOperation::WebSocket)
                    .with_market(instrument, timeframe),
                detail: "duplicate Hyperliquid subscribe acknowledgement",
            }
        }
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

impl MarketDataProvider for HyperliquidProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("hyperliquid").expect("static provider id")
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
        Box::pin(async move {
            reject_unsupported_timeframe(request.timeframe)?;
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
        HyperliquidProvider::rate_gate(self)
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

const OVERSIZED_CANDLE_ARRAY: &str = "Hyperliquid candle row limit exceeded";

struct BoundedCandleVisitor<'a> {
    kind: HistoryRequestKind,
    requested_limit: usize,
    instrument: &'a Instrument,
    timeframe: Timeframe,
    window_start: i64,
    window_end: i64,
    context: &'a ErrorContext,
}

impl<'de> Visitor<'de> for BoundedCandleVisitor<'_> {
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
            candle_market_matches(&raw, self.instrument, self.timeframe, self.context, "REST")
                .map_err(serde::de::Error::custom)?;
            validate_rest_candle_time_window(
                &raw,
                self.timeframe,
                self.window_start,
                self.window_end,
                previous_open,
                self.context,
            )
            .map_err(serde::de::Error::custom)?;
            previous_open = Some(raw.open_time);
            let (open, high, low, close, volume) =
                candle_ohlcv(&raw, self.context).map_err(serde::de::Error::custom)?;
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
            match self.kind {
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
    kind: HistoryRequestKind,
    requested_limit: u16,
    instrument: &Instrument,
    timeframe: Timeframe,
    window_start: i64,
    window_end: i64,
    context: ErrorContext,
) -> Result<Vec<Candle>, ProviderError> {
    if bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        != Some(b'[')
    {
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|_| payload(&context, PayloadError::MalformedJson))?;
        if let Some(error) = info_error_text(&value) {
            return Err(invalid_hyperliquid_symbol(context, error));
        }
        return Err(payload(&context, PayloadError::ExpectedArray));
    }
    let requested_limit = usize::from(requested_limit);
    if !(1..=1000).contains(&requested_limit) {
        return Err(payload(&context, PayloadError::MalformedProtocol));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let candles = serde::de::Deserializer::deserialize_seq(
        &mut deserializer,
        BoundedCandleVisitor {
            kind,
            requested_limit,
            instrument,
            timeframe,
            window_start,
            window_end,
            context: &context,
        },
    )
    .map_err(|error| {
        if error.to_string().contains(OVERSIZED_CANDLE_ARRAY) {
            payload(&context, PayloadError::MalformedProtocol)
        } else {
            payload(&context, PayloadError::MalformedProtocol)
        }
    })?;
    deserializer
        .end()
        .map_err(|_| payload(&context, PayloadError::MalformedJson))?;
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

fn gap_target_within_generation_span(
    timeframe: Timeframe,
    start: i64,
    target: i64,
    maximum_candles: usize,
) -> bool {
    if target < start {
        return true;
    }
    if timeframe == Timeframe::Month1 {
        let Some(start_month) = calendar_month_index(start) else {
            return false;
        };
        let Some(target_month) = calendar_month_index(target) else {
            return false;
        };
        return target_month
            .checked_sub(start_month)
            .and_then(|distance| usize::try_from(distance).ok())
            .is_some_and(|distance| distance <= maximum_candles);
    }
    let Some(interval) = fixed_timeframe_milliseconds(timeframe) else {
        return false;
    };
    if start.rem_euclid(interval) != 0 || target.rem_euclid(interval) != 0 {
        return false;
    }
    target
        .checked_sub(start)
        .and_then(|distance| {
            i64::try_from(maximum_candles)
                .ok()
                .and_then(|maximum| interval.checked_mul(maximum))
                .map(|maximum| distance <= maximum)
        })
        .unwrap_or(false)
}

fn fixed_timeframe_milliseconds(timeframe: Timeframe) -> Option<i64> {
    match timeframe {
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
    }
}

fn calendar_month_index(open_time: i64) -> Option<i64> {
    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(open_time) * 1_000_000).ok()?;
    if timestamp.time() != time::Time::MIDNIGHT || timestamp.day() != 1 {
        return None;
    }
    let month = i64::from(u8::from(timestamp.month()));
    i64::from(timestamp.year())
        .checked_mul(12)?
        .checked_add(month - 1)
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
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[must_use]
pub fn gap_target_within_generation_span_for_test(
    timeframe: Timeframe,
    start: i64,
    target: i64,
) -> bool {
    gap_target_within_generation_span(timeframe, start, target, MAX_GAP_RECONCILIATION_CANDLES)
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
