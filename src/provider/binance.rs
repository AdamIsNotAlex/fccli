//! Binance Spot and USD-M Perpetual REST history and raw WebSocket transport.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use reqwest::{StatusCode, Url, header::RETRY_AFTER};
use serde::{
    Deserialize,
    de::{IgnoredAny, SeqAccess, Visitor},
};
use serde_json::Value;
use time::{Date, Month, OffsetDateTime};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    cli::canonicalize_instrument,
    clock::{Clock, checked_deadline},
    error::{ErrorContext, ErrorOperation, PayloadError, ProviderError, SanitizedMessage},
    model::{
        Candle, HistoryRequest, Instrument, InstrumentSpec, Market, ProcessBlocker, ProviderId,
        Timeframe,
    },
    provider::{
        LiveFeed, LiveRequest, MarketDataProvider, ProviderCapabilities, ProviderFuture,
        RateGateSnapshot,
        runtime::{
            http::{HttpRuntime, RateLimitDecision},
            live::{
                ConnectionRotation, LiveAdapter, LiveConfig, LiveRateGate, LiveSocket,
                LiveSocketEvent, LiveSupervisorConfig, ProcessBlockPolicy, ReconciliationLimits,
                ReconciliationPolicy,
            },
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

pub const MAX_CONNECTION_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_FUTURE_CANDLE_SKEW: Duration = Duration::from_secs(5 * 60);
const MAX_GAP_RECONCILIATION_CANDLES: usize = 64_000;
const MAX_GAP_RECONCILIATION_PAGES: usize = 64;

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
    if let Err(error) = validate_live_candle_time_window(&kline, timeframe, &context) {
        return DecodedFrame::ProviderError(error);
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
fn validate_live_candle_time_window(
    candle: &WsKline,
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
    let nanos = i128::from(open_time).checked_mul(1_000_000)?;
    let date = OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()?
        .date();
    if date.day() != 1 || open_time.rem_euclid(86_400_000) != 0 {
        return None;
    }
    let (year, month) = if date.month() == Month::December {
        (date.year().checked_add(1)?, Month::January)
    } else {
        (date.year(), date.month().next())
    };
    let next = Date::from_calendar_date(year, month, 1).ok()?;
    i64::try_from(next.midnight().assume_utc().unix_timestamp_nanos() / 1_000_000).ok()
}

fn unix_now_ms() -> Result<i64, ProviderError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .ok_or(ProviderError::Invariant(
            "system time is outside millisecond range",
        ))
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
    http: HttpRuntime,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    base_url: Url,
    clock: Arc<dyn Clock>,
    rate_limit_fallback: Duration,
    live: LiveSupervisorConfig,
    max_connection_age: Duration,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    advertised_history_page_limit: u16,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    max_gap_reconciliation_candles: usize,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    max_gap_reconciliation_pages: usize,
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
            max_connection_age: MAX_CONNECTION_AGE,
            advertised_history_page_limit: 1000,
            max_gap_reconciliation_candles: MAX_GAP_RECONCILIATION_CANDLES,
            max_gap_reconciliation_pages: MAX_GAP_RECONCILIATION_PAGES,
        }
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct BinanceLiveTestConfig {
    pub rest: BinanceTestConfig,
    pub ws_base_url: String,
    pub live: LiveSupervisorConfig,
    pub max_connection_age: Duration,
    pub advertised_history_page_limit: u16,
    pub max_gap_reconciliation_candles: usize,
    pub max_gap_reconciliation_pages: usize,
}

pub(crate) struct BinanceLiveAdapter {
    provider: BinanceProvider,
}

impl BinanceLiveAdapter {
    fn new(provider: BinanceProvider) -> Self {
        Self { provider }
    }
}

pub(crate) struct BinanceLiveSocket {
    raw: RawWebSocket,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    stalled_write_probe_frames: usize,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    stalled_write_probe_payload_size: usize,
}

impl LiveSocket for BinanceLiveSocket {
    async fn read(&mut self) -> Result<LiveSocketEvent, ProviderError> {
        match self.raw.read().await? {
            DecodedFrame::Provider(BinanceDecoded::Candle(candle)) => {
                Ok(LiveSocketEvent::Candle(candle))
            }
            DecodedFrame::Ignored => Ok(LiveSocketEvent::Ignored),
            DecodedFrame::ProviderError(error) => Ok(LiveSocketEvent::DecodedError(error)),
            DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested => {
                Ok(LiveSocketEvent::ReconnectRequested)
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

impl LiveAdapter for BinanceLiveAdapter {
    type Socket = BinanceLiveSocket;

    fn validate_request(
        &self,
        _instrument: &Instrument,
        _timeframe: Timeframe,
    ) -> Result<(), ProviderError> {
        Ok(())
    }

    async fn connect_ready_socket(
        &self,
        instrument: Instrument,
        timeframe: Timeframe,
    ) -> Result<Self::Socket, ProviderError> {
        let raw = self
            .provider
            .connect_live_socket(&instrument, timeframe)
            .await?;
        Ok(BinanceLiveSocket {
            raw,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            stalled_write_probe_frames: self.provider.live.stalled_write_probe_frames,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            stalled_write_probe_payload_size: self
                .provider
                .live
                .ws_config
                .write_buffer_size
                .min(self.provider.live.ws_config.max_frame_size)
                .min(self.provider.live.ws_config.max_message_size)
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
            process_block: ProcessBlockPolicy::InvalidBanExpiry,
        }
    }

    fn live_config(&self) -> LiveConfig<'_> {
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        let (max_successors, max_pages) = (
            self.provider.max_gap_reconciliation_candles,
            self.provider.max_gap_reconciliation_pages,
        );
        #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
        let (max_successors, max_pages) =
            (MAX_GAP_RECONCILIATION_CANDLES, MAX_GAP_RECONCILIATION_PAGES);
        LiveConfig {
            supervisor: &self.provider.live,
            reconciliation: ReconciliationPolicy::Bounded(ReconciliationLimits {
                max_successors,
                max_pages,
                span_exceeded: "Binance gap reconciliation target exceeds the per-generation span limit",
                page_exceeded: "Binance gap reconciliation exceeded the per-generation page limit",
                distinct_exceeded: "Binance gap reconciliation exceeded the distinct buffered-candle limit",
            }),
        }
    }

    fn connection_rotation(&self) -> ConnectionRotation {
        ConnectionRotation::After {
            max_age: self.provider.max_connection_age,
            detail: "24-hour WebSocket connection age reached",
        }
    }
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
            MAX_CONNECTION_AGE,
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
            MAX_CONNECTION_AGE,
            1000,
            MAX_GAP_RECONCILIATION_CANDLES,
            MAX_GAP_RECONCILIATION_PAGES,
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
            config.max_connection_age,
            config.advertised_history_page_limit,
            config.max_gap_reconciliation_candles,
            config.max_gap_reconciliation_pages,
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
        max_connection_age: Duration,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        advertised_history_page_limit: u16,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        max_gap_reconciliation_candles: usize,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        max_gap_reconciliation_pages: usize,
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
        if max_connection_age.is_zero() {
            return Err(ProviderError::Configuration(
                "live connection max age must be positive",
            ));
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if max_gap_reconciliation_candles == 0 || max_gap_reconciliation_pages == 0 {
            return Err(ProviderError::Configuration(
                "live reconciliation limits must be positive",
            ));
        }
        let http = HttpRuntime::new(Arc::clone(&clock), request_timeout, body_limit)?;
        Ok(Self {
            http,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            base_url,
            clock,
            rate_limit_fallback,
            live,
            max_connection_age,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            advertised_history_page_limit,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            max_gap_reconciliation_candles,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            max_gap_reconciliation_pages,
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

            let response = self
                .http
                .send(
                    self.http.client().get(url).query(&query),
                    &cancellation,
                    &context,
                )
                .await?;

            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT {
                return self.handle_rate_limit(&response, context);
            }
            let bytes = self
                .http
                .read_response(response, &cancellation, context.clone(), map_http_error)
                .await?;
            decode_klines(&bytes, request, timeframe, context)
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
            .await_gate(cancellation, context, |blocker| match blocker {
                ProcessBlocker::InvalidBanExpiry => ProviderError::InvalidBanExpiry,
            })
            .await
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
        let decision = deadline.map_or(
            RateLimitDecision::ProcessBlocked(ProcessBlocker::InvalidBanExpiry),
            RateLimitDecision::TimedUntil,
        );
        self.http.apply_rate_limit(decision, context, status)?;
        unreachable!("rate-limit application always returns an error")
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
        self.http.gate_snapshot()
    }
}

impl MarketDataProvider for BinanceProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("binance").expect("static provider id")
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            markets: &[Market::Spot, Market::Perpetual],
            timeframes: &Timeframe::ALL,
            history_page_limit: {
                #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
                {
                    self.advertised_history_page_limit
                }
                #[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
                {
                    1000
                }
            },
        }
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
        let adapter = BinanceLiveAdapter::new(self.clone());
        let clock = Arc::clone(&self.clock);
        let capabilities = MarketDataProvider::capabilities(self);
        Box::pin(crate::provider::runtime::live::open_live(
            adapter,
            clock,
            capabilities,
            request,
        ))
    }
    fn rate_gate(&self) -> RateGateSnapshot {
        BinanceProvider::rate_gate(self)
    }
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
    request: HistoryRequest,
    timeframe: Timeframe,
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
    let limit = usize::from(request.limit()).min(1000);
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let rows =
        serde::de::Deserializer::deserialize_seq(&mut deserializer, BoundedRowsVisitor { limit })
            .map_err(|_| payload(&context, PayloadError::MalformedJson))?;
    deserializer
        .end()
        .map_err(|_| payload(&context, PayloadError::MalformedJson))?;
    let mut candles = Vec::with_capacity(rows.len());
    let mut previous_open = None;
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
        let candle = Candle::from_rest(open_time, close_time, open, high, low, close, volume)
            .map_err(|source| ProviderError::Domain {
                context: context.clone(),
                source,
            })?;
        validate_rest_candle_time_window(
            open_time,
            close_time,
            timeframe,
            request,
            previous_open,
            &context,
        )?;
        previous_open = Some(open_time);
        candles.push(candle);
    }
    Ok(candles)
}

fn validate_rest_candle_time_window(
    open_time: i64,
    close_time: i64,
    timeframe: Timeframe,
    request: HistoryRequest,
    previous_open: Option<i64>,
    context: &ErrorContext,
) -> Result<(), ProviderError> {
    let expected_close = timeframe_successor_open(timeframe, open_time)
        .and_then(|successor| successor.checked_sub(1));
    let outside_start = request
        .start_time()
        .is_some_and(|start_time| open_time < start_time);
    let outside_end = request
        .end_time()
        .is_some_and(|end_time| open_time > end_time);
    if expected_close != Some(close_time)
        || outside_start
        || outside_end
        || previous_open.is_some_and(|previous| open_time <= previous)
    {
        return Err(payload(context, PayloadError::MalformedProtocol));
    }
    Ok(())
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
