use std::{fmt, str::FromStr};

use crate::error::{ModelError, ProviderError};

pub const CHART_PRICE_MAX: f64 = f64::MAX / 4.0;
pub const MIN_TIMESTAMP_MS: i64 = -377_705_116_800_000;
pub const MAX_TIMESTAMP_MS: i64 = 253_402_300_799_999;
pub const MAX_HISTORY_LIMIT: u16 = 1_000;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderId(String);

impl ProviderId {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(ModelError::InvalidProvider);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ProviderId {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Market {
    Spot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentSpec {
    provider: ProviderId,
    base: String,
    quote: Option<String>,
}

impl InstrumentSpec {
    pub fn new(
        provider: ProviderId,
        base: impl Into<String>,
        quote: Option<impl Into<String>>,
    ) -> Result<Self, ModelError> {
        let base = validate_component(base.into(), "base")?;
        let quote = quote
            .map(|value| validate_component(value.into(), "quote"))
            .transpose()?;
        Ok(Self {
            provider,
            base,
            quote,
        })
    }

    pub const fn provider(&self) -> &ProviderId {
        &self.provider
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn quote(&self) -> Option<&str> {
        self.quote.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Instrument {
    provider: ProviderId,
    market: Market,
    base: String,
    quote: String,
    display_pair: String,
    provider_symbol: String,
}

impl Instrument {
    pub fn new(
        provider: ProviderId,
        market: Market,
        base: impl Into<String>,
        quote: impl Into<String>,
        provider_symbol: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let base = validate_component(base.into(), "base")?;
        let quote = validate_component(quote.into(), "quote")?;
        let provider_symbol = provider_symbol.into();
        if provider_symbol.is_empty()
            || provider_symbol.len() > 256
            || provider_symbol.chars().any(char::is_control)
        {
            return Err(ModelError::InvalidInstrument);
        }
        let display_pair = format!("{base}/{quote}");
        Ok(Self {
            provider,
            market,
            base,
            quote,
            display_pair,
            provider_symbol,
        })
    }

    pub const fn provider(&self) -> &ProviderId {
        &self.provider
    }

    pub const fn market(&self) -> Market {
        self.market
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn quote(&self) -> &str {
        &self.quote
    }

    pub fn display_pair(&self) -> &str {
        &self.display_pair
    }

    pub fn provider_symbol(&self) -> &str {
        &self.provider_symbol
    }
}

fn validate_component(value: String, component: &'static str) -> Result<String, ModelError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(ModelError::InvalidComponent { component });
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Timeframe {
    Second1,
    Minute1,
    Minute3,
    Minute5,
    Minute15,
    Minute30,
    Hour1,
    Hour2,
    Hour4,
    Hour6,
    Hour8,
    Hour12,
    Day1,
    Day3,
    Week1,
    Month1,
}

impl Timeframe {
    pub const ALL: [Self; 16] = [
        Self::Second1,
        Self::Minute1,
        Self::Minute3,
        Self::Minute5,
        Self::Minute15,
        Self::Minute30,
        Self::Hour1,
        Self::Hour2,
        Self::Hour4,
        Self::Hour6,
        Self::Hour8,
        Self::Hour12,
        Self::Day1,
        Self::Day3,
        Self::Week1,
        Self::Month1,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Second1 => "1s",
            Self::Minute1 => "1m",
            Self::Minute3 => "3m",
            Self::Minute5 => "5m",
            Self::Minute15 => "15m",
            Self::Minute30 => "30m",
            Self::Hour1 => "1h",
            Self::Hour2 => "2h",
            Self::Hour4 => "4h",
            Self::Hour6 => "6h",
            Self::Hour8 => "8h",
            Self::Hour12 => "12h",
            Self::Day1 => "1d",
            Self::Day3 => "3d",
            Self::Week1 => "1w",
            Self::Month1 => "1M",
        }
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Timeframe {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|timeframe| timeframe.as_str() == value)
            .ok_or(ModelError::InvalidTimeframe)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UnixMillis(i64);

impl UnixMillis {
    pub fn new(value: i64) -> Result<Self, ModelError> {
        validate_timestamp(value)?;
        Ok(Self(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }

    pub fn checked_add(self, milliseconds: i64) -> Result<Self, ModelError> {
        let value = self
            .0
            .checked_add(milliseconds)
            .ok_or(ModelError::TimestampArithmetic)?;
        Self::new(value)
    }

    pub fn checked_sub(self, milliseconds: i64) -> Result<Self, ModelError> {
        let value = self
            .0
            .checked_sub(milliseconds)
            .ok_or(ModelError::TimestampArithmetic)?;
        Self::new(value)
    }
}

pub fn validate_timestamp(timestamp: i64) -> Result<(), ModelError> {
    if !(MIN_TIMESTAMP_MS..=MAX_TIMESTAMP_MS).contains(&timestamp) {
        return Err(ModelError::TimestampOutOfRange { timestamp });
    }
    Ok(())
}

pub fn checked_timestamp_add(timestamp: i64, milliseconds: i64) -> Result<i64, ModelError> {
    UnixMillis::new(timestamp)?
        .checked_add(milliseconds)
        .map(UnixMillis::get)
}

pub fn checked_timestamp_sub(timestamp: i64, milliseconds: i64) -> Result<i64, ModelError> {
    UnixMillis::new(timestamp)?
        .checked_sub(milliseconds)
        .map(UnixMillis::get)
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MonoInstant(u64);

impl MonoInstant {
    pub const ZERO: Self = Self(0);

    pub const fn from_nanos(nanoseconds: u64) -> Self {
        Self(nanoseconds)
    }

    pub fn from_millis(milliseconds: u64) -> Result<Self, ModelError> {
        milliseconds
            .checked_mul(1_000_000)
            .map(Self)
            .ok_or(ModelError::InvalidMonoInstant)
    }

    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    pub const fn as_millis(self) -> u64 {
        self.0 / 1_000_000
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FinalityAuthority {
    RestProvisionalOpen,
    RestProvisionalClosed,
    WsAuthoritativeOpen,
    WsAuthoritativeClosed,
}

impl FinalityAuthority {
    pub const fn is_closed(self) -> bool {
        matches!(
            self,
            Self::RestProvisionalClosed | Self::WsAuthoritativeClosed
        )
    }

    pub const fn is_authoritative(self) -> bool {
        matches!(
            self,
            Self::WsAuthoritativeOpen | Self::WsAuthoritativeClosed
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Candle {
    open_time: UnixMillis,
    close_time: UnixMillis,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    base_volume: f64,
    authority: FinalityAuthority,
}

impl Candle {
    #[allow(clippy::too_many_arguments)]
    pub fn from_rest(
        open_time: i64,
        close_time: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        base_volume: f64,
    ) -> Result<Self, ModelError> {
        Self::validated(
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            base_volume,
            FinalityAuthority::RestProvisionalOpen,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_ws(
        open_time: i64,
        close_time: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        base_volume: f64,
        is_closed: bool,
    ) -> Result<Self, ModelError> {
        let authority = if is_closed {
            FinalityAuthority::WsAuthoritativeClosed
        } else {
            FinalityAuthority::WsAuthoritativeOpen
        };
        Self::validated(
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            base_volume,
            authority,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn validated(
        open_time: i64,
        close_time: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        base_volume: f64,
        authority: FinalityAuthority,
    ) -> Result<Self, ModelError> {
        let open_time = UnixMillis::new(open_time)?;
        let close_time = UnixMillis::new(close_time)?;
        if open_time > close_time {
            return Err(ModelError::InvalidTimestampOrder);
        }
        for (field, value) in [
            ("open", open),
            ("high", high),
            ("low", low),
            ("close", close),
        ] {
            if !value.is_finite() {
                return Err(ModelError::NonFinite { field });
            }
            if value.abs() > CHART_PRICE_MAX {
                return Err(ModelError::PriceOutOfRange { field });
            }
        }
        if !base_volume.is_finite() {
            return Err(ModelError::NonFinite {
                field: "base_volume",
            });
        }
        if base_volume < 0.0 {
            return Err(ModelError::NegativeVolume);
        }
        if low > high {
            return Err(ModelError::InvalidOhlc);
        }
        if open < low || open > high || close < low || close > high {
            return Err(ModelError::InvalidBodyBounds);
        }
        Ok(Self {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            base_volume,
            authority,
        })
    }

    pub const fn open_time(&self) -> i64 {
        self.open_time.get()
    }
    pub const fn close_time(&self) -> i64 {
        self.close_time.get()
    }
    pub const fn open(&self) -> f64 {
        self.open
    }
    pub const fn high(&self) -> f64 {
        self.high
    }
    pub const fn low(&self) -> f64 {
        self.low
    }
    pub const fn close(&self) -> f64 {
        self.close
    }
    pub const fn base_volume(&self) -> f64 {
        self.base_volume
    }
    pub const fn authority(&self) -> FinalityAuthority {
        self.authority
    }

    pub const fn is_closed(&self) -> bool {
        self.authority.is_closed()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryRequestKind {
    Latest,
    Older,
    Gap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HistoryBounds {
    Latest,
    Older {
        end_time: UnixMillis,
    },
    Gap {
        start_time: UnixMillis,
        end_time: UnixMillis,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryRequest {
    bounds: HistoryBounds,
    limit: u16,
}

impl HistoryRequest {
    pub fn latest(limit: u16) -> Result<Self, ModelError> {
        validate_limit(limit)?;
        Ok(Self {
            bounds: HistoryBounds::Latest,
            limit,
        })
    }

    pub fn older(oldest_open_time: i64, limit: u16) -> Result<Self, ModelError> {
        validate_limit(limit)?;
        let end_time = UnixMillis::new(checked_timestamp_sub(oldest_open_time, 1)?)?;
        Ok(Self {
            bounds: HistoryBounds::Older { end_time },
            limit,
        })
    }

    pub fn gap(start_time: i64, end_time: i64, limit: u16) -> Result<Self, ModelError> {
        validate_limit(limit)?;
        let start_time = UnixMillis::new(start_time)?;
        let end_time = UnixMillis::new(end_time)?;
        if start_time > end_time {
            return Err(ModelError::InvalidRange);
        }
        Ok(Self {
            bounds: HistoryBounds::Gap {
                start_time,
                end_time,
            },
            limit,
        })
    }

    pub const fn kind(self) -> HistoryRequestKind {
        match self.bounds {
            HistoryBounds::Latest => HistoryRequestKind::Latest,
            HistoryBounds::Older { .. } => HistoryRequestKind::Older,
            HistoryBounds::Gap { .. } => HistoryRequestKind::Gap,
        }
    }

    pub const fn start_time(self) -> Option<i64> {
        match self.bounds {
            HistoryBounds::Gap { start_time, .. } => Some(start_time.get()),
            HistoryBounds::Latest | HistoryBounds::Older { .. } => None,
        }
    }

    pub const fn end_time(self) -> Option<i64> {
        match self.bounds {
            HistoryBounds::Older { end_time } | HistoryBounds::Gap { end_time, .. } => {
                Some(end_time.get())
            }
            HistoryBounds::Latest => None,
        }
    }

    pub const fn limit(self) -> u16 {
        self.limit
    }

    pub fn next_inclusive_start(last_returned_open_time: i64) -> Result<i64, ModelError> {
        checked_timestamp_add(last_returned_open_time, 1)
    }
}

fn validate_limit(limit: u16) -> Result<(), ModelError> {
    if limit == 0 || limit > MAX_HISTORY_LIMIT {
        return Err(ModelError::InvalidLimit {
            limit: usize::from(limit),
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GapGeneration(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplayRevision(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    GapSync,
    Connected,
    Backoff,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessBlocker {
    InvalidBanExpiry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateGateState {
    Open,
    TimedUntil(MonoInstant),
    ProcessBlocked(ProcessBlocker),
}

#[derive(Clone, Debug, PartialEq)]
pub enum MarketEvent {
    Status {
        generation: Option<GapGeneration>,
        status: ConnectionStatus,
    },
    ReconcileBatch {
        generation: GapGeneration,
        revision: ReplayRevision,
        target_open_time: i64,
        candles: Vec<Candle>,
    },
    Candle {
        generation: GapGeneration,
        candle: Candle,
    },
    RecoverableError {
        generation: Option<GapGeneration>,
        error: ProviderError,
        rate_gate_deadline: Option<MonoInstant>,
    },
    TerminalError(ProviderError),
}
