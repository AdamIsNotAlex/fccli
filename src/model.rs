use std::{collections::VecDeque, fmt, ops::Range, str::FromStr, sync::Arc};

use time::{Date, Month, OffsetDateTime};

use crate::error::{ModelError, ProviderError};

pub const CHART_PRICE_MAX: f64 = f64::MAX / 4.0;
pub const MIN_TIMESTAMP_MS: i64 = -377_705_116_800_000;
pub const MAX_TIMESTAMP_MS: i64 = 253_402_300_799_999;
pub const MAX_HISTORY_LIMIT: u16 = 1_000;
/// Maximum byte length of a provider-native instrument symbol.
pub const MAX_PROVIDER_SYMBOL_LEN: usize = 256;

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
            || provider_symbol.len() > MAX_PROVIDER_SYMBOL_LEN
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("candle series replacement is only valid during empty-series initialization")]
pub struct CandleSeriesInitializationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationKind {
    Inserted,
    Replaced,
    Unchanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedMutation {
    pub open_time: i64,
    pub final_index: usize,
    pub kind: MutationKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexMapping {
    Identity {
        len: usize,
    },
    ShiftSuffix {
        len: usize,
        from: usize,
        delta: isize,
    },
    Explicit(Vec<usize>),
}

impl IndexMapping {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Identity { len } | Self::ShiftSuffix { len, .. } => *len,
            Self::Explicit(indices) => indices.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn map(&self, old_index: usize) -> Option<usize> {
        match self {
            Self::Identity { len } => (old_index < *len).then_some(old_index),
            Self::ShiftSuffix { len, from, delta } => {
                if old_index >= *len {
                    return None;
                }
                if old_index < *from {
                    return Some(old_index);
                }
                old_index.checked_add_signed(*delta)
            }
            Self::Explicit(indices) => indices.get(old_index).copied(),
        }
    }
}

#[derive(Debug)]
enum AdaptiveIndexMapping {
    Identity,
    ShiftSuffix { from: usize, delta: isize },
    Explicit(Vec<usize>),
}

impl AdaptiveIndexMapping {
    fn observe(&mut self, old_index: usize, new_index: usize, old_len: usize) {
        match self {
            Self::Identity if old_index == new_index => {}
            Self::Identity => {
                *self = Self::ShiftSuffix {
                    from: old_index,
                    delta: mapping_delta(old_index, new_index),
                };
            }
            Self::ShiftSuffix { delta, .. }
                if old_index.checked_add_signed(*delta) == Some(new_index) => {}
            Self::ShiftSuffix { from, delta } => {
                let mut indices = Vec::with_capacity(old_len);
                indices.extend((0..old_index).map(|index| {
                    if index < *from {
                        index
                    } else {
                        index
                            .checked_add_signed(*delta)
                            .expect("observed index mapping remains in bounds")
                    }
                }));
                indices.push(new_index);
                *self = Self::Explicit(indices);
            }
            Self::Explicit(indices) => indices.push(new_index),
        }
    }

    fn finish(self, old_len: usize) -> IndexMapping {
        match self {
            Self::Identity => IndexMapping::Identity { len: old_len },
            Self::ShiftSuffix { from, delta } => IndexMapping::ShiftSuffix {
                len: old_len,
                from,
                delta,
            },
            Self::Explicit(indices) => {
                debug_assert_eq!(indices.len(), old_len);
                IndexMapping::Explicit(indices)
            }
        }
    }
}

fn mapping_delta(old_index: usize, new_index: usize) -> isize {
    let delta = new_index
        .checked_sub(old_index)
        .expect("merge never moves an existing candle backward");
    isize::try_from(delta).expect("index delta fits in isize")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationSummary {
    pub inserted: usize,
    pub replaced: usize,
    pub unchanged: usize,
    pub old_to_new: IndexMapping,
    pub resolved: Vec<ResolvedMutation>,
    pub empty_input: bool,
    pub duplicate_only: bool,
    pub no_progress: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandleSeries {
    timeframe: Timeframe,
    candles: VecDeque<Candle>,
}

impl CandleSeries {
    #[must_use]
    pub const fn new(timeframe: Timeframe) -> Self {
        Self {
            timeframe,
            candles: VecDeque::new(),
        }
    }

    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.candles.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.candles.is_empty()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Candle> {
        self.candles.get(index)
    }

    #[must_use]
    pub fn range(&self, range: Range<usize>) -> Option<impl Iterator<Item = &Candle>> {
        (range.start <= range.end && range.end <= self.len()).then(|| {
            self.candles
                .iter()
                .skip(range.start)
                .take(range.end - range.start)
        })
    }

    #[must_use]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Candle> + ExactSizeIterator {
        self.candles.iter()
    }
    /// Consumes the series and moves its candles into shared contiguous storage.
    #[must_use]
    pub fn into_arc(self) -> Arc<[Candle]> {
        Arc::from(self.candles.into_iter().collect::<Vec<_>>())
    }

    #[must_use]
    pub fn oldest_open_time(&self) -> Option<i64> {
        self.candles.front().map(Candle::open_time)
    }

    #[must_use]
    pub fn newest_open_time(&self) -> Option<i64> {
        self.candles.back().map(Candle::open_time)
    }

    #[must_use]
    pub fn index_of_open_time(&self, open_time: i64) -> Option<usize> {
        self.candles
            .binary_search_by_key(&open_time, Candle::open_time)
            .ok()
    }

    #[must_use]
    pub fn candle_at_open_time(&self, open_time: i64) -> Option<&Candle> {
        self.index_of_open_time(open_time)
            .and_then(|index| self.candles.get(index))
    }

    #[must_use]
    pub fn is_contiguous(&self) -> bool {
        self.is_range_contiguous(0..self.len())
    }

    #[must_use]
    pub fn is_range_contiguous(&self, range: Range<usize>) -> bool {
        let Some(candles) = self.range(range) else {
            return false;
        };
        let mut candles = candles.peekable();
        let Some(mut previous) = candles.next() else {
            return true;
        };
        candles.all(|next| {
            let contiguous =
                timeframe_successor(self.timeframe, previous.open_time()) == Some(next.open_time());
            previous = next;
            contiguous
        })
    }

    #[must_use]
    pub fn is_contiguous_through(&self, start_open_time: i64, end_open_time: i64) -> bool {
        let (Some(start), Some(end)) = (
            self.index_of_open_time(start_open_time),
            self.index_of_open_time(end_open_time),
        ) else {
            return false;
        };
        start <= end && self.is_range_contiguous(start..end.saturating_add(1))
    }

    pub fn replace(
        &mut self,
        candles: Vec<Candle>,
    ) -> Result<MutationSummary, CandleSeriesInitializationError> {
        if !self.candles.is_empty() {
            return Err(CandleSeriesInitializationError);
        }
        Ok(self.initialize(candles))
    }

    #[must_use]
    pub fn merge(&mut self, candles: Vec<Candle>) -> MutationSummary {
        if candles.len() == 1 {
            return self.upsert(candles.into_iter().next().expect("length checked"));
        }
        self.merge_batch(candles)
    }

    #[must_use]
    pub fn prepend(&mut self, candles: Vec<Candle>) -> MutationSummary {
        self.merge(candles)
    }

    #[must_use]
    pub fn upsert(&mut self, candle: Candle) -> MutationSummary {
        self.upsert_one(candle)
    }

    #[must_use]
    pub fn append(&mut self, candle: Candle) -> MutationSummary {
        self.upsert_one(candle)
    }

    fn initialize(&mut self, mut input: Vec<Candle>) -> MutationSummary {
        if input.is_empty() {
            return empty_summary(0);
        }
        normalize_input(&mut input);
        let mut candles: VecDeque<_> = input.into();
        let affected: Vec<_> = (0..candles.len()).collect();
        recompute_rest_adjacency(self.timeframe, &mut candles, &affected);
        let resolved = candles
            .iter()
            .enumerate()
            .map(|(final_index, candle)| ResolvedMutation {
                open_time: candle.open_time(),
                final_index,
                kind: MutationKind::Inserted,
            })
            .collect();
        let inserted = candles.len();
        self.candles = candles;
        MutationSummary {
            inserted,
            replaced: 0,
            unchanged: 0,
            old_to_new: IndexMapping::Identity { len: 0 },
            resolved,
            empty_input: false,
            duplicate_only: false,
            no_progress: false,
        }
    }

    fn upsert_one(&mut self, candidate: Candle) -> MutationSummary {
        let old_len = self.candles.len();
        let open_time = candidate.open_time();
        let position = match self.candles.back() {
            Some(newest) if open_time > newest.open_time() => Err(old_len),
            Some(newest) if open_time == newest.open_time() => Ok(old_len - 1),
            _ => self
                .candles
                .binary_search_by_key(&open_time, Candle::open_time),
        };
        match position {
            Ok(index) => {
                let current = self.candles[index].clone();
                let merged = merge_equal(&current, candidate);
                self.candles[index] = merged;
                recompute_rest_adjacency(self.timeframe, &mut self.candles, &[index]);
                let unchanged = usize::from(self.candles[index] == current);
                let replaced = 1 - unchanged;
                MutationSummary {
                    inserted: 0,
                    replaced,
                    unchanged,
                    old_to_new: IndexMapping::Identity { len: old_len },
                    resolved: vec![ResolvedMutation {
                        open_time,
                        final_index: index,
                        kind: if replaced == 1 {
                            MutationKind::Replaced
                        } else {
                            MutationKind::Unchanged
                        },
                    }],
                    empty_input: false,
                    duplicate_only: replaced == 0,
                    no_progress: true,
                }
            }
            Err(index) => {
                let predecessor =
                    timeframe_predecessor(self.timeframe, open_time).and_then(|predecessor_time| {
                        self.candles
                            .back()
                            .filter(|candle| candle.open_time() == predecessor_time)
                            .map(|_| old_len - 1)
                            .or_else(|| {
                                self.candles
                                    .binary_search_by_key(&predecessor_time, Candle::open_time)
                                    .ok()
                            })
                    });
                if index == old_len {
                    self.candles.push_back(candidate);
                } else if index == 0 {
                    self.candles.push_front(candidate);
                } else {
                    self.candles.insert(index, candidate);
                }
                let affected = match predecessor {
                    Some(predecessor) => [predecessor, index],
                    None => [index, index],
                };
                let affected_len = usize::from(affected[0] != affected[1]) + 1;
                recompute_rest_adjacency(
                    self.timeframe,
                    &mut self.candles,
                    &affected[..affected_len],
                );
                MutationSummary {
                    inserted: 1,
                    replaced: 0,
                    unchanged: 0,
                    old_to_new: if index == old_len {
                        IndexMapping::Identity { len: old_len }
                    } else {
                        IndexMapping::ShiftSuffix {
                            len: old_len,
                            from: index,
                            delta: 1,
                        }
                    },
                    resolved: vec![ResolvedMutation {
                        open_time,
                        final_index: index,
                        kind: MutationKind::Inserted,
                    }],
                    empty_input: false,
                    duplicate_only: false,
                    no_progress: false,
                }
            }
        }
    }

    fn merge_batch(&mut self, mut input: Vec<Candle>) -> MutationSummary {
        if input.is_empty() {
            return empty_summary(self.candles.len());
        }
        normalize_input(&mut input);
        let accepted_times: Vec<_> = input.iter().map(Candle::open_time).collect();
        let old_len = self.candles.len();
        let mut old = std::mem::take(&mut self.candles).into_iter().peekable();
        let mut incoming = input.into_iter().peekable();
        let mut merged = VecDeque::with_capacity(old_len + incoming.len());
        let mut old_to_new = AdaptiveIndexMapping::Identity;
        let mut old_index = 0;
        let mut drafts = Vec::with_capacity(incoming.len());

        while old.peek().is_some() || incoming.peek().is_some() {
            let final_index = merged.len();
            match (old.peek(), incoming.peek()) {
                (Some(current), Some(candidate)) if current.open_time() < candidate.open_time() => {
                    old_to_new.observe(old_index, final_index, old_len);
                    old_index += 1;
                    merged.push_back(old.next().expect("peeked candle exists"));
                }
                (Some(current), Some(candidate))
                    if current.open_time() == candidate.open_time() =>
                {
                    let current = old.next().expect("peeked candle exists");
                    let candidate = incoming.next().expect("peeked candle exists");
                    let open_time = candidate.open_time();
                    let result = merge_equal(&current, candidate);
                    old_to_new.observe(old_index, final_index, old_len);
                    old_index += 1;
                    drafts.push((open_time, final_index, Some(current)));
                    merged.push_back(result);
                }
                (_, Some(_)) => {
                    let candidate = incoming.next().expect("peeked candle exists");
                    drafts.push((candidate.open_time(), final_index, None));
                    merged.push_back(candidate);
                }
                (Some(_), None) => {
                    old_to_new.observe(old_index, final_index, old_len);
                    old_index += 1;
                    merged.push_back(old.next().expect("peeked candle exists"));
                }
                (None, None) => break,
            }
        }
        debug_assert_eq!(old_index, old_len);

        let mut affected = Vec::with_capacity(drafts.len().saturating_mul(2));
        affected.extend(drafts.iter().map(|(_, index, _)| *index));
        let mut accepted_index = 0;
        for (index, candle) in merged.iter().enumerate() {
            let Some(successor) = timeframe_successor(self.timeframe, candle.open_time()) else {
                continue;
            };
            while accepted_index < accepted_times.len()
                && accepted_times[accepted_index] < successor
            {
                accepted_index += 1;
            }
            if accepted_times.get(accepted_index) == Some(&successor) {
                affected.push(index);
            }
        }
        affected.sort_unstable();
        affected.dedup();
        recompute_rest_adjacency(self.timeframe, &mut merged, &affected);

        let mut inserted = 0;
        let mut replaced = 0;
        let mut unchanged = 0;
        let resolved = drafts
            .into_iter()
            .map(|(open_time, final_index, previous)| {
                let kind = match previous {
                    None => {
                        inserted += 1;
                        MutationKind::Inserted
                    }
                    Some(previous) if previous == merged[final_index] => {
                        unchanged += 1;
                        MutationKind::Unchanged
                    }
                    Some(_) => {
                        replaced += 1;
                        MutationKind::Replaced
                    }
                };
                ResolvedMutation {
                    open_time,
                    final_index,
                    kind,
                }
            })
            .collect();
        self.candles = merged;
        MutationSummary {
            inserted,
            replaced,
            unchanged,
            old_to_new: old_to_new.finish(old_len),
            resolved,
            empty_input: false,
            duplicate_only: inserted == 0 && replaced == 0,
            no_progress: inserted == 0,
        }
    }
}

fn empty_summary(old_len: usize) -> MutationSummary {
    MutationSummary {
        inserted: 0,
        replaced: 0,
        unchanged: 0,
        old_to_new: IndexMapping::Identity { len: old_len },
        resolved: Vec::new(),
        empty_input: true,
        duplicate_only: false,
        no_progress: true,
    }
}

fn normalize_input(input: &mut Vec<Candle>) {
    input.sort_by_key(Candle::open_time);
    let mut write = 0;
    for read in 0..input.len() {
        if write > 0 && input[write - 1].open_time() == input[read].open_time() {
            let candidate = input[read].clone();
            input[write - 1] = merge_equal(&input[write - 1], candidate);
        } else {
            input.swap(write, read);
            write += 1;
        }
    }
    input.truncate(write);
}

fn merge_equal(current: &Candle, candidate: Candle) -> Candle {
    use FinalityAuthority::{
        RestProvisionalClosed, RestProvisionalOpen, WsAuthoritativeClosed, WsAuthoritativeOpen,
    };
    match (current.authority(), candidate.authority()) {
        (_, WsAuthoritativeClosed) => candidate,
        (WsAuthoritativeClosed, _) => current.clone(),
        (WsAuthoritativeOpen, RestProvisionalOpen | RestProvisionalClosed) => current.clone(),
        (RestProvisionalOpen | RestProvisionalClosed, WsAuthoritativeOpen) => candidate,
        (WsAuthoritativeOpen, WsAuthoritativeOpen)
        | (
            RestProvisionalOpen | RestProvisionalClosed,
            RestProvisionalOpen | RestProvisionalClosed,
        ) => candidate,
    }
}

fn recompute_rest_adjacency(
    timeframe: Timeframe,
    candles: &mut VecDeque<Candle>,
    affected: &[usize],
) {
    let mut search_index = 0;
    for &index in affected {
        if candles[index].authority().is_authoritative() {
            continue;
        }
        let expected = timeframe_successor(timeframe, candles[index].open_time());
        search_index = search_index.max(index.saturating_add(1));
        while search_index < candles.len()
            && expected.is_some_and(|open_time| candles[search_index].open_time() < open_time)
        {
            search_index += 1;
        }
        let has_successor = expected.is_some_and(|open_time| {
            candles
                .get(search_index)
                .is_some_and(|candidate| candidate.open_time() == open_time)
        });
        candles[index].authority = if has_successor {
            FinalityAuthority::RestProvisionalClosed
        } else {
            FinalityAuthority::RestProvisionalOpen
        };
    }
}

fn timeframe_predecessor(timeframe: Timeframe, open_time: i64) -> Option<i64> {
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
        return checked_timestamp_add(open_time, -milliseconds).ok();
    }

    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(open_time) * 1_000_000).ok()?;
    let (year, month) = match timestamp.month() {
        Month::January => (timestamp.year().checked_sub(1)?, Month::December),
        month => (timestamp.year(), month.previous()),
    };
    let date = Date::from_calendar_date(year, month, timestamp.day()).ok()?;
    let predecessor = timestamp.replace_date(date).unix_timestamp_nanos() / 1_000_000;
    i64::try_from(predecessor)
        .ok()
        .filter(|value| validate_timestamp(*value).is_ok())
}

fn timeframe_successor(timeframe: Timeframe, open_time: i64) -> Option<i64> {
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
        return checked_timestamp_add(open_time, milliseconds).ok();
    }

    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(open_time) * 1_000_000).ok()?;
    let (year, month) = match timestamp.month() {
        Month::December => (timestamp.year().checked_add(1)?, Month::January),
        month => (timestamp.year(), month.next()),
    };
    let date = Date::from_calendar_date(year, month, timestamp.day()).ok()?;
    let successor = timestamp.replace_date(date).unix_timestamp_nanos() / 1_000_000;
    i64::try_from(successor)
        .ok()
        .filter(|value| validate_timestamp(*value).is_ok())
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
