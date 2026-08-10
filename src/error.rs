//! Provider-neutral, sanitized error types.
//!
//! Network and terminal libraries are deliberately kept out of this module. Callers map
//! implementation errors to [`SanitizedCause`] at the boundary, preventing URLs, headers,
//! payloads, control sequences, or other secrets from escaping through `Display` or `source`.

use std::{error::Error, fmt};

use crate::model::{GapGeneration, ReplayRevision};

const CONTEXT_MAX_CHARS: usize = 64;
const REDACTED: &str = "[redacted]";

/// Stable context attached to provider failures without retaining untrusted provider text.
#[derive(Clone, Eq, PartialEq)]
pub struct ErrorContext {
    provider: Option<ContextValue>,
    symbol: Option<ContextValue>,
    timeframe: Option<ContextValue>,
    operation: ContextValue,
}

impl ErrorContext {
    pub fn operation(operation: &str) -> Self {
        Self {
            provider: None,
            symbol: None,
            timeframe: None,
            operation: ContextValue::new(operation),
        }
    }

    pub fn with_market(mut self, provider: &str, symbol: &str, timeframe: &str) -> Self {
        self.provider = Some(ContextValue::new(provider));
        self.symbol = Some(ContextValue::new(symbol));
        self.timeframe = Some(ContextValue::new(timeframe));
        self
    }
}

impl fmt::Debug for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorContext")
            .field("provider", &self.provider)
            .field("symbol", &self.symbol)
            .field("timeframe", &self.timeframe)
            .field("operation", &self.operation)
            .finish()
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operation {}", self.operation)?;
        if let Some(provider) = &self.provider {
            write!(f, ", provider {provider}")?;
        }
        if let Some(symbol) = &self.symbol {
            write!(f, ", symbol {symbol}")?;
        }
        if let Some(timeframe) = &self.timeframe {
            write!(f, ", timeframe {timeframe}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
struct ContextValue(String);

impl ContextValue {
    fn new(value: &str) -> Self {
        Self(sanitize_context(value))
    }
}

impl fmt::Debug for ContextValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ContextValue").field(&self.0).finish()
    }
}

impl fmt::Display for ContextValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn sanitize_context(value: &str) -> String {
    if looks_sensitive(value) {
        return REDACTED.to_owned();
    }

    let mut clean = String::with_capacity(value.len().min(CONTEXT_MAX_CHARS));
    for (count, character) in value.chars().enumerate() {
        if count == CONTEXT_MAX_CHARS {
            break;
        }
        if is_unsafe_format_character(character) {
            clean.push(' ');
        } else if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | '/') {
            clean.push(character);
        } else {
            clean.push('_');
        }
    }

    let clean = clean.trim().to_owned();
    if clean.is_empty() {
        "unknown".to_owned()
    } else {
        clean
    }
}

fn looks_sensitive(value: &str) -> bool {
    let lower: String = value
        .chars()
        .take(CONTEXT_MAX_CHARS)
        .flat_map(char::to_lowercase)
        .collect();
    value.contains("://")
        || value.contains('@')
        || value.contains('?')
        || value.contains('=')
        || [
            "authorization",
            "proxy-authorization",
            "api-key",
            "apikey",
            "bearer ",
            "cookie",
            "credential",
            "password",
            "secret",
            "token",
        ]
        .iter()
        .any(|marker| lower.contains(marker))
}

fn is_unsafe_format_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{061c}'
                | '\u{200e}'
                | '\u{200f}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{2069}'
        )
}

/// A stable provider-message category. Raw provider text is never retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SanitizedMessage {
    InvalidSymbol,
    TooManyRequests,
    IpBanned,
    ServiceUnavailable,
    Redacted,
}

impl SanitizedMessage {
    #[must_use]
    pub fn new(message: &str) -> Self {
        let lower: String = message
            .chars()
            .take(256)
            .flat_map(char::to_lowercase)
            .collect();
        if lower.contains("invalid symbol") {
            Self::InvalidSymbol
        } else if lower.contains("too many requests") {
            Self::TooManyRequests
        } else if lower.contains("ip banned") || lower.contains("ip has been banned") {
            Self::IpBanned
        } else if lower.contains("service unavailable") {
            Self::ServiceUnavailable
        } else {
            Self::Redacted
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidSymbol => "invalid symbol",
            Self::TooManyRequests => "too many requests",
            Self::IpBanned => "IP banned",
            Self::ServiceUnavailable => "service unavailable",
            Self::Redacted => "provider message redacted",
        }
    }
}

impl fmt::Display for SanitizedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An implementation-independent cause category. It intentionally does not retain the raw error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SanitizedCause {
    Connection,
    Dns,
    Tls,
    Io,
    Closed,
    Cancelled,
    InvalidData,
    Other,
}

impl fmt::Display for SanitizedCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connection => "connection failure",
            Self::Dns => "DNS failure",
            Self::Tls => "TLS failure",
            Self::Io => "I/O failure",
            Self::Closed => "channel or stream closed",
            Self::Cancelled => "operation cancelled",
            Self::InvalidData => "invalid data",
            Self::Other => "implementation failure",
        })
    }
}

impl Error for SanitizedCause {}

#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum ModelError {
    #[error("invalid provider")]
    InvalidProvider,
    #[error("invalid instrument component: {component}")]
    InvalidComponent { component: &'static str },
    #[error("invalid instrument")]
    InvalidInstrument,
    #[error("invalid timeframe")]
    InvalidTimeframe,
    #[error("timestamp {timestamp} is outside the supported range")]
    TimestampOutOfRange { timestamp: i64 },
    #[error("timestamp arithmetic overflow")]
    TimestampArithmetic,
    #[error("invalid history limit {limit}")]
    InvalidLimit { limit: usize },
    #[error("invalid history range")]
    InvalidRange,
    #[error("{field} must be finite")]
    NonFinite { field: &'static str },
    #[error("{field} is outside the chart-safe price range")]
    PriceOutOfRange { field: &'static str },
    #[error("volume must be non-negative")]
    NegativeVolume,
    #[error("OHLC values are inconsistent")]
    InvalidOhlc,
    #[error("candle body is outside its high/low bounds")]
    InvalidBodyBounds,
    #[error("candle close time precedes its open time")]
    InvalidTimestampOrder,
    #[error("invalid monotonic instant")]
    InvalidMonoInstant,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum PayloadError {
    #[error("malformed JSON payload")]
    MalformedJson,
    #[error("payload root is not an array")]
    ExpectedArray,
    #[error("payload item has wrong arity: expected {expected}, received {actual}")]
    WrongArity { expected: usize, actual: usize },
    #[error("payload field `{field}` is invalid")]
    InvalidField { field: &'static str },
    #[error("payload exceeds the {limit_bytes}-byte limit")]
    OverBudget { limit_bytes: usize },
    #[error("malformed protocol payload")]
    MalformedProtocol,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutKind {
    Request,
    FirstKline,
    StalledWrite,
    ProducerJoin,
    HistoryJoin,
}

impl fmt::Display for TimeoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Request => "request",
            Self::FirstKline => "first kline",
            Self::StalledWrite => "stalled write",
            Self::ProducerJoin => "producer join",
            Self::HistoryJoin => "history join",
        })
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum TerminalError {
    #[error("terminal setup failed during {operation}: {cause}")]
    Setup {
        operation: &'static str,
        #[source]
        cause: SanitizedCause,
    },
    #[error("terminal input failed: {0}")]
    Input(#[source] SanitizedCause),
    #[error("terminal restoration failed during {operation}: {cause}")]
    Restore {
        operation: &'static str,
        #[source]
        cause: SanitizedCause,
    },
    #[error("interactive mode requires both stdin and stdout to be terminals")]
    TtyRequired,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum RenderError {
    #[error("terminal is too small to render the requested output")]
    InsufficientSpace,
    #[error("render output failed: {0}")]
    Output(#[source] SanitizedCause),
    #[error("render invariant failed: {0}")]
    Invariant(&'static str),
}

/// Provider failures consumed by history, live events, and provider ownership APIs.
#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum ProviderError {
    #[error("domain validation failed ({context}): {source}")]
    Domain {
        context: ErrorContext,
        #[source]
        source: ModelError,
    },
    #[error("invalid symbol ({context}); provider code {code}: {message}")]
    InvalidSymbol {
        context: ErrorContext,
        code: i64,
        message: SanitizedMessage,
    },
    #[error("non-retryable HTTP client status {status} ({context})")]
    ClientStatus {
        context: ErrorContext,
        status: u16,
        code: Option<i64>,
        message: Option<SanitizedMessage>,
    },
    #[error("recoverable HTTP server status {status} ({context})")]
    ServerStatus { context: ErrorContext, status: u16 },
    #[error("rate limited with HTTP status {status} ({context})")]
    RateLimited { context: ErrorContext, status: u16 },
    #[error("HTTP ban expiry is missing or invalid")]
    InvalidBanExpiry,
    #[error("{kind} timed out ({context})")]
    Timeout {
        context: ErrorContext,
        kind: TimeoutKind,
    },
    #[error("transport failed ({context}): {cause}")]
    Transport {
        context: ErrorContext,
        #[source]
        cause: SanitizedCause,
    },
    #[error("payload failed validation ({context}): {source}")]
    Payload {
        context: ErrorContext,
        #[source]
        source: PayloadError,
    },
    #[error("protocol failure ({context}): {detail}")]
    Protocol {
        context: ErrorContext,
        detail: &'static str,
    },
    #[error("reconciliation failure ({context}): {detail}")]
    Reconciliation {
        context: ErrorContext,
        detail: &'static str,
    },
    #[error(
        "gap synchronization made no progress toward {target_open_time}; last open time: {last_open_time:?}"
    )]
    GapSyncNoProgress {
        target_open_time: i64,
        last_open_time: Option<i64>,
    },
    #[error(
        "reconciliation acknowledgement timed out for generation {generation:?}, revision {revision:?}, target open time {target_open_time}"
    )]
    ReconcileAckTimeout {
        generation: GapGeneration,
        revision: ReplayRevision,
        target_open_time: i64,
    },
    #[error("provider configuration is invalid: {0}")]
    Configuration(&'static str),
    #[error("provider invariant failed: {0}")]
    Invariant(&'static str),
    #[error("provider event queue saturated")]
    QueueSaturated,
    #[error("provider channel or stream closed ({context})")]
    ChannelClosed { context: ErrorContext },
}

impl ProviderError {
    /// Whether an older-history request may be retried under the shared gate/backoff policy.
    #[must_use]
    pub const fn is_recoverable_for_history(&self) -> bool {
        matches!(
            self,
            Self::ServerStatus { .. }
                | Self::RateLimited { .. }
                | Self::Timeout {
                    kind: TimeoutKind::Request,
                    ..
                }
                | Self::Transport { .. }
        )
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] ModelError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Terminal(#[from] TerminalError),
    #[error(transparent)]
    Render(#[from] RenderError),
    #[error("application invariant failed: {0}")]
    Invariant(&'static str),
}
