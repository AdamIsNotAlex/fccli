//! Provider-neutral, sanitized error types.
//!
//! Network and terminal libraries are deliberately kept out of this module. Callers map
//! implementation errors to [`SanitizedCause`] at the boundary, preventing URLs, headers,
//! payloads, control sequences, or other secrets from escaping through `Display` or `source`.

use std::{error::Error, fmt};

/// Stable context attached to provider failures without depending on provider implementations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ErrorContext {
    pub provider: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub operation: &'static str,
}

impl ErrorContext {
    #[must_use]
    pub const fn operation(operation: &'static str) -> Self {
        Self {
            provider: None,
            symbol: None,
            timeframe: None,
            operation,
        }
    }

    #[must_use]
    pub fn with_market(
        mut self,
        provider: impl Into<String>,
        symbol: impl Into<String>,
        timeframe: impl Into<String>,
    ) -> Self {
        self.provider = Some(provider.into());
        self.symbol = Some(symbol.into());
        self.timeframe = Some(timeframe.into());
        self
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

/// A bounded, single-line message safe to present to users.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SanitizedMessage(String);

impl SanitizedMessage {
    pub const MAX_CHARS: usize = 256;

    #[must_use]
    pub fn new(message: &str) -> Self {
        let mut clean = String::with_capacity(message.len().min(Self::MAX_CHARS));
        for (index, character) in message.chars().enumerate() {
            if index == Self::MAX_CHARS {
                break;
            }
            clean.push(if character.is_control() {
                ' '
            } else {
                character
            });
        }
        Self(clean.trim().to_owned())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SanitizedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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
    ReconcileAck,
    StalledWrite,
    ProducerJoin,
    HistoryJoin,
}

impl fmt::Display for TimeoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Request => "request",
            Self::FirstKline => "first kline",
            Self::ReconcileAck => "reconciliation acknowledgement",
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
    #[error("reconciliation acknowledgement timed out")]
    ReconcileAckTimeout,
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
                | Self::Timeout { .. }
                | Self::Transport { .. }
                | Self::ChannelClosed { .. }
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
