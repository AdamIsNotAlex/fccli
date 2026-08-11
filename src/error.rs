//! Provider-neutral, sanitized error types.
//!
//! Network and terminal libraries are deliberately kept out of this module. Callers map
//! implementation errors to [`SanitizedCause`] at the boundary, preventing URLs, headers,
//! payloads, control sequences, or other secrets from escaping through `Display` or `source`.

use std::{error::Error, fmt};

use crate::model::{GapGeneration, Instrument, ProviderId, ReplayRevision, Timeframe};

/// Closed names for provider operations that may be rendered in errors.
///
/// Keeping this list closed prevents request data, headers, credentials, URLs, or provider
/// payloads from being retained as an operation label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorOperation {
    History,
    Rest,
    WebSocket,
    LiveFeed,
    Reconciliation,
    Channel,
}

impl fmt::Display for ErrorOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::History => "history",
            Self::Rest => "REST",
            Self::WebSocket => "websocket",
            Self::LiveFeed => "live feed",
            Self::Reconciliation => "reconciliation",
            Self::Channel => "channel",
        })
    }
}

/// Stable context attached to provider failures without retaining untrusted provider text.
///
/// Every stored value is either a closed enum or derived from validated domain types. Fields
/// remain private so callers cannot bypass that construction boundary.
#[derive(Clone, Eq, PartialEq)]
pub struct ErrorContext {
    provider: Option<ProviderId>,
    instrument: Option<String>,
    timeframe: Option<Timeframe>,
    operation: ErrorOperation,
}

impl ErrorContext {
    #[must_use]
    pub const fn operation(operation: ErrorOperation) -> Self {
        Self {
            provider: None,
            instrument: None,
            timeframe: None,
            operation,
        }
    }

    #[must_use]
    pub fn with_provider(mut self, provider: &ProviderId) -> Self {
        self.provider = Some(provider.clone());
        self
    }

    #[must_use]
    pub fn with_market(mut self, instrument: &Instrument, timeframe: Timeframe) -> Self {
        self.provider = Some(instrument.provider().clone());
        self.instrument = Some(instrument.display_pair().to_owned());
        self.timeframe = Some(timeframe);
        self
    }
}

impl fmt::Debug for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorContext")
            .field("provider", &self.provider)
            .field("instrument", &self.instrument)
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
        if let Some(instrument) = &self.instrument {
            write!(f, ", instrument {instrument}")?;
        }
        if let Some(timeframe) = self.timeframe {
            write!(f, ", timeframe {timeframe}")?;
        }
        Ok(())
    }
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
    WebSocketInactivity,
    ProducerJoin,
    HistoryJoin,
}

impl fmt::Display for TimeoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Request => "request",
            Self::FirstKline => "first kline",
            Self::StalledWrite => "stalled write",
            Self::WebSocketInactivity => "websocket message inactivity",
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
    #[error(
        "interactive mode requires both stdin and stdout to be terminals; run without --interactive to render a snapshot"
    )]
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
    #[error("WebSocket configuration is invalid ({context}): {detail}")]
    WebSocketConfiguration {
        context: ErrorContext,
        detail: &'static str,
    },
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
        match self {
            Self::ServerStatus { .. } | Self::RateLimited { .. } => true,
            Self::Timeout {
                kind: TimeoutKind::Request,
                ..
            } => true,
            Self::Transport { cause, .. } => !matches!(cause, SanitizedCause::Cancelled),
            _ => false,
        }
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
    #[error("{primary}; secondary failure: {secondary}")]
    PrimaryWithSecondary {
        primary: Box<AppError>,
        secondary: Box<AppError>,
    },
}
