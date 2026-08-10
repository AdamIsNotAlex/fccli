//! Binance Spot REST history transport.

use std::{sync::Arc, time::Duration};

use reqwest::{Client, StatusCode, Url, header::RETRY_AFTER};
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    cli::canonicalize_binance,
    clock::{Clock, checked_deadline},
    error::{
        ErrorContext, ErrorOperation, PayloadError, ProviderError, SanitizedCause,
        SanitizedMessage, TimeoutKind,
    },
    model::{
        Candle, HistoryRequest, Instrument, InstrumentSpec, ProcessBlocker, RateGateState,
        Timeframe,
    },
    provider::{RateGateSender, RateGateSnapshot, rate_gate_channel},
};

const KLINES_PATH: &str = "/api/v3/klines";
pub const REST_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const REST_BODY_LIMIT: usize = 2 * 1024 * 1024;
pub const RATE_LIMIT_FALLBACK: Duration = Duration::from_secs(30);

#[cfg(feature = "production-transport")]
const PRODUCTION_REST_BASE: &str = "https://data-api.binance.vision";

#[derive(Clone)]
pub struct BinanceProvider {
    client: Client,
    base_url: Url,
    clock: Arc<dyn Clock>,
    gate_sender: RateGateSender,
    gate_snapshot: RateGateSnapshot,
    body_limit: usize,
    rate_limit_fallback: Duration,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Debug)]
pub struct BinanceTestConfig {
    pub base_url: String,
    pub request_timeout: Duration,
    pub body_limit: usize,
    pub rate_limit_fallback: Duration,
}

#[cfg(feature = "test-transport")]
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
}

impl BinanceProvider {
    #[cfg(feature = "production-transport")]
    pub fn new(clock: Arc<dyn Clock>) -> Result<Self, ProviderError> {
        Self::build(
            Url::parse(PRODUCTION_REST_BASE)
                .map_err(|_| ProviderError::Configuration("invalid production REST base URL"))?,
            clock,
            REST_REQUEST_TIMEOUT,
            REST_BODY_LIMIT,
            RATE_LIMIT_FALLBACK,
        )
    }

    #[cfg(feature = "test-transport")]
    pub fn new_test(
        base_url: impl AsRef<str>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ProviderError> {
        Self::new_test_with_config_and_clock(BinanceTestConfig::loopback(base_url.as_ref()), clock)
    }

    #[cfg(feature = "test-transport")]
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
        )
    }

    fn build(
        base_url: Url,
        clock: Arc<dyn Clock>,
        request_timeout: Duration,
        body_limit: usize,
        rate_limit_fallback: Duration,
    ) -> Result<Self, ProviderError> {
        if request_timeout.is_zero() || body_limit == 0 || rate_limit_fallback.is_zero() {
            return Err(ProviderError::Configuration(
                "REST timeout, body limit, and fallback must be positive",
            ));
        }
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
            base_url,
            clock,
            gate_sender,
            gate_snapshot,
            body_limit,
            rate_limit_fallback,
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

        let mut url = self.base_url.clone();
        url.set_path(KLINES_PATH);
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
            let bytes = match read_capped(response, self.body_limit, &cancellation, &context).await
            {
                Ok(bytes) => bytes,
                Err(error) if is_cancelled(&error) => return Err(error),
                Err(_) => Vec::new(),
            };
            return Err(map_http_error(status, &bytes, context));
        }
        let bytes = read_capped(response, self.body_limit, &cancellation, &context).await?;
        decode_klines(&bytes, request.limit(), context)
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
    pub fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        canonicalize_binance(spec)
            .map_err(|_| ProviderError::Configuration("instrument is not valid for Binance Spot"))
    }

    #[must_use]
    pub fn rate_gate(&self) -> RateGateSnapshot {
        self.gate_snapshot.clone()
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
