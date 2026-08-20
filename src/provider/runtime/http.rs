use std::{sync::Arc, time::Duration};

use reqwest::{Client, RequestBuilder, Response, StatusCode};
use tokio_util::sync::CancellationToken;

use crate::{
    clock::Clock,
    error::{ErrorContext, PayloadError, ProviderError, SanitizedCause, TimeoutKind},
    model::{ProcessBlocker, RateGateState},
    provider::{RateGateSender, RateGateSnapshot, rate_gate_channel},
};

#[derive(Clone)]
pub struct HttpRuntime {
    client: Client,
    clock: Arc<dyn Clock>,
    gate_sender: RateGateSender,
    gate_snapshot: RateGateSnapshot,
    body_limit: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateLimitDecision {
    TimedUntil(crate::model::MonoInstant),
    ProcessBlocked(ProcessBlocker),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusDisposition {
    Success,
    Redirection,
    ClientError,
    ServerError,
}

impl HttpRuntime {
    pub fn new(
        clock: Arc<dyn Clock>,
        request_timeout: Duration,
        body_limit: usize,
    ) -> Result<Self, ProviderError> {
        if request_timeout.is_zero() || body_limit == 0 {
            return Err(ProviderError::Configuration(
                "REST timeout and body limit must be positive",
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
            clock,
            gate_sender,
            gate_snapshot,
            body_limit,
        })
    }

    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    #[must_use]
    pub fn gate_snapshot(&self) -> RateGateSnapshot {
        self.gate_snapshot.clone()
    }

    pub async fn send(
        &self,
        request: RequestBuilder,
        cancellation: &CancellationToken,
        context: &ErrorContext,
    ) -> Result<Response, ProviderError> {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(cancelled(context.clone())),
            result = request.send() => result.map_err(|error| map_transport(error, context)),
        }
    }

    pub async fn read_capped(
        &self,
        mut response: Response,
        cancellation: &CancellationToken,
        context: &ErrorContext,
    ) -> Result<Vec<u8>, ProviderError> {
        if response
            .content_length()
            .is_some_and(|length| length > self.body_limit as u64)
        {
            return Err(payload(
                context,
                PayloadError::OverBudget {
                    limit_bytes: self.body_limit,
                },
            ));
        }
        let mut body = Vec::with_capacity(
            response
                .content_length()
                .unwrap_or(0)
                .min(self.body_limit as u64) as usize,
        );
        loop {
            let chunk = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(cancelled(context.clone())),
                chunk = response.chunk() => chunk.map_err(|error| map_transport(error, context))?,
            };
            let Some(chunk) = chunk else { break };
            if body
                .len()
                .checked_add(chunk.len())
                .is_none_or(|size| size > self.body_limit)
            {
                return Err(payload(
                    context,
                    PayloadError::OverBudget {
                        limit_bytes: self.body_limit,
                    },
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
    pub async fn read_response<F>(
        &self,
        response: Response,
        cancellation: &CancellationToken,
        context: ErrorContext,
        map_client_error: F,
    ) -> Result<Vec<u8>, ProviderError>
    where
        F: FnOnce(StatusCode, &[u8], ErrorContext) -> ProviderError,
    {
        let status = response.status();
        match classify_status(status) {
            StatusDisposition::ServerError => Err(ProviderError::ServerStatus {
                context,
                status: status.as_u16(),
            }),
            StatusDisposition::Redirection => Err(ProviderError::ClientStatus {
                context,
                status: status.as_u16(),
                code: None,
                message: None,
            }),
            StatusDisposition::ClientError => {
                let bytes = match self.read_capped(response, cancellation, &context).await {
                    Ok(bytes) => bytes,
                    Err(error) if is_cancelled(&error) => return Err(error),
                    Err(_) => Vec::new(),
                };
                Err(map_client_error(status, &bytes, context))
            }
            StatusDisposition::Success => self.read_capped(response, cancellation, &context).await,
        }
    }

    pub async fn await_gate<F>(
        &self,
        cancellation: &CancellationToken,
        context: &ErrorContext,
        process_block_error: F,
    ) -> Result<(), ProviderError>
    where
        F: Fn(ProcessBlocker) -> ProviderError,
    {
        let mut snapshot = self.gate_snapshot.clone();
        loop {
            match snapshot
                .current()
                .map_err(|_| ProviderError::Invariant("rate gate closed"))?
            {
                RateGateState::Open => return Ok(()),
                RateGateState::ProcessBlocked(blocker) => return Err(process_block_error(blocker)),
                RateGateState::TimedUntil(deadline) if deadline <= self.clock.now() => {
                    return Ok(());
                }
                RateGateState::TimedUntil(deadline) => tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(cancelled(context.clone())),
                    changed = snapshot.changed() => { changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?; }
                    () = self.clock.sleep_until(deadline) => {}
                },
            }
        }
    }

    pub fn apply_rate_limit(
        &self,
        decision: RateLimitDecision,
        context: ErrorContext,
        status: StatusCode,
    ) -> Result<(), ProviderError> {
        let requested = match decision {
            RateLimitDecision::TimedUntil(deadline) => RateGateState::TimedUntil(deadline),
            RateLimitDecision::ProcessBlocked(blocker) => RateGateState::ProcessBlocked(blocker),
        };
        self.gate_sender
            .publish(requested)
            .map_err(|_| ProviderError::Invariant("rate gate closed"))?;
        match self
            .gate_snapshot
            .current()
            .map_err(|_| ProviderError::Invariant("rate gate closed"))?
        {
            RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry) => {
                Err(ProviderError::InvalidBanExpiry)
            }
            _ => Err(ProviderError::RateLimited {
                context,
                status: status.as_u16(),
            }),
        }
    }
}

#[must_use]
pub fn classify_status(status: StatusCode) -> StatusDisposition {
    if status.is_server_error() {
        StatusDisposition::ServerError
    } else if status.is_redirection() {
        StatusDisposition::Redirection
    } else if status.is_client_error() {
        StatusDisposition::ClientError
    } else {
        StatusDisposition::Success
    }
}

#[must_use]
pub fn is_cancelled(error: &ProviderError) -> bool {
    matches!(
        error,
        ProviderError::Transport {
            cause: SanitizedCause::Cancelled,
            ..
        }
    )
}

fn map_transport(error: reqwest::Error, context: &ErrorContext) -> ProviderError {
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
