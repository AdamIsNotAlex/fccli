//! Older-history request coordination for the interactive application.

use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::{
    chart::ChartViewState,
    clock::Clock,
    error::ProviderError,
    model::{
        Candle, CandleSeries, HistoryRequest, Instrument, MonoInstant, MutationSummary,
        ProcessBlocker, RateGateState, Timeframe,
    },
    provider::{CancellationToken, MarketDataProvider, RateGateSnapshot},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryBoundary {
    Outside,
    Inside,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HistoryProgress {
    Idle,
    RequestStarted,
    PageReady,
    PageApplied,
    EndReached,
    RetryDeferred(MonoInstant),
    Cancelled,
    PermanentlyDisabled,
    TerminalFailure(ProviderError),
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistoryApplyResult {
    pub progress: HistoryProgress,
    pub mutation: Option<MutationSummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryJoinError {
    DeadlineElapsed,
    Aborted,
    JoinFailure,
}

impl HistoryApplyResult {
    #[must_use]
    pub fn changed(&self) -> bool {
        self.mutation
            .as_ref()
            .is_some_and(|summary| summary.inserted != 0 || summary.replaced != 0)
    }
}

impl PartialEq<HistoryProgress> for HistoryApplyResult {
    fn eq(&self, other: &HistoryProgress) -> bool {
        &self.progress == other
    }
}

enum InFlightWake {
    Completed(Result<Result<Vec<Candle>, ProviderError>, tokio::task::JoinError>),
    Cancelled,
    Gate(Result<RateGateState, crate::provider::RateGateClosed>),
}

/// Owns history scheduling state, but never owns the candle store or chart view.
pub struct HistoryCoordinator {
    provider: Arc<dyn MarketDataProvider>,
    instrument: Instrument,
    timeframe: Timeframe,
    clock: Arc<dyn Clock>,
    gate: RateGateSnapshot,
    cancellation: CancellationToken,
    boundary: HistoryBoundary,
    /// Allows one same-boundary re-evaluation after an accepted page changes the series.
    /// Ordinary recoverable failures leave this disarmed until a genuine re-cross.
    boundary_recheck_armed: bool,
    oldest_open_time: Option<i64>,
    in_flight: Option<JoinHandle<Result<Vec<Candle>, ProviderError>>>,
    completed_page: Option<Vec<Candle>>,
    retry_deadline: Option<MonoInstant>,
    end_latched: bool,
    process_blocker: Option<ProcessBlocker>,
    client_disabled: bool,
    terminal_disabled: bool,
    last_error: Option<ProviderError>,
}

impl HistoryCoordinator {
    #[must_use]
    pub fn new(
        provider: Arc<dyn MarketDataProvider>,
        instrument: Instrument,
        timeframe: Timeframe,
        clock: Arc<dyn Clock>,
        cancellation: CancellationToken,
    ) -> Self {
        let gate = provider.rate_gate();
        Self {
            provider,
            instrument,
            timeframe,
            clock,
            gate,
            cancellation,
            boundary: HistoryBoundary::Outside,
            boundary_recheck_armed: false,
            oldest_open_time: None,
            in_flight: None,
            completed_page: None,
            retry_deadline: None,
            end_latched: false,
            process_blocker: None,
            client_disabled: false,
            terminal_disabled: false,
            last_error: None,
        }
    }

    #[must_use]
    pub const fn threshold(loaded_len: usize) -> usize {
        if loaded_len == 0 {
            0
        } else {
            loaded_len.div_ceil(10)
        }
    }

    #[must_use]
    pub const fn in_flight(&self) -> bool {
        self.in_flight.is_some() || self.completed_page.is_some()
    }
    #[must_use]
    pub const fn has_completed_page(&self) -> bool {
        self.completed_page.is_some()
    }
    /// Returns whether the coordinator still owns an actual spawned history task.
    ///
    /// This is intentionally narrower than [`Self::in_flight`], which also includes a
    /// completed page retained until the App can apply it.
    #[must_use]
    pub const fn has_owned_task(&self) -> bool {
        self.in_flight.is_some()
    }
    #[must_use]
    pub const fn retry_deadline(&self) -> Option<MonoInstant> {
        self.retry_deadline
    }
    #[must_use]
    pub const fn end_latched(&self) -> bool {
        self.end_latched
    }
    #[must_use]
    pub const fn process_blocker(&self) -> Option<ProcessBlocker> {
        self.process_blocker
    }
    #[must_use]
    pub const fn client_disabled(&self) -> bool {
        self.client_disabled
    }
    #[must_use]
    pub const fn terminal_disabled(&self) -> bool {
        self.terminal_disabled
    }
    #[must_use]
    pub fn last_error(&self) -> Option<&ProviderError> {
        self.last_error.as_ref()
    }

    /// Requests shutdown without relinquishing ownership of an in-flight task.
    pub fn request_shutdown(&self) {
        self.cancellation.cancel();
    }

    /// Waits for the owned request task until an absolute injected-clock deadline.
    ///
    /// The handle remains in `self` until termination is observed, so cancelling this join
    /// future cannot detach the task. A deadline abort is also awaited before returning.
    pub async fn join(&mut self, deadline: MonoInstant) -> Result<(), HistoryJoinError> {
        self.request_shutdown();
        self.retry_deadline = None;
        self.completed_page = None;
        let clock = Arc::clone(&self.clock);
        let outcome = {
            let Some(task) = self.in_flight.as_mut() else {
                return Ok(());
            };
            tokio::select! {
                biased;
                result = task => Some(result),
                () = clock.sleep_until(deadline) => None,
            }
        };

        match outcome {
            Some(result) => {
                self.in_flight = None;
                match result {
                    Ok(_) => Ok(()),
                    Err(error) if error.is_cancelled() => Err(HistoryJoinError::Aborted),
                    Err(_) => Err(HistoryJoinError::JoinFailure),
                }
            }
            None => {
                let task = self
                    .in_flight
                    .as_mut()
                    .expect("history task remains owned through deadline cleanup");
                task.abort();
                let _ = task.await;
                self.in_flight = None;
                Err(HistoryJoinError::DeadlineElapsed)
            }
        }
    }

    /// Updates the visible-left boundary and starts at most one request.
    pub fn update_boundary(
        &mut self,
        visible_left_source_index: usize,
        series: &CandleSeries,
    ) -> HistoryProgress {
        let was_inside = self.boundary == HistoryBoundary::Inside;
        self.oldest_open_time = series.oldest_open_time();
        let threshold = Self::threshold(series.len());
        let next = if threshold != 0 && visible_left_source_index < threshold {
            HistoryBoundary::Inside
        } else {
            HistoryBoundary::Outside
        };
        self.boundary = next;
        if next == HistoryBoundary::Outside {
            self.boundary_recheck_armed = false;
            self.retry_deadline = None;
            return HistoryProgress::Idle;
        }
        if self.cancellation.is_cancelled() {
            return HistoryProgress::Cancelled;
        }
        if self.client_disabled || self.terminal_disabled || self.process_blocker.is_some() {
            return HistoryProgress::PermanentlyDisabled;
        }
        let crossed_inside = !was_inside;
        if !crossed_inside && !self.boundary_recheck_armed {
            return HistoryProgress::Idle;
        }
        self.boundary_recheck_armed = false;
        self.try_start()
    }

    /// Waits for the currently actionable request or gate state without borrowing App state.
    pub async fn drive(&mut self) -> HistoryProgress {
        loop {
            if self.cancellation.is_cancelled() {
                self.abort_in_flight_and_wait().await;
                return HistoryProgress::Cancelled;
            }
            if self.completed_page.is_some() {
                return HistoryProgress::PageReady;
            }

            if self.in_flight.is_some() {
                let cancellation = self.cancellation.clone();
                let wake = {
                    let task = self.in_flight.as_mut().expect("checked above");
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => InFlightWake::Cancelled,
                        changed = self.gate.changed() => InFlightWake::Gate(changed),
                        result = task => InFlightWake::Completed(result),
                    }
                };
                let result = match wake {
                    InFlightWake::Cancelled => {
                        self.abort_in_flight_and_wait().await;
                        return HistoryProgress::Cancelled;
                    }
                    InFlightWake::Gate(Ok(RateGateState::ProcessBlocked(blocker))) => {
                        self.abort_in_flight_and_wait().await;
                        return self.disable_blocked(blocker, ProviderError::InvalidBanExpiry);
                    }
                    InFlightWake::Gate(Err(_)) => {
                        self.abort_in_flight_and_wait().await;
                        return self.terminal_failure(ProviderError::Invariant("rate gate closed"));
                    }
                    InFlightWake::Gate(Ok(RateGateState::Open | RateGateState::TimedUntil(_))) => {
                        continue;
                    }
                    InFlightWake::Completed(result) => result,
                };
                self.in_flight = None;
                return match result {
                    Ok(Ok(page)) => {
                        self.completed_page = Some(page);
                        HistoryProgress::PageReady
                    }
                    Ok(Err(error)) => self.handle_error(error),
                    Err(error) if error.is_cancelled() => HistoryProgress::Cancelled,
                    Err(_) => {
                        self.terminal_failure(ProviderError::Invariant("history task failed"))
                    }
                };
            }

            let Some(mut deadline) = self.retry_deadline else {
                return HistoryProgress::Idle;
            };
            if self.boundary == HistoryBoundary::Outside {
                self.retry_deadline = None;
                return HistoryProgress::Idle;
            }

            match self.observe_gate() {
                Ok(RateGateState::TimedUntil(observed)) => {
                    deadline = deadline.max(observed);
                    self.retry_deadline = Some(deadline);
                }
                Ok(RateGateState::ProcessBlocked(blocker)) => {
                    return self.disable_blocked(blocker, ProviderError::InvalidBanExpiry);
                }
                Ok(RateGateState::Open) => {}
                Err(error) => return self.terminal_failure(error),
            }

            if self.clock.now() >= deadline {
                self.retry_deadline = None;
                return self.try_start();
            }

            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => {
                    self.cancel_in_flight();
                    return HistoryProgress::Cancelled;
                }
                changed = self.gate.changed() => {
                    match changed {
                        Ok(RateGateState::TimedUntil(extended)) => {
                            self.retry_deadline = Some(deadline.max(extended));
                        }
                        Ok(RateGateState::ProcessBlocked(blocker)) => {
                            return self.disable_blocked(blocker, ProviderError::InvalidBanExpiry);
                        }
                        Ok(RateGateState::Open) => {}
                        Err(_) => {
                            return self.terminal_failure(ProviderError::Invariant("rate gate closed"));
                        }
                    }
                }
                () = self.clock.sleep_until(deadline) => {}
            }
        }
    }

    fn try_start(&mut self) -> HistoryProgress {
        if self.cancellation.is_cancelled() {
            return HistoryProgress::Cancelled;
        }
        if self.in_flight.is_some()
            || self.completed_page.is_some()
            || self.end_latched
            || self.client_disabled
            || self.terminal_disabled
            || self.process_blocker.is_some()
            || self.boundary == HistoryBoundary::Outside
        {
            return if self.client_disabled
                || self.terminal_disabled
                || self.process_blocker.is_some()
            {
                HistoryProgress::PermanentlyDisabled
            } else {
                HistoryProgress::Idle
            };
        }
        let Some(oldest) = self.oldest_open_time else {
            return HistoryProgress::Idle;
        };
        let capabilities = self.provider.capabilities();
        if !capabilities.markets.contains(&self.instrument.market()) {
            return self.terminal_failure(ProviderError::Configuration(
                "provider does not support market",
            ));
        }
        if !capabilities.timeframes.contains(&self.timeframe) {
            return self.terminal_failure(ProviderError::Configuration(
                "provider does not support timeframe",
            ));
        }
        if capabilities.history_page_limit == 0 {
            return self.terminal_failure(ProviderError::Configuration(
                "provider history page limit must be non-zero",
            ));
        }
        match self.observe_gate() {
            Ok(RateGateState::Open) => {}
            Ok(RateGateState::TimedUntil(deadline)) if deadline > self.clock.now() => {
                self.retry_deadline = Some(
                    self.retry_deadline
                        .map_or(deadline, |current| current.max(deadline)),
                );
                return HistoryProgress::RetryDeferred(self.retry_deadline.expect("set above"));
            }
            Ok(RateGateState::TimedUntil(_)) => {}
            Ok(RateGateState::ProcessBlocked(blocker)) => {
                return self.disable_blocked(blocker, ProviderError::InvalidBanExpiry);
            }
            Err(error) => return self.terminal_failure(error),
        }
        let request = match HistoryRequest::older(oldest, capabilities.history_page_limit) {
            Ok(request) => request,
            Err(_) => {
                return self
                    .disable_terminal(ProviderError::Invariant("invalid older-history boundary"));
            }
        };
        let provider = Arc::clone(&self.provider);
        let instrument = self.instrument.clone();
        let timeframe = self.timeframe;
        let cancellation = self.cancellation.clone();
        self.in_flight = Some(tokio::spawn(async move {
            provider
                .history(&instrument, timeframe, request, cancellation)
                .await
        }));
        HistoryProgress::RequestStarted
    }

    /// Applies the page produced by [`Self::drive`] synchronously.
    pub fn apply_completed(
        &mut self,
        series: &mut CandleSeries,
        view: &mut ChartViewState,
        plot_width: usize,
    ) -> HistoryApplyResult {
        let Some(page) = self.completed_page.take() else {
            return HistoryApplyResult {
                progress: HistoryProgress::Idle,
                mutation: None,
            };
        };
        if self.cancellation.is_cancelled() {
            return HistoryApplyResult {
                progress: HistoryProgress::Cancelled,
                mutation: None,
            };
        }
        let previous_oldest = series.oldest_open_time();
        let summary = series.merge(page);
        view.apply_mutation(series, &summary, plot_width);
        self.oldest_open_time = series.oldest_open_time();
        self.last_error = None;
        self.retry_deadline = None;
        let advanced_older = matches!(
            (previous_oldest, self.oldest_open_time),
            (Some(previous), Some(current)) if current < previous
        );
        let progress = if summary.empty_input
            || summary.duplicate_only
            || summary.no_progress
            || !advanced_older
        {
            self.end_latched = true;
            HistoryProgress::EndReached
        } else {
            self.end_latched = false;
            // The inserted page changes both the threshold and source-index mapping. Allow
            // exactly one canonical re-evaluation even when the viewport remains inside.
            self.boundary_recheck_armed = true;
            HistoryProgress::PageApplied
        };
        HistoryApplyResult {
            progress,
            mutation: Some(summary),
        }
    }

    fn handle_error(&mut self, error: ProviderError) -> HistoryProgress {
        self.last_error = Some(error.clone());
        match error {
            ProviderError::InvalidBanExpiry => {
                self.disable_blocked(ProcessBlocker::InvalidBanExpiry, error)
            }
            ProviderError::ClientStatus { .. } => {
                self.retry_deadline = None;
                self.client_disabled = true;
                HistoryProgress::PermanentlyDisabled
            }
            _ if error.is_recoverable_for_history() => {
                // A timed shared gate owns one bounded retry. With an open gate, do not turn
                // repeated Inside observations into an unbounded request loop: only a later
                // Outside -> Inside crossing may start again.
                self.boundary_recheck_armed = false;
                if self.boundary == HistoryBoundary::Inside {
                    match self.observe_gate() {
                        Ok(RateGateState::TimedUntil(deadline)) => {
                            self.retry_deadline = Some(
                                self.retry_deadline
                                    .map_or(deadline, |old| old.max(deadline)),
                            );
                            return HistoryProgress::RetryDeferred(
                                self.retry_deadline.expect("set above"),
                            );
                        }
                        Err(error) => return self.terminal_failure(error),
                        Ok(RateGateState::Open | RateGateState::ProcessBlocked(_)) => {}
                    }
                }
                HistoryProgress::Idle
            }
            _ => {
                self.retry_deadline = None;
                self.terminal_disabled = true;
                HistoryProgress::PermanentlyDisabled
            }
        }
    }

    fn observe_gate(&self) -> Result<RateGateState, ProviderError> {
        self.gate
            .current()
            .map_err(|_| ProviderError::Invariant("rate gate closed"))
    }

    fn disable_blocked(
        &mut self,
        blocker: ProcessBlocker,
        error: ProviderError,
    ) -> HistoryProgress {
        self.retry_deadline = None;
        self.process_blocker = Some(blocker);
        self.last_error = Some(error);
        HistoryProgress::PermanentlyDisabled
    }

    fn disable_terminal(&mut self, error: ProviderError) -> HistoryProgress {
        self.retry_deadline = None;
        self.terminal_disabled = true;
        self.last_error = Some(error);
        HistoryProgress::PermanentlyDisabled
    }

    fn terminal_failure(&mut self, error: ProviderError) -> HistoryProgress {
        self.retry_deadline = None;
        self.terminal_disabled = true;
        self.last_error = Some(error.clone());
        HistoryProgress::TerminalFailure(error)
    }

    async fn abort_in_flight_and_wait(&mut self) {
        self.retry_deadline = None;
        let Some(task) = self.in_flight.as_mut() else {
            return;
        };
        task.abort();
        let _ = task.await;
        self.in_flight = None;
    }

    fn cancel_in_flight(&mut self) {
        self.retry_deadline = None;
        if let Some(task) = self.in_flight.take() {
            task.abort();
        }
    }
}

impl Drop for HistoryCoordinator {
    fn drop(&mut self) {
        self.cancel_in_flight();
    }
}
