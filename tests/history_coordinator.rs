use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, mpsc},
};

use fccli::{
    chart::ChartViewState,
    clock::ManualClock,
    error::{ErrorContext, ErrorOperation, ProviderError, SanitizedCause, TimeoutKind},
    history::{HISTORY_PAGE_LIMIT, HistoryCoordinator, HistoryJoinError, HistoryProgress},
    model::{
        Candle, CandleSeries, HistoryRequest, Instrument, InstrumentSpec, Market, MonoInstant,
        ProcessBlocker, ProviderId, RateGateState, Timeframe,
    },
    provider::{
        CancellationToken, LiveFeed, LiveRequest, MarketDataProvider, ProviderFuture,
        RateGateSender, RateGateSnapshot, rate_gate_channel,
    },
};
use tokio::sync::oneshot;

enum Response {
    Ready(Result<Vec<Candle>, ProviderError>),
    Pending(oneshot::Receiver<Result<Vec<Candle>, ProviderError>>),
    Panic,
    SignaledPanic(oneshot::Sender<()>),
    BlockingPanic {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    },
}

#[derive(Clone)]
struct FakeProvider {
    responses: Arc<Mutex<VecDeque<Response>>>,
    requests: Arc<Mutex<Vec<HistoryRequest>>>,
    gate_sender: Arc<Mutex<Option<RateGateSender>>>,
    gate: RateGateSnapshot,
}

impl FakeProvider {
    fn new(responses: impl IntoIterator<Item = Response>) -> Self {
        let (sender, gate) = rate_gate_channel(RateGateState::Open);
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
            gate_sender: Arc::new(Mutex::new(Some(sender))),
            gate,
        }
    }

    fn publish(&self, state: RateGateState) {
        self.gate_sender
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .publish(state)
            .unwrap();
    }

    fn close_gate(&self) {
        self.gate_sender.lock().unwrap().take();
    }
    fn requests(&self) -> Vec<HistoryRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl MarketDataProvider for FakeProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("binance").unwrap()
    }
    fn canonicalize(&self, _spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        Ok(instrument())
    }
    fn history<'a>(
        &'a self,
        _instrument: &'a Instrument,
        _timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>> {
        self.requests.lock().unwrap().push(request);
        let response = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("queued response");
        Box::pin(async move {
            match response {
                Response::Ready(result) => result,
                Response::Pending(receiver) => tokio::select! {
                    biased;
                    () = cancellation.cancelled() => Err(ProviderError::Transport { context: context(), cause: SanitizedCause::Cancelled }),
                    result = receiver => result.expect("test response sender"),
                },
                Response::Panic => panic!("provider task panic must be sanitized"),
                Response::SignaledPanic(panicked) => {
                    struct SignalOnDrop(Option<oneshot::Sender<()>>);
                    impl Drop for SignalOnDrop {
                        fn drop(&mut self) {
                            if let Some(sender) = self.0.take() {
                                let _ = sender.send(());
                            }
                        }
                    }
                    let _signal = SignalOnDrop(Some(panicked));
                    panic!("provider task panic must lose to authoritative control state");
                }
                Response::BlockingPanic { entered, release } => {
                    entered.send(()).expect("cleanup race observer");
                    release.recv().expect("release blocked provider task");
                    panic!("provider task panic after cleanup cancellation");
                }
            }
        })
    }
    fn open_live<'a>(&'a self, _request: LiveRequest) -> ProviderFuture<'a, LiveFeed> {
        Box::pin(async { Err(ProviderError::Configuration("unused")) })
    }
    fn rate_gate(&self) -> RateGateSnapshot {
        self.gate.clone()
    }
}

fn context() -> ErrorContext {
    ErrorContext::operation(ErrorOperation::History).with_market(&instrument(), Timeframe::Minute1)
}
fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("binance").unwrap(),
        Market::Spot,
        "BTC",
        "USDT",
        "BTCUSDT",
    )
    .unwrap()
}
fn candle(open_time: i64) -> Candle {
    Candle::from_rest(open_time, open_time + 59_999, 10.0, 11.0, 9.0, 10.5, 1.0).unwrap()
}
fn initial_series(len: usize) -> CandleSeries {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let candles = (0..len)
        .map(|index| candle(1_000_000 + i64::try_from(index).unwrap() * 60_000))
        .collect();
    series.replace(candles).unwrap();
    series
}

struct Harness {
    provider: Arc<FakeProvider>,
    clock: Arc<ManualClock>,
    cancellation: CancellationToken,
    coordinator: HistoryCoordinator,
    series: CandleSeries,
    view: ChartViewState,
}

fn harness(responses: impl IntoIterator<Item = Response>, len: usize) -> Harness {
    let provider = Arc::new(FakeProvider::new(responses));
    let clock = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let cancellation = CancellationToken::new();
    let series = initial_series(len);
    let view = ChartViewState::interactive(&series, 20);
    let coordinator = HistoryCoordinator::new(
        provider.clone(),
        instrument(),
        Timeframe::Minute1,
        clock.clone(),
        cancellation.clone(),
    );
    Harness {
        provider,
        clock,
        cancellation,
        coordinator,
        series,
        view,
    }
}

#[test]
fn threshold_is_exact_ceiling_ten_percent_and_empty_never_triggers() {
    assert_eq!(HistoryCoordinator::threshold(0), 0);
    assert_eq!(HistoryCoordinator::threshold(1), 1);
    assert_eq!(HistoryCoordinator::threshold(10), 1);
    assert_eq!(HistoryCoordinator::threshold(11), 2);
    assert_eq!(
        HistoryCoordinator::threshold(usize::MAX),
        usize::MAX.div_ceil(10)
    );
    let mut h = harness([], 0);
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::Idle
    );
    assert!(h.provider.requests().is_empty());
}

#[tokio::test]
async fn crossing_boundary_requests_checked_oldest_minus_one_limit_1000_and_applies_page() {
    let mut h = harness([Response::Ready(Ok(vec![candle(940_000)]))], 10);
    assert_eq!(
        h.coordinator.update_boundary(1, &h.series),
        HistoryProgress::Idle
    );
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::Idle
    );
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(
        h.coordinator.drive().await,
        HistoryProgress::PageReady,
        "a completed page remains actionable until the caller can apply it"
    );
    let applied = h
        .coordinator
        .apply_completed(&mut h.series, &mut h.view, 20);
    assert_eq!(applied.progress, HistoryProgress::PageApplied);
    assert!(applied.changed());
    let summary = applied
        .mutation
        .as_ref()
        .expect("completed page has a mutation summary");
    assert_eq!((summary.inserted, summary.replaced), (1, 0));
    let requests = h.provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].end_time(), Some(999_999));
    assert_eq!(requests[0].limit(), HISTORY_PAGE_LIMIT);
    assert_eq!(h.series.oldest_open_time(), Some(940_000));
}

#[tokio::test]
async fn single_flight_coalesces_repeated_triggers() {
    let (tx, rx) = oneshot::channel();
    let mut h = harness([Response::Pending(rx)], 20);
    assert_eq!(
        h.coordinator.update_boundary(1, &h.series),
        HistoryProgress::RequestStarted
    );
    for _ in 0..5 {
        assert_eq!(
            h.coordinator.update_boundary(0, &h.series),
            HistoryProgress::Idle
        );
    }
    tokio::task::yield_now().await;
    assert_eq!(h.provider.requests().len(), 1);
    tx.send(Ok(vec![candle(940_000)])).unwrap();
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::PageApplied
    );
}

#[tokio::test]
async fn empty_and_duplicate_pages_latch_end_and_unrelated_insertions_do_not_reenable() {
    let mut h = harness([Response::Ready(Ok(vec![]))], 10);
    h.coordinator.update_boundary(0, &h.series);
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::EndReached
    );
    assert!(h.coordinator.end_latched());
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::Idle
    );
    assert_eq!(h.provider.requests().len(), 1);

    let duplicate = h.series.get(0).unwrap().clone();
    let mut duplicate_h = harness([Response::Ready(Ok(vec![duplicate]))], 10);
    duplicate_h
        .coordinator
        .update_boundary(0, &duplicate_h.series);
    assert_eq!(
        duplicate_h.coordinator.drive().await,
        HistoryProgress::PageReady
    );
    let duplicate_apply =
        duplicate_h
            .coordinator
            .apply_completed(&mut duplicate_h.series, &mut duplicate_h.view, 20);
    assert_eq!(duplicate_apply.progress, HistoryProgress::EndReached);
    assert!(!duplicate_apply.changed());
    let duplicate_summary = duplicate_apply
        .mutation
        .as_ref()
        .expect("duplicate page has a mutation summary");
    assert_eq!(
        (duplicate_summary.inserted, duplicate_summary.replaced),
        (0, 0)
    );
    assert!(duplicate_h.coordinator.end_latched());

    let mut latched = harness([Response::Ready(Ok(vec![]))], 10);
    latched.coordinator.update_boundary(0, &latched.series);
    assert_eq!(
        latched.coordinator.drive().await,
        HistoryProgress::PageReady
    );
    assert_eq!(
        latched
            .coordinator
            .apply_completed(&mut latched.series, &mut latched.view, 20),
        HistoryProgress::EndReached
    );

    let insertion = latched.series.merge(vec![candle(880_000)]);
    assert_eq!((insertion.inserted, insertion.replaced), (1, 0));
    latched.view.apply_mutation(&latched.series, &insertion, 20);
    assert_eq!(
        latched.coordinator.update_boundary(0, &latched.series),
        HistoryProgress::Idle,
        "only a coordinator-owned accepted page may clear the end latch"
    );
    assert!(latched.coordinator.end_latched());
    assert_eq!(latched.provider.requests().len(), 1);
}

#[tokio::test]
async fn timed_rate_limit_defers_rereads_and_extends_shared_gate_then_retries_once() {
    let error = ProviderError::RateLimited {
        context: context(),
        status: 429,
    };
    let mut h = harness(
        [
            Response::Ready(Err(error)),
            Response::Ready(Ok(vec![candle(940_000)])),
        ],
        10,
    );
    let first = MonoInstant::from_nanos(30);
    let extended = MonoInstant::from_nanos(90);
    h.provider.publish(RateGateState::TimedUntil(first));
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RetryDeferred(first)
    );
    h.provider.publish(RateGateState::TimedUntil(extended));
    h.clock.advance_to(first).unwrap();
    let mut drive = Box::pin(h.coordinator.drive());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut drive)
            .await
            .is_err()
    );
    h.clock.advance_to(extended).unwrap();
    assert_eq!(drive.await, HistoryProgress::RequestStarted);
    assert_eq!(
        h.coordinator.drive().await,
        HistoryProgress::RetryDeferred(extended)
    );
    h.clock.advance_to(extended).unwrap();
    assert_eq!(h.coordinator.drive().await, HistoryProgress::RequestStarted);
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::PageApplied
    );
    assert_eq!(h.provider.requests().len(), 2);
}

#[tokio::test]
async fn leaving_boundary_disarms_deferred_retry() {
    let mut h = harness([], 10);
    let deadline = MonoInstant::from_nanos(30);
    h.provider.publish(RateGateState::TimedUntil(deadline));
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RetryDeferred(deadline)
    );
    assert_eq!(
        h.coordinator.update_boundary(2, &h.series),
        HistoryProgress::Idle
    );
    h.clock.advance_to(deadline).unwrap();
    assert_eq!(h.coordinator.drive().await, HistoryProgress::Idle);
    assert!(h.provider.requests().is_empty());
}

#[tokio::test]
async fn client_4xx_is_permanent_and_preserves_accepted_series_and_view() {
    let error = ProviderError::ClientStatus {
        context: context(),
        status: 403,
        code: None,
        message: None,
    };
    let mut h = harness([Response::Ready(Err(error))], 10);
    let before = h.series.iter().cloned().collect::<Vec<_>>();
    let view_before = h.view.clone();
    h.coordinator.update_boundary(0, &h.series);
    assert_eq!(
        h.coordinator.drive().await,
        HistoryProgress::PermanentlyDisabled
    );
    assert!(h.coordinator.client_disabled());
    assert_eq!(h.series.iter().cloned().collect::<Vec<_>>(), before);
    assert_eq!(h.view, view_before);
    h.coordinator.update_boundary(2, &h.series);
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::PermanentlyDisabled
    );
    assert_eq!(h.provider.requests().len(), 1);
}

#[tokio::test]
async fn invalid_ban_and_process_block_are_nonfatal_but_closed_observer_is_terminal() {
    let mut invalid = harness([Response::Ready(Err(ProviderError::InvalidBanExpiry))], 10);
    invalid.coordinator.update_boundary(0, &invalid.series);
    assert_eq!(
        invalid.coordinator.drive().await,
        HistoryProgress::PermanentlyDisabled
    );
    assert_eq!(
        invalid.coordinator.process_blocker(),
        Some(ProcessBlocker::InvalidBanExpiry)
    );

    let mut blocked = harness([], 10);
    blocked.provider.publish(RateGateState::ProcessBlocked(
        ProcessBlocker::InvalidBanExpiry,
    ));
    assert_eq!(
        blocked.coordinator.update_boundary(0, &blocked.series),
        HistoryProgress::PermanentlyDisabled
    );

    let mut closed = harness([], 10);
    closed.provider.close_gate();
    assert_eq!(
        closed.coordinator.update_boundary(0, &closed.series),
        HistoryProgress::TerminalFailure(ProviderError::Invariant("rate gate closed"))
    );
    assert!(closed.coordinator.terminal_disabled());
    assert!(matches!(
        closed.coordinator.last_error(),
        Some(ProviderError::Invariant("rate gate closed"))
    ));
}

#[tokio::test]
async fn only_recoverable_errors_can_retry_and_generic_nonrecoverable_is_terminal() {
    let recoverable = ProviderError::Timeout {
        context: context(),
        kind: TimeoutKind::Request,
    };
    let mut retry = harness(
        [
            Response::Ready(Err(recoverable)),
            Response::Ready(Ok(vec![candle(940_000)])),
        ],
        10,
    );
    retry.coordinator.update_boundary(0, &retry.series);
    assert_eq!(retry.coordinator.drive().await, HistoryProgress::Idle);
    retry.coordinator.update_boundary(2, &retry.series);
    assert_eq!(
        retry.coordinator.update_boundary(0, &retry.series),
        HistoryProgress::RequestStarted
    );
    assert_eq!(retry.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(
        retry
            .coordinator
            .apply_completed(&mut retry.series, &mut retry.view, 20),
        HistoryProgress::PageApplied
    );

    let mut terminal = harness(
        [Response::Ready(Err(ProviderError::Protocol {
            context: context(),
            detail: "bad frame",
        }))],
        10,
    );
    terminal.coordinator.update_boundary(0, &terminal.series);
    assert_eq!(
        terminal.coordinator.drive().await,
        HistoryProgress::PermanentlyDisabled
    );
    assert!(terminal.coordinator.terminal_disabled());
    assert_eq!(
        terminal.coordinator.update_boundary(0, &terminal.series),
        HistoryProgress::PermanentlyDisabled
    );
}

#[tokio::test]
async fn recoverable_open_gate_repeated_inside_updates_do_not_restart() {
    let recoverable = ProviderError::Timeout {
        context: context(),
        kind: TimeoutKind::Request,
    };
    let mut h = harness([Response::Ready(Err(recoverable))], 10);

    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    assert_eq!(h.coordinator.drive().await, HistoryProgress::Idle);
    for _ in 0..5 {
        assert_eq!(
            h.coordinator.update_boundary(0, &h.series),
            HistoryProgress::Idle
        );
    }
    assert_eq!(
        h.provider.requests().len(),
        1,
        "inside updates must not loop requests"
    );
}

#[tokio::test]
async fn recoverable_open_gate_recrosses_and_page_insertion_arms_one_recheck() {
    let recoverable = ProviderError::Timeout {
        context: context(),
        kind: TimeoutKind::Request,
    };
    let mut h = harness(
        [
            Response::Ready(Err(recoverable)),
            Response::Ready(Ok(vec![candle(940_000)])),
            Response::Ready(Ok(vec![candle(880_000)])),
        ],
        10,
    );

    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    assert_eq!(h.coordinator.drive().await, HistoryProgress::Idle);
    assert_eq!(h.provider.requests().len(), 1);

    assert_eq!(
        h.coordinator.update_boundary(2, &h.series),
        HistoryProgress::Idle
    );
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::PageApplied
    );

    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted,
        "an inserted page must arm one canonical same-boundary re-evaluation"
    );
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::Idle
    );
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(h.provider.requests().len(), 3);
}

#[tokio::test]
async fn cancellation_aborts_inflight_and_never_commits_late_page() {
    let (tx, rx) = oneshot::channel();
    let mut h = harness([Response::Pending(rx)], 10);
    let before = h.series.iter().cloned().collect::<Vec<_>>();
    h.coordinator.update_boundary(0, &h.series);
    tokio::task::yield_now().await;
    h.cancellation.cancel();
    assert_eq!(h.coordinator.drive().await, HistoryProgress::Cancelled);
    assert!(
        tx.send(Ok(vec![candle(940_000)])).is_err(),
        "drive must await task termination and drop the provider receiver before returning"
    );
    assert_eq!(h.series.iter().cloned().collect::<Vec<_>>(), before);
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::Cancelled
    );
}

#[tokio::test]
async fn provider_task_panic_becomes_sanitized_terminal_state() {
    let mut h = harness([Response::Panic], 10);
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    assert_eq!(
        h.coordinator.drive().await,
        HistoryProgress::TerminalFailure(ProviderError::Invariant("history task failed"))
    );
    assert!(h.coordinator.terminal_disabled());
    assert!(matches!(
        h.coordinator.last_error(),
        Some(ProviderError::Invariant("history task failed"))
    ));
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::PermanentlyDisabled,
        "the fatal task failure is emitted exactly once"
    );
}

#[tokio::test]
async fn already_panicked_task_loses_to_cancellation_block_and_gate_closure() {
    let (cancel_panicked_tx, cancel_panicked_rx) = oneshot::channel();
    let mut cancelled = harness([Response::SignaledPanic(cancel_panicked_tx)], 10);
    assert_eq!(
        cancelled.coordinator.update_boundary(0, &cancelled.series),
        HistoryProgress::RequestStarted
    );
    cancel_panicked_rx
        .await
        .expect("provider task reached panic");
    tokio::task::yield_now().await;
    cancelled.cancellation.cancel();
    assert_eq!(
        cancelled.coordinator.drive().await,
        HistoryProgress::Cancelled
    );
    assert!(!cancelled.coordinator.terminal_disabled());

    let (block_panicked_tx, block_panicked_rx) = oneshot::channel();
    let mut blocked = harness([Response::SignaledPanic(block_panicked_tx)], 10);
    assert_eq!(
        blocked.coordinator.update_boundary(0, &blocked.series),
        HistoryProgress::RequestStarted
    );
    block_panicked_rx
        .await
        .expect("provider task reached panic");
    tokio::task::yield_now().await;
    blocked.provider.publish(RateGateState::ProcessBlocked(
        ProcessBlocker::InvalidBanExpiry,
    ));
    assert_eq!(
        blocked.coordinator.drive().await,
        HistoryProgress::PermanentlyDisabled
    );
    assert_eq!(
        blocked.coordinator.process_blocker(),
        Some(ProcessBlocker::InvalidBanExpiry)
    );
    assert!(matches!(
        blocked.coordinator.last_error(),
        Some(ProviderError::InvalidBanExpiry)
    ));

    let (closed_panicked_tx, closed_panicked_rx) = oneshot::channel();
    let mut closed = harness([Response::SignaledPanic(closed_panicked_tx)], 10);
    assert_eq!(
        closed.coordinator.update_boundary(0, &closed.series),
        HistoryProgress::RequestStarted
    );
    closed_panicked_rx
        .await
        .expect("provider task reached panic");
    tokio::task::yield_now().await;
    closed.provider.close_gate();
    assert_eq!(
        closed.coordinator.drive().await,
        HistoryProgress::TerminalFailure(ProviderError::Invariant("rate gate closed"))
    );
    assert!(closed.coordinator.terminal_disabled());
    assert!(matches!(
        closed.coordinator.last_error(),
        Some(ProviderError::Invariant("rate gate closed"))
    ));
    assert_eq!(
        closed.coordinator.update_boundary(0, &closed.series),
        HistoryProgress::PermanentlyDisabled,
        "the fatal gate closure is emitted exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_drive_during_abort_wait_retains_task_ownership() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut h = harness(
        [Response::BlockingPanic {
            entered: entered_tx,
            release: release_rx,
        }],
        10,
    );
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    entered_rx
        .recv()
        .expect("provider task entered blocking poll");
    h.cancellation.cancel();

    {
        let mut drive = Box::pin(h.coordinator.drive());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut drive)
                .await
                .is_err(),
            "abort cleanup must wait for the running task to leave its poll"
        );
    }
    assert!(
        h.coordinator.in_flight(),
        "dropping drive during cleanup must leave the JoinHandle owned by the coordinator"
    );

    release_tx.send(()).expect("release provider task");
    assert_eq!(h.coordinator.drive().await, HistoryProgress::Cancelled);
    assert!(!h.coordinator.in_flight());
    assert!(!h.coordinator.terminal_disabled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_history_join_retains_handle_until_aborted_task_terminates() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut h = harness(
        [Response::BlockingPanic {
            entered: entered_tx,
            release: release_rx,
        }],
        10,
    );
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    entered_rx
        .recv()
        .expect("provider task entered blocking poll");
    let deadline = MonoInstant::from_nanos(5);
    h.clock.advance_to(deadline).unwrap();

    {
        let mut join = Box::pin(h.coordinator.join(deadline));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut join)
                .await
                .is_err(),
            "deadline cleanup must await actual task termination"
        );
    }
    assert!(
        h.coordinator.in_flight(),
        "cancelling join must leave the JoinHandle owned by the coordinator"
    );

    release_tx.send(()).expect("release provider task");
    assert!(matches!(
        h.coordinator.join(deadline).await,
        Err(HistoryJoinError::DeadlineElapsed
            | HistoryJoinError::Aborted
            | HistoryJoinError::JoinFailure)
    ));
    assert!(!h.coordinator.in_flight());
}

#[tokio::test]
async fn non_older_insert_is_no_progress_and_cancellation_discards_ready_page() {
    let mut malformed = harness([Response::Ready(Ok(vec![candle(1_600_000)]))], 10);
    malformed.coordinator.update_boundary(0, &malformed.series);
    assert_eq!(
        malformed.coordinator.drive().await,
        HistoryProgress::PageReady
    );
    assert_eq!(
        malformed
            .coordinator
            .apply_completed(&mut malformed.series, &mut malformed.view, 20),
        HistoryProgress::EndReached
    );
    assert!(malformed.coordinator.end_latched());

    let mut cancelled = harness([Response::Ready(Ok(vec![candle(940_000)]))], 10);
    let before = cancelled.series.iter().cloned().collect::<Vec<_>>();
    cancelled.coordinator.update_boundary(0, &cancelled.series);
    assert_eq!(
        cancelled.coordinator.drive().await,
        HistoryProgress::PageReady
    );
    cancelled.cancellation.cancel();
    assert_eq!(
        cancelled
            .coordinator
            .apply_completed(&mut cancelled.series, &mut cancelled.view, 20),
        HistoryProgress::Cancelled
    );
    assert_eq!(cancelled.series.iter().cloned().collect::<Vec<_>>(), before);
}

#[tokio::test]
async fn dropped_drive_future_keeps_request_owned_and_ready_page_blocks_stale_cursor() {
    let (tx, rx) = oneshot::channel();
    let mut h = harness([Response::Pending(rx)], 10);
    h.coordinator.update_boundary(0, &h.series);
    {
        let drive = h.coordinator.drive();
        tokio::pin!(drive);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), &mut drive)
                .await
                .is_err()
        );
    }
    assert!(h.coordinator.in_flight());
    assert!(h.coordinator.has_owned_task());
    tx.send(Ok(vec![candle(940_000)])).unwrap();
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert!(h.coordinator.in_flight());
    assert!(h.coordinator.has_completed_page());
    assert!(
        !h.coordinator.has_owned_task(),
        "a retained completed page is in-flight App work but no longer an owned task"
    );
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::Idle
    );
    assert_eq!(h.provider.requests().len(), 1);
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::PageApplied
    );
}

#[tokio::test]
async fn hung_request_observes_timed_gate_then_process_block_without_late_commit_or_retry() {
    let (tx, rx) = oneshot::channel();
    let mut h = harness([Response::Pending(rx)], 10);
    let before = h.series.iter().cloned().collect::<Vec<_>>();
    let view_before = h.view.clone();
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    tokio::task::yield_now().await;

    let deadline = MonoInstant::from_nanos(60);
    h.provider.publish(RateGateState::TimedUntil(deadline));
    let mut drive = Box::pin(h.coordinator.drive());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut drive)
            .await
            .is_err(),
        "a timed gate update must not replace or complete the current request"
    );
    h.provider.publish(RateGateState::ProcessBlocked(
        ProcessBlocker::InvalidBanExpiry,
    ));
    assert_eq!(drive.await, HistoryProgress::PermanentlyDisabled);
    assert_eq!(
        h.coordinator.process_blocker(),
        Some(ProcessBlocker::InvalidBanExpiry)
    );
    assert!(tx.send(Ok(vec![candle(940_000)])).is_err());
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::Idle
    );
    assert_eq!(h.series.iter().cloned().collect::<Vec<_>>(), before);
    assert_eq!(h.view, view_before);
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::PermanentlyDisabled
    );
    assert_eq!(h.provider.requests().len(), 1);
}

#[tokio::test]
async fn hung_request_observes_gate_closure_without_late_commit_or_retry() {
    let (tx, rx) = oneshot::channel();
    let mut h = harness([Response::Pending(rx)], 10);
    let before = h.series.iter().cloned().collect::<Vec<_>>();
    let view_before = h.view.clone();
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::RequestStarted
    );
    tokio::task::yield_now().await;

    h.provider.close_gate();
    assert_eq!(
        h.coordinator.drive().await,
        HistoryProgress::TerminalFailure(ProviderError::Invariant("rate gate closed"))
    );
    assert!(h.coordinator.terminal_disabled());
    assert!(matches!(
        h.coordinator.last_error(),
        Some(ProviderError::Invariant("rate gate closed"))
    ));
    assert!(tx.send(Ok(vec![candle(940_000)])).is_err());
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::Idle
    );
    assert_eq!(h.series.iter().cloned().collect::<Vec<_>>(), before);
    assert_eq!(h.view, view_before);
    assert_eq!(
        h.coordinator.update_boundary(0, &h.series),
        HistoryProgress::PermanentlyDisabled
    );
    assert_eq!(h.provider.requests().len(), 1);
}

#[tokio::test]
async fn valid_418_uses_shared_timed_gate_for_one_deferred_retry() {
    let (tx, rx) = oneshot::channel();
    let mut h = harness(
        [
            Response::Pending(rx),
            Response::Ready(Ok(vec![candle(940_000)])),
        ],
        10,
    );
    h.coordinator.update_boundary(0, &h.series);
    tokio::task::yield_now().await;
    let deadline = MonoInstant::from_nanos(60);
    h.provider.publish(RateGateState::TimedUntil(deadline));
    tx.send(Err(ProviderError::RateLimited {
        context: context(),
        status: 418,
    }))
    .unwrap();
    assert_eq!(
        h.coordinator.drive().await,
        HistoryProgress::RetryDeferred(deadline)
    );
    h.clock.advance_to(deadline).unwrap();
    assert_eq!(h.coordinator.drive().await, HistoryProgress::RequestStarted);
    assert_eq!(h.coordinator.drive().await, HistoryProgress::PageReady);
    assert_eq!(
        h.coordinator
            .apply_completed(&mut h.series, &mut h.view, 20),
        HistoryProgress::PageApplied
    );
    assert_eq!(h.provider.requests().len(), 2);
}
