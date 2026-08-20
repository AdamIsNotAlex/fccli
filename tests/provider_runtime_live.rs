#![cfg(feature = "test-transport")]

use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use fccli::{
    clock::ManualClock,
    error::{
        ErrorContext, ErrorOperation, ModelError, PayloadError, ProviderError, SanitizedCause,
        SanitizedMessage, TimeoutKind,
    },
    model::{
        Candle, ConnectionStatus, GapGeneration, HistoryRequest, Instrument, Market, MarketEvent,
        MonoInstant, ProviderId, RateGateState, ReplayRevision, Timeframe,
    },
    provider::{
        LiveCompletionDisposition, LiveErrorDisposition, LiveFeed, LiveInBandEventDisposition,
        LiveInputClassification, LiveRequest, LiveSocketEvent, LiveSupervisorConfig,
        ProducerCompletion, ProviderCapabilities, ReconcileAck, accepted_watermark_channel,
        classify_live_error_for_test, classify_live_input_for_test, rate_gate_channel,
        reconcile_ack_channel,
        test_transport::{
            ConnectionRotation, LiveAdapter, LiveConfig, LiveRateGate, LiveSocket,
            ProcessBlockPolicy, ReconciliationLimits, ReconciliationPolicy,
            gap_target_within_generation_span_for_test, open_live,
            reconciliation_distinct_key_allowed_for_test, reconciliation_page_guard_for_test,
        },
    },
};
use futures_util::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("runtimecontract").expect("provider"),
        Market::Spot,
        "BASE",
        "QUOTE",
        "BASEQUOTE",
    )
    .expect("instrument")
}

#[test]
fn supervisor_configuration_rejects_zero_and_unbounded_runtime_capacities() {
    let valid = LiveSupervisorConfig::default();
    assert!(valid.validate().is_ok());

    for invalid in [
        LiveSupervisorConfig {
            keyed_candle_capacity: 0,
            ..valid.clone()
        },
        LiveSupervisorConfig {
            control_capacity: 0,
            ..valid.clone()
        },
        LiveSupervisorConfig {
            market_event_capacity: 0,
            ..valid.clone()
        },
        LiveSupervisorConfig {
            first_kline_timeout: Duration::ZERO,
            ..valid.clone()
        },
        LiveSupervisorConfig {
            reconcile_ack_timeout: Duration::ZERO,
            ..valid
        },
    ] {
        assert!(matches!(
            invalid.validate(),
            Err(ProviderError::Configuration(_))
        ));
    }
}

#[test]
fn reconciliation_page_and_distinct_buffer_bounds_include_exact_boundary() {
    assert!(reconciliation_page_guard_for_test(63, 64).is_ok());
    assert!(reconciliation_page_guard_for_test(64, 64).is_ok());
    assert!(matches!(
        reconciliation_page_guard_for_test(65, 64),
        Err(ProviderError::Protocol { .. })
    ));
    assert!(reconciliation_distinct_key_allowed_for_test(
        64_000, true, 64_000
    ));
    assert!(!reconciliation_distinct_key_allowed_for_test(
        64_001, false, 64_000
    ));
}

#[test]
fn reconciliation_span_arithmetic_covers_fixed_and_calendar_exact_limits() {
    const START: i64 = 1_699_999_980_000;
    assert!(gap_target_within_generation_span_for_test(
        Timeframe::Minute1,
        START,
        START + 64_000 * 60_000,
        64_000,
    ));
    assert!(!gap_target_within_generation_span_for_test(
        Timeframe::Minute1,
        START,
        START + 64_001 * 60_000,
        64_000,
    ));

    const JANUARY_1970: i64 = 0;
    const MONTH_SUCCESSOR_64_000: i64 = 168_303_571_200_000;
    const MONTH_SUCCESSOR_64_001: i64 = 168_306_249_600_000;
    assert!(gap_target_within_generation_span_for_test(
        Timeframe::Month1,
        JANUARY_1970,
        MONTH_SUCCESSOR_64_000,
        64_000,
    ));
    assert!(!gap_target_within_generation_span_for_test(
        Timeframe::Month1,
        JANUARY_1970,
        MONTH_SUCCESSOR_64_001,
        64_000,
    ));
    assert!(!gap_target_within_generation_span_for_test(
        Timeframe::Month1,
        JANUARY_1970,
        i64::MAX,
        64_000,
    ));
}

#[test]
fn supervisor_classifies_completion_errors_and_socket_outcomes_by_semantics() {
    use LiveCompletionDisposition::{FinishedErr, Running};
    use LiveErrorDisposition::{Recoverable, Terminal};
    use LiveInBandEventDisposition::{RecoverableInBand, TerminalInBand};

    let market = instrument();
    let context = || {
        ErrorContext::operation(ErrorOperation::WebSocket).with_market(&market, Timeframe::Minute1)
    };
    let cases = vec![
        (
            ProviderError::ServerStatus {
                context: context(),
                status: 503,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            ProviderError::InvalidSymbol {
                context: context(),
                code: -1121,
                message: SanitizedMessage::InvalidSymbol,
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            ProviderError::Protocol {
                context: context(),
                detail: "invalid WebSocket framing",
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            ProviderError::Payload {
                context: context(),
                source: PayloadError::MalformedProtocol,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            ProviderError::Transport {
                context: context(),
                cause: SanitizedCause::Io,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            ProviderError::Timeout {
                context: context(),
                kind: TimeoutKind::StalledWrite,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            ProviderError::ClientStatus {
                context: context(),
                status: 403,
                code: None,
                message: None,
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            ProviderError::Configuration("invalid live configuration"),
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            ProviderError::WebSocketConfiguration {
                context: context(),
                detail: "invalid WebSocket configuration",
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            ProviderError::Invariant("live invariant"),
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            ProviderError::ChannelClosed {
                context: ErrorContext::operation(ErrorOperation::Channel)
                    .with_market(&market, Timeframe::Minute1),
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            ProviderError::Domain {
                context: context(),
                source: ModelError::InvalidRange,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
    ];

    for (error, disposition, event, completion, retries) in cases {
        let actual = classify_live_error_for_test(&error);
        assert_eq!(actual.disposition, disposition);
        assert_eq!(actual.event, event);
        assert_eq!(actual.completion, completion);
        assert_eq!(actual.retries, retries);
    }

    let LiveInputClassification::Error { error, policy } = classify_live_input_for_test(
        Ok(LiveSocketEvent::ReconnectRequested),
        &market,
        Timeframe::Minute1,
    ) else {
        panic!("peer reconnect must classify as an error");
    };
    assert!(matches!(
        error,
        ProviderError::Protocol {
            detail: "WebSocket peer requested reconnect",
            ..
        }
    ));
    assert_eq!(policy.disposition, Recoverable);
    assert_eq!(policy.event, RecoverableInBand);
    assert_eq!(policy.completion, Running);
    assert!(policy.retries);
}

const OPEN: i64 = 1_699_999_980_000;

fn candle(open: i64, closed: bool) -> Candle {
    Candle::from_ws(open, open + 59_999, 1.0, 2.0, 0.5, 1.5, 3.0, closed).expect("candle")
}

fn rest_candle(open: i64) -> Candle {
    Candle::from_rest(open, open + 59_999, 1.0, 2.0, 0.5, 1.5, 3.0).expect("candle")
}

struct HarnessSocket {
    events: mpsc::UnboundedReceiver<Result<LiveSocketEvent, ProviderError>>,
}

impl LiveSocket for HarnessSocket {
    fn read(&mut self) -> impl Future<Output = Result<LiveSocketEvent, ProviderError>> + Send + '_ {
        async move {
            self.events
                .recv()
                .await
                .unwrap_or_else(|| Ok(LiveSocketEvent::ReconnectRequested))
        }
    }

    fn after_gap_sync_test_probe(
        &mut self,
    ) -> impl Future<Output = Result<(), ProviderError>> + Send + '_ {
        std::future::ready(Ok(()))
    }
}

struct HistoryStep {
    started: Option<oneshot::Sender<HistoryRequest>>,
    response: oneshot::Receiver<Result<Vec<Candle>, ProviderError>>,
}

#[derive(Clone)]
struct HarnessAdapter {
    sockets: Arc<Mutex<VecDeque<HarnessSocket>>>,
    history: Arc<Mutex<VecDeque<HistoryStep>>>,
    supervisor: LiveSupervisorConfig,
    gate: fccli::provider::RateGateSnapshot,
    reconciliation: ReconciliationPolicy,
}

impl LiveAdapter for HarnessAdapter {
    type Socket = HarnessSocket;

    fn validate_request(
        &self,
        _instrument: &Instrument,
        _timeframe: Timeframe,
    ) -> Result<(), ProviderError> {
        Ok(())
    }

    fn connect_ready_socket(
        &self,
        _instrument: Instrument,
        _timeframe: Timeframe,
    ) -> impl Future<Output = Result<Self::Socket, ProviderError>> + Send + '_ {
        let socket = self.sockets.lock().expect("sockets").pop_front();
        async move {
            socket.ok_or(ProviderError::Transport {
                context: ErrorContext::operation(ErrorOperation::WebSocket),
                cause: SanitizedCause::Io,
            })
        }
    }

    fn history(
        &self,
        _instrument: Instrument,
        _timeframe: Timeframe,
        request: HistoryRequest,
        _cancellation: fccli::provider::CancellationToken,
    ) -> impl Future<Output = Result<Vec<Candle>, ProviderError>> + Send + '_ {
        let step = self.history.lock().expect("history").pop_front();
        async move {
            let mut step = step.expect("unexpected history request");
            if let Some(started) = step.started.take() {
                let _ = started.send(request);
            }
            step.response.await.expect("history response sender")
        }
    }

    fn rate_gate(&self) -> LiveRateGate {
        LiveRateGate {
            snapshot: self.gate.clone(),
            process_block: ProcessBlockPolicy::Forbidden("runtime test process block"),
        }
    }

    fn live_config(&self) -> LiveConfig<'_> {
        LiveConfig {
            supervisor: &self.supervisor,
            reconciliation: self.reconciliation,
        }
    }

    fn connection_rotation(&self) -> ConnectionRotation {
        ConnectionRotation::Never
    }
}

struct Harness {
    adapter: HarnessAdapter,
    socket_senders: Vec<mpsc::UnboundedSender<Result<LiveSocketEvent, ProviderError>>>,
    history_started: Vec<oneshot::Receiver<HistoryRequest>>,
    history_responses: Vec<oneshot::Sender<Result<Vec<Candle>, ProviderError>>>,
    clock: Arc<ManualClock>,
    gate_sender: fccli::provider::RateGateSender,
}

impl Harness {
    fn new(generations: usize, history_steps: usize, supervisor: LiveSupervisorConfig) -> Self {
        let mut sockets = VecDeque::new();
        let mut socket_senders = Vec::new();
        for _ in 0..generations {
            let (sender, events) = mpsc::unbounded_channel();
            socket_senders.push(sender);
            sockets.push_back(HarnessSocket { events });
        }
        let mut history = VecDeque::new();
        let mut history_started = Vec::new();
        let mut history_responses = Vec::new();
        for _ in 0..history_steps {
            let (started_tx, started_rx) = oneshot::channel();
            let (response_tx, response_rx) = oneshot::channel();
            history.push_back(HistoryStep {
                started: Some(started_tx),
                response: response_rx,
            });
            history_started.push(started_rx);
            history_responses.push(response_tx);
        }
        let (gate_sender, gate) = rate_gate_channel(RateGateState::Open);
        Self {
            adapter: HarnessAdapter {
                sockets: Arc::new(Mutex::new(sockets)),
                history: Arc::new(Mutex::new(history)),
                supervisor,
                gate,
                reconciliation: ReconciliationPolicy::Bounded(ReconciliationLimits {
                    max_successors: 64_000,
                    max_pages: 64,
                    span_exceeded: "runtime reconciliation span exceeded",
                    page_exceeded: "runtime reconciliation page limit exceeded",
                    distinct_exceeded: "runtime reconciliation distinct buffer exceeded",
                }),
            },
            socket_senders,
            history_started,
            history_responses,
            clock: Arc::new(ManualClock::new(MonoInstant::ZERO)),
            gate_sender,
        }
    }

    async fn open(
        &self,
        startup: Option<i64>,
    ) -> (
        LiveFeed,
        fccli::provider::AcceptedWatermarkSender,
        fccli::provider::ReconcileAckSender,
        fccli::provider::CancellationToken,
    ) {
        let (watermark_tx, watermark_rx) = accepted_watermark_channel(startup);
        let (ack_tx, ack_rx) = reconcile_ack_channel();
        let cancellation = fccli::provider::CancellationToken::new();
        let feed = open_live(
            self.adapter.clone(),
            self.clock.clone(),
            ProviderCapabilities {
                markets: &[Market::Spot],
                timeframes: &[Timeframe::Minute1],
                history_page_limit: 1_000,
            },
            LiveRequest {
                instrument: instrument(),
                timeframe: Timeframe::Minute1,
                startup_watermark: startup,
                accepted_watermark_rx: watermark_rx,
                reconcile_ack_rx: ack_rx,
                cancellation: cancellation.clone(),
            },
        )
        .await
        .expect("open shared runtime");
        (feed, watermark_tx, ack_tx, cancellation)
    }
}

async fn event(feed: &mut LiveFeed) -> MarketEvent {
    timeout(Duration::from_secs(2), feed.events.next())
        .await
        .expect("bounded event")
        .expect("event stream")
        .expect("market event")
}

async fn reconcile_batch(feed: &mut LiveFeed) -> (GapGeneration, ReplayRevision, i64, Vec<Candle>) {
    loop {
        if let MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time,
            candles,
        } = event(feed).await
        {
            return (generation, revision, target_open_time, candles);
        }
    }
}

#[tokio::test]
async fn shared_runtime_reconciles_while_ws_runs_and_acknowledgement_gates_connected() {
    let mut harness = Harness::new(1, 1, LiveSupervisorConfig::default());
    let (mut feed, _watermark, ack, cancellation) = harness.open(Some(OPEN)).await;
    harness.socket_senders[0]
        .send(Ok(LiveSocketEvent::Candle(candle(OPEN + 60_000, true))))
        .expect("first candle");
    let request = timeout(Duration::from_secs(2), harness.history_started.remove(0))
        .await
        .expect("bounded history start")
        .expect("history started");
    assert_eq!(request.start_time(), Some(OPEN));
    harness.socket_senders[0]
        .send(Ok(LiveSocketEvent::Candle(candle(OPEN + 120_000, true))))
        .expect("concurrent WS candle");
    harness
        .history_responses
        .remove(0)
        .send(Ok(vec![
            rest_candle(OPEN),
            rest_candle(OPEN + 60_000),
            rest_candle(OPEN + 120_000),
        ]))
        .expect("history response");
    let (generation, revision, target, candles) = reconcile_batch(&mut feed).await;
    assert_eq!(target, OPEN + 120_000);
    assert_eq!(candles.len(), 3);
    tokio::task::yield_now().await;
    assert!(feed.events.next().now_or_never().is_none());
    ack.publish(ReconcileAck {
        generation,
        revision,
        through: target,
    })
    .expect("matching ack");
    assert_eq!(
        event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::Connected
        }
    );
    cancellation.cancel();
}

#[tokio::test]
async fn stale_ack_is_ignored_and_accepted_watermark_grows_revision_and_suffix() {
    let mut harness = Harness::new(1, 2, LiveSupervisorConfig::default());
    let (mut feed, watermark, ack, cancellation) = harness.open(Some(OPEN)).await;
    harness.socket_senders[0]
        .send(Ok(LiveSocketEvent::Candle(candle(OPEN, true))))
        .expect("first");
    harness
        .history_started
        .remove(0)
        .await
        .expect("first history");
    harness
        .history_responses
        .remove(0)
        .send(Ok(vec![rest_candle(OPEN)]))
        .expect("first page");
    let (generation, revision, target, _) = reconcile_batch(&mut feed).await;
    ack.publish(ReconcileAck {
        generation,
        revision: ReplayRevision(revision.0.saturating_sub(1)),
        through: target,
    })
    .expect_err("stale ack rejected by control channel");
    watermark
        .publish(Some(OPEN + 60_000))
        .expect("advance watermark");
    harness.socket_senders[0]
        .send(Ok(LiveSocketEvent::Candle(candle(OPEN + 60_000, true))))
        .expect("new target");
    let suffix = harness
        .history_started
        .remove(0)
        .await
        .expect("suffix history");
    assert_eq!(suffix.start_time(), Some(OPEN + 1));
    harness
        .history_responses
        .remove(0)
        .send(Ok(vec![rest_candle(OPEN + 60_000)]))
        .expect("suffix response");
    let (_, next_revision, next_target, _) = reconcile_batch(&mut feed).await;
    assert!(next_revision > revision);
    assert_eq!(next_target, OPEN + 60_000);
    ack.publish(ReconcileAck {
        generation,
        revision: next_revision,
        through: next_target,
    })
    .expect("latest ack");
    assert_eq!(
        event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::Connected
        }
    );
    cancellation.cancel();
}

#[tokio::test]
async fn cancellation_precedes_ready_history_and_first_candle_timeout_uses_manual_clock() {
    let mut harness = Harness::new(2, 1, LiveSupervisorConfig::default());
    let (mut feed, _watermark, _ack, cancellation) = harness.open(Some(OPEN)).await;
    harness.socket_senders[0]
        .send(Ok(LiveSocketEvent::Candle(candle(OPEN, true))))
        .expect("first");
    harness
        .history_started
        .remove(0)
        .await
        .expect("history held");
    cancellation.cancel();
    harness
        .history_responses
        .remove(0)
        .send(Ok(vec![rest_candle(OPEN)]))
        .expect("ready history");
    assert_eq!(
        event(&mut feed).await,
        MarketEvent::Status {
            generation: None,
            status: ConnectionStatus::Stopped
        }
    );
    assert_eq!(
        feed.producer_completion
            .changed()
            .await
            .expect("completion"),
        ProducerCompletion::Finished(Ok(()))
    );

    let harness = Harness::new(2, 0, LiveSupervisorConfig::default());
    let (mut feed, _watermark, _ack, cancellation) = harness.open(None).await;
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connecting,
            ..
        }
    ));
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    harness
        .clock
        .advance_by(Duration::from_secs(10))
        .expect("advance timeout");
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Timeout {
                kind: TimeoutKind::FirstKline,
                ..
            },
            ..
        }
    ));
    cancellation.cancel();
}

#[tokio::test]
async fn recoverable_generations_back_off_exponentially_purge_stale_events_and_honor_rate_gate() {
    let harness = Harness::new(4, 0, LiveSupervisorConfig::default());
    let (mut feed, _watermark, _ack, cancellation) = harness.open(None).await;
    for (index, seconds) in [1_u64, 2, 4].into_iter().enumerate() {
        let generation = GapGeneration(u64::try_from(index + 1).expect("generation"));
        harness.socket_senders[index]
            .send(Ok(LiveSocketEvent::ReconnectRequested))
            .expect("reconnect");
        loop {
            match event(&mut feed).await {
                MarketEvent::RecoverableError {
                    generation: Some(actual),
                    ..
                } => assert_eq!(actual, generation),
                MarketEvent::Status {
                    generation: Some(actual),
                    status: ConnectionStatus::Backoff,
                } => {
                    assert_eq!(actual, generation);
                    break;
                }
                MarketEvent::ReconcileBatch {
                    generation: actual, ..
                }
                | MarketEvent::Candle {
                    generation: actual, ..
                } => panic!("stale generation event escaped purge: {actual:?}"),
                _ => {}
            }
        }
        if index == 1 {
            harness
                .gate_sender
                .publish(RateGateState::TimedUntil(MonoInstant::from_nanos(
                    10_000_000_000,
                )))
                .expect("rate gate");
            harness
                .clock
                .advance_to(MonoInstant::from_nanos(7_000_000_000))
                .expect("before gate");
        } else {
            harness
                .clock
                .advance_by(Duration::from_secs(seconds))
                .expect("backoff");
        }
        if index == 1 {
            harness
                .clock
                .advance_to(MonoInstant::from_nanos(10_000_000_000))
                .expect("gate deadline");
            harness
                .gate_sender
                .publish(RateGateState::Open)
                .expect("open gate observation");
        }
        assert!(
            matches!(event(&mut feed).await, MarketEvent::Status { generation: Some(next), status: ConnectionStatus::Connecting } if next.0 == generation.0 + 1)
        );
    }
    cancellation.cancel();
}

#[tokio::test]
async fn saturation_emits_unscoped_emergency_pair_before_retry() {
    let mut supervisor = LiveSupervisorConfig::default();
    supervisor.keyed_candle_capacity = 1;
    supervisor.market_event_capacity = 1;
    let saturation = Arc::new(tokio::sync::Notify::new());
    supervisor.saturation_test_hook = Some(Arc::clone(&saturation));
    let mut harness = Harness::new(2, 1, supervisor);
    let (mut feed, _watermark, ack, cancellation) = harness.open(Some(OPEN)).await;
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(GapGeneration(1)),
            status: ConnectionStatus::Connecting,
        }
    ));
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(GapGeneration(1)),
            status: ConnectionStatus::GapSync,
        }
    ));
    harness.socket_senders[0]
        .send(Ok(LiveSocketEvent::Candle(candle(OPEN, true))))
        .expect("first");
    timeout(Duration::from_secs(2), harness.history_started.remove(0))
        .await
        .expect("bounded history start")
        .expect("history started");
    harness
        .history_responses
        .remove(0)
        .send(Ok(vec![rest_candle(OPEN)]))
        .expect("history response");
    let (generation, revision, target, _) = reconcile_batch(&mut feed).await;
    ack.publish(ReconcileAck {
        generation,
        revision,
        through: target,
    })
    .expect("ack");
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connected,
            ..
        }
    ));
    for offset in [60_000, 120_000, 180_000] {
        harness.socket_senders[0]
            .send(Ok(LiveSocketEvent::Candle(candle(OPEN + offset, true))))
            .expect("saturating candle");
    }
    timeout(Duration::from_secs(2), saturation.notified())
        .await
        .expect("saturation hook");
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: None,
            error: ProviderError::QueueSaturated,
            ..
        }
    ));
    assert_eq!(
        event(&mut feed).await,
        MarketEvent::Status {
            generation: None,
            status: ConnectionStatus::Backoff
        }
    );
    harness
        .clock
        .advance_by(Duration::from_secs(1))
        .expect("backoff");
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(GapGeneration(2)),
            status: ConnectionStatus::Connecting
        }
    ));
    cancellation.cancel();
}

#[tokio::test]
async fn terminal_error_is_in_band_once_and_completion_matches_runtime_outcome() {
    let harness = Harness::new(1, 0, LiveSupervisorConfig::default());
    let (mut feed, _watermark, _ack, _cancellation) = harness.open(None).await;
    harness.socket_senders[0]
        .send(Ok(LiveSocketEvent::DecodedError(
            ProviderError::InvalidSymbol {
                context: ErrorContext::operation(ErrorOperation::WebSocket),
                code: -1,
                message: SanitizedMessage::InvalidSymbol,
            },
        )))
        .expect("terminal socket error");
    assert!(matches!(
        event(&mut feed).await,
        MarketEvent::TerminalError(ProviderError::InvalidSymbol { .. })
    ));
    assert!(feed.events.next().await.is_none());
    assert!(matches!(
        feed.producer_completion
            .changed()
            .await
            .expect("completion"),
        ProducerCompletion::Finished(Err(ProviderError::InvalidSymbol { .. }))
    ));
}
