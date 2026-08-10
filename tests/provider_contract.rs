use std::{sync::Arc, time::Duration};

use fccli::{
    clock::{Clock, ManualClock, SleepOutcome, checked_deadline, sleep_until_or_cancelled},
    error::ProviderError,
    model::{
        Candle, ConnectionStatus, GapGeneration, HistoryRequest, Instrument, InstrumentSpec,
        Market, MarketEvent, MonoInstant, ProviderId, ReplayRevision, Timeframe,
    },
    provider::{
        AcceptedWatermarkUpdateError, ExpectationUpdate, LiveFeed, LiveRequest, MarketDataProvider,
        MarketEventStream, ProcessBlocker, ProducerCompletion, ProviderFuture, RateGateSnapshot,
        RateGateState, ReconcileAck, ReconcileAckPublishError, ReconcileAckUpdate,
        ReconcileExpectation, ReconcileExpectationError, WatermarkUpdate,
        accepted_watermark_channel, reconcile_ack_channel,
    },
};
use futures_util::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("fake").expect("valid provider"),
        Market::Spot,
        "BTC",
        "USDT",
        "BTCUSDT",
    )
    .expect("valid instrument")
}

fn candle(open_time: i64, closed: bool) -> Candle {
    Candle::from_ws(
        open_time,
        open_time + 59_999,
        10.0,
        12.0,
        9.0,
        11.0,
        5.0,
        closed,
    )
    .expect("valid candle")
}

#[derive(Clone)]
struct FakeProvider {
    gate: RateGateSnapshot,
}

impl MarketDataProvider for FakeProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("fake").expect("valid provider")
    }

    fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        Instrument::new(
            spec.provider().clone(),
            Market::Spot,
            spec.base(),
            spec.quote().unwrap_or("USDT"),
            format!("{}{}", spec.base(), spec.quote().unwrap_or("USDT")),
        )
        .map_err(|source| ProviderError::Domain {
            context: fccli::error::ErrorContext::operation(fccli::error::ErrorOperation::LiveFeed),
            source,
        })
    }

    fn history<'a>(
        &'a self,
        _instrument: &'a Instrument,
        _timeframe: Timeframe,
        _request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(ProviderError::Invariant("history cancelled"));
            }
            Ok(vec![candle(1_700_000_000_000, true)])
        })
    }

    fn open_live<'a>(&'a self, request: LiveRequest) -> ProviderFuture<'a, LiveFeed> {
        Box::pin(async move {
            let generation = GapGeneration(3);
            let event_candle = candle(1_700_000_060_000, false);
            let events: MarketEventStream = Box::pin(stream::iter([
                Ok(MarketEvent::Status {
                    generation: Some(generation),
                    status: ConnectionStatus::Connecting,
                }),
                Ok(MarketEvent::Candle {
                    generation,
                    candle: event_candle,
                }),
            ]));
            let cancellation = request.cancellation;
            let supervisor_cancellation = cancellation.clone();
            Ok(LiveFeed::spawn(events, cancellation, async move {
                supervisor_cancellation.cancelled().await;
                Ok(())
            }))
        })
    }

    fn rate_gate(&self) -> RateGateSnapshot {
        self.gate.clone()
    }
}

fn assert_object_safe(_: Arc<dyn MarketDataProvider>) {}

#[tokio::test]
async fn fake_provider_is_object_safe_and_constructs_exact_event_payloads() {
    let (gate_tx, gate) = fccli::provider::rate_gate_channel(RateGateState::Open);
    let provider = FakeProvider { gate };
    assert_object_safe(Arc::new(provider.clone()));

    let spec = InstrumentSpec::new(provider.id(), "BTC", Some("USDT"))
        .expect("valid provider-neutral spec");
    assert_eq!(
        provider.canonicalize(&spec).expect("canonicalized"),
        instrument()
    );
    let history = provider
        .history(
            &instrument(),
            Timeframe::Minute1,
            HistoryRequest::latest(500).expect("valid request"),
            CancellationToken::new(),
        )
        .await
        .expect("history result");
    assert_eq!(history, vec![candle(1_700_000_000_000, true)]);

    let (watermark_tx, watermark_rx) = accepted_watermark_channel(None);
    let (_ack_tx, ack_rx) = reconcile_ack_channel();
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(LiveRequest {
            instrument: instrument(),
            timeframe: Timeframe::Minute1,
            startup_watermark: None,
            accepted_watermark_rx: watermark_rx,
            reconcile_ack_rx: ack_rx,
            cancellation: cancellation.clone(),
        })
        .await
        .expect("live feed");
    drop(watermark_tx);

    assert_eq!(
        feed.events
            .next()
            .await
            .expect("status item")
            .expect("status"),
        MarketEvent::Status {
            generation: Some(GapGeneration(3)),
            status: ConnectionStatus::Connecting,
        }
    );
    assert_eq!(
        feed.events
            .next()
            .await
            .expect("candle item")
            .expect("candle"),
        MarketEvent::Candle {
            generation: GapGeneration(3),
            candle: candle(1_700_000_060_000, false),
        }
    );
    assert!(feed.events.next().await.is_none());
    assert_eq!(provider.rate_gate().current(), Ok(RateGateState::Open));
    let mut gate_observer = provider.rate_gate();
    gate_tx
        .publish(RateGateState::TimedUntil(MonoInstant::from_nanos(50)))
        .expect("observed gate remains open");
    assert_eq!(
        gate_observer.changed().await,
        Ok(RateGateState::TimedUntil(MonoInstant::from_nanos(50)))
    );
    gate_tx
        .publish(RateGateState::ProcessBlocked(
            ProcessBlocker::InvalidBanExpiry,
        ))
        .expect("observed gate remains open");
    assert_eq!(
        gate_observer.changed().await,
        Ok(RateGateState::ProcessBlocked(
            ProcessBlocker::InvalidBanExpiry
        ))
    );
    drop(gate_tx);
    assert!(gate_observer.current().is_err());
    assert!(gate_observer.changed().await.is_err());

    feed.request_shutdown();
    feed.join(MonoInstant::from_nanos(u64::MAX))
        .await
        .expect("cancelled supervisor joins cleanly");
}

#[test]
fn provider_neutral_event_payloads_preserve_every_field_exactly() {
    let generation = GapGeneration(8);
    let revision = ReplayRevision(13);
    let value = candle(1_700_000_120_000, true);
    let events = [
        MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time: value.open_time(),
            candles: vec![value.clone()],
        },
        MarketEvent::RecoverableError {
            generation: Some(generation),
            error: ProviderError::Configuration("recoverable fake"),
            rate_gate_deadline: Some(MonoInstant::from_nanos(77)),
        },
        MarketEvent::TerminalError(ProviderError::Configuration("terminal fake")),
    ];

    assert_eq!(
        events[0],
        MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time: 1_700_000_120_000,
            candles: vec![value],
        }
    );
    assert_eq!(
        events[1],
        MarketEvent::RecoverableError {
            generation: Some(generation),
            error: ProviderError::Configuration("recoverable fake"),
            rate_gate_deadline: Some(MonoInstant::from_nanos(77)),
        }
    );
    assert_eq!(
        events[2],
        MarketEvent::TerminalError(ProviderError::Configuration("terminal fake"))
    );
}

#[tokio::test]
async fn accepted_watermark_is_monotonic_latest_value_with_per_clone_cursors_and_closure() {
    let (sender, mut first) = accepted_watermark_channel(None);
    let mut second = first.clone();
    assert_eq!(first.current(), Ok(None));
    assert_eq!(sender.publish(None), Ok(WatermarkUpdate::Unchanged));
    assert_eq!(sender.publish(Some(10)), Ok(WatermarkUpdate::Advanced));
    assert_eq!(first.changed().await, Ok(Some(10)));
    assert_eq!(second.changed().await, Ok(Some(10)));
    assert_eq!(sender.publish(Some(10)), Ok(WatermarkUpdate::Unchanged));
    assert_eq!(
        sender.publish(None),
        Err(AcceptedWatermarkUpdateError::Regression)
    );
    assert_eq!(
        sender.publish(Some(9)),
        Err(AcceptedWatermarkUpdateError::Regression)
    );
    assert_eq!(sender.publish(Some(11)), Ok(WatermarkUpdate::Advanced));
    assert_eq!(first.changed().await, Ok(Some(11)));
    assert_eq!(second.changed().await, Ok(Some(11)));

    drop(first);
    drop(second);
    assert_eq!(
        sender.publish(Some(12)),
        Err(AcceptedWatermarkUpdateError::Closed)
    );

    let (sender, mut receiver) = accepted_watermark_channel(Some(1));
    drop(sender);
    assert!(receiver.current().is_err());
    assert!(receiver.changed().await.is_err());
}

#[tokio::test]
async fn reconcile_ack_registration_and_proof_rules_are_exact() {
    let (sender, mut receiver) = reconcile_ack_channel();
    let first = ReconcileExpectation {
        generation: GapGeneration(2),
        revision: ReplayRevision(4),
        target_open_time: 100,
    };
    assert_eq!(receiver.current_expectation(), Ok(None));
    assert_eq!(receiver.current(), Ok(None));
    assert_eq!(
        sender.publish(ReconcileAck {
            generation: first.generation,
            revision: first.revision,
            through: 100,
        }),
        Err(ReconcileAckPublishError::NoExpectation)
    );
    assert_eq!(
        receiver.register_expectation(first),
        Ok(ExpectationUpdate::Registered)
    );
    assert_eq!(
        receiver.register_expectation(first),
        Ok(ExpectationUpdate::Unchanged)
    );
    assert_eq!(
        receiver.register_expectation(ReconcileExpectation {
            target_open_time: 101,
            ..first
        }),
        Err(ReconcileExpectationError::Conflict)
    );
    assert_eq!(
        receiver.register_expectation(ReconcileExpectation {
            revision: ReplayRevision(3),
            ..first
        }),
        Err(ReconcileExpectationError::Regression)
    );
    assert_eq!(
        sender.publish(ReconcileAck {
            generation: GapGeneration(1),
            revision: first.revision,
            through: 100,
        }),
        Err(ReconcileAckPublishError::Stale)
    );
    assert_eq!(
        sender.publish(ReconcileAck {
            generation: first.generation,
            revision: ReplayRevision(5),
            through: 100,
        }),
        Err(ReconcileAckPublishError::UnexpectedKey)
    );
    assert_eq!(
        sender.publish(ReconcileAck {
            generation: first.generation,
            revision: first.revision,
            through: 99,
        }),
        Err(ReconcileAckPublishError::ThroughBeforeTarget)
    );
    let ack = ReconcileAck {
        generation: first.generation,
        revision: first.revision,
        through: 100,
    };
    assert_eq!(sender.publish(ack), Ok(ReconcileAckUpdate::Published));
    assert_eq!(sender.publish(ack), Ok(ReconcileAckUpdate::Unchanged));
    assert_eq!(
        sender.publish(ReconcileAck {
            through: 101,
            ..ack
        }),
        Err(ReconcileAckPublishError::ConflictingThrough)
    );
    assert_eq!(receiver.changed().await, Ok(ack));
    assert_eq!(receiver.current(), Ok(Some(ack)));

    let next = ReconcileExpectation {
        generation: GapGeneration(2),
        revision: ReplayRevision(5),
        target_open_time: 200,
    };
    assert_eq!(
        receiver.register_expectation(next),
        Ok(ExpectationUpdate::Registered)
    );
    assert_eq!(receiver.current(), Ok(None));

    drop(receiver);
    assert_eq!(
        sender.publish(ReconcileAck {
            generation: next.generation,
            revision: next.revision,
            through: next.target_open_time,
        }),
        Err(ReconcileAckPublishError::Closed)
    );

    let (sender, mut receiver) = reconcile_ack_channel();
    drop(sender);
    assert_eq!(
        receiver.register_expectation(first),
        Err(ReconcileExpectationError::Closed)
    );
    assert!(receiver.current_expectation().is_err());
    assert!(receiver.current().is_err());
    assert!(receiver.changed().await.is_err());
}

#[tokio::test]
async fn producer_completion_is_observed_without_consuming_join_ownership() {
    let cancellation = CancellationToken::new();
    let supervisor_cancellation = cancellation.clone();
    let events: MarketEventStream = Box::pin(stream::empty());
    let feed = LiveFeed::spawn(events, cancellation, async move {
        supervisor_cancellation.cancelled().await;
        Err(ProviderError::Configuration("fake completion"))
    });
    let mut completion = feed.producer_completion.clone();
    assert_eq!(completion.current(), Ok(ProducerCompletion::Running));

    feed.request_shutdown();
    assert_eq!(
        completion.changed().await,
        Ok(ProducerCompletion::Finished(Err(
            ProviderError::Configuration("fake completion")
        )))
    );
    assert_eq!(
        completion.current(),
        Ok(ProducerCompletion::Finished(Err(
            ProviderError::Configuration("fake completion")
        )))
    );
    assert_eq!(
        feed.join(MonoInstant::from_nanos(u64::MAX)).await,
        Err(fccli::provider::LiveFeedJoinError::Producer(
            ProviderError::Configuration("fake completion")
        ))
    );
}

#[tokio::test]
async fn manual_clock_drives_deadlines_timeouts_and_cancellation_deterministically() {
    let clock = ManualClock::new(MonoInstant::from_nanos(100));
    assert_eq!(clock.now(), MonoInstant::from_nanos(100));
    let deadline =
        checked_deadline(clock.now(), Duration::from_nanos(50)).expect("checked deadline");
    assert_eq!(deadline, MonoInstant::from_nanos(150));
    assert!(checked_deadline(MonoInstant::from_nanos(u64::MAX), Duration::from_nanos(1)).is_err());
    assert!(clock.advance_to(MonoInstant::from_nanos(99)).is_err());

    let sleeper_clock = clock.clone();
    let sleeper = tokio::spawn(async move {
        sleeper_clock
            .sleep_until(MonoInstant::from_nanos(150))
            .await;
        sleeper_clock.now()
    });
    tokio::task::yield_now().await;
    assert!(!sleeper.is_finished());
    clock
        .advance_to(MonoInstant::from_nanos(149))
        .expect("advance");
    tokio::task::yield_now().await;
    assert!(!sleeper.is_finished());
    clock.advance_by(Duration::from_nanos(1)).expect("deadline");
    assert_eq!(
        sleeper.await.expect("sleep task"),
        MonoInstant::from_nanos(150)
    );

    let cancellation = CancellationToken::new();
    let cancelled = cancellation.clone();
    let cancelled_clock = clock.clone();
    let wait = tokio::spawn(async move {
        sleep_until_or_cancelled(&cancelled_clock, MonoInstant::from_nanos(200), &cancelled).await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    assert_eq!(wait.await.expect("cancel wait"), SleepOutcome::Cancelled);

    let deadline_clock = clock.clone();
    let uncancelled = CancellationToken::new();
    let wait = tokio::spawn(async move {
        sleep_until_or_cancelled(&deadline_clock, MonoInstant::from_nanos(200), &uncancelled).await
    });
    tokio::task::yield_now().await;
    clock
        .advance_to(MonoInstant::from_nanos(200))
        .expect("advance");
    assert_eq!(wait.await.expect("deadline wait"), SleepOutcome::Deadline);
}
