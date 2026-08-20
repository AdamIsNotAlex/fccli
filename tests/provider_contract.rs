use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use fccli::{
    cli::canonicalize_instrument,
    clock::{Clock, ManualClock, SleepOutcome, checked_deadline, sleep_until_or_cancelled},
    error::ProviderError,
    model::{
        Candle, ConnectionStatus, GapGeneration, HistoryRequest, Instrument, InstrumentSpec,
        Market, MarketEvent, MonoInstant, ProviderId, ReplayRevision, Timeframe,
    },
    provider::{
        AcceptedWatermarkUpdateError, ExpectationUpdate, LiveFeed, LiveRequest, MarketDataProvider,
        MarketEventStream, ProcessBlocker, ProducerCompletion, ProviderCapabilities,
        ProviderFuture, ProviderRegistry, RateGateSnapshot, RateGateState, ReconcileAck,
        ReconcileAckPublishError, ReconcileAckUpdate, ReconcileExpectation,
        ReconcileExpectationError, WatermarkUpdate, accepted_watermark_channel,
        binance::BinanceProvider, hyperliquid::HyperliquidProvider, reconcile_ack_channel,
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
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            markets: &[Market::Spot],
            timeframes: &[Timeframe::Minute1],
            history_page_limit: 1000,
        }
    }

    fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        Instrument::new(
            spec.provider().clone(),
            spec.market(),
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
            Ok(LiveFeed::spawn(
                events,
                cancellation,
                Arc::new(ManualClock::new(MonoInstant::from_nanos(0))),
                async move {
                    supervisor_cancellation.cancelled().await;
                    Ok(())
                },
            ))
        })
    }

    fn rate_gate(&self) -> RateGateSnapshot {
        self.gate.clone()
    }
}

fn assert_object_safe(_: Arc<dyn MarketDataProvider>) {}

#[test]
fn capabilities_are_public_data_support_and_nonzero_history_maximum_only() {
    let (gate_tx, gate) = fccli::provider::rate_gate_channel(RateGateState::Open);
    let provider = FakeProvider { gate };
    let ProviderCapabilities {
        markets,
        timeframes,
        history_page_limit,
    } = provider.capabilities();
    assert_eq!(markets, &[Market::Spot]);
    assert_eq!(timeframes, &[Timeframe::Minute1]);
    assert_ne!(history_page_limit, 0);
    drop(gate_tx);
}

#[test]
fn real_provider_capabilities_match_public_support_contract() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let binance = BinanceProvider::new_test("http://127.0.0.1:1", Arc::clone(&clock))
        .expect("test Binance provider");
    let binance = binance.capabilities();
    assert_eq!(binance.timeframes, &Timeframe::ALL);
    assert!(!binance.markets.is_empty());
    assert_ne!(binance.history_page_limit, 0);

    let hyperliquid = HyperliquidProvider::new_test("http://127.0.0.1:1", clock)
        .expect("test Hyperliquid provider");
    let hyperliquid = hyperliquid.capabilities();
    assert!(!hyperliquid.markets.is_empty());
    assert_ne!(hyperliquid.history_page_limit, 0);
    assert!(!hyperliquid.timeframes.contains(&Timeframe::Second1));
    assert!(!hyperliquid.timeframes.contains(&Timeframe::Hour6));
    for supported in [
        Timeframe::Minute1,
        Timeframe::Hour1,
        Timeframe::Day1,
        Timeframe::Month1,
    ] {
        assert!(hyperliquid.timeframes.contains(&supported));
    }
}

#[test]
fn registry_registers_two_providers_and_supports_borrowed_lookup() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let binance: Arc<dyn MarketDataProvider> = Arc::new(
        BinanceProvider::new_test("http://127.0.0.1:1", Arc::clone(&clock))
            .expect("test Binance provider"),
    );
    let hyperliquid: Arc<dyn MarketDataProvider> = Arc::new(
        HyperliquidProvider::new_test("http://127.0.0.1:1", clock)
            .expect("test Hyperliquid provider"),
    );
    let registry = ProviderRegistry::new([Arc::clone(&binance), Arc::clone(&hyperliquid)])
        .expect("unique providers");

    let binance_id = ProviderId::new("binance").expect("provider id");
    let selected = registry.get(&binance_id).expect("registered Binance");
    assert!(Arc::ptr_eq(&selected, &binance));
    let hyperliquid_id = ProviderId::new("hyperliquid").expect("provider id");
    assert!(Arc::ptr_eq(
        &registry
            .get(&hyperliquid_id)
            .expect("registered Hyperliquid"),
        &hyperliquid,
    ));
}

#[test]
fn registry_rejects_duplicate_ids_and_accepts_an_ordinary_test_provider() {
    let (first_gate_tx, first_gate) = fccli::provider::rate_gate_channel(RateGateState::Open);
    let (duplicate_gate_tx, duplicate_gate) =
        fccli::provider::rate_gate_channel(RateGateState::Open);
    let first: Arc<dyn MarketDataProvider> = Arc::new(FakeProvider { gate: first_gate });
    let duplicate: Arc<dyn MarketDataProvider> = Arc::new(FakeProvider {
        gate: duplicate_gate,
    });

    assert!(matches!(
        ProviderRegistry::new([Arc::clone(&first), Arc::clone(&duplicate)]),
        Err(ProviderError::Configuration(
            "duplicate market-data provider"
        ))
    ));

    let mut registry = ProviderRegistry::new([Arc::clone(&first)]).expect("unique fake provider");
    assert!(matches!(
        registry.register(duplicate),
        Err(ProviderError::Configuration(
            "duplicate market-data provider"
        ))
    ));
    let fake_id = ProviderId::new("fake").expect("provider id");
    assert!(Arc::ptr_eq(
        &registry.get(&fake_id).expect("registered fake provider"),
        &first,
    ));
    drop((first_gate_tx, duplicate_gate_tx));
}

#[test]
fn canonicalization_metadata_does_not_register_a_transport() {
    let registry = ProviderRegistry::new(std::iter::empty::<Arc<dyn MarketDataProvider>>())
        .expect("empty registry");
    let okx_id = ProviderId::new("okx").expect("provider id");
    let specification = InstrumentSpec::new(okx_id.clone(), "btc", None::<String>)
        .expect("known provider specification");
    let instrument = canonicalize_instrument(&specification).expect("metadata canonicalization");
    assert_eq!(instrument.quote(), "USDT");
    assert!(matches!(
        registry.get(&okx_id),
        Err(ProviderError::Configuration(
            "unsupported market-data provider"
        ))
    ));
    let unknown_id = ProviderId::new("unknown").expect("provider id");
    assert!(matches!(
        registry.get(&unknown_id),
        Err(ProviderError::Configuration(
            "unsupported market-data provider"
        ))
    ));
}

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
    fn assert_eq_bound<T: Eq>() {}
    assert_eq_bound::<ProducerCompletion>();

    let cancellation = CancellationToken::new();
    let supervisor_cancellation = cancellation.clone();
    let clock = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let events: MarketEventStream = Box::pin(stream::empty());
    let feed = LiveFeed::spawn(events, cancellation, clock, async move {
        supervisor_cancellation.cancelled().await;
        Err(ProviderError::Configuration("fake completion"))
    });
    let mut completion = feed.producer_completion.clone();
    let mut independent_cursor = completion.clone();
    assert_eq!(completion.current(), Ok(ProducerCompletion::Running));

    feed.request_shutdown();
    let finished =
        ProducerCompletion::Finished(Err(ProviderError::Configuration("fake completion")));
    assert_eq!(completion.changed().await, Ok(finished.clone()));
    assert_eq!(completion.current(), Ok(finished.clone()));
    assert!(completion.changed().await.is_err());

    assert_eq!(independent_cursor.changed().await, Ok(finished.clone()));
    let mut clone_after_delivery = independent_cursor.clone();
    assert!(independent_cursor.changed().await.is_err());
    assert!(clone_after_delivery.changed().await.is_err());

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

struct DropProbe {
    dropped: Arc<AtomicBool>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

async fn wait_until_true(flag: &AtomicBool) {
    for _ in 0..100 {
        if flag.load(Ordering::SeqCst) {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert!(flag.load(Ordering::SeqCst), "task cleanup did not complete");
}

#[tokio::test]
async fn manual_clock_bounds_pending_supervisor_join_and_abort_cleanup() {
    let clock = Arc::new(ManualClock::new(MonoInstant::from_nanos(1_000)));
    let cancellation = CancellationToken::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe {
        dropped: dropped.clone(),
    };
    let feed = LiveFeed::spawn(
        Box::pin(stream::empty()),
        cancellation,
        clock.clone(),
        async move {
            let _probe = probe;
            std::future::pending::<Result<(), ProviderError>>().await
        },
    );
    let deadline = checked_deadline(clock.now(), Duration::from_nanos(50)).expect("deadline");
    let join = tokio::spawn(feed.join(deadline));
    tokio::task::yield_now().await;
    assert!(!join.is_finished());

    clock
        .advance_to(MonoInstant::from_nanos(1_049))
        .expect("before deadline");
    tokio::task::yield_now().await;
    assert!(!join.is_finished());
    clock.advance_to(deadline).expect("exact deadline");
    assert_eq!(
        join.await.expect("join task"),
        Err(fccli::provider::LiveFeedJoinError::DeadlineElapsed)
    );
    wait_until_true(&dropped).await;
}

#[tokio::test]
async fn dropped_feed_and_cancelled_join_future_abort_supervisor_tasks() {
    let dropped_feed = Arc::new(AtomicBool::new(false));
    let feed_probe = DropProbe {
        dropped: dropped_feed.clone(),
    };
    let feed = LiveFeed::spawn(
        Box::pin(stream::empty()),
        CancellationToken::new(),
        Arc::new(ManualClock::new(MonoInstant::from_nanos(0))),
        async move {
            let _probe = feed_probe;
            std::future::pending::<Result<(), ProviderError>>().await
        },
    );
    tokio::task::yield_now().await;
    drop(feed);
    wait_until_true(&dropped_feed).await;

    let dropped_join = Arc::new(AtomicBool::new(false));
    let join_probe = DropProbe {
        dropped: dropped_join.clone(),
    };
    let feed = LiveFeed::spawn(
        Box::pin(stream::empty()),
        CancellationToken::new(),
        Arc::new(ManualClock::new(MonoInstant::from_nanos(0))),
        async move {
            let _probe = join_probe;
            std::future::pending::<Result<(), ProviderError>>().await
        },
    );
    let join = tokio::spawn(feed.join(MonoInstant::from_nanos(100)));
    tokio::task::yield_now().await;
    join.abort();
    assert!(join.await.expect_err("join future aborted").is_cancelled());
    wait_until_true(&dropped_join).await;
}

#[test]
fn manual_clock_concurrent_advances_are_monotonic_and_lossless() {
    const WORKERS: usize = 8;
    const INCREMENTS: usize = 1_000;
    let clock = ManualClock::new(MonoInstant::from_nanos(0));
    let barrier = Arc::new(Barrier::new(WORKERS));
    let mut workers = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let clock = clock.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..INCREMENTS {
                clock
                    .advance_by(Duration::from_nanos(1))
                    .expect("increment");
            }
        }));
    }
    for worker in workers {
        worker.join().expect("clock worker");
    }
    assert_eq!(
        clock.now(),
        MonoInstant::from_nanos((WORKERS * INCREMENTS) as u64)
    );

    let later = MonoInstant::from_nanos(clock.now().as_nanos() + 2);
    clock.advance_to(later).expect("advance later");
    assert!(
        clock
            .advance_to(MonoInstant::from_nanos(later.as_nanos() - 1))
            .is_err()
    );
    assert_eq!(clock.now(), later);
}

#[tokio::test]
async fn rate_gate_deadlines_never_shorten_and_process_block_is_absorbing() {
    let (sender, snapshot) = fccli::provider::rate_gate_channel(RateGateState::Open);
    let barrier = Arc::new(Barrier::new(3));
    let later_sender = sender.clone();
    let later_barrier = barrier.clone();
    let later = std::thread::spawn(move || {
        later_barrier.wait();
        later_sender
            .publish(RateGateState::TimedUntil(MonoInstant::from_nanos(300)))
            .expect("publish later deadline");
    });
    let earlier_sender = sender.clone();
    let earlier_barrier = barrier.clone();
    let earlier = std::thread::spawn(move || {
        earlier_barrier.wait();
        earlier_sender
            .publish(RateGateState::TimedUntil(MonoInstant::from_nanos(200)))
            .expect("publish earlier deadline");
    });
    barrier.wait();
    later.join().expect("later publisher");
    earlier.join().expect("earlier publisher");
    assert_eq!(
        snapshot.current(),
        Ok(RateGateState::TimedUntil(MonoInstant::from_nanos(300)))
    );

    sender
        .publish(RateGateState::ProcessBlocked(
            ProcessBlocker::InvalidBanExpiry,
        ))
        .expect("block process");
    sender.publish(RateGateState::Open).expect("ignored open");
    sender
        .publish(RateGateState::TimedUntil(MonoInstant::from_nanos(u64::MAX)))
        .expect("ignored deadline");
    assert_eq!(
        snapshot.current(),
        Ok(RateGateState::ProcessBlocked(
            ProcessBlocker::InvalidBanExpiry
        ))
    );
}
