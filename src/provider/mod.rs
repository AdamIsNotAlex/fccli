//! Provider-neutral market-data interfaces and ownership primitives.

pub mod binance;
pub mod hyperliquid;
pub(crate) mod runtime;
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[doc(hidden)]
pub mod test_transport;
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[doc(hidden)]
pub use runtime::live::{
    CONTROL_CAPACITY, EMERGENCY_CONTROL_CAPACITY, FIRST_KLINE_HANDSHAKE_TIMEOUT,
    KEYED_CANDLE_CAPACITY, LiveCompletionDisposition, LiveErrorClassification,
    LiveErrorDisposition, LiveInBandEventDisposition, LiveInputClassification, LiveSocketEvent,
    LiveSupervisorConfig, MARKET_EVENT_CHANNEL_CAPACITY, RECONCILE_ACK_TIMEOUT,
    classify_live_error_for_test, classify_live_input_for_test,
    gap_target_within_generation_span_for_test, reconciliation_distinct_key_allowed_for_test,
    reconciliation_page_guard_for_test,
};

use futures_util::Stream;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, watch};

use crate::{
    clock::Clock,
    error::ProviderError,
    model::{
        Candle, GapGeneration, HistoryRequest, Instrument, InstrumentSpec, Market, MarketEvent,
        MonoInstant, ProviderId, ReplayRevision, Timeframe,
    },
};

pub type CancellationToken = tokio_util::sync::CancellationToken;

pub type ProviderFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProviderError>> + Send + 'a>>;
pub type MarketEventStream =
    Pin<Box<dyn Stream<Item = Result<MarketEvent, ProviderError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderCapabilities {
    pub markets: &'static [Market],
    pub timeframes: &'static [Timeframe],
    /// Maximum rows accepted by one provider history request. Implementations must return non-zero.
    pub history_page_limit: u16,
}

pub trait MarketDataProvider: Send + Sync {
    fn id(&self) -> ProviderId;
    fn capabilities(&self) -> ProviderCapabilities;
    fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError>;
    fn history<'a>(
        &'a self,
        instrument: &'a Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>>;
    fn open_live<'a>(&'a self, request: LiveRequest) -> ProviderFuture<'a, LiveFeed>;
    fn rate_gate(&self) -> RateGateSnapshot;
}

#[derive(Clone)]
pub struct ProviderRegistry {
    binance: Arc<binance::BinanceProvider>,
    hyperliquid: Option<Arc<hyperliquid::HyperliquidProvider>>,
    #[cfg(feature = "test-transport")]
    injected: Option<Arc<dyn MarketDataProvider>>,
}

impl ProviderRegistry {
    #[must_use]
    pub fn new(binance: Arc<binance::BinanceProvider>) -> Self {
        Self {
            binance,
            hyperliquid: None,
            #[cfg(feature = "test-transport")]
            injected: None,
        }
    }

    #[must_use]
    pub fn with_hyperliquid(mut self, provider: Arc<hyperliquid::HyperliquidProvider>) -> Self {
        self.hyperliquid = Some(provider);
        self
    }

    #[cfg(feature = "test-transport")]
    #[must_use]
    pub fn with_test_provider(
        binance: Arc<binance::BinanceProvider>,
        provider: Arc<dyn MarketDataProvider>,
    ) -> Self {
        Self {
            binance,
            hyperliquid: None,
            injected: Some(provider),
        }
    }

    pub fn get(&self, id: ProviderId) -> Result<Arc<dyn MarketDataProvider>, ProviderError> {
        #[cfg(feature = "test-transport")]
        if id.as_str() == "binance"
            && let Some(provider) = &self.injected
        {
            return Ok(Arc::clone(provider));
        }
        match id.as_str() {
            "binance" => Ok(self.binance.clone()),
            "hyperliquid" => self
                .hyperliquid
                .clone()
                .map(|provider| provider as _)
                .ok_or(ProviderError::Configuration(
                    "unsupported market-data provider",
                )),
            _ => Err(ProviderError::Configuration(
                "unsupported market-data provider",
            )),
        }
    }

    #[must_use]
    pub fn binance(&self) -> Arc<binance::BinanceProvider> {
        Arc::clone(&self.binance)
    }
}

pub struct LiveRequest {
    pub instrument: Instrument,
    pub timeframe: Timeframe,
    pub startup_watermark: Option<i64>,
    pub accepted_watermark_rx: AcceptedWatermarkReceiver,
    pub reconcile_ack_rx: ReconcileAckReceiver,
    pub cancellation: CancellationToken,
}

pub type AcceptedWatermark = Option<i64>;

#[derive(Clone)]
pub struct AcceptedWatermarkSender(Arc<Mutex<watch::Sender<AcceptedWatermark>>>);

#[derive(Clone)]
pub struct AcceptedWatermarkReceiver(watch::Receiver<AcceptedWatermark>);

pub fn accepted_watermark_channel(
    initial: AcceptedWatermark,
) -> (AcceptedWatermarkSender, AcceptedWatermarkReceiver) {
    let (sender, receiver) = watch::channel(initial);
    (
        AcceptedWatermarkSender(Arc::new(Mutex::new(sender))),
        AcceptedWatermarkReceiver(receiver),
    )
}

impl AcceptedWatermarkSender {
    pub fn publish(
        &self,
        value: AcceptedWatermark,
    ) -> Result<WatermarkUpdate, AcceptedWatermarkUpdateError> {
        let sender = self.0.lock().expect("watermark mutex poisoned");
        if sender.is_closed() {
            return Err(AcceptedWatermarkUpdateError::Closed);
        }
        let current = *sender.borrow();
        if value < current {
            return Err(AcceptedWatermarkUpdateError::Regression);
        }
        if value == current {
            return Ok(WatermarkUpdate::Unchanged);
        }
        sender
            .send(value)
            .map_err(|_| AcceptedWatermarkUpdateError::Closed)?;
        Ok(WatermarkUpdate::Advanced)
    }
}

impl AcceptedWatermarkReceiver {
    pub fn current(&self) -> Result<AcceptedWatermark, AcceptedWatermarkClosed> {
        self.0.has_changed().map_err(|_| AcceptedWatermarkClosed)?;
        Ok(*self.0.borrow())
    }

    pub async fn changed(&mut self) -> Result<AcceptedWatermark, AcceptedWatermarkClosed> {
        self.0
            .changed()
            .await
            .map_err(|_| AcceptedWatermarkClosed)?;
        Ok(*self.0.borrow_and_update())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatermarkUpdate {
    Advanced,
    Unchanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptedWatermarkUpdateError {
    Closed,
    Regression,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptedWatermarkClosed;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconcileAck {
    pub generation: GapGeneration,
    pub revision: ReplayRevision,
    pub through: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconcileExpectation {
    pub generation: GapGeneration,
    pub revision: ReplayRevision,
    pub target_open_time: i64,
}

#[derive(Default)]
struct AckState {
    expectation: Option<ReconcileExpectation>,
    ack: Option<ReconcileAck>,
    ack_version: u64,
    sender_count: usize,
    receiver_open: bool,
}

struct AckShared {
    state: Mutex<AckState>,
    notify: Notify,
}

pub struct ReconcileAckSender(Arc<AckShared>);

pub struct ReconcileAckReceiver {
    shared: Arc<AckShared>,
    seen_ack_version: u64,
}

pub fn reconcile_ack_channel() -> (ReconcileAckSender, ReconcileAckReceiver) {
    let shared = Arc::new(AckShared {
        state: Mutex::new(AckState {
            sender_count: 1,
            receiver_open: true,
            ..AckState::default()
        }),
        notify: Notify::new(),
    });
    (
        ReconcileAckSender(Arc::clone(&shared)),
        ReconcileAckReceiver {
            shared,
            seen_ack_version: 0,
        },
    )
}

impl Clone for ReconcileAckSender {
    fn clone(&self) -> Self {
        self.0
            .state
            .lock()
            .expect("ack mutex poisoned")
            .sender_count += 1;
        Self(Arc::clone(&self.0))
    }
}

impl Drop for ReconcileAckSender {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("ack mutex poisoned");
        state.sender_count -= 1;
        let closed = state.sender_count == 0;
        drop(state);
        if closed {
            self.0.notify.notify_waiters();
        }
    }
}

impl ReconcileAckSender {
    pub fn publish(
        &self,
        value: ReconcileAck,
    ) -> Result<ReconcileAckUpdate, ReconcileAckPublishError> {
        let mut state = self.0.state.lock().expect("ack mutex poisoned");
        if !state.receiver_open {
            return Err(ReconcileAckPublishError::Closed);
        }
        let expected = state
            .expectation
            .ok_or(ReconcileAckPublishError::NoExpectation)?;
        let actual_key = (value.generation, value.revision);
        let expected_key = (expected.generation, expected.revision);
        if actual_key < expected_key {
            return Err(ReconcileAckPublishError::Stale);
        }
        if actual_key != expected_key {
            return Err(ReconcileAckPublishError::UnexpectedKey);
        }
        if value.through < expected.target_open_time {
            return Err(ReconcileAckPublishError::ThroughBeforeTarget);
        }
        if let Some(current) = state.ack {
            if current == value {
                return Ok(ReconcileAckUpdate::Unchanged);
            }
            return Err(ReconcileAckPublishError::ConflictingThrough);
        }
        state.ack = Some(value);
        state.ack_version = state.ack_version.wrapping_add(1);
        drop(state);
        self.0.notify.notify_waiters();
        Ok(ReconcileAckUpdate::Published)
    }
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    #[doc(hidden)]
    pub fn inject_unchecked_for_test(
        &self,
        value: ReconcileAck,
    ) -> Result<(), ReconcileAckPublishError> {
        let mut state = self.0.state.lock().expect("ack mutex poisoned");
        if !state.receiver_open {
            return Err(ReconcileAckPublishError::Closed);
        }
        state.ack = Some(value);
        state.ack_version = state.ack_version.wrapping_add(1);
        drop(state);
        self.0.notify.notify_waiters();
        Ok(())
    }
}

impl ReconcileAckReceiver {
    pub fn register_expectation(
        &mut self,
        expected: ReconcileExpectation,
    ) -> Result<ExpectationUpdate, ReconcileExpectationError> {
        let mut state = self.shared.state.lock().expect("ack mutex poisoned");
        if state.sender_count == 0 {
            return Err(ReconcileExpectationError::Closed);
        }
        if let Some(current) = state.expectation {
            let current_key = (current.generation, current.revision);
            let next_key = (expected.generation, expected.revision);
            if next_key < current_key {
                return Err(ReconcileExpectationError::Regression);
            }
            if next_key == current_key {
                return if current == expected {
                    Ok(ExpectationUpdate::Unchanged)
                } else {
                    Err(ReconcileExpectationError::Conflict)
                };
            }
        }
        state.expectation = Some(expected);
        state.ack = None;
        self.seen_ack_version = state.ack_version;
        Ok(ExpectationUpdate::Registered)
    }

    pub fn current_expectation(&self) -> Result<Option<ReconcileExpectation>, ReconcileAckClosed> {
        let state = self.shared.state.lock().expect("ack mutex poisoned");
        if state.sender_count == 0 {
            return Err(ReconcileAckClosed);
        }
        Ok(state.expectation)
    }

    pub fn current(&self) -> Result<Option<ReconcileAck>, ReconcileAckClosed> {
        let state = self.shared.state.lock().expect("ack mutex poisoned");
        if state.sender_count == 0 {
            return Err(ReconcileAckClosed);
        }
        Ok(state.ack)
    }

    pub async fn changed(&mut self) -> Result<ReconcileAck, ReconcileAckClosed> {
        loop {
            let notified = self.shared.notify.notified();
            {
                let state = self.shared.state.lock().expect("ack mutex poisoned");
                if state.ack_version != self.seen_ack_version {
                    let ack = state.ack.expect("ack version requires an acknowledgement");
                    self.seen_ack_version = state.ack_version;
                    return Ok(ack);
                }
                if state.sender_count == 0 {
                    return Err(ReconcileAckClosed);
                }
            }
            notified.await;
        }
    }
}

impl Drop for ReconcileAckReceiver {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .expect("ack mutex poisoned")
            .receiver_open = false;
        self.shared.notify.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectationUpdate {
    Registered,
    Unchanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileAckUpdate {
    Published,
    Unchanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileAckPublishError {
    Closed,
    NoExpectation,
    Stale,
    UnexpectedKey,
    ConflictingThrough,
    ThroughBeforeTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconcileAckClosed;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileExpectationError {
    Closed,
    Regression,
    Conflict,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProducerCompletion {
    Running,
    Finished(Result<(), ProviderError>),
}

impl Eq for ProducerCompletion {}

pub struct ProducerCompletionReceiver {
    receiver: watch::Receiver<ProducerCompletion>,
    delivered_finished: bool,
}

impl Clone for ProducerCompletionReceiver {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
            delivered_finished: self.delivered_finished,
        }
    }
}

impl ProducerCompletionReceiver {
    pub fn current(&self) -> Result<ProducerCompletion, ProducerCompletionClosed> {
        match self.receiver.has_changed() {
            Ok(_) => Ok(self.receiver.borrow().clone()),
            Err(_) => self.final_value_or_closed(),
        }
    }

    pub async fn changed(&mut self) -> Result<ProducerCompletion, ProducerCompletionClosed> {
        match self.receiver.changed().await {
            Ok(()) => {
                let value = self.receiver.borrow_and_update().clone();
                if matches!(value, ProducerCompletion::Finished(_)) {
                    self.delivered_finished = true;
                }
                Ok(value)
            }
            Err(_) if !self.delivered_finished => {
                let value = self.final_value_or_closed()?;
                self.delivered_finished = true;
                Ok(value)
            }
            Err(_) => Err(ProducerCompletionClosed),
        }
    }

    fn final_value_or_closed(&self) -> Result<ProducerCompletion, ProducerCompletionClosed> {
        let value = self.receiver.borrow().clone();
        match value {
            ProducerCompletion::Finished(_) => Ok(value),
            ProducerCompletion::Running => Err(ProducerCompletionClosed),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerCompletionClosed;

pub struct LiveFeed {
    pub events: MarketEventStream,
    pub producer_completion: ProducerCompletionReceiver,
    cancellation: CancellationToken,
    clock: Arc<dyn Clock>,
    supervisor: Option<tokio::task::JoinHandle<Result<(), ProviderError>>>,
}

impl LiveFeed {
    pub fn spawn<F>(
        events: MarketEventStream,
        cancellation: CancellationToken,
        clock: Arc<dyn Clock>,
        supervisor: F,
    ) -> Self
    where
        F: Future<Output = Result<(), ProviderError>> + Send + 'static,
    {
        let (completion_tx, completion_rx) = watch::channel(ProducerCompletion::Running);
        let task = tokio::spawn(async move {
            let result = supervisor.await;
            completion_tx.send_replace(ProducerCompletion::Finished(result.clone()));
            result
        });
        Self {
            events,
            producer_completion: ProducerCompletionReceiver {
                receiver: completion_rx,
                delivered_finished: false,
            },
            cancellation,
            clock,
            supervisor: Some(task),
        }
    }

    pub fn request_shutdown(&self) {
        self.cancellation.cancel();
    }

    pub async fn join(mut self, deadline: MonoInstant) -> Result<(), LiveFeedJoinError> {
        let clock = Arc::clone(&self.clock);
        let outcome = {
            let supervisor = self
                .supervisor
                .as_mut()
                .expect("live-feed supervisor must be owned until join completes");
            tokio::select! {
                biased;
                result = supervisor => Some(result),
                () = clock.sleep_until(deadline) => None,
            }
        };

        match outcome {
            Some(result) => {
                self.supervisor = None;
                map_join_result(result)
            }
            None => {
                let supervisor = self
                    .supervisor
                    .take()
                    .expect("live-feed supervisor must be owned until timeout cleanup");
                supervisor.abort();
                let _ = supervisor.await;
                Err(LiveFeedJoinError::DeadlineElapsed)
            }
        }
    }
}

impl Drop for LiveFeed {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.abort();
        }
    }
}

fn map_join_result(
    result: Result<Result<(), ProviderError>, tokio::task::JoinError>,
) -> Result<(), LiveFeedJoinError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(LiveFeedJoinError::Producer(error)),
        Err(error) if error.is_cancelled() => Err(LiveFeedJoinError::Aborted),
        Err(_) => Err(LiveFeedJoinError::JoinFailure),
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum LiveFeedJoinError {
    #[error("live-feed producer failed: {0}")]
    Producer(ProviderError),
    #[error("live-feed join deadline elapsed")]
    DeadlineElapsed,
    #[error("live-feed supervisor was aborted")]
    Aborted,
    #[error("live-feed supervisor join failed")]
    JoinFailure,
}

pub use crate::model::{ProcessBlocker, RateGateState};

#[derive(Clone)]
pub struct RateGateSnapshot(watch::Receiver<RateGateState>);

impl RateGateSnapshot {
    pub fn current(&self) -> Result<RateGateState, RateGateClosed> {
        self.0.has_changed().map_err(|_| RateGateClosed)?;
        Ok(*self.0.borrow())
    }

    pub async fn changed(&mut self) -> Result<RateGateState, RateGateClosed> {
        self.0.changed().await.map_err(|_| RateGateClosed)?;
        Ok(*self.0.borrow_and_update())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateGateClosed;

#[derive(Clone)]
pub struct RateGateSender(Arc<Mutex<watch::Sender<RateGateState>>>);

pub fn rate_gate_channel(initial: RateGateState) -> (RateGateSender, RateGateSnapshot) {
    let (sender, receiver) = watch::channel(initial);
    (
        RateGateSender(Arc::new(Mutex::new(sender))),
        RateGateSnapshot(receiver),
    )
}

impl RateGateSender {
    pub fn publish(&self, requested: RateGateState) -> Result<(), RateGateClosed> {
        let sender = self.0.lock().expect("rate gate mutex poisoned");
        if sender.is_closed() {
            return Err(RateGateClosed);
        }

        let current = *sender.borrow();
        let effective = match (current, requested) {
            (RateGateState::ProcessBlocked(blocker), _) => RateGateState::ProcessBlocked(blocker),
            (_, RateGateState::ProcessBlocked(blocker)) => RateGateState::ProcessBlocked(blocker),
            (RateGateState::TimedUntil(current), RateGateState::TimedUntil(requested)) => {
                RateGateState::TimedUntil(current.max(requested))
            }
            (RateGateState::TimedUntil(deadline), RateGateState::Open) => {
                RateGateState::TimedUntil(deadline)
            }
            (RateGateState::Open, next) => next,
        };

        if effective != current {
            sender.send_replace(effective);
        }
        Ok(())
    }
}
