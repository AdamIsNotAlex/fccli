use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
};

#[cfg(feature = "test-transport")]
use std::sync::atomic::AtomicUsize;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{
    error::{ErrorContext, ErrorOperation, ProviderError},
    model::{Candle, ConnectionStatus, GapGeneration, MarketEvent},
};

pub(crate) type StatusKey = (Option<GapGeneration>, ConnectionStatus);

pub(crate) struct EventEnvelope {
    item: Option<Result<MarketEvent, ProviderError>>,
    pub(crate) generation: Option<GapGeneration>,
    pub(crate) purge_on_invalidate: bool,
    connected_delivered: Arc<AtomicU64>,
    control_key: Option<StatusKey>,
    pending_controls: Arc<Mutex<Vec<StatusKey>>>,
    _regular_permit: Option<OwnedSemaphorePermit>,
    _control_permit: Option<OwnedSemaphorePermit>,
    pub(crate) emergency_slot: Option<u8>,
    pub(crate) emergency_barrier: Option<Arc<EmergencyBarrier>>,
}

impl EventEnvelope {
    pub(crate) fn into_item(mut self) -> Result<MarketEvent, ProviderError> {
        let item = self.item.take().expect("event envelope contains an item");
        if let Ok(MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::Connected,
        }) = &item
        {
            self.connected_delivered
                .fetch_max(generation.0, Ordering::AcqRel);
        }
        item
    }

    pub(crate) fn is_stopped(&self) -> bool {
        matches!(
            self.item.as_ref(),
            Some(Ok(MarketEvent::Status {
                generation: None,
                status: ConnectionStatus::Stopped,
            }))
        )
    }
}

impl Drop for EventEnvelope {
    fn drop(&mut self) {
        if let Some(key) = self.control_key {
            let mut pending = self
                .pending_controls
                .lock()
                .expect("control mutex poisoned");
            if let Some(index) = pending.iter().position(|pending_key| *pending_key == key) {
                pending.swap_remove(index);
            }
        }
        if let (Some(slot), Some(barrier)) = (self.emergency_slot, &self.emergency_barrier) {
            barrier.dequeued(slot);
        }
    }
}

pub(crate) struct EmergencyBarrier {
    pending: AtomicU8,
    suppressed: AtomicU8,
    notify: Notify,
}

impl EmergencyBarrier {
    const fn new() -> Self {
        Self {
            pending: AtomicU8::new(0),
            suppressed: AtomicU8::new(0),
            notify: Notify::const_new(),
        }
    }

    fn begin_pair(&self) -> Result<(), ProviderError> {
        self.suppressed.store(0, Ordering::Release);
        self.pending
            .compare_exchange(0, 0b11, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| ProviderError::Invariant("emergency barrier already active"))
    }

    fn dequeued(&self, slot: u8) {
        self.pending.fetch_and(!(1 << slot), Ordering::AcqRel);
        self.notify.notify_one();
    }

    fn suppress_pending(&self) {
        self.suppressed
            .fetch_or(self.pending.load(Ordering::Acquire), Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    fn begin_shutdown(&self) {
        self.suppressed.store(0, Ordering::Release);
        self.pending.store(0b01, Ordering::Release);
    }

    pub(crate) fn is_suppressed(&self, slot: u8) -> bool {
        self.suppressed.load(Ordering::Acquire) & (1 << slot) != 0
    }

    #[cfg(feature = "test-transport")]
    async fn wait_suppressed(&self, slot: u8) {
        while !self.is_suppressed(slot) {
            let notified = self.notify.notified();
            if self.is_suppressed(slot) {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn is_dequeued(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0
    }

    pub(crate) async fn wait_dequeued(&self) {
        while !self.is_dequeued() {
            let notified = self.notify.notified();
            if self.is_dequeued() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
pub(crate) struct EventEmitter {
    sender: mpsc::Sender<EventEnvelope>,
    regular_permits: Arc<Semaphore>,
    control_permits: Arc<Semaphore>,
    pub(crate) invalidated_through: Arc<AtomicU64>,
    connected_delivered: Arc<AtomicU64>,
    pending_controls: Arc<Mutex<Vec<StatusKey>>>,
    pub(crate) emergency_barrier: Arc<EmergencyBarrier>,
    #[cfg(feature = "test-transport")]
    control_saturation_attempts: Arc<AtomicUsize>,
    #[cfg(feature = "test-transport")]
    control_saturation_notify: Arc<Notify>,
    shutdown: Arc<AtomicBool>,
}
pub(crate) struct KeyedCandleBuffer {
    pending: BTreeMap<i64, Candle>,
    capacity: Option<usize>,
}

impl KeyedCandleBuffer {
    pub(crate) fn unbounded() -> Self {
        Self {
            pending: BTreeMap::new(),
            capacity: None,
        }
    }

    pub(crate) fn bounded(capacity: usize) -> Self {
        Self {
            pending: BTreeMap::new(),
            capacity: Some(capacity),
        }
    }

    pub(crate) fn push(&mut self, candidate: Candle) -> Result<bool, ProviderError> {
        use crate::model::FinalityAuthority::{
            RestProvisionalClosed, RestProvisionalOpen, WsAuthoritativeClosed, WsAuthoritativeOpen,
        };

        let key = candidate.open_time();
        let Some(current) = self.pending.get(&key) else {
            if self
                .capacity
                .is_some_and(|capacity| self.pending.len() == capacity)
            {
                return Err(ProviderError::QueueSaturated);
            }
            self.pending.insert(key, candidate);
            return Ok(true);
        };
        let replace = match (current.authority(), candidate.authority()) {
            (_, WsAuthoritativeClosed) => true,
            (WsAuthoritativeClosed, _) => false,
            (WsAuthoritativeOpen, RestProvisionalOpen | RestProvisionalClosed) => false,
            (RestProvisionalOpen | RestProvisionalClosed, WsAuthoritativeOpen) => true,
            (WsAuthoritativeOpen, WsAuthoritativeOpen) => true,
            (RestProvisionalClosed, RestProvisionalOpen) => false,
            (RestProvisionalOpen, RestProvisionalClosed)
            | (RestProvisionalOpen, RestProvisionalOpen)
            | (RestProvisionalClosed, RestProvisionalClosed) => true,
        };
        if replace {
            self.pending.insert(key, candidate);
        }
        Ok(replace)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn contains_key(&self, key: i64) -> bool {
        self.pending.contains_key(&key)
    }

    pub(crate) fn pop_first(&mut self) -> Option<Candle> {
        self.pending.pop_first().map(|(_, candle)| candle)
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &Candle> {
        self.pending.values()
    }
}

#[cfg(feature = "test-transport")]
pub struct EventEmitterTestFacade {
    emitter: EventEmitter,
    receiver: mpsc::Receiver<EventEnvelope>,
    keyed: KeyedCandleBuffer,
    generation: GapGeneration,
}
#[cfg(feature = "test-transport")]
#[derive(Clone)]
pub struct EventEmitterTestSender(EventEmitter);

#[cfg(feature = "test-transport")]
impl EventEmitterTestSender {
    pub async fn send(&self, event: MarketEvent) -> Result<(), ProviderError> {
        self.0.send_regular(event).await
    }

    pub async fn shutdown(&self) {
        self.0.shutdown().await;
    }

    pub async fn wait_emergency_suppressed(&self, slot: u8) {
        self.0.emergency_barrier.wait_suppressed(slot).await;
    }

    pub async fn wait_control_saturation_attempts(&self, expected: usize) {
        while self.0.control_saturation_attempts.load(Ordering::Acquire) < expected {
            let notified = self.0.control_saturation_notify.notified();
            if self.0.control_saturation_attempts.load(Ordering::Acquire) >= expected {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(feature = "test-transport")]
impl EventEmitterTestFacade {
    pub fn new(keyed_capacity: usize) -> Self {
        let physical_capacity = keyed_capacity
            .checked_add(2)
            .expect("test emitter capacity overflow");
        let (sender, receiver) = mpsc::channel(physical_capacity);
        Self {
            emitter: EventEmitter::new(sender, keyed_capacity, 1),
            receiver,
            keyed: KeyedCandleBuffer::bounded(keyed_capacity),
            generation: GapGeneration(1),
        }
    }

    pub fn queue_candle(&mut self, candle: Candle) -> Result<bool, ProviderError> {
        self.keyed.push(candle)
    }

    pub async fn flush_one(&mut self) -> Result<bool, ProviderError> {
        let Some(candle) = self.keyed.pop_first() else {
            return Ok(false);
        };
        self.emitter
            .send_regular(MarketEvent::Candle {
                generation: self.generation,
                candle,
            })
            .await?;
        Ok(true)
    }

    pub async fn send(&self, event: MarketEvent) -> Result<(), ProviderError> {
        self.emitter.send_regular(event).await
    }

    pub fn queue_emergency_pair(
        &self,
        first: MarketEvent,
        second: MarketEvent,
    ) -> Result<(), ProviderError> {
        self.emitter.queue_emergency_pair(first, second).map(|_| ())
    }

    pub async fn shutdown(&self) {
        self.emitter.shutdown().await;
    }

    pub async fn recv(&mut self) -> Option<Result<MarketEvent, ProviderError>> {
        loop {
            let envelope = self.receiver.recv().await?;
            let suppressed = envelope.emergency_slot.is_some_and(|slot| {
                envelope
                    .emergency_barrier
                    .as_ref()
                    .is_some_and(|barrier| barrier.is_suppressed(slot))
            });
            if !suppressed {
                return Some(envelope.into_item());
            }
        }
    }

    pub fn try_recv(&mut self) -> Option<Result<MarketEvent, ProviderError>> {
        loop {
            let envelope = self.receiver.try_recv().ok()?;
            let suppressed = envelope.emergency_slot.is_some_and(|slot| {
                envelope
                    .emergency_barrier
                    .as_ref()
                    .is_some_and(|barrier| barrier.is_suppressed(slot))
            });
            if !suppressed {
                return Some(envelope.into_item());
            }
        }
    }

    pub fn close_receiver(&mut self) {
        let (_sender, receiver) = mpsc::channel(1);
        let previous = std::mem::replace(&mut self.receiver, receiver);
        drop(previous);
    }

    pub fn clone_emitter(&self) -> EventEmitterTestSender {
        EventEmitterTestSender(self.emitter.clone())
    }
}

impl EventEmitter {
    pub(crate) fn new(
        sender: mpsc::Sender<EventEnvelope>,
        regular_capacity: usize,
        control_capacity: usize,
    ) -> Self {
        Self {
            sender,
            regular_permits: Arc::new(Semaphore::new(regular_capacity)),
            control_permits: Arc::new(Semaphore::new(control_capacity)),
            invalidated_through: Arc::new(AtomicU64::new(0)),
            connected_delivered: Arc::new(AtomicU64::new(0)),
            pending_controls: Arc::new(Mutex::new(Vec::new())),
            emergency_barrier: Arc::new(EmergencyBarrier::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "test-transport")]
            control_saturation_attempts: Arc::new(AtomicUsize::new(0)),
            #[cfg(feature = "test-transport")]
            control_saturation_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) async fn reserve_regular(&self) -> Result<OwnedSemaphorePermit, ProviderError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(live_channel_closed());
        }
        Arc::clone(&self.regular_permits)
            .acquire_owned()
            .await
            .map_err(|_| live_channel_closed())
    }

    pub(crate) fn send_reserved(
        &self,
        permit: OwnedSemaphorePermit,
        event: MarketEvent,
    ) -> Result<(), ProviderError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(live_channel_closed());
        }
        let generation = event_generation(&event);
        let purge_on_invalidate = event_purges_with_generation(&event);
        self.sender
            .try_send(EventEnvelope {
                item: Some(Ok(event)),
                generation,
                purge_on_invalidate,
                connected_delivered: Arc::clone(&self.connected_delivered),
                control_key: None,
                pending_controls: Arc::clone(&self.pending_controls),
                _regular_permit: Some(permit),
                _control_permit: None,
                emergency_slot: None,
                emergency_barrier: None,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Closed(_) => live_channel_closed(),
                mpsc::error::TrySendError::Full(_) => {
                    ProviderError::Invariant("reserved market event channel capacity exhausted")
                }
            })
    }

    pub(crate) async fn wait_closed(&self) {
        self.sender.closed().await;
    }

    pub(crate) async fn send_regular(&self, event: MarketEvent) -> Result<(), ProviderError> {
        let control_key = status_key(&event);
        if control_key.is_some_and(|key| {
            self.pending_controls
                .lock()
                .expect("control mutex poisoned")
                .contains(&key)
        }) {
            return Ok(());
        }
        let control_permit = if is_control_event(&event) {
            #[cfg(feature = "test-transport")]
            if self.control_permits.available_permits() == 0 {
                self.control_saturation_attempts
                    .fetch_add(1, Ordering::AcqRel);
                self.control_saturation_notify.notify_waiters();
            }
            Some(
                Arc::clone(&self.control_permits)
                    .acquire_owned()
                    .await
                    .map_err(|_| live_channel_closed())?,
            )
        } else {
            None
        };
        let permit = self.reserve_regular().await?;
        if self.shutdown.load(Ordering::Acquire) {
            return Err(live_channel_closed());
        }
        if let Some(key) = control_key {
            self.pending_controls
                .lock()
                .expect("control mutex poisoned")
                .push(key);
        }
        let generation = event_generation(&event);
        let purge_on_invalidate = event_purges_with_generation(&event);
        self.sender
            .try_send(EventEnvelope {
                item: Some(Ok(event)),
                generation,
                purge_on_invalidate,
                connected_delivered: Arc::clone(&self.connected_delivered),
                control_key,
                pending_controls: Arc::clone(&self.pending_controls),
                _regular_permit: Some(permit),
                _control_permit: control_permit,
                emergency_slot: None,
                emergency_barrier: None,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Closed(_) => live_channel_closed(),
                mpsc::error::TrySendError::Full(_) => {
                    ProviderError::Invariant("reserved market event channel capacity exhausted")
                }
            })
    }

    pub(crate) fn connected_delivered(&self, generation: GapGeneration) -> bool {
        self.connected_delivered.load(Ordering::Acquire) >= generation.0
    }

    pub(crate) fn invalidate_generation(&self, generation: GapGeneration) {
        self.invalidated_through
            .fetch_max(generation.0, Ordering::AcqRel);
    }

    pub(crate) fn queue_emergency_pair(
        &self,
        first: MarketEvent,
        second: MarketEvent,
    ) -> Result<Arc<EmergencyBarrier>, ProviderError> {
        self.emergency_barrier.begin_pair()?;
        for (slot, event) in [first, second].into_iter().enumerate() {
            self.sender
                .try_send(EventEnvelope {
                    item: Some(Ok(event)),
                    generation: None,
                    purge_on_invalidate: false,
                    connected_delivered: Arc::clone(&self.connected_delivered),
                    control_key: None,
                    pending_controls: Arc::clone(&self.pending_controls),
                    _regular_permit: None,
                    _control_permit: None,
                    emergency_slot: Some(slot as u8),
                    emergency_barrier: Some(Arc::clone(&self.emergency_barrier)),
                })
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Closed(_) => live_channel_closed(),
                    mpsc::error::TrySendError::Full(_) => {
                        ProviderError::Invariant("emergency market event reservation exhausted")
                    }
                })?;
        }
        Ok(Arc::clone(&self.emergency_barrier))
    }

    pub(crate) async fn queue_terminal_pair(
        &self,
        first: MarketEvent,
        second: MarketEvent,
    ) -> Result<(), ProviderError> {
        self.emergency_barrier.suppress_pending();
        self.emergency_barrier.wait_dequeued().await;
        let _ = self.queue_emergency_pair(first, second)?;
        Ok(())
    }

    pub(crate) async fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.invalidated_through.store(u64::MAX, Ordering::Release);
        self.emergency_barrier.suppress_pending();
        // Cancellation makes the stream discard every non-Stopped envelope. Wait until any
        // reserved saturation pair has actually left the bounded channel before reusing its
        // reservation for the sole terminal event. Receiver drop also drops the queued
        // envelopes and releases this barrier, so shutdown cannot deadlock on a closed stream.
        self.emergency_barrier.wait_dequeued().await;
        self.emergency_barrier.begin_shutdown();
        let envelope = EventEnvelope {
            item: Some(Ok(MarketEvent::Status {
                generation: None,
                status: ConnectionStatus::Stopped,
            })),
            generation: None,
            purge_on_invalidate: false,
            connected_delivered: Arc::clone(&self.connected_delivered),
            control_key: None,
            pending_controls: Arc::clone(&self.pending_controls),
            _regular_permit: None,
            _control_permit: None,
            emergency_slot: Some(0),
            emergency_barrier: Some(Arc::clone(&self.emergency_barrier)),
        };
        let _ = self.sender.try_send(envelope);
    }
}

fn event_generation(event: &MarketEvent) -> Option<GapGeneration> {
    match event {
        MarketEvent::Status { generation, .. }
        | MarketEvent::RecoverableError { generation, .. } => *generation,
        MarketEvent::ReconcileBatch { generation, .. } | MarketEvent::Candle { generation, .. } => {
            Some(*generation)
        }
        MarketEvent::TerminalError(_) => None,
    }
}
fn is_control_event(event: &MarketEvent) -> bool {
    !matches!(
        event,
        MarketEvent::Candle { .. } | MarketEvent::ReconcileBatch { .. }
    )
}

fn event_purges_with_generation(event: &MarketEvent) -> bool {
    matches!(
        event,
        MarketEvent::Candle { .. }
            | MarketEvent::ReconcileBatch { .. }
            | MarketEvent::Status {
                generation: Some(_),
                status: ConnectionStatus::Connecting
                    | ConnectionStatus::GapSync
                    | ConnectionStatus::Connected,
            }
    )
}
fn status_key(event: &MarketEvent) -> Option<(Option<GapGeneration>, ConnectionStatus)> {
    match event {
        MarketEvent::Status { generation, status } => Some((*generation, *status)),
        _ => None,
    }
}

pub(crate) fn live_channel_closed() -> ProviderError {
    ProviderError::ChannelClosed {
        context: ErrorContext::operation(ErrorOperation::LiveFeed),
    }
}
