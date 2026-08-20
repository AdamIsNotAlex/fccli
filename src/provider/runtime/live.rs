use std::{
    future::Future,
    num::NonZeroU16,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use futures_util::{FutureExt, stream};
use time::OffsetDateTime;
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
use tokio::sync::Notify;
use tokio::sync::mpsc;

use crate::{
    clock::{Clock, checked_deadline},
    error::{ErrorContext, ErrorOperation, ProviderError, TimeoutKind},
    model::{
        Candle, ConnectionStatus, GapGeneration, HistoryRequest, Instrument, MarketEvent,
        MonoInstant, RateGateState, ReplayRevision, Timeframe,
    },
    provider::{
        CancellationToken, LiveFeed, LiveRequest, ProviderCapabilities, RateGateSnapshot,
        ReconcileAck, ReconcileExpectation, ReconcileExpectationError,
        runtime::{
            emitter::{EventEmitter, KeyedCandleBuffer, live_channel_closed},
            websocket::WsConfig,
        },
    },
};

pub const KEYED_CANDLE_CAPACITY: usize = 1024;
pub const CONTROL_CAPACITY: usize = 64;
pub const EMERGENCY_CONTROL_CAPACITY: usize = 2;
pub const MARKET_EVENT_CHANNEL_CAPACITY: usize = 256;
pub const FIRST_KLINE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
pub const RECONCILE_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SUPERVISOR_CAPACITY: usize = 65_536;

#[derive(Clone, Debug)]
pub struct LiveSupervisorConfig {
    pub keyed_candle_capacity: usize,
    pub control_capacity: usize,
    pub market_event_capacity: usize,
    pub first_kline_timeout: Duration,
    pub reconcile_ack_timeout: Duration,
    pub ws_config: WsConfig,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub stalled_write_probe_frames: usize,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub saturation_test_hook: Option<Arc<Notify>>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub generation_invalidated_test_hook: Option<Arc<Notify>>,
}

impl Default for LiveSupervisorConfig {
    fn default() -> Self {
        Self {
            keyed_candle_capacity: KEYED_CANDLE_CAPACITY,
            control_capacity: CONTROL_CAPACITY,
            market_event_capacity: MARKET_EVENT_CHANNEL_CAPACITY,
            first_kline_timeout: FIRST_KLINE_HANDSHAKE_TIMEOUT,
            reconcile_ack_timeout: RECONCILE_ACK_TIMEOUT,
            ws_config: WsConfig::default(),
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            stalled_write_probe_frames: 0,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            saturation_test_hook: None,
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            generation_invalidated_test_hook: None,
        }
    }
}

impl LiveSupervisorConfig {
    pub fn validate(&self) -> Result<(), ProviderError> {
        for capacity in [
            self.keyed_candle_capacity,
            self.control_capacity,
            self.market_event_capacity,
        ] {
            if !(1..=MAX_SUPERVISOR_CAPACITY).contains(&capacity) {
                return Err(ProviderError::Configuration(
                    "live supervisor capacity is outside 1..=65536",
                ));
            }
        }
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if self.stalled_write_probe_frames > MAX_SUPERVISOR_CAPACITY {
            return Err(ProviderError::Configuration(
                "live supervisor stalled-write probe is outside 0..=65536",
            ));
        }
        for timeout in [self.first_kline_timeout, self.reconcile_ack_timeout] {
            if !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&timeout) {
                return Err(ProviderError::Configuration(
                    "live supervisor timeout is outside 1ms..=60s",
                ));
            }
        }
        self.ws_config.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum LiveSocketEvent {
    Candle(Candle),
    Ignored,
    DecodedError(ProviderError),
    ReconnectRequested,
    ProtocolViolation(&'static str),
}

pub trait LiveSocket: Send {
    fn read(&mut self) -> impl Future<Output = Result<LiveSocketEvent, ProviderError>> + Send + '_;

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    fn after_gap_sync_test_probe(
        &mut self,
    ) -> impl Future<Output = Result<(), ProviderError>> + Send + '_;
}

pub trait LiveAdapter: Send + Sync + 'static {
    type Socket: LiveSocket + 'static;

    fn validate_request(
        &self,
        instrument: &Instrument,
        timeframe: Timeframe,
    ) -> Result<(), ProviderError>;

    fn connect_ready_socket(
        &self,
        instrument: Instrument,
        timeframe: Timeframe,
    ) -> impl Future<Output = Result<Self::Socket, ProviderError>> + Send + '_;

    fn history(
        &self,
        instrument: Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> impl Future<Output = Result<Vec<Candle>, ProviderError>> + Send + '_;

    fn rate_gate(&self) -> LiveRateGate;
    fn live_config(&self) -> LiveConfig<'_>;
    fn connection_rotation(&self) -> ConnectionRotation;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionRotation {
    Never,
    After {
        max_age: Duration,
        detail: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessBlockPolicy {
    InvalidBanExpiry,
    Forbidden(&'static str),
}

#[derive(Clone)]
pub struct LiveRateGate {
    pub snapshot: RateGateSnapshot,
    pub process_block: ProcessBlockPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconciliationPolicy {
    Unbounded,
    Bounded(ReconciliationLimits),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconciliationLimits {
    pub max_successors: usize,
    pub max_pages: usize,
    pub span_exceeded: &'static str,
    pub page_exceeded: &'static str,
    pub distinct_exceeded: &'static str,
}

pub struct LiveConfig<'a> {
    pub supervisor: &'a LiveSupervisorConfig,
    pub reconciliation: ReconciliationPolicy,
}

pub(crate) fn validate_runtime_contract(
    capabilities: ProviderCapabilities,
    config: LiveConfig<'_>,
    rotation: ConnectionRotation,
) -> Result<u16, ProviderError> {
    config.supervisor.validate()?;
    if let ReconciliationPolicy::Bounded(limits) = config.reconciliation {
        if limits.max_successors == 0
            || limits.max_pages == 0
            || limits.max_successors.checked_add(1).is_none()
        {
            return Err(ProviderError::Configuration(
                "live reconciliation limits must be positive and representable",
            ));
        }
    }
    if matches!(rotation, ConnectionRotation::After { max_age, .. } if max_age.is_zero()) {
        return Err(ProviderError::Configuration(
            "live connection max age must be positive",
        ));
    }
    if capabilities.history_page_limit == 0 {
        return Err(ProviderError::Configuration(
            "provider history page limit must be non-zero",
        ));
    }
    Ok(capabilities.history_page_limit)
}

pub(crate) fn advance_reconciliation_target(
    target_open_time: &mut i64,
    candidate: i64,
    generation_start: i64,
    timeframe: Timeframe,
    policy: ReconciliationPolicy,
) -> Result<(), ProviderError> {
    let candidate_target = (*target_open_time).max(candidate);
    if let ReconciliationPolicy::Bounded(limits) = policy
        && !gap_target_within_generation_span(
            timeframe,
            generation_start,
            candidate_target,
            limits.max_successors,
        )
    {
        return Err(ProviderError::Protocol {
            context: ErrorContext::operation(ErrorOperation::Reconciliation),
            detail: limits.span_exceeded,
        });
    }
    *target_open_time = candidate_target;
    Ok(())
}

pub(crate) fn apply_reconciliation_candle(
    pending: &mut KeyedCandleBuffer,
    candidate: Candle,
    target_open_time: &mut i64,
    generation_start: i64,
    timeframe: Timeframe,
    policy: ReconciliationPolicy,
) -> Result<bool, ProviderError> {
    let open_time = candidate.open_time();
    if let ReconciliationPolicy::Bounded(limits) = policy {
        let distinct_limit =
            limits
                .max_successors
                .checked_add(1)
                .ok_or(ProviderError::Invariant(
                    "reconciliation buffer bound overflow",
                ))?;
        if !pending.contains_key(open_time) && pending.len() >= distinct_limit {
            return Err(ProviderError::Protocol {
                context: ErrorContext::operation(ErrorOperation::Reconciliation),
                detail: limits.distinct_exceeded,
            });
        }
    }
    advance_reconciliation_target(
        target_open_time,
        open_time,
        generation_start,
        timeframe,
        policy,
    )?;
    pending.push(candidate)
}

pub(crate) fn advance_reconciliation_page(
    pages: &mut usize,
    policy: ReconciliationPolicy,
    context: ErrorContext,
) -> Result<(), ProviderError> {
    *pages = pages.checked_add(1).ok_or(ProviderError::Invariant(
        "reconciliation page count overflow",
    ))?;
    if let ReconciliationPolicy::Bounded(limits) = policy
        && *pages > limits.max_pages
    {
        return Err(ProviderError::Protocol {
            context,
            detail: limits.page_exceeded,
        });
    }
    Ok(())
}

fn gap_target_within_generation_span(
    timeframe: Timeframe,
    start: i64,
    target: i64,
    maximum_successors: usize,
) -> bool {
    if target < start {
        return true;
    }
    let Ok(maximum_successors) = i64::try_from(maximum_successors) else {
        return false;
    };
    if let Some(interval) = fixed_timeframe_milliseconds(timeframe) {
        return maximum_successors
            .checked_mul(interval)
            .and_then(|span| start.checked_add(span))
            .is_some_and(|maximum| target <= maximum);
    }
    if timeframe != Timeframe::Month1 {
        return false;
    }
    let Some(start_index) = calendar_month_index(start) else {
        return false;
    };
    let Some(target_index) = calendar_month_index(target) else {
        return false;
    };
    target_index
        .checked_sub(start_index)
        .is_some_and(|distance| distance <= maximum_successors)
}

fn fixed_timeframe_milliseconds(timeframe: Timeframe) -> Option<i64> {
    match timeframe {
        Timeframe::Second1 => Some(1_000),
        Timeframe::Minute1 => Some(60_000),
        Timeframe::Minute3 => Some(3 * 60_000),
        Timeframe::Minute5 => Some(5 * 60_000),
        Timeframe::Minute15 => Some(15 * 60_000),
        Timeframe::Minute30 => Some(30 * 60_000),
        Timeframe::Hour1 => Some(60 * 60_000),
        Timeframe::Hour2 => Some(2 * 60 * 60_000),
        Timeframe::Hour4 => Some(4 * 60 * 60_000),
        Timeframe::Hour6 => Some(6 * 60 * 60_000),
        Timeframe::Hour8 => Some(8 * 60 * 60_000),
        Timeframe::Hour12 => Some(12 * 60 * 60_000),
        Timeframe::Day1 => Some(24 * 60 * 60_000),
        Timeframe::Day3 => Some(3 * 24 * 60 * 60_000),
        Timeframe::Week1 => Some(7 * 24 * 60 * 60_000),
        Timeframe::Month1 => None,
    }
}

fn calendar_month_index(open_time: i64) -> Option<i64> {
    let nanos = i128::from(open_time).checked_mul(1_000_000)?;
    let date = OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()?
        .date();
    if date.day() != 1 || open_time.rem_euclid(86_400_000) != 0 {
        return None;
    }
    let month = i64::from(u8::from(date.month()));
    i64::from(date.year())
        .checked_mul(12)?
        .checked_add(month - 1)
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn gap_target_within_generation_span_for_test(
    timeframe: Timeframe,
    start: i64,
    target: i64,
    maximum_successors: usize,
) -> bool {
    gap_target_within_generation_span(timeframe, start, target, maximum_successors)
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
pub fn reconciliation_page_guard_for_test(
    pages: usize,
    maximum_pages: usize,
) -> Result<(), ProviderError> {
    let policy = ReconciliationPolicy::Bounded(ReconciliationLimits {
        max_successors: 1,
        max_pages: maximum_pages,
        span_exceeded: "span",
        page_exceeded: "Hyperliquid gap reconciliation exceeded the per-generation page limit",
        distinct_exceeded: "distinct",
    });
    let mut observed = 0;
    for _ in 0..pages {
        advance_reconciliation_page(
            &mut observed,
            policy,
            ErrorContext::operation(ErrorOperation::Reconciliation),
        )?;
    }
    Ok(())
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[must_use]
pub fn reconciliation_distinct_key_allowed_for_test(
    existing_len: usize,
    key_exists: bool,
    maximum_successors: usize,
) -> bool {
    key_exists || existing_len < maximum_successors.saturating_add(1)
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveErrorDisposition {
    Recoverable,
    Terminal,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveInBandEventDisposition {
    RecoverableInBand,
    TerminalInBand,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveCompletionDisposition {
    Running,
    FinishedErr,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveErrorClassification {
    pub disposition: LiveErrorDisposition,
    pub event: LiveInBandEventDisposition,
    pub completion: LiveCompletionDisposition,
    pub retries: bool,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Debug, PartialEq)]
pub enum LiveInputClassification {
    Continue,
    Error {
        error: ProviderError,
        policy: LiveErrorClassification,
    },
}

#[cfg(feature = "test-transport")]
#[must_use]
pub fn classify_live_error_for_test(error: &ProviderError) -> LiveErrorClassification {
    if is_terminal_live_error(error) {
        LiveErrorClassification {
            disposition: LiveErrorDisposition::Terminal,
            event: LiveInBandEventDisposition::TerminalInBand,
            completion: LiveCompletionDisposition::FinishedErr,
            retries: false,
        }
    } else {
        LiveErrorClassification {
            disposition: LiveErrorDisposition::Recoverable,
            event: LiveInBandEventDisposition::RecoverableInBand,
            completion: LiveCompletionDisposition::Running,
            retries: true,
        }
    }
}

#[cfg(feature = "test-transport")]
#[must_use]
pub fn classify_live_input_for_test(
    input: Result<LiveSocketEvent, ProviderError>,
    instrument: &Instrument,
    timeframe: Timeframe,
) -> LiveInputClassification {
    let error = match input {
        Ok(LiveSocketEvent::Candle(_) | LiveSocketEvent::Ignored) => {
            return LiveInputClassification::Continue;
        }
        Ok(LiveSocketEvent::ReconnectRequested) => ProviderError::Protocol {
            context: ErrorContext::operation(ErrorOperation::WebSocket)
                .with_market(instrument, timeframe),
            detail: "WebSocket peer requested reconnect",
        },
        Ok(LiveSocketEvent::ProtocolViolation(detail)) => ProviderError::Protocol {
            context: ErrorContext::operation(ErrorOperation::WebSocket)
                .with_market(instrument, timeframe),
            detail,
        },
        Ok(LiveSocketEvent::DecodedError(error)) | Err(error) => error,
    };
    LiveInputClassification::Error {
        policy: classify_live_error_for_test(&error),
        error,
    }
}

pub(crate) fn is_terminal_live_error(error: &ProviderError) -> bool {
    matches!(
        error,
        ProviderError::Configuration(_)
            | ProviderError::WebSocketConfiguration { .. }
            | ProviderError::Invariant(_)
            | ProviderError::ClientStatus { .. }
            | ProviderError::InvalidSymbol { .. }
            | ProviderError::ChannelClosed { .. }
    )
}

pub(crate) fn process_block_error(policy: ProcessBlockPolicy) -> ProviderError {
    match policy {
        ProcessBlockPolicy::InvalidBanExpiry => ProviderError::InvalidBanExpiry,
        ProcessBlockPolicy::Forbidden(detail) => ProviderError::Invariant(detail),
    }
}

#[derive(Clone, Copy)]
struct ActiveRotation {
    deadline: Option<MonoInstant>,
    detail: Option<&'static str>,
}

impl ActiveRotation {
    fn new(policy: ConnectionRotation, now: MonoInstant) -> Result<Self, ProviderError> {
        match policy {
            ConnectionRotation::Never => Ok(Self {
                deadline: None,
                detail: None,
            }),
            ConnectionRotation::After { max_age, detail } => Ok(Self {
                deadline: Some(checked_deadline(now, max_age).map_err(|_| {
                    ProviderError::Invariant("live connection age deadline overflow")
                })?),
                detail: Some(detail),
            }),
        }
    }

    fn is_elapsed(self, now: MonoInstant) -> bool {
        self.deadline.is_some_and(|deadline| now >= deadline)
    }
}

async fn sleep_until_rotation(clock: &dyn Clock, deadline: Option<MonoInstant>) {
    match deadline {
        Some(deadline) => clock.sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

enum GenerationOutcome {
    Cancelled,
    AcknowledgedReconnect(ProviderError),
    Reconnect(ProviderError),
}

struct LiveEngine<A: LiveAdapter> {
    adapter: A,
    clock: Arc<dyn Clock>,
    config: LiveSupervisorConfig,
    gate_snapshot: RateGateSnapshot,
    process_block: ProcessBlockPolicy,
    reconciliation: ReconciliationPolicy,
    rotation: ConnectionRotation,
    gap_page_limit: NonZeroU16,
}

async fn send_market(
    sender: &EventEmitter,
    cancellation: &CancellationToken,
    event: MarketEvent,
) -> Result<(), ProviderError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        result = sender.send_regular(event) => result,
    }
}

fn control_channel_closed(instrument: &Instrument, timeframe: Timeframe) -> ProviderError {
    ProviderError::ChannelClosed {
        context: ErrorContext::operation(ErrorOperation::Reconciliation)
            .with_market(instrument, timeframe),
    }
}

fn live_protocol_error(request: &LiveRequest, detail: &'static str) -> ProviderError {
    ProviderError::Protocol {
        context: ErrorContext::operation(ErrorOperation::WebSocket)
            .with_market(&request.instrument, request.timeframe),
        detail,
    }
}

fn next_gap_cursor(_timeframe: Timeframe, value: i64) -> Result<i64, ProviderError> {
    value
        .checked_add(1)
        .ok_or(ProviderError::Invariant("gap cursor overflow"))
}

fn buffer_reconciliation_candle(
    pending: &mut KeyedCandleBuffer,
    candidate: Candle,
    revision: &mut ReplayRevision,
    target_open_time: &mut i64,
    generation_start: i64,
    timeframe: Timeframe,
    policy: ReconciliationPolicy,
) -> Result<(), ProviderError> {
    let _ = apply_reconciliation_candle(
        pending,
        candidate,
        target_open_time,
        generation_start,
        timeframe,
        policy,
    )?;
    revision.0 = revision
        .0
        .checked_add(1)
        .ok_or(ProviderError::Invariant("replay revision overflow"))?;
    Ok(())
}

fn is_process_block_error(error: &ProviderError, policy: ProcessBlockPolicy) -> bool {
    error == &process_block_error(policy)
}

pub async fn open_live<A: LiveAdapter>(
    adapter: A,
    clock: Arc<dyn Clock>,
    capabilities: ProviderCapabilities,
    request: LiveRequest,
) -> Result<LiveFeed, ProviderError> {
    adapter.validate_request(&request.instrument, request.timeframe)?;
    let live_config = adapter.live_config();
    let config = live_config.supervisor.clone();
    let reconciliation = live_config.reconciliation;
    let rotation = adapter.connection_rotation();
    let gap_page_limit = NonZeroU16::new(validate_runtime_contract(
        capabilities,
        LiveConfig {
            supervisor: &config,
            reconciliation,
        },
        rotation,
    )?)
    .expect("validated non-zero history page limit");
    let LiveRateGate {
        snapshot: gate_snapshot,
        process_block,
    } = adapter.rate_gate();
    let physical_capacity = config
        .market_event_capacity
        .checked_add(EMERGENCY_CONTROL_CAPACITY)
        .ok_or(ProviderError::Invariant("market event capacity overflow"))?;
    let (sender, receiver) = mpsc::channel(physical_capacity);
    let sender = EventEmitter::new(
        sender,
        config.market_event_capacity,
        config.control_capacity,
    );
    let invalidated_through = Arc::clone(&sender.invalidated_through);
    let emergency_barrier = Arc::clone(&sender.emergency_barrier);
    let cancellation = request.cancellation.clone();
    let stream_cancellation = cancellation.clone();
    let events = stream::unfold(receiver, move |mut receiver| {
        let invalidated_through = Arc::clone(&invalidated_through);
        let emergency_barrier = Arc::clone(&emergency_barrier);
        let cancellation = stream_cancellation.clone();
        async move {
            loop {
                let cancelled = cancellation.is_cancelled();
                let envelope = if cancelled {
                    receiver.recv().await?
                } else {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => continue,
                        envelope = receiver.recv() => envelope?,
                    }
                };
                if cancellation.is_cancelled() && !envelope.is_stopped() {
                    drop(envelope);
                    continue;
                }
                let invalidated = envelope.purge_on_invalidate
                    && envelope.generation.is_some_and(|generation| {
                        generation.0 <= invalidated_through.load(Ordering::Acquire)
                    });
                let suppressed = envelope
                    .emergency_slot
                    .is_some_and(|slot| emergency_barrier.is_suppressed(slot));
                if invalidated || suppressed {
                    drop(envelope);
                    continue;
                }
                return Some((envelope.into_item(), receiver));
            }
        }
    });
    let engine = LiveEngine {
        adapter,
        clock: Arc::clone(&clock),
        config,
        gate_snapshot,
        process_block,
        reconciliation,
        rotation,
        gap_page_limit,
    };
    Ok(LiveFeed::spawn(
        Box::pin(events),
        cancellation,
        clock,
        async move { engine.supervise_live(request, sender).await },
    ))
}
impl<A: LiveAdapter> LiveEngine<A> {
    async fn supervise_live(
        self,
        mut request: LiveRequest,
        sender: EventEmitter,
    ) -> Result<(), ProviderError> {
        let mut generation_number = 0_u64;
        let mut backoff_index = 0_usize;
        loop {
            if request.cancellation.is_cancelled() {
                sender.shutdown().await;
                return Ok(());
            }
            if matches!(
                self.gate_snapshot.current(),
                Ok(RateGateState::ProcessBlocked(_))
            ) {
                self.send_process_block_and_stop(&sender).await;
                return Err(process_block_error(self.process_block));
            }
            generation_number = generation_number
                .checked_add(1)
                .ok_or(ProviderError::Invariant("gap generation overflow"))?;
            let generation = GapGeneration(generation_number);
            send_market(
                &sender,
                &request.cancellation,
                MarketEvent::Status {
                    generation: Some(generation),
                    status: ConnectionStatus::Connecting,
                },
            )
            .await?;
            let connect_result = {
                let connect_instrument = request.instrument.clone();
                let connect_timeframe = request.timeframe;
                let connect = self
                    .adapter
                    .connect_ready_socket(connect_instrument, connect_timeframe);
                tokio::pin!(connect);
                let mut gate = self.gate_snapshot.clone();
                loop {
                    tokio::select! {
                        biased;
                        () = request.cancellation.cancelled() => {
                            sender.shutdown().await;
                            return Ok(());
                        }
                        changed = request.accepted_watermark_rx.changed() => {
                            if changed.is_err() {
                                break Err(control_channel_closed(&request.instrument, request.timeframe));
                            }
                        }
                        ack = request.reconcile_ack_rx.changed() => {
                            if ack.is_err() {
                                break Err(control_channel_closed(&request.instrument, request.timeframe));
                            }
                        }
                        changed = gate.changed() => match changed {
                            Err(_) => break Err(ProviderError::Invariant("rate gate closed")),
                            Ok(RateGateState::ProcessBlocked(_)) => {
                                self.send_process_block_and_stop(&sender).await;
                                return Err(process_block_error(self.process_block));
                            }
                            Ok(RateGateState::Open | RateGateState::TimedUntil(_)) => {}
                        },
                        result = &mut connect => break result,
                    }
                }
            };
            let mut socket = match connect_result {
                Ok(socket) => socket,
                Err(error) if is_terminal_live_error(&error) => {
                    sender.invalidate_generation(generation);
                    send_market(
                        &sender,
                        &request.cancellation,
                        MarketEvent::TerminalError(error.clone()),
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => {
                    sender.invalidate_generation(generation);
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?;
                    continue;
                }
            };
            let rotation = ActiveRotation::new(self.rotation, self.clock.now())?;
            let outcome = self
                .run_generation(&mut request, &sender, &mut socket, generation, rotation)
                .await;
            drop(socket);
            if !matches!(&outcome, Ok(GenerationOutcome::Cancelled)) {
                sender.invalidate_generation(generation);
                #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
                if let Some(hook) = &self.config.generation_invalidated_test_hook {
                    hook.notify_one();
                }
            }
            match outcome {
                Ok(GenerationOutcome::Cancelled) => {
                    sender.shutdown().await;
                    return Ok(());
                }
                Ok(GenerationOutcome::AcknowledgedReconnect(error)) => {
                    if sender.connected_delivered(generation) {
                        backoff_index = 0;
                    }
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?;
                }
                Ok(GenerationOutcome::Reconnect(error)) => {
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?;
                }
                Err(error) if is_process_block_error(&error, self.process_block) => {
                    self.send_process_block_and_stop(&sender).await;
                    return Err(error);
                }
                Err(error) if is_terminal_live_error(&error) => {
                    send_market(
                        &sender,
                        &request.cancellation,
                        MarketEvent::TerminalError(error.clone()),
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => {
                    self.recover_and_backoff(
                        &sender,
                        &mut request,
                        Some(generation),
                        error,
                        &mut backoff_index,
                    )
                    .await?
                }
            }
        }
    }

    async fn run_generation(
        &self,
        request: &mut LiveRequest,
        sender: &EventEmitter,
        socket: &mut A::Socket,
        generation: GapGeneration,
        rotation: ActiveRotation,
    ) -> Result<GenerationOutcome, ProviderError> {
        send_market(
            sender,
            &request.cancellation,
            MarketEvent::Status {
                generation: Some(generation),
                status: ConnectionStatus::GapSync,
            },
        )
        .await?;
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        tokio::select! {
            biased;
            () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
            result = socket.after_gap_sync_test_probe() => result?,
        }
        let mut gate = self.gate_snapshot.clone();
        let first_deadline = checked_deadline(self.clock.now(), self.config.first_kline_timeout)
            .map_err(|_| ProviderError::Invariant("first-kline deadline overflow"))?;
        let first = loop {
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                changed = request.accepted_watermark_rx.changed() => { changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                ack = request.reconcile_ack_rx.changed() => { ack.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                changed = gate.changed() => if matches!(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?, RateGateState::ProcessBlocked(_)) { return Err(process_block_error(self.process_block)); },
                frame = socket.read() => match frame? {
                    LiveSocketEvent::Candle(candle) => break candle,
                    LiveSocketEvent::Ignored => {
                        let now = self.clock.now();
                        if now >= first_deadline {
                            return Ok(GenerationOutcome::Reconnect(ProviderError::Timeout { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&request.instrument, request.timeframe), kind: TimeoutKind::FirstKline }));
                        }
                        if rotation.is_elapsed(now) {
                            return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, rotation.detail.expect("rotation deadline has detail"))));
                        }
                    }
                    LiveSocketEvent::ReconnectRequested => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect"))),
                    LiveSocketEvent::ProtocolViolation(detail) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, detail))),
                    LiveSocketEvent::DecodedError(error) if is_terminal_live_error(&error) => return Err(error),
                    LiveSocketEvent::DecodedError(error) => return Ok(GenerationOutcome::Reconnect(error)),
                },
                () = sleep_until_rotation(&*self.clock, rotation.deadline) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, rotation.detail.expect("rotation deadline has detail")))),
                () = self.clock.sleep_until(first_deadline) => {
                    return Ok(GenerationOutcome::Reconnect(ProviderError::Timeout { context: ErrorContext::operation(ErrorOperation::WebSocket).with_market(&request.instrument, request.timeframe), kind: TimeoutKind::FirstKline }));
                }
            }
        };
        let confirmed = request
            .accepted_watermark_rx
            .current()
            .map_err(|_| ProviderError::ChannelClosed {
                context: ErrorContext::operation(ErrorOperation::Reconciliation)
                    .with_market(&request.instrument, request.timeframe),
            })?
            .max(request.startup_watermark);
        let start = confirmed.unwrap_or_else(|| first.open_time());
        let mut target_open_time = start;
        advance_reconciliation_target(
            &mut target_open_time,
            first.open_time(),
            start,
            request.timeframe,
            self.reconciliation,
        )?;
        let mut revision = ReplayRevision(1);
        let mut buffered = KeyedCandleBuffer::unbounded();
        if first.open_time() >= start {
            let _ = apply_reconciliation_candle(
                &mut buffered,
                first,
                &mut target_open_time,
                start,
                request.timeframe,
                self.reconciliation,
            )?;
        }
        let mut deferred_reconnect: Option<ProviderError> = None;
        let mut rest_synced_through = None;
        let mut reconciliation_pages = 0;

        loop {
            let mut cursor = match rest_synced_through {
                Some(last) => next_gap_cursor(request.timeframe, last)?,
                None => start,
            };
            while cursor <= target_open_time {
                advance_reconciliation_page(
                    &mut reconciliation_pages,
                    self.reconciliation,
                    ErrorContext::operation(ErrorOperation::Reconciliation)
                        .with_market(&request.instrument, request.timeframe),
                )?;
                let request_target = target_open_time;
                let history_request =
                    HistoryRequest::gap(cursor, request_target, self.gap_page_limit.get())
                        .map_err(|_| ProviderError::Invariant("invalid gap history request"))?;
                let page = {
                    let history_instrument = request.instrument.clone();
                    let history_timeframe = request.timeframe;
                    let history_cancel = request.cancellation.child_token();
                    let history = self.adapter.history(
                        history_instrument,
                        history_timeframe,
                        history_request,
                        history_cancel,
                    );
                    tokio::pin!(history);
                    enum ReconcileWake {
                        Cancelled,
                        AcceptedWatermark(Result<Option<i64>, ProviderError>),
                        Ack(Result<(), ProviderError>),
                        ConnectionAged,
                        Gate(Result<RateGateState, ProviderError>),
                        Socket(Result<LiveSocketEvent, ProviderError>),
                        Page(Result<Vec<Candle>, ProviderError>),
                    }

                    loop {
                        if request.cancellation.is_cancelled() {
                            return Ok(GenerationOutcome::Cancelled);
                        }
                        let wake = tokio::select! {
                            () = request.cancellation.cancelled() => ReconcileWake::Cancelled,
                            changed = request.accepted_watermark_rx.changed() => ReconcileWake::AcceptedWatermark(changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))),
                            ack = request.reconcile_ack_rx.changed() => ReconcileWake::Ack(ack.map(|_| ()).map_err(|_| control_channel_closed(&request.instrument, request.timeframe))),
                            () = sleep_until_rotation(&*self.clock, rotation.deadline) => ReconcileWake::ConnectionAged,
                            changed = gate.changed() => ReconcileWake::Gate(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))),
                            frame = socket.read() => ReconcileWake::Socket(frame),
                            page = &mut history => ReconcileWake::Page(page),
                        };
                        if request.cancellation.is_cancelled() {
                            return Ok(GenerationOutcome::Cancelled);
                        }
                        match wake {
                            ReconcileWake::Cancelled => return Ok(GenerationOutcome::Cancelled),
                            ReconcileWake::AcceptedWatermark(changed) => {
                                if let Some(watermark) = changed? {
                                    advance_reconciliation_target(
                                        &mut target_open_time,
                                        watermark,
                                        start,
                                        request.timeframe,
                                        self.reconciliation,
                                    )?;
                                }
                            }
                            ReconcileWake::Ack(changed) => changed?,
                            ReconcileWake::ConnectionAged => {
                                return Ok(GenerationOutcome::Reconnect(live_protocol_error(
                                    request,
                                    rotation.detail.expect("rotation deadline has detail"),
                                )));
                            }
                            ReconcileWake::Gate(changed) => {
                                if matches!(changed?, RateGateState::ProcessBlocked(_)) {
                                    return Err(process_block_error(self.process_block));
                                }
                            }
                            ReconcileWake::Socket(frame) => match frame {
                                Ok(LiveSocketEvent::Candle(candle)) => {
                                    buffer_reconciliation_candle(
                                        &mut buffered,
                                        candle,
                                        &mut revision,
                                        &mut target_open_time,
                                        start,
                                        request.timeframe,
                                        self.reconciliation,
                                    )?;
                                }
                                Ok(LiveSocketEvent::Ignored) => {}
                                Ok(LiveSocketEvent::ProtocolViolation(detail)) => {
                                    return Ok(GenerationOutcome::Reconnect(live_protocol_error(
                                        request, detail,
                                    )));
                                }
                                Ok(LiveSocketEvent::ReconnectRequested) => {
                                    return Ok(GenerationOutcome::Reconnect(live_protocol_error(
                                        request,
                                        "WebSocket peer requested reconnect",
                                    )));
                                }
                                Ok(LiveSocketEvent::DecodedError(error))
                                    if is_terminal_live_error(&error) =>
                                {
                                    return Err(error);
                                }
                                Err(error) if is_terminal_live_error(&error) => return Err(error),
                                Ok(LiveSocketEvent::DecodedError(error)) | Err(error) => {
                                    return Ok(GenerationOutcome::Reconnect(error));
                                }
                            },
                            ReconcileWake::Page(page) => {
                                if request.cancellation.is_cancelled() {
                                    return Ok(GenerationOutcome::Cancelled);
                                }
                                let terminal = async {
                                tokio::select! {
                                    biased;
                                    () = request.cancellation.cancelled() => Ok((Some(GenerationOutcome::Cancelled), false)),
                                    changed = request.accepted_watermark_rx.changed() => changed
                                        .map_err(|_| control_channel_closed(&request.instrument, request.timeframe))
                                        .and_then(|watermark| {
                                            if let Some(watermark) = watermark {
                                                advance_reconciliation_target(
                                                    &mut target_open_time,
                                                    watermark,
                                                    start,
                                                    request.timeframe,
                                                    self.reconciliation,
                                                )?;
                                            }
                                            Ok((None, true))
                                        }),
                                    ack = request.reconcile_ack_rx.changed() => ack
                                        .map(|_| (None, false))
                                        .map_err(|_| control_channel_closed(&request.instrument, request.timeframe)),
                                    () = sleep_until_rotation(&*self.clock, rotation.deadline) => Ok((Some(GenerationOutcome::Reconnect(live_protocol_error(request, rotation.detail.expect("rotation deadline has detail")))), false)),
                                    changed = gate.changed() => match changed {
                                        Ok(RateGateState::ProcessBlocked(_)) => Err(process_block_error(self.process_block)),
                                        Ok(_) => Ok((None, false)),
                                        Err(_) => Err(ProviderError::Invariant("rate gate closed")),
                                    },
                                    frame = socket.read() => match frame {
                                        Ok(LiveSocketEvent::Candle(candle)) => {
                                            buffer_reconciliation_candle(
                                                &mut buffered,
                                                candle,
                                                &mut revision,
                                                &mut target_open_time,
                                                start,
                                                request.timeframe,
                                                self.reconciliation,
                                            )?;
                                            Ok((None, false))
                                        }
                                        Ok(LiveSocketEvent::Ignored) => Ok((None, false)),
                                        Ok(LiveSocketEvent::ProtocolViolation(detail)) => Ok((Some(GenerationOutcome::Reconnect(live_protocol_error(request, detail))), false)),
                                        Ok(LiveSocketEvent::ReconnectRequested) => Ok((Some(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect"))), false)),
                                        Ok(LiveSocketEvent::DecodedError(error)) | Err(error) if is_terminal_live_error(&error) => Err(error),
                                        Ok(LiveSocketEvent::DecodedError(error)) | Err(error) => Ok((Some(GenerationOutcome::Reconnect(error)), false)),
                                    },
                                }
                            }
                            .now_or_never()
                            .transpose()?;
                                if request.cancellation.is_cancelled() {
                                    return Ok(GenerationOutcome::Cancelled);
                                }
                                let watermark_consumed = match terminal {
                                    Some((Some(outcome), _)) => return Ok(outcome),
                                    Some((None, true)) => true,
                                    _ => false,
                                };
                                if watermark_consumed {
                                    let follow_up = async {
                                    tokio::select! {
                                        biased;
                                        () = request.cancellation.cancelled() => Ok(Some(GenerationOutcome::Cancelled)),
                                        frame = socket.read() => match frame {
                                            Ok(LiveSocketEvent::Candle(candle)) => {
                                                buffer_reconciliation_candle(
                                                    &mut buffered,
                                                    candle,
                                                    &mut revision,
                                                    &mut target_open_time,
                                                    start,
                                                    request.timeframe,
                                                    self.reconciliation,
                                                )?;
                                                Ok(None)
                                            }
                                            Ok(LiveSocketEvent::Ignored) => Ok(None),
                                            Ok(LiveSocketEvent::ProtocolViolation(detail)) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, detail)))),
                                            Ok(LiveSocketEvent::ReconnectRequested) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect")))),
                                            Ok(LiveSocketEvent::DecodedError(error)) | Err(error) if is_terminal_live_error(&error) => Err(error),
                                            Ok(LiveSocketEvent::DecodedError(error)) | Err(error) => Ok(Some(GenerationOutcome::Reconnect(error))),
                                        },
                                        () = sleep_until_rotation(&*self.clock, rotation.deadline) => Ok(Some(GenerationOutcome::Reconnect(live_protocol_error(request, rotation.detail.expect("rotation deadline has detail"))))),
                                        changed = gate.changed() => match changed {
                                            Ok(RateGateState::ProcessBlocked(_)) => Err(process_block_error(self.process_block)),
                                            Ok(_) => Ok(None),
                                            Err(_) => Err(ProviderError::Invariant("rate gate closed")),
                                        },
                                        ack = request.reconcile_ack_rx.changed() => ack
                                            .map(|_| None)
                                            .map_err(|_| control_channel_closed(&request.instrument, request.timeframe)),
                                    }
                                }
                                .now_or_never()
                                .transpose()?;
                                    if request.cancellation.is_cancelled() {
                                        return Ok(GenerationOutcome::Cancelled);
                                    }
                                    if let Some(Some(outcome)) = follow_up {
                                        return Ok(outcome);
                                    }
                                }
                                break page;
                            }
                        }
                    }
                };
                let page = page?;
                let last = page.last().map(Candle::open_time);
                if page.is_empty() {
                    if confirmed.is_none() && cursor == start {
                        break;
                    }
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: None,
                        },
                    ));
                }
                if last.is_some_and(|value| value < cursor) {
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: last,
                        },
                    ));
                }
                let page_len = page.len();
                let mut accepted_any = false;
                for candle in page {
                    accepted_any |= buffered.push(candle)?;
                }
                let Some(last) = last else { unreachable!() };
                rest_synced_through = Some(last);
                if last >= target_open_time {
                    break;
                }
                if last < request_target && page_len < usize::from(self.gap_page_limit.get()) {
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: Some(last),
                        },
                    ));
                }
                if !accepted_any && last < request_target {
                    return Ok(GenerationOutcome::Reconnect(
                        ProviderError::GapSyncNoProgress {
                            target_open_time: request_target,
                            last_open_time: Some(last),
                        },
                    ));
                }
                cursor = next_gap_cursor(request.timeframe, last)?;
            }
            let page_candles = buffered.values().cloned().collect();
            let expected = ReconcileExpectation {
                generation,
                revision,
                target_open_time,
            };
            request
                .reconcile_ack_rx
                .register_expectation(expected)
                .map_err(|error| match error {
                    ReconcileExpectationError::Closed => {
                        control_channel_closed(&request.instrument, request.timeframe)
                    }
                    ReconcileExpectationError::Regression | ReconcileExpectationError::Conflict => {
                        ProviderError::Invariant("reconciliation expectation invariant violated")
                    }
                })?;
            send_market(
                sender,
                &request.cancellation,
                MarketEvent::ReconcileBatch {
                    generation,
                    revision,
                    target_open_time,
                    candles: page_candles,
                },
            )
            .await?;
            let ack_deadline =
                checked_deadline(self.clock.now(), self.config.reconcile_ack_timeout)
                    .map_err(|_| ProviderError::Invariant("ack deadline overflow"))?;
            loop {
                tokio::select! {
                    biased;
                    () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                    changed = request.accepted_watermark_rx.changed() => { changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                    () = sleep_until_rotation(&*self.clock, rotation.deadline) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, rotation.detail.expect("rotation deadline has detail")))),
                    changed = gate.changed() => if matches!(changed.map_err(|_| ProviderError::Invariant("rate gate closed"))?, RateGateState::ProcessBlocked(_)) { return Err(process_block_error(self.process_block)); },
                    frame = socket.read(), if deferred_reconnect.is_none() => match frame? {
                        LiveSocketEvent::Candle(candle) => {
                            buffer_reconciliation_candle(
                                &mut buffered,
                                candle,
                                &mut revision,
                                &mut target_open_time,
                                start,
                                request.timeframe,
                                self.reconciliation,
                            )?;
                            break;
                        }
                        LiveSocketEvent::Ignored => {
                            if let Some(ack) = request
                                .reconcile_ack_rx
                                .current()
                                .map_err(|_| {
                                    control_channel_closed(&request.instrument, request.timeframe)
                                })?
                                && ack.generation == generation
                                && ack.revision == revision
                                && ack.through >= target_open_time
                            {
                                return if let Some(error) = deferred_reconnect.take() {
                                    Ok(GenerationOutcome::Reconnect(error))
                                } else {
                                    self.connected_loop(request, sender, socket, generation, rotation).await
                                };
                            }
                            if self.clock.now() >= ack_deadline {
                                return Ok(GenerationOutcome::Reconnect(ProviderError::ReconcileAckTimeout { generation, revision, target_open_time }));
                            }
                        }
                        LiveSocketEvent::ReconnectRequested => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, "WebSocket peer requested reconnect"))),
                        LiveSocketEvent::ProtocolViolation(detail) => return Ok(GenerationOutcome::Reconnect(live_protocol_error(request, detail))),
                        LiveSocketEvent::DecodedError(error) if is_terminal_live_error(&error) => return Err(error),
                        LiveSocketEvent::DecodedError(error) => deferred_reconnect = Some(error),
                    },
                    ack = request.reconcile_ack_rx.changed() => {
                        let ReconcileAck { generation: ack_generation, revision: ack_revision, through } = ack.map_err(|_| ProviderError::ChannelClosed { context: ErrorContext::operation(ErrorOperation::Reconciliation).with_market(&request.instrument, request.timeframe) })?;
                        if ack_generation == generation && ack_revision == revision && through >= target_open_time {
                            return if let Some(error) = deferred_reconnect.take() {
                                Ok(GenerationOutcome::Reconnect(error))
                            } else {
                                self.connected_loop(request, sender, socket, generation, rotation).await
                            };
                        }
                    },
                    () = self.clock.sleep_until(ack_deadline) => {
                        if let Ok(Some(ack)) = request.reconcile_ack_rx.current()
                            && ack.generation == generation
                            && ack.revision == revision
                            && ack.through >= target_open_time
                        {
                            return if let Some(error) = deferred_reconnect.take() {
                                Ok(GenerationOutcome::Reconnect(error))
                            } else {
                                self.connected_loop(request, sender, socket, generation, rotation).await
                            };
                        }
                        return Ok(GenerationOutcome::Reconnect(ProviderError::ReconcileAckTimeout { generation, revision, target_open_time }));
                    }
                }
            }
        }
    }

    async fn connected_loop(
        &self,
        request: &mut LiveRequest,
        sender: &EventEmitter,
        socket: &mut A::Socket,
        generation: GapGeneration,
        rotation: ActiveRotation,
    ) -> Result<GenerationOutcome, ProviderError> {
        let mut connected_queued = false;
        let mut pending = KeyedCandleBuffer::bounded(self.config.keyed_candle_capacity);
        let mut gate = self.gate_snapshot.clone();
        loop {
            if matches!(gate.current(), Ok(RateGateState::ProcessBlocked(_))) {
                return Err(process_block_error(self.process_block));
            }
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => return Ok(GenerationOutcome::Cancelled),
                changed = gate.changed() => match changed.map_err(|_| ProviderError::Invariant("rate gate closed"))? {
                    RateGateState::ProcessBlocked(_) => return Err(process_block_error(self.process_block)),
                    RateGateState::Open | RateGateState::TimedUntil(_) => {}
                },
                changed = request.accepted_watermark_rx.changed() => { changed.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                ack = request.reconcile_ack_rx.changed() => { ack.map_err(|_| control_channel_closed(&request.instrument, request.timeframe))?; },
                result = send_market(sender, &request.cancellation, MarketEvent::Status { generation: Some(generation), status: ConnectionStatus::Connected }), if !connected_queued => {
                    result?;
                    connected_queued = true;
                },
                permit = sender.reserve_regular(), if connected_queued && !pending.is_empty() => {
                    let permit = permit?;
                    let candle = pending.pop_first().expect("pending is nonempty");
                    sender.send_reserved(permit, MarketEvent::Candle { generation, candle })?;
                },
                frame = socket.read() => {
                    if rotation.is_elapsed(self.clock.now()) {
                        let error = live_protocol_error(request, rotation.detail.expect("rotation deadline has detail"));
                        return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                    }
                    match frame {
                        Ok(LiveSocketEvent::Candle(candle)) => {
                            if let Err(outcome) = pending.push(candle) {
                                #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
                                if let Some(hook) = &self.config.saturation_test_hook {
                                    hook.notify_one();
                                }
                                return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(outcome) } else { GenerationOutcome::Reconnect(outcome) });
                            }
                        }
                        Ok(LiveSocketEvent::Ignored) => {}
                        Ok(LiveSocketEvent::ProtocolViolation(detail)) => {
                            let error = live_protocol_error(request, detail);
                            return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                        }
                        Ok(LiveSocketEvent::ReconnectRequested) => {
                            let error = live_protocol_error(request, "WebSocket peer requested reconnect");
                            return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                        }
                        Ok(LiveSocketEvent::DecodedError(error)) if is_terminal_live_error(&error) => return Err(error),
                        Err(error) if is_terminal_live_error(&error) => return Err(error),
                        Ok(LiveSocketEvent::DecodedError(error)) | Err(error) => {
                            return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                        }
                    }
                },
                () = sleep_until_rotation(&*self.clock, rotation.deadline) => {
                    let error = live_protocol_error(request, rotation.detail.expect("rotation deadline has detail"));
                    return Ok(if connected_queued { GenerationOutcome::AcknowledgedReconnect(error) } else { GenerationOutcome::Reconnect(error) });
                },
            }
        }
    }

    async fn recover_and_backoff(
        &self,
        sender: &EventEmitter,
        request: &mut LiveRequest,
        generation: Option<GapGeneration>,
        error: ProviderError,
        backoff_index: &mut usize,
    ) -> Result<(), ProviderError> {
        let cancellation = request.cancellation.clone();
        let mut gate = self.gate_snapshot.clone();
        let initial_gate = match gate.current() {
            Ok(state) => state,
            Err(_) => {
                let error = ProviderError::Invariant("rate gate closed");
                send_market(
                    sender,
                    &cancellation,
                    MarketEvent::TerminalError(error.clone()),
                )
                .await?;
                return Err(error);
            }
        };
        if matches!(initial_gate, RateGateState::ProcessBlocked(_)) {
            self.send_process_block_and_stop(sender).await;
            return Err(process_block_error(self.process_block));
        }
        let gate_deadline = match initial_gate {
            RateGateState::TimedUntil(deadline) => Some(deadline),
            RateGateState::Open | RateGateState::ProcessBlocked(_) => None,
        };
        let seconds = [1_u64, 2, 4, 8, 16, 30]
            .get(*backoff_index)
            .copied()
            .unwrap_or(30);
        *backoff_index = backoff_index.saturating_add(1);
        let backoff = checked_deadline(self.clock.now(), Duration::from_secs(seconds))
            .map_err(|_| ProviderError::Invariant("backoff deadline overflow"))?;
        let mut deadline = gate_deadline.map_or(backoff, |value| value.max(backoff));
        let queue_saturated = matches!(&error, ProviderError::QueueSaturated);
        let control_generation = if queue_saturated { None } else { generation };
        let recoverable = MarketEvent::RecoverableError {
            generation: control_generation,
            error,
            rate_gate_deadline: gate_deadline,
        };
        let backoff_status = MarketEvent::Status {
            generation: control_generation,
            status: ConnectionStatus::Backoff,
        };
        let emergency_barrier = if queue_saturated {
            if let Some(generation) = generation {
                sender.invalidate_generation(generation);
            }
            Some(sender.queue_emergency_pair(recoverable, backoff_status)?)
        } else {
            send_market(sender, &cancellation, recoverable).await?;
            send_market(sender, &cancellation, backoff_status).await?;
            None
        };
        let mut deadline_elapsed = false;
        loop {
            let barrier_elapsed = emergency_barrier
                .as_ref()
                .is_none_or(|barrier| barrier.is_dequeued());
            if deadline_elapsed && barrier_elapsed {
                return Ok(());
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    sender.shutdown().await;
                    return Ok(());
                },
                changed = request.accepted_watermark_rx.changed() => {
                    if changed.is_err() {
                        let error = control_channel_closed(&request.instrument, request.timeframe);
                        send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                        return Err(error);
                    }
                },
                ack = request.reconcile_ack_rx.changed() => {
                    if ack.is_err() {
                        let error = control_channel_closed(&request.instrument, request.timeframe);
                        send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                        return Err(error);
                    }
                },
                () = sender.wait_closed() => return Err(live_channel_closed()),
                changed = gate.changed() => match changed {
                    Err(_) => {
                        let error = ProviderError::Invariant("rate gate closed");
                        send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                        return Err(error);
                    }
                    Ok(RateGateState::ProcessBlocked(_)) => {
                        self.send_process_block_and_stop(sender).await;
                        return Err(process_block_error(self.process_block));
                    }
                    Ok(RateGateState::TimedUntil(value)) => {
                        deadline = deadline.max(value);
                        deadline_elapsed = self.clock.now() >= deadline;
                    }
                    Ok(RateGateState::Open) => {}
                },
                () = self.clock.sleep_until(deadline), if !deadline_elapsed => {
                    match gate.current() {
                        Err(_) => {
                            let error = ProviderError::Invariant("rate gate closed");
                            send_market(sender, &cancellation, MarketEvent::TerminalError(error.clone())).await?;
                            return Err(error);
                        }
                        Ok(RateGateState::ProcessBlocked(_)) => {
                            self.send_process_block_and_stop(sender).await;
                            return Err(process_block_error(self.process_block));
                        }
                        Ok(RateGateState::TimedUntil(value)) if value > deadline => deadline = value,
                        Ok(RateGateState::Open | RateGateState::TimedUntil(_)) => deadline_elapsed = true,
                    }
                },
                () = async { if let Some(barrier) = &emergency_barrier { barrier.wait_dequeued().await } }, if !barrier_elapsed => {}
            }
        }
    }

    async fn send_process_block_and_stop(&self, sender: &EventEmitter) {
        let _ = sender
            .queue_terminal_pair(
                MarketEvent::RecoverableError {
                    generation: None,
                    error: process_block_error(self.process_block),
                    rate_gate_deadline: None,
                },
                MarketEvent::Status {
                    generation: None,
                    status: ConnectionStatus::Stopped,
                },
            )
            .await;
    }
}
