//! Sole application dispatcher and provider-neutral interactive reducer.

use std::{
    collections::VecDeque,
    ffi::OsString,
    io::{self, Write},
    process::ExitCode,
    sync::{Arc, mpsc as std_mpsc},
    thread::{self, JoinHandle as ThreadJoinHandle},
    time::Duration,
};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent};
use futures_util::{FutureExt, StreamExt, future};
use ratatui::{
    buffer::Buffer,
    layout::{Rect, Size},
};
use tokio::{sync::mpsc, task::JoinHandle as TokioJoinHandle};

use crate::{
    chart::{
        ChartLayoutResult, ChartViewState, ChartWidget, DisplayStatus, FooterPresentation,
        InteractionAction, InteractionController, InteractiveChartState, LayoutMode, RenderMode,
        RenderPolicy, RendererSnapshot, calculate_chart_layout,
    },
    cli::{Cli, MarketTarget, Mode, canonicalize_binance, parse_market_target},
    clock::{Clock, checked_deadline},
    error::{AppError, ProviderError, RenderError, SanitizedCause, TerminalError},
    history::{HistoryApplyResult, HistoryCoordinator, HistoryJoinError, HistoryProgress},
    model::{
        CandleSeries, ConnectionStatus, GapGeneration, MarketEvent, MonoInstant, MutationSummary,
        RateGateState, ReplayRevision,
    },
    provider::{
        AcceptedWatermarkSender, CancellationToken, LiveFeed, LiveFeedJoinError, LiveRequest,
        MarketDataProvider, ProducerCompletion, ProviderRegistry, RateGateClosed, RateGateSnapshot,
        ReconcileAck, ReconcileAckSender, accepted_watermark_channel, reconcile_ack_channel,
    },
    snapshot::{SnapshotOutputTarget, run_snapshot},
    terminal::{TerminalLifecycleError, enter_with_tty_preflight},
};

pub const MAX_EVENTS_PER_SOURCE_PER_EPOCH: usize = 32;
pub const PRODUCER_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
pub const HISTORY_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const INPUT_POLL_TIMEOUT: Duration = Duration::from_millis(10);
const INITIAL_HISTORY_LIMIT: u16 = 500;
const MAX_COMMAND_BYTES: usize = 512;

#[derive(Default)]
struct CommandEditor {
    text: String,
    cursor: usize,
}

impl CommandEditor {
    fn apply(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char(character)
                if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
            {
                if self.text.len() + character.len_utf8() <= MAX_COMMAND_BYTES {
                    self.text.insert(self.cursor, character);
                    self.cursor += character.len_utf8();
                }
            }
            KeyCode::Left => {
                self.cursor = self.text[..self.cursor]
                    .char_indices()
                    .next_back()
                    .map_or(0, |(index, _)| index)
            }
            KeyCode::Right => {
                self.cursor = self.text[self.cursor..]
                    .char_indices()
                    .nth(1)
                    .map_or(self.text.len(), |(index, _)| self.cursor + index)
            }
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.text.len(),
            KeyCode::Backspace if self.cursor != 0 => {
                let previous = self.text[..self.cursor]
                    .char_indices()
                    .next_back()
                    .map_or(0, |(index, _)| index);
                self.text.drain(previous..self.cursor);
                self.cursor = previous;
            }
            KeyCode::Delete if self.cursor != self.text.len() => {
                let next = self.text[self.cursor..]
                    .char_indices()
                    .nth(1)
                    .map_or(self.text.len(), |(index, _)| self.cursor + index);
                self.text.drain(self.cursor..next);
            }
            KeyCode::Enter => return true,
            _ => {}
        }
        false
    }
}
#[cfg(feature = "test-transport")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EpochStop {
    Quit,
    TerminalFailure,
    LiveTerminalError,
}

#[cfg(feature = "test-transport")]
#[derive(Clone, Debug)]
pub struct EpochObservation {
    pub source_counts: [usize; 7],
    pub active_generation: Option<GapGeneration>,
    pub invalidated_generation: Option<GapGeneration>,
    pub stale_generation_events: usize,
    pub stop: Option<EpochStop>,
    pub snapshot: RendererSnapshot,
    pub renderer_candle_revision: u64,
    pub layout_pending: bool,
}

#[cfg(feature = "test-transport")]
pub type EpochObserver = Arc<dyn Fn(EpochObservation) + Send + Sync>;

/// Result of one bounded terminal-input poll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TerminalInputPoll {
    Event(Event),
    Idle,
    Closed,
}

/// Cancellation-safe terminal input owned by the interactive input task.
///
/// Implementations must return within `timeout`, allowing the owner to be stopped and joined
/// definitively before terminal restoration.
pub trait TerminalInput: Send {
    fn poll(&mut self, timeout: Duration) -> io::Result<TerminalInputPoll>;
}

/// Production input backed by Crossterm's bounded event polling.
#[derive(Default)]
pub struct CrosstermTerminalInput;

impl CrosstermTerminalInput {
    pub const fn new() -> Self {
        Self
    }
}

impl TerminalInput for CrosstermTerminalInput {
    fn poll(&mut self, timeout: Duration) -> io::Result<TerminalInputPoll> {
        if !event::poll(timeout)? {
            return Ok(TerminalInputPoll::Idle);
        }
        event::read().map(TerminalInputPoll::Event)
    }
}

/// Deterministic input sequence for tests and injected runners.
pub struct ScriptedTerminalInput {
    events: VecDeque<(Duration, Event)>,
    close_when_empty: bool,
}

impl ScriptedTerminalInput {
    pub fn new(events: impl IntoIterator<Item = Event>) -> Self {
        Self {
            events: events
                .into_iter()
                .map(|event| (Duration::ZERO, event))
                .collect(),
            close_when_empty: true,
        }
    }
    pub fn with_delays(events: impl IntoIterator<Item = (Duration, Event)>) -> Self {
        Self {
            events: events.into_iter().collect(),
            close_when_empty: true,
        }
    }

    pub fn delayed(delay: Duration, event: Event) -> Self {
        Self {
            events: VecDeque::from([(delay, event)]),
            close_when_empty: true,
        }
    }

    pub const fn pending() -> Self {
        Self {
            events: VecDeque::new(),
            close_when_empty: false,
        }
    }
}

impl TerminalInput for ScriptedTerminalInput {
    fn poll(&mut self, timeout: Duration) -> io::Result<TerminalInputPoll> {
        let Some((remaining, _)) = self.events.front_mut() else {
            if self.close_when_empty {
                return Ok(TerminalInputPoll::Closed);
            }
            thread::sleep(timeout);
            return Ok(TerminalInputPoll::Idle);
        };
        let waited = timeout.min(*remaining);
        if !waited.is_zero() {
            thread::sleep(waited);
            *remaining -= waited;
        }
        if !remaining.is_zero() {
            return Ok(TerminalInputPoll::Idle);
        }
        let (_, event) = self.events.pop_front().expect("front event exists");
        Ok(TerminalInputPoll::Event(event))
    }
}

/// Sender half for deterministic channel-driven terminal input.
pub type ChannelTerminalInputSender = std_mpsc::Sender<Event>;

pub struct ChannelTerminalInput {
    receiver: std_mpsc::Receiver<Event>,
}

impl ChannelTerminalInput {
    pub fn channel() -> (ChannelTerminalInputSender, Self) {
        let (sender, receiver) = std_mpsc::channel();
        (sender, Self { receiver })
    }
}

impl TerminalInput for ChannelTerminalInput {
    fn poll(&mut self, timeout: Duration) -> io::Result<TerminalInputPoll> {
        match self.receiver.recv_timeout(timeout) {
            Ok(event) => Ok(TerminalInputPoll::Event(event)),
            Err(std_mpsc::RecvTimeoutError::Timeout) => Ok(TerminalInputPoll::Idle),
            Err(std_mpsc::RecvTimeoutError::Disconnected) => Ok(TerminalInputPoll::Closed),
        }
    }
}

/// Complete injected boundary for the sole dispatcher.
pub struct RunDependencies {
    pub providers: ProviderRegistry,
    pub clock: Arc<dyn Clock>,
    pub terminal: Arc<dyn crate::terminal::TerminalDriver>,
    pub input: Box<dyn TerminalInput>,
    pub stdout: Box<dyn Write + Send>,
    pub stderr: Box<dyn Write + Send>,
    pub stdin_is_tty: bool,
    pub stdout_is_tty: bool,
    pub render_policy: RenderPolicy,
    #[cfg(feature = "test-transport")]
    pub epoch_observer: Option<EpochObserver>,
}

/// Parses before touching runtime dependencies, then dispatches the only real snapshot or
/// interactive implementation.
pub async fn run_with_dependencies<I, T>(
    args: I,
    mut dependencies: RunDependencies,
) -> Result<ExitCode, AppError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = if error.use_stderr() { 2 } else { 0 };
            let target: &mut dyn Write = if error.use_stderr() {
                &mut dependencies.stderr
            } else {
                &mut dependencies.stdout
            };
            target
                .write_all(error.to_string().as_bytes())
                .map_err(output_error)?;
            return Ok(ExitCode::from(exit_code));
        }
    };
    canonicalize_binance(cli.instrument())
        .map_err(|_| ProviderError::Configuration("instrument is not valid for Binance Spot"))?;

    let provider = dependencies
        .providers
        .get(cli.instrument().provider().clone())?;
    match cli.mode() {
        Mode::Snapshot => {
            let render_policy = if dependencies.stdout_is_tty {
                dependencies.render_policy
            } else {
                RenderPolicy::StyleFree
            };
            let target = if dependencies.stdout_is_tty {
                let (width, height) =
                    dependencies
                        .terminal
                        .size()
                        .map_err(|_| TerminalError::Setup {
                            operation: "query terminal size",
                            cause: SanitizedCause::Io,
                        })?;
                SnapshotOutputTarget::Tty {
                    physical_size: Size::new(width, height),
                }
            } else {
                SnapshotOutputTarget::NonTty
            };
            run_snapshot(
                provider.as_ref(),
                cli.instrument(),
                cli.timeframe(),
                target,
                render_policy,
                CancellationToken::new(),
                &mut dependencies.stdout,
            )
            .await?;
            Ok(ExitCode::SUCCESS)
        }
        Mode::Interactive => {
            if !dependencies.stdin_is_tty || !dependencies.stdout_is_tty {
                return Err(TerminalError::TtyRequired.into());
            }
            run_interactive(cli, provider, dependencies).await?;
            Ok(ExitCode::SUCCESS)
        }
    }
}
enum InputEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
    ResizeRequested,
    InputFailure,
}

enum EpochSource {
    Cancellation,
    Input,
    Live,
    History,
    ProducerCompletion,
    RateGate,
    Timer,
}
#[derive(Clone, Copy)]
enum LiveAdmission {
    Tagged(GapGeneration),
    GenerationlessInvalidator,
}

#[derive(Default)]
struct Epoch {
    cancelled: bool,
    quit: bool,
    terminal_lifecycle_failures: Vec<AppError>,
    stream_failures: Vec<AppError>,
    channel_failures: Vec<AppError>,
    producer_failures: Vec<AppError>,
    history_failures: Vec<AppError>,
    terminal_events: Vec<ProviderError>,
    recoverable_events: Vec<(Option<GapGeneration>, ProviderError, Option<MonoInstant>)>,
    status_events: Vec<(Option<GapGeneration>, ConnectionStatus)>,
    history: Vec<HistoryProgress>,
    reconciliation: Vec<(
        GapGeneration,
        ReplayRevision,
        i64,
        Vec<crate::model::Candle>,
    )>,
    rate_gate_states: Vec<RateGateState>,
    candles: Vec<(GapGeneration, crate::model::Candle)>,
    resizes: Vec<Size>,
    keys: Vec<KeyEvent>,
    pointers: Vec<crossterm::event::MouseEvent>,
    timers: Vec<(MonoInstant, u64)>,
    live_admissions: Vec<LiveAdmission>,
    source_counts: [usize; 7],
    next_admission_sequence: u64,
}

impl Epoch {
    const fn source_index(source: EpochSource) -> usize {
        match source {
            EpochSource::Cancellation => 0,
            EpochSource::Input => 1,
            EpochSource::Live => 2,
            EpochSource::History => 3,
            EpochSource::ProducerCompletion => 4,
            EpochSource::RateGate => 5,
            EpochSource::Timer => 6,
        }
    }

    fn admit(&mut self, source: EpochSource) -> bool {
        let count = &mut self.source_counts[Self::source_index(source)];
        if *count >= MAX_EVENTS_PER_SOURCE_PER_EPOCH {
            return false;
        }
        *count += 1;
        self.next_admission_sequence = self.next_admission_sequence.saturating_add(1);
        true
    }

    fn precomputed_generation(
        &self,
        current: Option<GapGeneration>,
        invalidated: Option<GapGeneration>,
    ) -> (Option<GapGeneration>, Option<GapGeneration>) {
        let mut floor = invalidated;
        let mut candidate =
            current.filter(|generation| floor.is_none_or(|invalidated| *generation > invalidated));
        for admission in &self.live_admissions {
            match *admission {
                LiveAdmission::Tagged(generation)
                    if floor.is_none_or(|invalidated| generation > invalidated) =>
                {
                    candidate = Some(candidate.map_or(generation, |active| active.max(generation)));
                }
                LiveAdmission::Tagged(_) => {}
                LiveAdmission::GenerationlessInvalidator => {
                    if let Some(generation) = candidate {
                        floor = Some(
                            floor.map_or(generation, |invalidated| invalidated.max(generation)),
                        );
                        candidate = None;
                    }
                }
            }
        }
        (candidate, floor)
    }
}

struct App {
    instrument: crate::model::Instrument,
    timeframe: crate::model::Timeframe,
    series: CandleSeries,
    renderer_candles: Arc<[crate::model::Candle]>,
    #[cfg(feature = "test-transport")]
    renderer_candle_revision: u64,
    chart_state: InteractiveChartState,
    #[cfg(feature = "test-transport")]
    epoch_observer: Option<EpochObserver>,
    layout: ChartLayoutResult,
    interaction: InteractionController,
    history: HistoryCoordinator,
    rate_gate_observer: RateGateSnapshot,
    clock: Arc<dyn Clock>,
    pending_rate_gate: Option<Result<RateGateState, RateGateClosed>>,
    accepted_watermark: AcceptedWatermarkSender,
    reconcile_ack: ReconcileAckSender,
    active_generation: Option<GapGeneration>,
    invalidated_generation: Option<GapGeneration>,
    continuity_start: Option<i64>,
    display_status: DisplayStatus,
    connection_status: ConnectionStatus,
    status_detail: Option<ProviderError>,
    rate_gate: RateGateState,
    dirty: bool,
    footer: FooterPresentation,
    editor: Option<CommandEditor>,
    providers: ProviderRegistry,
    switch_generation: u64,
    switch: Option<SwitchPreparation>,
    retired: Vec<TokioJoinHandle<Result<(), AppError>>>,
    root_cancellation: CancellationToken,
    quit_requested: bool,
}

struct PreparedMarket {
    instrument: crate::model::Instrument,
    timeframe: crate::model::Timeframe,
    series: CandleSeries,
    live: LiveFeed,
    history: HistoryCoordinator,
    rate_gate_observer: RateGateSnapshot,
    rate_gate: RateGateState,
    accepted_watermark: AcceptedWatermarkSender,
    reconcile_ack: ReconcileAckSender,
    startup_watermark: Option<i64>,
}

struct SwitchPreparation {
    generation: u64,
    cancellation: CancellationToken,
    task: TokioJoinHandle<Result<PreparedMarket, AppError>>,
}

impl App {
    fn reap_retired(&mut self) {
        let mut pending = Vec::with_capacity(self.retired.len());
        for mut task in self.retired.drain(..) {
            match (&mut task).now_or_never() {
                Some(Ok(Ok(()))) => {}
                Some(Err(error)) if error.is_cancelled() => {}
                Some(Ok(Err(error))) => {
                    self.footer = FooterPresentation::Error {
                        message: error.to_string(),
                    };
                    self.dirty = true;
                }
                Some(Err(_)) => {
                    self.footer = FooterPresentation::Error {
                        message: "market cleanup failed".to_owned(),
                    };
                    self.dirty = true;
                }
                None => pending.push(task),
            }
        }
        self.retired = pending;
    }

    fn poll_switch(&mut self, live: &mut LiveFeed) {
        self.reap_retired();
        let Some(preparation) = self.switch.as_mut() else {
            return;
        };
        let Some(result) = (&mut preparation.task).now_or_never() else {
            return;
        };
        let generation = preparation.generation;
        self.switch = None;
        match result {
            Ok(Ok(prepared)) if generation == self.switch_generation => {
                let renderer_candles = prepared.series.iter().cloned().collect();
                let chart_state = match &self.layout {
                    ChartLayoutResult::LayoutPending { .. } => InteractiveChartState::LayoutPending,
                    ChartLayoutResult::Ready { layout } => {
                        InteractiveChartState::Ready(ChartViewState::interactive(
                            &prepared.series,
                            usize::from(layout.main_plot.width),
                        ))
                    }
                };
                let old_live = std::mem::replace(live, prepared.live);
                old_live.request_shutdown();
                let old_history = std::mem::replace(&mut self.history, prepared.history);
                old_history.request_shutdown();
                let clock = Arc::clone(&self.clock);
                self.retired.push(tokio::spawn(async move {
                    let deadline =
                        checked_deadline(clock.now(), PRODUCER_JOIN_TIMEOUT).unwrap_or(clock.now());
                    old_live.join(deadline).await.map_err(map_live_join)?;
                    let mut old_history = old_history;
                    if old_history.in_flight() {
                        let deadline = checked_deadline(clock.now(), HISTORY_JOIN_TIMEOUT)
                            .unwrap_or(clock.now());
                        old_history.join(deadline).await.map_err(map_history_join)?;
                    }
                    Ok(())
                }));
                self.instrument = prepared.instrument;
                self.timeframe = prepared.timeframe;
                self.series = prepared.series;
                self.renderer_candles = renderer_candles;
                self.chart_state = chart_state;
                self.interaction = InteractionController::new();
                self.rate_gate_observer = prepared.rate_gate_observer;
                self.pending_rate_gate = Some(Ok(prepared.rate_gate));
                self.rate_gate = prepared.rate_gate;
                self.accepted_watermark = prepared.accepted_watermark;
                self.reconcile_ack = prepared.reconcile_ack;
                self.active_generation = None;
                self.invalidated_generation = None;
                self.continuity_start = prepared.startup_watermark;
                self.connection_status = ConnectionStatus::Connecting;
                self.status_detail = None;
                self.footer = FooterPresentation::Help;
                self.dirty = true;
            }
            Ok(Err(error)) if generation == self.switch_generation => {
                self.footer = FooterPresentation::Error {
                    message: error.to_string(),
                };
                self.dirty = true;
            }
            Err(error) if generation == self.switch_generation && !error.is_cancelled() => {
                self.footer = FooterPresentation::Error {
                    message: "market preparation failed".to_owned(),
                };
                self.dirty = true;
            }
            _ => {}
        }
    }
    fn begin_switch(&mut self, target: MarketTarget, root: &CancellationToken) {
        if let Some(pending) = self.switch.take() {
            pending.cancellation.cancel();
            pending.task.abort();
            let clock = Arc::clone(&self.clock);
            self.retired.push(tokio::spawn(async move {
                match pending.task.await {
                    Ok(Ok(prepared)) => {
                        prepared.live.request_shutdown();
                        let deadline = checked_deadline(clock.now(), PRODUCER_JOIN_TIMEOUT)
                            .unwrap_or(clock.now());
                        prepared.live.join(deadline).await.map_err(map_live_join)
                    }
                    Ok(Err(_)) => Ok(()),
                    Err(error) if error.is_cancelled() => Ok(()),
                    Err(_) => Err(AppError::Invariant("market preparation join failed")),
                }
            }));
        }
        let provider = match self.providers.get(target.instrument.provider().clone()) {
            Ok(provider) => provider,
            Err(error) => {
                self.footer = FooterPresentation::Error {
                    message: error.to_string(),
                };
                self.dirty = true;
                return;
            }
        };
        let instrument = match provider.canonicalize(&target.instrument) {
            Ok(instrument) => instrument,
            Err(error) => {
                self.footer = FooterPresentation::Error {
                    message: error.to_string(),
                };
                self.dirty = true;
                return;
            }
        };
        if instrument == self.instrument && target.timeframe == self.timeframe {
            self.footer = FooterPresentation::Help;
            self.dirty = true;
            return;
        }
        self.switch_generation = self.switch_generation.wrapping_add(1);
        let generation = self.switch_generation;
        let cancellation = root.child_token();
        let task_cancellation = cancellation.clone();
        let clock = Arc::clone(&self.clock);
        let label = format!("{} {}", instrument.provider_symbol(), target.timeframe);
        self.footer = FooterPresentation::Preparing { target: label };
        self.dirty = true;
        let task = tokio::spawn(async move {
            let candles = provider
                .history(
                    &instrument,
                    target.timeframe,
                    crate::model::HistoryRequest::latest(INITIAL_HISTORY_LIMIT)?,
                    task_cancellation.child_token(),
                )
                .await?;
            let mut series = CandleSeries::new(target.timeframe);
            series
                .replace(candles)
                .map_err(|_| AppError::Invariant("switch series initialized twice"))?;
            let startup_watermark = series.newest_open_time();
            let (accepted_watermark, accepted_watermark_rx) =
                accepted_watermark_channel(startup_watermark);
            let (reconcile_ack, reconcile_ack_rx) = reconcile_ack_channel();
            let live = provider
                .open_live(LiveRequest {
                    instrument: instrument.clone(),
                    timeframe: target.timeframe,
                    startup_watermark,
                    accepted_watermark_rx,
                    reconcile_ack_rx,
                    cancellation: task_cancellation.child_token(),
                })
                .await?;
            let rate_gate_observer = provider.rate_gate();
            let rate_gate = rate_gate_observer
                .current()
                .map_err(|_| ProviderError::Invariant("provider rate gate closed"))?;
            let history = HistoryCoordinator::new(
                Arc::clone(&provider),
                instrument.clone(),
                target.timeframe,
                clock,
                task_cancellation.child_token(),
            );
            Ok(PreparedMarket {
                instrument,
                timeframe: target.timeframe,
                series,
                live,
                history,
                rate_gate_observer,
                rate_gate,
                accepted_watermark,
                reconcile_ack,
                startup_watermark,
            })
        });
        self.switch = Some(SwitchPreparation {
            generation,
            cancellation,
            task,
        });
    }

    fn handle_editor_key(&mut self, key: KeyEvent) -> bool {
        if self.editor.is_none() {
            if key.code == KeyCode::Char(':') && key.modifiers.is_empty() {
                self.editor = Some(CommandEditor::default());
                self.footer = FooterPresentation::Editing {
                    text: String::new(),
                    cursor: 0,
                };
                self.dirty = true;
                return true;
            }
            return false;
        }
        let submitted = self.editor.as_mut().is_some_and(|editor| editor.apply(key));
        if submitted {
            let text = self.editor.take().expect("editor exists").text;
            match parse_market_target(&text) {
                Ok(target) => {
                    let root = self.root_cancellation.clone();
                    self.begin_switch(target, &root)
                }
                Err(message) => {
                    self.footer = FooterPresentation::Error { message };
                    self.dirty = true;
                }
            }
        } else if let Some(editor) = &self.editor {
            self.footer = FooterPresentation::Editing {
                text: editor.text.clone(),
                cursor: editor.cursor,
            };
            self.dirty = true;
        }
        true
    }
    fn effective_rate_gate(&self) -> RateGateState {
        match self.rate_gate {
            RateGateState::TimedUntil(deadline) if self.clock.now() >= deadline => {
                RateGateState::Open
            }
            state => state,
        }
    }

    fn active_rate_gate_deadline(&self) -> Option<MonoInstant> {
        match self.rate_gate {
            RateGateState::TimedUntil(deadline) if self.clock.now() < deadline => Some(deadline),
            RateGateState::Open
            | RateGateState::TimedUntil(_)
            | RateGateState::ProcessBlocked(_) => None,
        }
    }

    fn snapshot(&self) -> RendererSnapshot {
        RendererSnapshot {
            mode: RenderMode::Interactive,
            display_status: self.display_status,
            status_detail: self.status_detail.clone(),
            rate_gate: self.effective_rate_gate(),
            instrument: self.instrument.clone(),
            timeframe: self.timeframe,
            candles: Arc::clone(&self.renderer_candles),
            chart_state: self.chart_state.clone(),
            footer: self.footer.clone(),
        }
    }
    fn refresh_renderer_candles(&mut self, summary: &MutationSummary) {
        if summary.inserted == 0 && summary.replaced == 0 {
            return;
        }
        self.renderer_candles = self.series.iter().cloned().collect();
        #[cfg(feature = "test-transport")]
        {
            self.renderer_candle_revision += 1;
        }
    }

    #[cfg(feature = "test-transport")]
    fn observe_epoch(
        &self,
        source_counts: [usize; 7],
        active_generation: Option<GapGeneration>,
        stale_generation_events: usize,
        stop: Option<EpochStop>,
    ) {
        if let Some(observer) = &self.epoch_observer {
            observer(EpochObservation {
                source_counts,
                active_generation,
                invalidated_generation: self.invalidated_generation,
                stale_generation_events,
                stop,
                snapshot: self.snapshot(),
                renderer_candle_revision: self.renderer_candle_revision,
                layout_pending: matches!(self.layout, ChartLayoutResult::LayoutPending { .. }),
            });
        }
    }
    fn refresh_display_status(&mut self) {
        self.display_status = match self.connection_status {
            ConnectionStatus::Stopped => DisplayStatus::Stopped,
            ConnectionStatus::GapSync => DisplayStatus::GapSync,
            ConnectionStatus::Backoff => DisplayStatus::Backoff,
            ConnectionStatus::Connecting if self.history.in_flight() => DisplayStatus::Backfilling,
            ConnectionStatus::Connected if self.history.in_flight() => DisplayStatus::Backfilling,
            ConnectionStatus::Connecting => DisplayStatus::Connecting,
            ConnectionStatus::Connected => DisplayStatus::Connected,
        };
    }
    fn apply_completed_history(&mut self) -> HistoryProgress {
        let (ChartLayoutResult::Ready { layout }, InteractiveChartState::Ready(view)) =
            (&self.layout, &mut self.chart_state)
        else {
            return HistoryProgress::Idle;
        };
        let HistoryApplyResult { progress, mutation } = self.history.apply_completed(
            &mut self.series,
            view,
            usize::from(layout.main_plot.width),
        );
        if matches!(
            &progress,
            HistoryProgress::PageApplied | HistoryProgress::EndReached
        ) {
            self.interaction
                .sync_after_view_change(view, &self.series, layout);
            let _ = self
                .accepted_watermark
                .publish(self.series.newest_open_time());
        }
        if let Some(summary) = mutation.as_ref() {
            self.refresh_renderer_candles(summary);
        }
        progress
    }

    fn apply_resize(&mut self, size: Size) {
        let next = calculate_chart_layout(
            Rect::new(0, 0, size.width, size.height),
            LayoutMode::Interactive,
        );
        match (&next, &mut self.chart_state) {
            (ChartLayoutResult::Ready { layout }, InteractiveChartState::LayoutPending) => {
                self.chart_state = InteractiveChartState::Ready(ChartViewState::interactive(
                    &self.series,
                    usize::from(layout.main_plot.width),
                ));
            }
            (ChartLayoutResult::Ready { layout }, InteractiveChartState::Ready(view)) => {
                view.resize(&self.series, usize::from(layout.main_plot.width));
                self.interaction
                    .sync_after_view_change(view, &self.series, layout);
            }
            (ChartLayoutResult::LayoutPending { .. }, _) => {}
        }
        self.layout = next;
        if self.history.in_flight() {
            let _ = self.apply_completed_history();
            self.refresh_display_status();
        }
        self.dirty = true;
    }

    fn apply_series_mutation(&mut self, candles: Vec<crate::model::Candle>) {
        let summary = self.series.merge(candles);
        if let (ChartLayoutResult::Ready { layout }, InteractiveChartState::Ready(view)) =
            (&self.layout, &mut self.chart_state)
        {
            view.apply_mutation(&self.series, &summary, usize::from(layout.main_plot.width));
            self.interaction
                .sync_after_view_change(view, &self.series, layout);
        }
        let _ = self
            .accepted_watermark
            .publish(self.series.newest_open_time());
        self.refresh_renderer_candles(&summary);
        self.dirty |= summary.inserted != 0 || summary.replaced != 0;
    }

    fn prove_reconciliation(
        &mut self,
        generation: GapGeneration,
        revision: ReplayRevision,
        target_open_time: i64,
    ) {
        let through = self.series.newest_open_time();
        let proved = match (self.continuity_start, through) {
            (Some(start), Some(through)) => {
                through >= target_open_time
                    && self.series.is_contiguous_through(start, target_open_time)
            }
            (None, Some(through)) => {
                through >= target_open_time
                    && self.series.index_of_open_time(target_open_time).is_some()
                    && self.series.is_contiguous()
            }
            _ => false,
        };
        if proved {
            let _ = self.reconcile_ack.publish(ReconcileAck {
                generation,
                revision,
                through: target_open_time,
            });
        }
    }

    fn reduce(&mut self, mut epoch: Epoch) -> Option<AppError> {
        #[cfg(feature = "test-transport")]
        let source_counts = epoch.source_counts;
        #[cfg(feature = "test-transport")]
        let generation_event_count = epoch.recoverable_events.len()
            + epoch.status_events.len()
            + epoch.reconciliation.len()
            + epoch.candles.len();
        // Scan admitted live arrivals in source FIFO before bucket priority reorders variants.
        // A generationless emergency invalidates only the generation active/candidate at that
        // point; a strictly newer tagged generation arriving after the pair remains admissible.
        let (candidate_generation, invalidated_generation) =
            epoch.precomputed_generation(self.active_generation, self.invalidated_generation);
        self.invalidated_generation = invalidated_generation;
        let admitted_generation = candidate_generation;
        let generation_is_current = |generation: Option<GapGeneration>| {
            generation.is_none()
                || generation.is_some_and(|generation| {
                    Some(generation) == admitted_generation
                        && self
                            .invalidated_generation
                            .is_none_or(|invalidated| generation > invalidated)
                })
        };
        epoch
            .recoverable_events
            .retain(|(generation, _, _)| generation_is_current(*generation));
        epoch
            .status_events
            .retain(|(generation, _)| generation_is_current(*generation));
        epoch
            .reconciliation
            .retain(|(generation, ..)| Some(*generation) == admitted_generation);
        epoch
            .candles
            .retain(|(generation, _)| Some(*generation) == admitted_generation);
        #[cfg(feature = "test-transport")]
        let stale_generation_events = generation_event_count
            - (epoch.recoverable_events.len()
                + epoch.status_events.len()
                + epoch.reconciliation.len()
                + epoch.candles.len());

        // Bucket 1: cancellation and admitted quit keys short-circuit every lower bucket.
        if epoch.cancelled || epoch.quit {
            #[cfg(feature = "test-transport")]
            self.observe_epoch(
                source_counts,
                admitted_generation,
                stale_generation_events,
                Some(EpochStop::Quit),
            );
            return None;
        }

        // Bucket 2: exact failure subtype order. `combine_errors` preserves the first as primary
        // and appends every already-admitted later failure as ordered secondary detail.
        let terminal_failures = epoch
            .terminal_lifecycle_failures
            .into_iter()
            .chain(epoch.stream_failures)
            .chain(epoch.channel_failures)
            .chain(epoch.producer_failures)
            .chain(epoch.history_failures)
            .collect::<Vec<_>>();
        if !terminal_failures.is_empty() {
            self.display_status = DisplayStatus::Disconnected;
            self.dirty = true;
            #[cfg(feature = "test-transport")]
            self.observe_epoch(
                source_counts,
                admitted_generation,
                stale_generation_events,
                Some(EpochStop::TerminalFailure),
            );
            return combine_errors(terminal_failures);
        }
        for observed in epoch.rate_gate_states {
            self.rate_gate = match (self.rate_gate, observed) {
                (RateGateState::ProcessBlocked(blocker), _) => {
                    RateGateState::ProcessBlocked(blocker)
                }
                (_, RateGateState::ProcessBlocked(blocker)) => {
                    RateGateState::ProcessBlocked(blocker)
                }
                (RateGateState::TimedUntil(current), RateGateState::TimedUntil(observed)) => {
                    RateGateState::TimedUntil(current.max(observed))
                }
                (RateGateState::TimedUntil(deadline), RateGateState::Open) => {
                    RateGateState::TimedUntil(deadline)
                }
                (RateGateState::Open, next) => next,
            };
            self.dirty = true;
        }

        if admitted_generation != self.active_generation {
            self.active_generation = admitted_generation;
            if admitted_generation.is_some_and(|generation| {
                self.invalidated_generation
                    .is_some_and(|invalidated| generation > invalidated)
            }) {
                self.invalidated_generation = None;
            }
            self.continuity_start = self.series.newest_open_time();
        }

        // Bucket 3: priority overrides cross-variant live FIFO; FIFO is retained within each
        // exact subtype. A terminal event records display state and stops buckets 4-7.
        if !epoch.terminal_events.is_empty() {
            let mut errors = Vec::with_capacity(epoch.terminal_events.len());
            for error in epoch.terminal_events {
                self.display_status = DisplayStatus::TerminalError;
                self.status_detail = Some(error.clone());
                self.dirty = true;
                errors.push(error.into());
            }
            #[cfg(feature = "test-transport")]
            self.observe_epoch(
                source_counts,
                admitted_generation,
                stale_generation_events,
                Some(EpochStop::LiveTerminalError),
            );
            return combine_errors(errors);
        }
        for (_, error, deadline) in epoch.recoverable_events {
            if matches!(error, ProviderError::InvalidBanExpiry) {
                self.rate_gate =
                    RateGateState::ProcessBlocked(crate::model::ProcessBlocker::InvalidBanExpiry);
            } else if let Some(deadline) = deadline {
                self.rate_gate = match self.rate_gate {
                    RateGateState::ProcessBlocked(blocker) => {
                        RateGateState::ProcessBlocked(blocker)
                    }
                    RateGateState::TimedUntil(current) => {
                        RateGateState::TimedUntil(current.max(deadline))
                    }
                    RateGateState::Open => RateGateState::TimedUntil(deadline),
                };
            }
            self.status_detail = Some(error);
            self.dirty = true;
        }
        for (_, status) in epoch.status_events {
            self.connection_status = status;
            self.refresh_display_status();
            self.dirty = true;
        }

        // Bucket 4: history source FIFO, before every simultaneous live data mutation.
        for progress in epoch.history {
            match progress {
                HistoryProgress::RequestStarted => {
                    self.refresh_display_status();
                    self.dirty = true;
                }
                HistoryProgress::PageReady => {
                    let _ = self.apply_completed_history();
                    self.refresh_display_status();
                    self.dirty = true;
                }
                _ => {}
            }
        }

        // Bucket 5: reconciliation is key-ordered; Rust's stable sort preserves admission FIFO
        // for equal keys. Ordinary candles retain admission FIFO.
        epoch
            .reconciliation
            .sort_by_key(|(generation, revision, ..)| (*generation, *revision));
        for (generation, revision, target, candles) in epoch.reconciliation {
            self.apply_series_mutation(candles);
            self.prove_reconciliation(generation, revision, target);
        }
        for (_, candle) in epoch.candles {
            self.apply_series_mutation(vec![candle]);
        }

        // Bucket 6: resize first, then keyboard, then pointer. Pointer projection therefore sees
        // the latest retained layout admitted in this epoch.
        for size in epoch.resizes {
            self.apply_resize(size);
        }
        for key in epoch.keys {
            if is_soft_quit(key) {
                if self.editor.is_none() {
                    self.quit_requested = true;
                    break;
                }
                // q/Esc cancel editing and remain active; force-quit (Ctrl-C/Ctrl-D) is
                // already handled by `epoch.quit` in bucket 1.
                if key.code == KeyCode::Esc {
                    self.editor = None;
                    self.footer = FooterPresentation::Help;
                    self.dirty = true;
                    continue;
                }
            }
            if self.handle_editor_key(key) {
                continue;
            }
            if let (ChartLayoutResult::Ready { layout }, InteractiveChartState::Ready(view)) =
                (&self.layout, &mut self.chart_state)
            {
                match self.interaction.key(key, view, &self.series, layout) {
                    InteractionAction::Redraw => self.dirty = true,
                    InteractionAction::Quit | InteractionAction::Ignored => {}
                }
            }
        }
        for pointer in epoch.pointers {
            if let (ChartLayoutResult::Ready { layout }, InteractiveChartState::Ready(view)) =
                (&self.layout, &mut self.chart_state)
                && matches!(
                    self.interaction.mouse(pointer, view, &self.series, layout),
                    InteractionAction::Redraw
                )
            {
                self.dirty = true;
            }
        }

        // Bucket 7: deadline order with admission-FIFO ties. A gate deadline is display-only:
        // the shared monotonic gate remains TimedUntil, while equality redraws it effectively Open.
        epoch
            .timers
            .sort_by_key(|(deadline, sequence)| (*deadline, *sequence));
        self.dirty |= !epoch.timers.is_empty();
        #[cfg(feature = "test-transport")]
        self.observe_epoch(
            source_counts,
            admitted_generation,
            stale_generation_events,
            None,
        );
        None
    }
}

async fn run_interactive(
    cli: Cli,
    provider: Arc<dyn MarketDataProvider>,
    mut dependencies: RunDependencies,
) -> Result<(), AppError> {
    let cancellation = CancellationToken::new();
    let instrument = provider.canonicalize(cli.instrument())?;
    let initial = provider
        .history(
            &instrument,
            cli.timeframe(),
            crate::model::HistoryRequest::latest(INITIAL_HISTORY_LIMIT)?,
            cancellation.child_token(),
        )
        .await?;
    let mut series = CandleSeries::new(cli.timeframe());
    series
        .replace(initial)
        .map_err(|_| AppError::Invariant("interactive series initialized twice"))?;
    let renderer_candles = series.iter().cloned().collect();
    let startup_watermark = series.newest_open_time();
    let (accepted_watermark, accepted_watermark_rx) = accepted_watermark_channel(startup_watermark);
    let (reconcile_ack, reconcile_ack_rx) = reconcile_ack_channel();
    let live = provider
        .open_live(LiveRequest {
            instrument: instrument.clone(),
            timeframe: cli.timeframe(),
            startup_watermark,
            accepted_watermark_rx,
            reconcile_ack_rx,
            cancellation: cancellation.child_token(),
        })
        .await?;
    let mut history = HistoryCoordinator::new(
        Arc::clone(&provider),
        instrument.clone(),
        cli.timeframe(),
        Arc::clone(&dependencies.clock),
        cancellation.child_token(),
    );

    let mut session = enter_with_tty_preflight(
        dependencies.stdin_is_tty,
        dependencies.stdout_is_tty,
        || Arc::clone(&dependencies.terminal),
    )
    .map_err(map_terminal_lifecycle)?;

    let size = match dependencies.terminal.size() {
        Ok((width, height)) => Size::new(width, height),
        Err(_) => {
            let mut primary = Some(AppError::Terminal(TerminalError::Setup {
                operation: "query terminal size",
                cause: SanitizedCause::Io,
            }));
            cancellation.cancel();
            live.request_shutdown();
            if let Err(failures) = session.restore() {
                primary = Some(attach_secondary(
                    primary,
                    AppError::Terminal(TerminalError::Restore {
                        operation: "interactive terminal cleanup",
                        cause: if failures.is_empty() {
                            SanitizedCause::Other
                        } else {
                            SanitizedCause::Io
                        },
                    }),
                ));
            }
            drop(session);
            let now = dependencies.clock.now();
            let deadline = cleanup_deadline(
                now,
                PRODUCER_JOIN_TIMEOUT,
                &mut primary,
                "live producer join deadline overflow",
            );
            if let Err(error) = live.join(deadline).await {
                primary = Some(attach_secondary(primary, map_live_join(error)));
            }
            history.request_shutdown();
            if history.in_flight() {
                let now = dependencies.clock.now();
                let deadline = if history.has_owned_task() {
                    cleanup_deadline(
                        now,
                        HISTORY_JOIN_TIMEOUT,
                        &mut primary,
                        "history join deadline overflow",
                    )
                } else {
                    now
                };
                if let Err(error) = history.join(deadline).await {
                    primary = Some(attach_secondary(primary, map_history_join(error)));
                }
            }
            return Err(primary.expect("terminal size failure is primary"));
        }
    };
    let layout = calculate_chart_layout(
        Rect::new(0, 0, size.width, size.height),
        LayoutMode::Interactive,
    );
    let chart_state = match &layout {
        ChartLayoutResult::LayoutPending { .. } => InteractiveChartState::LayoutPending,
        ChartLayoutResult::Ready { layout } => InteractiveChartState::Ready(
            ChartViewState::interactive(&series, usize::from(layout.main_plot.width)),
        ),
    };
    let rate_gate_observer = provider.rate_gate();
    let initial_rate_gate = match rate_gate_observer.current() {
        Ok(state) => state,
        Err(_) => {
            let mut primary = Some(AppError::Provider(ProviderError::Invariant(
                "provider rate gate closed",
            )));
            cancellation.cancel();
            live.request_shutdown();
            history.request_shutdown();

            if let Err(failures) = session.restore() {
                primary = Some(attach_secondary(
                    primary,
                    AppError::Terminal(TerminalError::Restore {
                        operation: "interactive terminal cleanup",
                        cause: if failures.is_empty() {
                            SanitizedCause::Other
                        } else {
                            SanitizedCause::Io
                        },
                    }),
                ));
            }
            drop(session);

            let now = dependencies.clock.now();
            let deadline = cleanup_deadline(
                now,
                PRODUCER_JOIN_TIMEOUT,
                &mut primary,
                "live producer join deadline overflow",
            );
            if let Err(error) = live.join(deadline).await {
                primary = Some(attach_secondary(primary, map_live_join(error)));
            }

            if history.in_flight() {
                let now = dependencies.clock.now();
                let deadline = if history.has_owned_task() {
                    cleanup_deadline(
                        now,
                        HISTORY_JOIN_TIMEOUT,
                        &mut primary,
                        "history join deadline overflow",
                    )
                } else {
                    now
                };
                if let Err(error) = history.join(deadline).await {
                    primary = Some(attach_secondary(primary, map_history_join(error)));
                }
            }

            return Err(primary.expect("closed rate gate is primary"));
        }
    };
    let rate_gate = initial_rate_gate;
    let mut app = App {
        instrument,
        timeframe: cli.timeframe(),
        series,
        renderer_candles,
        #[cfg(feature = "test-transport")]
        renderer_candle_revision: 0,
        chart_state,
        layout,
        interaction: InteractionController::new(),
        history,
        rate_gate_observer,
        clock: Arc::clone(&dependencies.clock),
        pending_rate_gate: Some(Ok(initial_rate_gate)),
        accepted_watermark,
        reconcile_ack,
        active_generation: None,
        invalidated_generation: None,
        continuity_start: startup_watermark,
        display_status: DisplayStatus::Connecting,
        connection_status: ConnectionStatus::Connecting,
        status_detail: None,
        rate_gate,
        dirty: true,
        footer: FooterPresentation::Help,
        editor: None,
        providers: dependencies.providers.clone(),
        switch_generation: 0,
        switch: None,
        retired: Vec::new(),
        root_cancellation: cancellation.clone(),
        quit_requested: false,
        #[cfg(feature = "test-transport")]
        epoch_observer: dependencies.epoch_observer.clone(),
    };

    let mut live = Some(live);
    // This inner guard scope is nested inside the terminal session's scope. If this future is
    // dropped while the App loop is pending, InputReaderTask joins its bounded-poll owner before
    // unwinding reaches TerminalSession.
    let mut primary = {
        let (input_tx, mut input_rx) = mpsc::unbounded_channel();
        let mut input_task = InputReaderTask::spawn(dependencies.input, input_tx);
        let mut primary = run_app_loop(
            &mut app,
            live.as_mut().expect("live feed exists"),
            &cancellation,
            &mut input_rx,
            dependencies.terminal.as_ref(),
            dependencies.render_policy,
            &mut dependencies.stdout,
        )
        .await;

        input_rx.close();
        cancellation.cancel();
        if let Some(feed) = live.as_ref() {
            feed.request_shutdown();
        }
        if !input_task.cancel_and_join() {
            primary = Some(attach_secondary(
                primary,
                TerminalError::Input(SanitizedCause::Other).into(),
            ));
        }
        primary
    };
    let cleanup = session.restore().err();
    drop(session);

    if let Some(failures) = cleanup {
        let cleanup_error = AppError::Terminal(TerminalError::Restore {
            operation: "interactive terminal cleanup",
            cause: if failures.is_empty() {
                SanitizedCause::Other
            } else {
                SanitizedCause::Io
            },
        });
        primary = Some(attach_secondary(primary, cleanup_error));
    }

    if let Some(feed) = live.take() {
        let now = dependencies.clock.now();
        let deadline = cleanup_deadline(
            now,
            PRODUCER_JOIN_TIMEOUT,
            &mut primary,
            "live producer join deadline overflow",
        );
        if let Err(error) = feed.join(deadline).await {
            primary = Some(attach_secondary(primary, map_live_join(error)));
        }
    }

    app.history.request_shutdown();
    if let Some(preparation) = app.switch.take() {
        preparation.cancellation.cancel();
        preparation.task.abort();
    }
    for handle in app.retired.drain(..) {
        let _ = handle.await;
    }
    if app.history.in_flight() {
        let now = dependencies.clock.now();
        let history_deadline = if app.history.has_owned_task() {
            cleanup_deadline(
                now,
                HISTORY_JOIN_TIMEOUT,
                &mut primary,
                "history join deadline overflow",
            )
        } else {
            now
        };
        if let Err(error) = app.history.join(history_deadline).await {
            primary = Some(attach_secondary(primary, map_history_join(error)));
        }
    }
    drop(app);

    match primary {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn run_app_loop(
    app: &mut App,
    live: &mut LiveFeed,
    cancellation: &CancellationToken,
    input_rx: &mut mpsc::UnboundedReceiver<InputEvent>,
    terminal: &dyn crate::terminal::TerminalDriver,
    render_policy: RenderPolicy,
    output: &mut dyn Write,
) -> Option<AppError> {
    let mut input_open = true;
    let mut live_open = true;
    let mut producer_finished = false;
    loop {
        let sampled = match terminal.size() {
            Ok((width, height)) => Size::new(width, height),
            Err(_) => {
                let mut failure_epoch = Epoch::default();
                failure_epoch
                    .terminal_lifecycle_failures
                    .push(terminal_size_error());
                return app.reduce(failure_epoch);
            }
        };
        let current = match &app.layout {
            ChartLayoutResult::LayoutPending { actual, .. } => *actual,
            ChartLayoutResult::Ready { layout } => {
                Size::new(layout.frame.width, layout.frame.height)
            }
        };
        if sampled != current {
            let mut resize_epoch = Epoch::default();
            if resize_epoch.admit(EpochSource::Input) {
                resize_epoch.resizes.push(sampled);
                if let Some(error) = app.reduce(resize_epoch) {
                    return Some(error);
                }
            }
        }
        if app.dirty {
            if let Err(error) = render(app, render_policy, output) {
                return Some(error);
            }
            app.dirty = false;
        }

        let history_active = (app.history.in_flight() && !app.history.has_completed_page())
            || app.history.retry_deadline().is_some();
        let rate_gate_deadline = app.active_rate_gate_deadline();
        let rate_gate_clock = Arc::clone(&app.clock);
        let mut epoch = Epoch::default();
        if let Some(state) = app.pending_rate_gate.take()
            && epoch.admit(EpochSource::RateGate)
        {
            classify_rate_gate(state, &mut epoch);
        }
        if epoch.source_counts[Epoch::source_index(EpochSource::RateGate)] == 0 {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    if epoch.admit(EpochSource::Cancellation) {
                        epoch.cancelled = true;
                    }
                },
                () = async {
                    if app.switch.is_some() {
                        tokio::time::sleep(INPUT_POLL_TIMEOUT).await;
                    } else {
                        future::pending().await
                    }
                } => {
                    app.poll_switch(live);
                },
                completion = live.producer_completion.changed(), if !producer_finished => {
                    if epoch.admit(EpochSource::ProducerCompletion) {
                        producer_finished = classify_producer_completion(completion, &mut epoch);
                    }
                }
                input = input_rx.recv(), if input_open => {
                    if epoch.admit(EpochSource::Input) {
                        input_open = classify_input(input, &mut epoch, terminal);
                    }
                }
                event = live.events.next(), if live_open => {
                    if epoch.admit(EpochSource::Live) {
                        live_open = classify_live(event, &mut epoch);
                    }
                }
                state = app.rate_gate_observer.changed() => {
                    if epoch.admit(EpochSource::RateGate) {
                        classify_rate_gate(state, &mut epoch);
                    }
                }
                () = async {
                    if let Some(deadline) = rate_gate_deadline {
                        rate_gate_clock.sleep_until(deadline).await;
                    } else {
                        future::pending().await
                    }
                } => {
                    if let Some(deadline) = rate_gate_deadline
                        && epoch.admit(EpochSource::Timer)
                    {
                        let sequence = epoch.next_admission_sequence;
                        epoch.timers.push((deadline, sequence));
                    }
                }
                progress = async {
                    if history_active { app.history.drive().await } else { future::pending().await }
                } => classify_history_progress(progress, &mut epoch),
            }
        }

        while input_open && epoch.admit(EpochSource::Input) {
            let Some(input) = input_rx.recv().now_or_never() else {
                // No event was admitted; undo the readiness-probe reservation.
                epoch.source_counts[Epoch::source_index(EpochSource::Input)] -= 1;
                break;
            };
            input_open = classify_input(input, &mut epoch, terminal);
            if !input_open {
                break;
            }
        }
        while live_open && epoch.admit(EpochSource::Live) {
            let Some(event) = live.events.next().now_or_never() else {
                epoch.source_counts[Epoch::source_index(EpochSource::Live)] -= 1;
                break;
            };
            live_open = classify_live(event, &mut epoch);
            if !live_open {
                break;
            }
        }
        while epoch.admit(EpochSource::RateGate) {
            let Some(state) = app.rate_gate_observer.changed().now_or_never() else {
                epoch.source_counts[Epoch::source_index(EpochSource::RateGate)] -= 1;
                break;
            };
            let closed = state.is_err();
            classify_rate_gate(state, &mut epoch);
            if closed {
                break;
            }
        }

        if !input_open && !epoch.quit && !epoch.keys.iter().copied().any(is_soft_quit) {
            epoch
                .channel_failures
                .push(AppError::Invariant("terminal input channel closed"));
        }
        if !live_open && !producer_finished {
            epoch
                .stream_failures
                .push(AppError::Invariant("market event stream closed"));
        }
        let should_finish = producer_finished && !live_open;
        if epoch.cancelled || epoch.quit {
            return None;
        }
        if let Some(error) = app.reduce(epoch) {
            return Some(error);
        }
        if app.quit_requested {
            return None;
        }
        if should_finish {
            return None;
        }
        if let (ChartLayoutResult::Ready { .. }, InteractiveChartState::Ready(view)) =
            (&app.layout, &app.chart_state)
        {
            let progress = app
                .history
                .update_boundary(view.visible_range().start, &app.series);
            if !matches!(progress, HistoryProgress::Idle) {
                let mut history_epoch = Epoch::default();
                classify_history_progress(progress, &mut history_epoch);
                if let Some(error) = app.reduce(history_epoch) {
                    return Some(error);
                }
            }
        }
    }
}

fn terminal_size_error() -> AppError {
    TerminalError::Setup {
        operation: "query terminal size",
        cause: SanitizedCause::Io,
    }
    .into()
}

fn classify_input(
    input: Option<InputEvent>,
    epoch: &mut Epoch,
    terminal: &dyn crate::terminal::TerminalDriver,
) -> bool {
    match input {
        // Ctrl-C/Ctrl-D always quit, even mid-edit. q/Esc quit only when not editing, so they
        // are pushed to the key queue for an editor-aware decision in `reduce`.
        Some(InputEvent::Key(key)) if is_force_quit(key) => {
            epoch.quit = true;
        }
        Some(InputEvent::Key(key)) => epoch.keys.push(key),
        Some(InputEvent::Mouse(mouse)) => epoch.pointers.push(mouse),
        Some(InputEvent::ResizeRequested) => match terminal.size() {
            Ok((width, height)) => epoch.resizes.push(Size::new(width, height)),
            Err(_) => epoch
                .terminal_lifecycle_failures
                .push(terminal_size_error()),
        },
        Some(InputEvent::InputFailure) => epoch
            .terminal_lifecycle_failures
            .push(TerminalError::Input(SanitizedCause::Io).into()),
        None => return false,
    }
    true
}
fn classify_producer_completion(
    completion: Result<ProducerCompletion, crate::provider::ProducerCompletionClosed>,
    epoch: &mut Epoch,
) -> bool {
    match completion {
        Ok(ProducerCompletion::Running) => false,
        Ok(ProducerCompletion::Finished(Ok(()))) => true,
        Ok(ProducerCompletion::Finished(Err(error))) => {
            epoch.producer_failures.push(error.into());
            true
        }
        Err(_) => {
            epoch
                .producer_failures
                .push(AppError::Invariant("producer completion channel closed"));
            true
        }
    }
}

fn classify_live(event: Option<Result<MarketEvent, ProviderError>>, epoch: &mut Epoch) -> bool {
    match event {
        Some(Ok(MarketEvent::TerminalError(error))) => epoch.terminal_events.push(error),
        Some(Ok(MarketEvent::RecoverableError {
            generation,
            error,
            rate_gate_deadline,
        })) => {
            match generation {
                Some(generation) => epoch
                    .live_admissions
                    .push(LiveAdmission::Tagged(generation)),
                None if matches!(error, ProviderError::QueueSaturated) => epoch
                    .live_admissions
                    .push(LiveAdmission::GenerationlessInvalidator),
                None => {}
            }
            epoch
                .recoverable_events
                .push((generation, error, rate_gate_deadline));
        }
        Some(Ok(MarketEvent::Status { generation, status })) => {
            match generation {
                Some(generation) => epoch
                    .live_admissions
                    .push(LiveAdmission::Tagged(generation)),
                None if matches!(
                    status,
                    ConnectionStatus::Backoff | ConnectionStatus::Stopped
                ) =>
                {
                    epoch
                        .live_admissions
                        .push(LiveAdmission::GenerationlessInvalidator);
                }
                None => {}
            }
            epoch.status_events.push((generation, status));
        }
        Some(Ok(MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time,
            candles,
        })) => {
            epoch
                .live_admissions
                .push(LiveAdmission::Tagged(generation));
            epoch
                .reconciliation
                .push((generation, revision, target_open_time, candles));
        }
        Some(Ok(MarketEvent::Candle { generation, candle })) => {
            epoch
                .live_admissions
                .push(LiveAdmission::Tagged(generation));
            epoch.candles.push((generation, candle));
        }
        Some(Err(error)) => epoch.stream_failures.push(error.into()),
        None => return false,
    }
    true
}

fn classify_rate_gate(state: Result<RateGateState, RateGateClosed>, epoch: &mut Epoch) {
    match state {
        Ok(state) => epoch.rate_gate_states.push(state),
        Err(_) => epoch
            .channel_failures
            .push(AppError::Invariant("provider rate gate closed")),
    }
}

fn classify_history_progress(progress: HistoryProgress, epoch: &mut Epoch) {
    match progress {
        HistoryProgress::RetryDeferred(deadline) => {
            if epoch.admit(EpochSource::Timer) {
                let sequence = epoch.next_admission_sequence;
                epoch.timers.push((deadline, sequence));
            }
        }
        HistoryProgress::TerminalFailure(error) => {
            if epoch.admit(EpochSource::History) {
                epoch.history_failures.push(error.into());
            }
        }
        progress => {
            if epoch.admit(EpochSource::History) {
                epoch.history.push(progress);
            }
        }
    }
}

fn is_force_quit(key: KeyEvent) -> bool {
    key.kind != KeyEventKind::Release
        && key.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(key.code, KeyCode::Char('c' | 'C' | 'd' | 'D'))
}

fn is_soft_quit(key: KeyEvent) -> bool {
    key.kind != KeyEventKind::Release
        && key.modifiers == KeyModifiers::NONE
        && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
}

struct InputReaderTask {
    cancel: Option<std_mpsc::Sender<()>>,
    owner: Option<ThreadJoinHandle<()>>,
}

impl InputReaderTask {
    fn spawn(mut input: Box<dyn TerminalInput>, sender: mpsc::UnboundedSender<InputEvent>) -> Self {
        let (cancel_tx, cancel_rx) = std_mpsc::channel();
        let owner = thread::spawn(move || {
            loop {
                match cancel_rx.try_recv() {
                    Ok(()) | Err(std_mpsc::TryRecvError::Disconnected) => break,
                    Err(std_mpsc::TryRecvError::Empty) => {}
                }
                match input.poll(INPUT_POLL_TIMEOUT) {
                    Ok(TerminalInputPoll::Event(Event::Key(key))) => {
                        if sender.send(InputEvent::Key(key)).is_err() {
                            break;
                        }
                    }
                    Ok(TerminalInputPoll::Event(Event::Mouse(mouse))) => {
                        if sender.send(InputEvent::Mouse(mouse)).is_err() {
                            break;
                        }
                    }
                    Ok(TerminalInputPoll::Event(Event::Resize(_, _))) => {
                        if sender.send(InputEvent::ResizeRequested).is_err() {
                            break;
                        }
                    }
                    Ok(TerminalInputPoll::Event(_)) | Ok(TerminalInputPoll::Idle) => {}
                    Ok(TerminalInputPoll::Closed) => break,
                    Err(_) => {
                        let _ = sender.send(InputEvent::InputFailure);
                        break;
                    }
                }
            }
        });
        Self {
            cancel: Some(cancel_tx),
            owner: Some(owner),
        }
    }

    fn cancel_and_join(&mut self) -> bool {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        self.owner.take().is_none_or(|owner| owner.join().is_ok())
    }
}

impl Drop for InputReaderTask {
    fn drop(&mut self) {
        let _ = self.cancel_and_join();
    }
}

fn render(app: &App, policy: RenderPolicy, output: &mut dyn Write) -> Result<(), AppError> {
    let frame = match &app.layout {
        ChartLayoutResult::LayoutPending { actual, .. } => {
            Rect::new(0, 0, actual.width, actual.height)
        }
        ChartLayoutResult::Ready { layout } => layout.frame,
    };
    let mut buffer = Buffer::empty(frame);
    let snapshot = app.snapshot();
    ChartWidget::new(&snapshot, &app.layout, policy).render_to(frame, &mut buffer);
    output.write_all(b"\x1b[H").map_err(output_error)?;
    crate::snapshot::serialize_frame(
        &buffer,
        SnapshotOutputTarget::Tty {
            physical_size: Size::new(frame.width, frame.height.saturating_add(1)),
        },
        policy,
        output,
    )
}

fn map_terminal_lifecycle(error: TerminalLifecycleError) -> AppError {
    match error {
        TerminalLifecycleError::Primary(primary) => primary,
        TerminalLifecycleError::PrimaryWithCleanup { primary, .. } => attach_secondary(
            Some(primary),
            TerminalError::Restore {
                operation: "interactive terminal setup rollback",
                cause: SanitizedCause::Io,
            }
            .into(),
        ),
        TerminalLifecycleError::Cleanup(_) => TerminalError::Restore {
            operation: "interactive terminal setup rollback",
            cause: SanitizedCause::Io,
        }
        .into(),
    }
}

fn map_live_join(error: LiveFeedJoinError) -> AppError {
    match error {
        LiveFeedJoinError::Producer(error) => error.into(),
        LiveFeedJoinError::DeadlineElapsed => {
            AppError::Invariant("live producer join deadline elapsed")
        }
        LiveFeedJoinError::Aborted => AppError::Invariant("live producer was aborted"),
        LiveFeedJoinError::JoinFailure => AppError::Invariant("live producer join failed"),
    }
}

fn map_history_join(error: HistoryJoinError) -> AppError {
    match error {
        HistoryJoinError::DeadlineElapsed => AppError::Invariant("history join deadline elapsed"),
        HistoryJoinError::Aborted => AppError::Invariant("history task was aborted"),
        HistoryJoinError::JoinFailure => AppError::Invariant("history task join failed"),
    }
}

fn cleanup_deadline(
    now: MonoInstant,
    timeout: Duration,
    primary: &mut Option<AppError>,
    overflow: &'static str,
) -> MonoInstant {
    match checked_deadline(now, timeout) {
        Ok(deadline) => deadline,
        Err(_) => {
            *primary = Some(attach_secondary(
                primary.take(),
                AppError::Invariant(overflow),
            ));
            now
        }
    }
}

fn attach_secondary(primary: Option<AppError>, secondary: AppError) -> AppError {
    match primary {
        Some(primary) => AppError::PrimaryWithSecondary {
            primary: Box::new(primary),
            secondary: Box::new(secondary),
        },
        None => secondary,
    }
}

fn combine_errors(errors: Vec<AppError>) -> Option<AppError> {
    let mut errors = errors.into_iter();
    let mut combined = errors.next()?;
    for secondary in errors {
        combined = attach_secondary(Some(combined), secondary);
    }
    Some(combined)
}

fn output_error(_error: std::io::Error) -> AppError {
    RenderError::Output(SanitizedCause::Io).into()
}

#[cfg(test)]
mod input_tests {
    use super::*;

    #[test]
    fn scripted_input_is_bounded_and_closes() {
        let key = Event::Key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE));
        let mut input = ScriptedTerminalInput::new([key.clone()]);
        assert_eq!(
            input.poll(Duration::ZERO).unwrap(),
            TerminalInputPoll::Event(key)
        );
        assert_eq!(
            input.poll(Duration::ZERO).unwrap(),
            TerminalInputPoll::Closed
        );
    }

    #[test]
    fn channel_input_reports_idle_event_and_closed() {
        let (sender, mut input) = ChannelTerminalInput::channel();
        assert_eq!(input.poll(Duration::ZERO).unwrap(), TerminalInputPoll::Idle);
        let key = Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        sender.send(key.clone()).unwrap();
        assert_eq!(
            input.poll(Duration::ZERO).unwrap(),
            TerminalInputPoll::Event(key)
        );
        drop(sender);
        assert_eq!(
            input.poll(Duration::ZERO).unwrap(),
            TerminalInputPoll::Closed
        );
    }
}
