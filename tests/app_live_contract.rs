#![cfg(feature = "test-transport")]

use std::{
    collections::VecDeque,
    future::Future,
    io::Write,
    process::ExitCode,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use assert_cmd::Command;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
use fccli::{
    app::{
        EpochObservation, EpochStop, RunDependencies, ScriptedTerminalInput, TerminalInput,
        TerminalInputPoll, run_with_dependencies,
    },
    chart::{ChartViewState, DisplayStatus, FooterPresentation, InteractiveChartState, PriceRange},
    cli::canonicalize_instrument,
    clock::{Clock, ManualClock},
    error::{AppError, ErrorContext, ErrorOperation, ProviderError, RenderError, TerminalError},
    model::{
        Candle, ConnectionStatus, GapGeneration, HistoryRequest, HistoryRequestKind, Instrument,
        InstrumentSpec, Market, MarketEvent, MonoInstant, ProcessBlocker, ProviderId,
        RateGateState, ReplayRevision, Timeframe,
    },
    provider::{
        CancellationToken, LiveFeed, LiveRequest, MarketDataProvider, MarketEventStream,
        ProviderFuture, ProviderRegistry, RateGateSender, RateGateSnapshot, ReconcileAck,
        rate_gate_channel,
    },
    terminal::TerminalDriver,
};
use futures_util::{FutureExt, stream};

trait MutexExt<T> {
    fn acquire(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn acquire(&self) -> MutexGuard<'_, T> {
        self.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
struct ProducerCleanupTrace(Arc<Mutex<Vec<&'static str>>>);

impl Drop for ProducerCleanupTrace {
    fn drop(&mut self) {
        self.0.acquire().push("producer-cleaned");
    }
}

struct HistoryCleanupTrace(Arc<Mutex<Vec<&'static str>>>);

impl Drop for HistoryCleanupTrace {
    fn drop(&mut self) {
        self.0.acquire().push("history-cleaned");
    }
}

#[derive(Clone, Default)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl Write for SharedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.acquire().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct PendingFrameWriter {
    output: SharedWriter,
    pending_observed: Mutex<Option<mpsc::Sender<()>>>,
    pending_resize_queued: Mutex<Option<mpsc::Receiver<()>>>,
    later_ready_observed: Mutex<Option<mpsc::Sender<()>>>,
}

impl PendingFrameWriter {
    fn new(
        output: SharedWriter,
        pending_observed: mpsc::Sender<()>,
        pending_resize_queued: mpsc::Receiver<()>,
        later_ready_observed: mpsc::Sender<()>,
    ) -> Self {
        Self {
            output,
            pending_observed: Mutex::new(Some(pending_observed)),
            pending_resize_queued: Mutex::new(Some(pending_resize_queued)),
            later_ready_observed: Mutex::new(Some(later_ready_observed)),
        }
    }
}

impl Write for PendingFrameWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.output.write(bytes)?;
        let output = self.output.0.acquire();
        let pending_rendered = output
            .windows(b"Resize terminal to at least 60x18".len())
            .any(|window| window == b"Resize terminal to at least 60x18");
        let later_ready_rendered = output
            .windows(b"BTC/USDT".len())
            .filter(|window| *window == b"BTC/USDT")
            .count()
            >= 2;
        drop(output);
        if pending_rendered && let Some(sender) = self.pending_observed.acquire().take() {
            sender.send(()).map_err(|_| {
                std::io::Error::other("layout input closed before observing pending frame")
            })?;
            let resize_queued = self
                .pending_resize_queued
                .acquire()
                .take()
                .expect("pending resize acknowledgement is installed");
            resize_queued
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| {
                    std::io::Error::other(format!(
                        "layout input did not queue the first adequate resize: {error}"
                    ))
                })?;
        }
        if later_ready_rendered && let Some(sender) = self.later_ready_observed.acquire().take() {
            let _ = sender.send(());
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct LayoutTransitionInput {
    terminal: Arc<TerminalLog>,
    pending_observed: mpsc::Receiver<()>,
    pending_resize_queued: mpsc::SyncSender<()>,
    first_ready_observed: mpsc::Receiver<()>,
    later_ready_observed: mpsc::Receiver<()>,
    phase: u8,
}

impl TerminalInput for LayoutTransitionInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        let signal = match self.phase {
            0 => self.pending_observed.recv_timeout(timeout),
            1 => self.first_ready_observed.recv_timeout(timeout),
            2 => self.later_ready_observed.recv_timeout(timeout),
            3 => {
                self.phase = 4;
                return Ok(TerminalInputPoll::Event(key(
                    KeyCode::Char('q'),
                    KeyModifiers::NONE,
                )));
            }
            _ => return Ok(TerminalInputPoll::Idle),
        };
        match signal {
            Ok(()) if self.phase == 0 => {
                self.terminal.set_size((60, 19));
                self.phase = 1;
                self.pending_resize_queued.send(()).map_err(|_| {
                    std::io::Error::other("pending frame writer closed before resize was queued")
                })?;
                Ok(TerminalInputPoll::Event(Event::Resize(60, 19)))
            }
            Ok(()) if self.phase == 1 => {
                self.terminal.set_size((100, 30));
                self.phase = 2;
                Ok(TerminalInputPoll::Event(Event::Resize(100, 30)))
            }
            Ok(()) => {
                self.phase = 3;
                Ok(TerminalInputPoll::Idle)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(TerminalInputPoll::Idle),
            Err(mpsc::RecvTimeoutError::Disconnected) => Ok(TerminalInputPoll::Closed),
        }
    }
}

struct TerminalLog {
    actions: Mutex<Vec<&'static str>>,
    sizes: Mutex<VecDeque<(u16, u16)>>,
    trace: Arc<Mutex<Vec<&'static str>>>,
    fail_size_at_call: AtomicUsize,
    size_calls: AtomicUsize,
    fail_raw_restore: AtomicBool,
}

impl Default for TerminalLog {
    fn default() -> Self {
        Self::with_sizes([(120, 36)])
    }
}

impl TerminalLog {
    fn with_sizes(sizes: impl IntoIterator<Item = (u16, u16)>) -> Self {
        Self {
            actions: Mutex::new(Vec::new()),
            sizes: Mutex::new(sizes.into_iter().collect()),
            trace: Arc::new(Mutex::new(Vec::new())),
            fail_size_at_call: AtomicUsize::new(usize::MAX),
            size_calls: AtomicUsize::new(0),
            fail_raw_restore: AtomicBool::new(false),
        }
    }
    fn with_trace(mut self, trace: Arc<Mutex<Vec<&'static str>>>) -> Self {
        self.trace = trace;
        self
    }

    fn actions(&self) -> Vec<&'static str> {
        self.actions.acquire().clone()
    }

    fn set_size(&self, size: (u16, u16)) {
        let mut sizes = self.sizes.acquire();
        sizes.clear();
        sizes.push_back(size);
    }
    fn fail_size(&self) {
        self.fail_size_at_call.store(0, Ordering::SeqCst);
    }
    fn fail_size_after_successes(&self, successes: usize) {
        self.fail_size_at_call.store(successes, Ordering::SeqCst);
    }
    fn fail_next_size(&self) {
        self.fail_size_at_call
            .store(self.size_calls.load(Ordering::SeqCst), Ordering::SeqCst);
    }
    fn fail_raw_restore(&self) {
        self.fail_raw_restore.store(true, Ordering::SeqCst);
    }
    fn record(&self, action: &'static str) {
        self.actions.acquire().push(action);
        self.trace.acquire().push(action);
    }
}

impl TerminalDriver for TerminalLog {
    fn enable_raw(&self) -> std::io::Result<()> {
        self.record("raw+");
        Ok(())
    }
    fn enter_alternate(&self) -> std::io::Result<()> {
        self.record("alt+");
        Ok(())
    }
    fn enable_mouse(&self) -> std::io::Result<()> {
        self.record("mouse+");
        Ok(())
    }
    fn hide_cursor(&self) -> std::io::Result<()> {
        self.record("cursor-");
        Ok(())
    }
    fn show_cursor(&self) -> std::io::Result<()> {
        self.record("cursor+");
        Ok(())
    }
    fn disable_mouse(&self) -> std::io::Result<()> {
        self.record("mouse-");
        Ok(())
    }
    fn leave_alternate(&self) -> std::io::Result<()> {
        self.record("alt-");
        Ok(())
    }
    fn disable_raw(&self) -> std::io::Result<()> {
        self.record("raw-");
        if self.fail_raw_restore.load(Ordering::SeqCst) {
            Err(std::io::Error::other("injected raw restore failure"))
        } else {
            Ok(())
        }
    }
    fn size(&self) -> std::io::Result<(u16, u16)> {
        let call = self.size_calls.fetch_add(1, Ordering::SeqCst);
        if call >= self.fail_size_at_call.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("injected terminal size failure"));
        }
        let mut sizes = self.sizes.acquire();
        let size = sizes.front().copied().unwrap_or((120, 36));
        if sizes.len() > 1 {
            sizes.pop_front();
        }
        Ok(size)
    }
}

fn key(code: KeyCode, modifiers: KeyModifiers) -> Event {
    Event::Key(KeyEvent::new(code, modifiers))
}

fn key_input(character: char) -> Box<dyn TerminalInput> {
    Box::new(ScriptedTerminalInput::new([key(
        KeyCode::Char(character),
        KeyModifiers::NONE,
    )]))
}

fn keys_input(events: impl IntoIterator<Item = Event>) -> Box<dyn TerminalInput> {
    Box::new(ScriptedTerminalInput::new(events))
}

fn switch_events(target: &str) -> Vec<Event> {
    let mut events = Vec::new();
    events.push(key(KeyCode::Char(':'), KeyModifiers::NONE));
    for character in target.chars() {
        events.push(key(KeyCode::Char(character), KeyModifiers::NONE));
    }
    events.push(key(KeyCode::Enter, KeyModifiers::NONE));
    events
}

fn switch_then_quit_input(targets: &[&str]) -> Box<dyn TerminalInput> {
    let mut events = Vec::new();
    for target in targets {
        events.extend(
            switch_events(target)
                .into_iter()
                .map(|event| (Duration::ZERO, event)),
        );
    }
    events.push((
        Duration::from_millis(500),
        key(KeyCode::Char('q'), KeyModifiers::NONE),
    ));
    Box::new(ScriptedTerminalInput::with_delays(events))
}

struct SupersedingSwitchInput {
    events: VecDeque<Event>,
    second_switch_offset: usize,
    emitted: usize,
    requests: Arc<Mutex<Vec<HistoryRequest>>>,
    observations: Arc<Mutex<Vec<EpochObservation>>>,
}

impl TerminalInput for SupersedingSwitchInput {
    fn poll(&mut self, _timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        if self.emitted == self.second_switch_offset && self.requests.acquire().len() < 2 {
            return Ok(TerminalInputPoll::Idle);
        }
        if let Some(event) = self.events.pop_front() {
            self.emitted += 1;
            return Ok(TerminalInputPoll::Event(event));
        }
        if self.observations.acquire().iter().any(|observation| {
            matches!(
                observation.snapshot.footer,
                FooterPresentation::Error { .. }
            )
        }) {
            return Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('q'),
                KeyModifiers::NONE,
            )));
        }
        Ok(TerminalInputPoll::Idle)
    }
}

fn run_with_observations(
    provider: Arc<FakeProvider>,
    input: Box<dyn TerminalInput>,
    clock: Arc<dyn Clock>,
    observations: Arc<Mutex<Vec<EpochObservation>>>,
) -> impl Future<Output = Result<ExitCode, AppError>> {
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        input,
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
}

struct FailingResizeInput {
    terminal: Arc<TerminalLog>,
    phase: u8,
}

impl TerminalInput for FailingResizeInput {
    fn poll(&mut self, _timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        if self.phase != 0 {
            return Ok(TerminalInputPoll::Idle);
        }
        if self.terminal.size_calls.load(Ordering::SeqCst) < 2 {
            std::thread::sleep(_timeout);
            return Ok(TerminalInputPoll::Idle);
        }
        self.phase = 1;
        self.terminal.fail_next_size();
        Ok(TerminalInputPoll::Event(Event::Resize(80, 24)))
    }
}

fn delayed_key(delay: Duration, character: char) -> Box<dyn TerminalInput> {
    Box::new(ScriptedTerminalInput::delayed(
        delay,
        key(KeyCode::Char(character), KeyModifiers::NONE),
    ))
}

fn pending_input() -> Box<dyn TerminalInput> {
    Box::new(ScriptedTerminalInput::pending())
}

struct HistoryThenResizeInput {
    pan_events: usize,
    history_started: mpsc::Receiver<()>,
    pending_page_observed: mpsc::Receiver<()>,
    ready_observed: mpsc::Receiver<()>,
    terminal: Arc<TerminalLog>,
    phase: u8,
}

impl TerminalInput for HistoryThenResizeInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        if self.pan_events > 0 {
            self.pan_events -= 1;
            return Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('a'),
                KeyModifiers::NONE,
            )));
        }
        let signal = match self.phase {
            0 => &self.history_started,
            1 => &self.pending_page_observed,
            _ => &self.ready_observed,
        };
        match signal.recv_timeout(timeout) {
            Ok(()) if self.phase == 0 => {
                self.terminal.set_size((59, 18));
                self.phase = 1;
                Ok(TerminalInputPoll::Event(Event::Resize(59, 18)))
            }
            Ok(()) if self.phase == 1 => {
                self.terminal.set_size((100, 30));
                self.phase = 2;
                Ok(TerminalInputPoll::Event(Event::Resize(100, 30)))
            }
            Ok(()) => Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('q'),
                KeyModifiers::NONE,
            ))),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(TerminalInputPoll::Idle),
            Err(mpsc::RecvTimeoutError::Disconnected) => Ok(TerminalInputPoll::Closed),
        }
    }
}
struct HistoryRetainedPageQuitInput {
    pan_events: usize,
    history_started: mpsc::Receiver<()>,
    pending_page_observed: mpsc::Receiver<()>,
    terminal: Arc<TerminalLog>,
    resized: bool,
}

impl TerminalInput for HistoryRetainedPageQuitInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        if self.pan_events > 0 {
            self.pan_events -= 1;
            return Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('a'),
                KeyModifiers::NONE,
            )));
        }
        if !self.resized {
            return match self.history_started.recv_timeout(timeout) {
                Ok(()) => {
                    self.terminal.set_size((59, 18));
                    self.resized = true;
                    Ok(TerminalInputPoll::Event(Event::Resize(59, 18)))
                }
                Err(mpsc::RecvTimeoutError::Timeout) => Ok(TerminalInputPoll::Idle),
                Err(mpsc::RecvTimeoutError::Disconnected) => Ok(TerminalInputPoll::Closed),
            };
        }
        match self.pending_page_observed.recv_timeout(timeout) {
            Ok(()) => Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('q'),
                KeyModifiers::NONE,
            ))),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(TerminalInputPoll::Idle),
            Err(mpsc::RecvTimeoutError::Disconnected) => Ok(TerminalInputPoll::Closed),
        }
    }
}
struct PanThenPendingInput {
    pan_events: usize,
}

impl TerminalInput for PanThenPendingInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        if self.pan_events > 0 {
            self.pan_events -= 1;
            return Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('a'),
                KeyModifiers::NONE,
            )));
        }
        std::thread::sleep(timeout);
        Ok(TerminalInputPoll::Idle)
    }
}

struct PanThenCompletionQuitInput {
    pan_events: usize,
    completed: Arc<AtomicBool>,
    settle_polls: usize,
}

impl TerminalInput for PanThenCompletionQuitInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        if self.pan_events > 0 {
            self.pan_events -= 1;
            return Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('a'),
                KeyModifiers::NONE,
            )));
        }
        if self.completed.load(Ordering::Acquire) {
            if self.settle_polls > 0 {
                self.settle_polls -= 1;
            } else {
                return Ok(TerminalInputPoll::Event(key(
                    KeyCode::Char('q'),
                    KeyModifiers::NONE,
                )));
            }
        }
        std::thread::sleep(timeout);
        Ok(TerminalInputPoll::Idle)
    }
}

struct PendingHistoryQuitInput {
    pan_events: usize,
    history_started: mpsc::Receiver<()>,
    quit_sent: bool,
}

impl TerminalInput for PendingHistoryQuitInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        if self.pan_events > 0 {
            self.pan_events -= 1;
            return Ok(TerminalInputPoll::Event(key(
                KeyCode::Char('a'),
                KeyModifiers::NONE,
            )));
        }
        if self.quit_sent {
            return Ok(TerminalInputPoll::Idle);
        }
        match self.history_started.recv_timeout(timeout) {
            Ok(()) => {
                self.quit_sent = true;
                Ok(TerminalInputPoll::Event(key(
                    KeyCode::Char('q'),
                    KeyModifiers::NONE,
                )))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(TerminalInputPoll::Idle),
            Err(mpsc::RecvTimeoutError::Disconnected) => Ok(TerminalInputPoll::Closed),
        }
    }
}
struct DropObservedInput {
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl TerminalInput for DropObservedInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        std::thread::sleep(timeout);
        Ok(TerminalInputPoll::Idle)
    }
}

impl Drop for DropObservedInput {
    fn drop(&mut self) {
        self.trace.acquire().push("input-owner-dropped");
    }
}

struct AbortObservedInput {
    trace: Arc<Mutex<Vec<&'static str>>>,
    started: Option<mpsc::Sender<()>>,
    alive: Arc<AtomicBool>,
}

impl TerminalInput for AbortObservedInput {
    fn poll(&mut self, timeout: Duration) -> std::io::Result<TerminalInputPoll> {
        self.alive.store(true, Ordering::SeqCst);
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        std::thread::sleep(timeout);
        Ok(TerminalInputPoll::Idle)
    }
}

impl Drop for AbortObservedInput {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
        self.trace.acquire().push("input-owner-dropped");
    }
}

struct FakeProvider {
    provider_id: ProviderId,
    history_pages: Mutex<VecDeque<Result<Vec<Candle>, ProviderError>>>,
    events: Mutex<Option<Vec<Result<MarketEvent, ProviderError>>>>,
    requests: Arc<Mutex<Vec<HistoryRequest>>>,
    history_calls: Arc<Mutex<Vec<(String, Timeframe, HistoryRequest)>>>,
    acknowledgements: Arc<Mutex<Vec<ReconcileAck>>>,
    clock: Arc<dyn Clock>,
    gate_tx: Mutex<Option<RateGateSender>>,
    gate: RateGateSnapshot,
    trace: Arc<Mutex<Vec<&'static str>>>,
    event_delay: Option<Duration>,
    initial_event_delay: Duration,
    wait_for_reconcile_ack: bool,
    canonicalize_calls: AtomicUsize,
    open_live_calls: AtomicUsize,
    rate_gate_calls: AtomicUsize,
    older_history_delay: Option<Duration>,
    older_history_completed: Option<Arc<AtomicBool>>,
    older_history_started: Option<mpsc::Sender<()>>,
    older_history_release: Option<Arc<AtomicBool>>,
    panic_older_history: bool,
    hung: bool,
    close_stream: bool,
    complete_producer: bool,
    capabilities: fccli::provider::ProviderCapabilities,
    switch_capabilities: Option<fccli::provider::ProviderCapabilities>,
    hold_switch_history: bool,
}

impl FakeProvider {
    fn new(
        initial: Vec<Candle>,
        events: Vec<Result<MarketEvent, ProviderError>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (gate_tx, gate) = rate_gate_channel(RateGateState::Open);
        Self {
            provider_id: ProviderId::new("binance").unwrap(),
            history_pages: Mutex::new(VecDeque::from([Ok(initial)])),
            events: Mutex::new(Some(events)),
            requests: Arc::new(Mutex::new(Vec::new())),
            history_calls: Arc::new(Mutex::new(Vec::new())),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
            clock,
            gate_tx: Mutex::new(Some(gate_tx)),
            gate,
            trace: Arc::new(Mutex::new(Vec::new())),
            event_delay: None,
            wait_for_reconcile_ack: true,
            initial_event_delay: Duration::ZERO,
            canonicalize_calls: AtomicUsize::new(0),
            open_live_calls: AtomicUsize::new(0),
            rate_gate_calls: AtomicUsize::new(0),
            older_history_delay: None,
            older_history_completed: None,
            older_history_started: None,
            older_history_release: None,
            panic_older_history: false,
            hung: false,
            close_stream: false,
            complete_producer: false,
            capabilities: fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot, Market::Perpetual],
                timeframes: &Timeframe::ALL,
                history_page_limit: 1000,
            },
            switch_capabilities: None,
            hold_switch_history: false,
        }
    }

    fn with_capabilities(mut self, capabilities: fccli::provider::ProviderCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    fn with_provider_id(mut self, provider: &str) -> Self {
        self.provider_id = ProviderId::new(provider).expect("test provider id");
        self
    }
    fn with_switch_capabilities(
        mut self,
        capabilities: fccli::provider::ProviderCapabilities,
    ) -> Self {
        self.switch_capabilities = Some(capabilities);
        self
    }
    fn holding_switch_history(mut self) -> Self {
        self.hold_switch_history = true;
        self
    }
    fn hung(mut self) -> Self {
        self.hung = true;
        self
    }
    fn closes_successfully(mut self) -> Self {
        self.close_stream = true;
        self.complete_producer = true;
        self
    }
    fn closes_stream_only(mut self) -> Self {
        self.close_stream = true;
        self
    }
    fn with_event_delay(mut self, delay: Duration) -> Self {
        self.event_delay = Some(delay);
        self
    }
    fn without_reconcile_ack_wait(mut self) -> Self {
        self.wait_for_reconcile_ack = false;
        self
    }
    fn with_initial_event_delay(mut self, delay: Duration) -> Self {
        self.initial_event_delay = delay;
        self
    }
    fn with_older_history_delay(mut self, delay: Duration) -> Self {
        self.older_history_delay = Some(delay);
        self
    }
    fn control_older_history(
        mut self,
        started: mpsc::Sender<()>,
        release: Arc<AtomicBool>,
        completed: Arc<AtomicBool>,
    ) -> Self {
        self.older_history_started = Some(started);
        self.older_history_release = Some(release);
        self.older_history_completed = Some(completed);
        self
    }
    fn with_panicking_older_history(mut self) -> Self {
        self.panic_older_history = true;
        self
    }
    fn with_history_pages(
        mut self,
        pages: impl IntoIterator<Item = Result<Vec<Candle>, ProviderError>>,
    ) -> Self {
        self.history_pages = Mutex::new(pages.into_iter().collect());
        self
    }
    fn with_trace(mut self, trace: Arc<Mutex<Vec<&'static str>>>) -> Self {
        self.trace = trace;
        self
    }
    fn publish_gate(&self, state: RateGateState) {
        self.gate_tx
            .acquire()
            .as_ref()
            .expect("rate gate sender remains open")
            .publish(state)
            .expect("rate gate observer remains open");
    }

    fn close_gate(&self) {
        self.gate_tx.acquire().take();
    }
}

impl MarketDataProvider for FakeProvider {
    fn id(&self) -> ProviderId {
        self.provider_id.clone()
    }
    fn capabilities(&self) -> fccli::provider::ProviderCapabilities {
        if self.requests.acquire().is_empty() {
            self.capabilities
        } else {
            self.switch_capabilities.unwrap_or(self.capabilities)
        }
    }

    fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        self.canonicalize_calls.fetch_add(1, Ordering::SeqCst);
        canonicalize_instrument(spec)
            .map_err(|_| ProviderError::Configuration("canonicalization failed"))
    }
    fn history<'a>(
        &'a self,
        instrument: &'a Instrument,
        timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>> {
        let hold_switch_history = self.hold_switch_history
            && request.kind() == HistoryRequestKind::Latest
            && !self.requests.acquire().is_empty();
        let delay = (request.kind() == HistoryRequestKind::Older)
            .then_some(self.older_history_delay)
            .flatten();
        let completion = (request.kind() == HistoryRequestKind::Older)
            .then(|| self.older_history_completed.clone())
            .flatten();
        let started = (request.kind() == HistoryRequestKind::Older)
            .then(|| self.older_history_started.clone())
            .flatten();
        let release = (request.kind() == HistoryRequestKind::Older)
            .then(|| self.older_history_release.clone())
            .flatten();
        let panic_older = request.kind() == HistoryRequestKind::Older && self.panic_older_history;
        let cleanup_trace = release
            .as_ref()
            .map(|_| HistoryCleanupTrace(Arc::clone(&self.trace)));
        self.history_calls.acquire().push((
            instrument.provider_symbol().to_owned(),
            timeframe,
            request,
        ));
        self.requests.acquire().push(request);
        self.trace.acquire().push("history");
        let result = self
            .history_pages
            .acquire()
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(async move {
            let _cleanup_trace = cleanup_trace;
            if hold_switch_history {
                cancellation.cancelled().await;
                return Err(ProviderError::Transport {
                    context: fccli::error::ErrorContext::operation(
                        fccli::error::ErrorOperation::History,
                    ),
                    cause: fccli::error::SanitizedCause::Cancelled,
                });
            }
            if panic_older {
                panic!("injected older-history task panic must be sanitized");
            }
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if let Some(started) = started {
                let _ = started.send(());
            }
            if let Some(release) = release {
                while !release.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            }
            if let Some(completion) = completion {
                completion.store(true, Ordering::Release);
            }
            result
        })
    }
    fn open_live<'a>(&'a self, request: LiveRequest) -> ProviderFuture<'a, LiveFeed> {
        self.open_live_calls.fetch_add(1, Ordering::SeqCst);
        let events = self.events.acquire().take().unwrap_or_default();
        let clock = Arc::clone(&self.clock);
        let acknowledgements = Arc::clone(&self.acknowledgements);
        let event_delay = self.event_delay;
        let wait_for_reconcile_ack = self.wait_for_reconcile_ack;
        let initial_event_delay = self.initial_event_delay;
        let hung = self.hung;
        let close_stream = self.close_stream;
        let complete_producer = self.complete_producer;
        let trace = Arc::clone(&self.trace);
        Box::pin(async move {
            let cancellation = request.cancellation.clone();
            let mut ack_rx = request.reconcile_ack_rx;
            let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
            let source = stream::unfold(event_rx, |mut receiver| async move {
                receiver.recv().await.map(|event| (event, receiver))
            });
            let events_stream: MarketEventStream = Box::pin(source);
            let mut event_tx = Some(event_tx);
            let sequential_events = if event_delay.is_none() {
                for event in events {
                    if let Ok(MarketEvent::ReconcileBatch {
                        generation,
                        revision,
                        target_open_time,
                        ..
                    }) = &event
                    {
                        ack_rx
                            .register_expectation(fccli::provider::ReconcileExpectation {
                                generation: *generation,
                                revision: *revision,
                                target_open_time: *target_open_time,
                            })
                            .map_err(|_| {
                                ProviderError::Invariant("expectation registration failed")
                            })?;
                    }
                    event_tx
                        .as_ref()
                        .expect("event sender remains available")
                        .send(event)
                        .map_err(|_| {
                            ProviderError::Invariant("event receiver closed during setup")
                        })?;
                }
                Vec::new()
            } else {
                events
            };
            let cleanup = ProducerCleanupTrace(Arc::clone(&trace));
            Ok(LiveFeed::spawn(
                events_stream,
                request.cancellation,
                clock,
                async move {
                    let _cleanup = cleanup;
                    tokio::time::sleep(initial_event_delay).await;
                    for event in sequential_events {
                        let expectation = if let Ok(MarketEvent::ReconcileBatch {
                            generation,
                            revision,
                            target_open_time,
                            ..
                        }) = &event
                        {
                            let expectation = fccli::provider::ReconcileExpectation {
                                generation: *generation,
                                revision: *revision,
                                target_open_time: *target_open_time,
                            };
                            ack_rx.register_expectation(expectation).map_err(|_| {
                                ProviderError::Invariant("expectation registration failed")
                            })?;
                            Some(expectation)
                        } else {
                            None
                        };
                        if event_tx
                            .as_ref()
                            .expect("event sender remains available")
                            .send(event)
                            .is_err()
                        {
                            return Ok(());
                        }
                        if wait_for_reconcile_ack && let Some(expectation) = expectation {
                            let ack = tokio::select! {
                                result = ack_rx.changed() => result,
                                () = cancellation.cancelled() => return Ok(()),
                            }
                            .map_err(|_| {
                                ProviderError::Invariant("reconcile acknowledgement channel closed")
                            })?;
                            if ack.generation != expectation.generation
                                || ack.revision != expectation.revision
                                || ack.through < expectation.target_open_time
                            {
                                return Err(ProviderError::Invariant(
                                    "unexpected reconciliation acknowledgement",
                                ));
                            }
                            acknowledgements.acquire().push(ack);
                        }
                        tokio::task::yield_now().await;
                        tokio::time::sleep(event_delay.expect("sequential events require a delay"))
                            .await;
                        while let Some(Ok(ack)) = ack_rx.changed().now_or_never() {
                            acknowledgements.acquire().push(ack);
                        }
                    }
                    if close_stream {
                        event_tx.take();
                        if complete_producer {
                            return Ok(());
                        }
                    }
                    if hung {
                        futures_util::future::pending::<()>().await;
                    } else if !complete_producer {
                        loop {
                            tokio::select! {
                                biased;
                                ack = ack_rx.changed() => {
                                    match ack {
                                        Ok(ack) => acknowledgements.acquire().push(ack),
                                        Err(_) => break,
                                    }
                                }
                                () = cancellation.cancelled() => break,
                            }
                        }
                    }
                    trace.acquire().push("producer-finished");
                    Ok(())
                },
            ))
        })
    }
    fn rate_gate(&self) -> RateGateSnapshot {
        self.rate_gate_calls.fetch_add(1, Ordering::SeqCst);
        self.gate.clone()
    }
}

fn candle(open_time: i64, close_time: i64) -> Candle {
    Candle::from_rest(open_time, close_time, 10.0, 11.0, 9.0, 10.5, 5.0).unwrap()
}

fn ws_candle(open_time: i64, close: f64) -> Candle {
    Candle::from_ws(
        open_time,
        open_time + 59_999,
        10.0,
        11.0,
        9.0,
        close,
        5.0,
        false,
    )
    .unwrap()
}

fn viewport_signature(
    observation: &EpochObservation,
) -> Option<(usize, i64, bool, PriceRange, bool)> {
    let InteractiveChartState::Ready(ChartViewState::Data(viewport)) =
        &observation.snapshot.chart_state
    else {
        return None;
    };
    Some((
        viewport.visible_count(),
        viewport.right_open_time(),
        viewport.follows_live(),
        viewport.y_range(),
        viewport.coordinate_hover().is_some(),
    ))
}

fn dependencies(
    provider: Arc<FakeProvider>,
    input: Box<dyn TerminalInput>,
    terminal: Arc<TerminalLog>,
    output: SharedWriter,
    clock: Arc<dyn Clock>,
) -> RunDependencies {
    let provider: Arc<dyn MarketDataProvider> = provider;
    RunDependencies {
        providers: ProviderRegistry::new([provider]).expect("unique fake provider"),
        clock,
        terminal,

        input,
        stdout: Box::new(output),
        stderr: Box::new(SharedWriter::default()),
        stdin_is_tty: true,
        stdout_is_tty: true,
        render_policy: fccli::chart::RenderPolicy::StyleFree,
        epoch_observer: None,
    }
}
#[tokio::test]
async fn initial_app_caps_desired_500_and_rejects_before_network_io() {
    for (maximum, expected) in [(1_000, 500), (7, 7)] {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
        let provider = Arc::new(
            FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock))
                .with_capabilities(fccli::provider::ProviderCapabilities {
                    markets: &[Market::Spot, Market::Perpetual],
                    timeframes: &Timeframe::ALL,
                    history_page_limit: maximum,
                }),
        );
        let terminal = Arc::new(TerminalLog::default());
        let result = run_with_dependencies(
            ["fccli", "btc", "1m", "--interactive"],
            dependencies(
                Arc::clone(&provider),
                delayed_key(Duration::from_millis(10), 'q'),
                terminal,
                SharedWriter::default(),
                clock,
            ),
        )
        .await;
        assert_eq!(result, Ok(ExitCode::SUCCESS));
        assert_eq!(provider.requests.acquire()[0].limit(), expected);
    }

    for (arguments, capabilities) in [
        (
            ["fccli", "btc.p", "1m", "--interactive"],
            fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot],
                timeframes: &Timeframe::ALL,
                history_page_limit: 500,
            },
        ),
        (
            ["fccli", "btc", "1s", "--interactive"],
            fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot, Market::Perpetual],
                timeframes: &[Timeframe::Minute1],
                history_page_limit: 500,
            },
        ),
        (
            ["fccli", "btc", "6h", "--interactive"],
            fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot, Market::Perpetual],
                timeframes: &[Timeframe::Minute1],
                history_page_limit: 500,
            },
        ),
        (
            ["fccli", "btc", "1m", "--interactive"],
            fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot, Market::Perpetual],
                timeframes: &Timeframe::ALL,
                history_page_limit: 0,
            },
        ),
    ] {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
        let provider = Arc::new(
            FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock))
                .with_capabilities(capabilities),
        );
        let error = run_with_dependencies(
            arguments,
            dependencies(
                Arc::clone(&provider),
                Box::new(ScriptedTerminalInput::new([])),
                Arc::new(TerminalLog::default()),
                SharedWriter::default(),
                clock,
            ),
        )
        .await
        .expect_err("unsupported capability must fail initial startup");
        assert!(error.to_string().contains("provider"));
        assert!(provider.requests.acquire().is_empty());
        assert_eq!(provider.canonicalize_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn switch_caps_desired_limit_and_rejects_capabilities_before_provider_io() {
    for (maximum, expected) in [(1_000, 500), (7, 7)] {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
        let initial = (0..100)
            .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
            .collect::<Vec<_>>();
        let switched = (0..100)
            .map(|index| candle(index * 3_600_000, index * 3_600_000 + 3_599_999))
            .collect::<Vec<_>>();
        let provider = Arc::new(
            FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock))
                .with_history_pages([Ok(initial), Ok(switched)])
                .with_switch_capabilities(fccli::provider::ProviderCapabilities {
                    markets: &[Market::Spot, Market::Perpetual],
                    timeframes: &Timeframe::ALL,
                    history_page_limit: maximum,
                }),
        );
        let result = run_with_observations(
            Arc::clone(&provider),
            switch_then_quit_input(&["eth 1h"]),
            clock,
            Arc::new(Mutex::new(Vec::new())),
        )
        .await;
        assert_eq!(result, Ok(ExitCode::SUCCESS));
        let calls = provider.history_calls.acquire();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0],
            (
                "BTCUSDT".to_owned(),
                Timeframe::Minute1,
                HistoryRequest::latest(500).unwrap()
            )
        );
        assert_eq!(
            calls[1],
            (
                "ETHUSDT".to_owned(),
                Timeframe::Hour1,
                HistoryRequest::latest(expected).unwrap()
            )
        );
        assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 2);
    }

    for (target, capabilities, expected_message) in [
        (
            "eth.p 1m",
            fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot],
                timeframes: &Timeframe::ALL,
                history_page_limit: 500,
            },
            "provider does not support market",
        ),
        (
            "eth 1h",
            fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot, Market::Perpetual],
                timeframes: &[Timeframe::Minute1],
                history_page_limit: 500,
            },
            "provider does not support timeframe",
        ),
        (
            "eth 1m",
            fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot, Market::Perpetual],
                timeframes: &Timeframe::ALL,
                history_page_limit: 0,
            },
            "history page limit must be non-zero",
        ),
    ] {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
        let initial = (0..100)
            .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
            .collect::<Vec<_>>();
        let provider = Arc::new(
            FakeProvider::new(initial, vec![], Arc::clone(&clock))
                .with_switch_capabilities(capabilities),
        );
        let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
        let result = run_with_observations(
            Arc::clone(&provider),
            switch_then_quit_input(&[target]),
            clock,
            Arc::clone(&observations),
        )
        .await;
        assert_eq!(result, Ok(ExitCode::SUCCESS));
        assert_eq!(
            provider.history_calls.acquire().as_slice(),
            &[(
                "BTCUSDT".to_owned(),
                Timeframe::Minute1,
                HistoryRequest::latest(500).unwrap(),
            )],
            "rejected switch must not perform history I/O",
        );
        assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 1);
        assert!(observations.acquire().iter().any(|observation| {
            matches!(&observation.snapshot.footer, FooterPresentation::Error { message } if message.contains(expected_message))
        }));
    }
}

#[tokio::test]
async fn rejected_switch_supersedes_pending_preparation_before_preflight() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = (0..100)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let provider = Arc::new(
        FakeProvider::new(initial, vec![], Arc::clone(&clock))
            .with_switch_capabilities(fccli::provider::ProviderCapabilities {
                markets: &[Market::Spot, Market::Perpetual],
                timeframes: &[Timeframe::Minute1],
                history_page_limit: 500,
            })
            .holding_switch_history(),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let first = switch_events("eth 1m");
    let second_switch_offset = first.len();
    let mut events = VecDeque::from(first);
    events.extend(switch_events("sol 1h"));
    let input = Box::new(SupersedingSwitchInput {
        events,
        second_switch_offset,
        emitted: 0,
        requests: Arc::clone(&provider.requests),
        observations: Arc::clone(&observations),
    });

    let result = run_with_observations(
        Arc::clone(&provider),
        input,
        clock,
        Arc::clone(&observations),
    )
    .await;
    assert_eq!(result, Ok(ExitCode::SUCCESS));
    assert_eq!(
        provider.history_calls.acquire().as_slice(),
        &[
            (
                "BTCUSDT".to_owned(),
                Timeframe::Minute1,
                HistoryRequest::latest(500).unwrap(),
            ),
            (
                "ETHUSDT".to_owned(),
                Timeframe::Minute1,
                HistoryRequest::latest(500).unwrap(),
            ),
        ],
        "A may start and be cancelled, while rejected B performs no history I/O",
    );
    assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 1);
    let observations = observations.acquire();
    assert!(observations.iter().any(|observation| {
        matches!(&observation.snapshot.footer, FooterPresentation::Error { message } if message.contains("provider does not support timeframe"))
    }));
    assert!(
        observations
            .iter()
            .all(|observation| { observation.snapshot.instrument.provider_symbol() == "BTCUSDT" })
    );
}

#[tokio::test]
async fn direct_interactive_dispatch_fetches_before_terminal_and_restores_on_q() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(
        FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock))
            .with_trace(Arc::clone(&trace)),
    );
    let terminal = Arc::new(TerminalLog::default().with_trace(Arc::clone(&trace)));
    let output = SharedWriter::default();
    let mut deps = dependencies(
        provider.clone(),
        delayed_key(Duration::from_millis(10), 'q'),
        terminal.clone(),
        output.clone(),
        clock,
    );
    deps.render_policy = fccli::chart::RenderPolicy::Color;
    let result = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps).await;
    assert_eq!(trace.acquire().first(), Some(&"history"));
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
    assert_eq!(provider.requests.acquire()[0].limit(), 500);
    assert_eq!(
        terminal.actions(),
        [
            "raw+", "alt+", "mouse+", "cursor-", "cursor+", "mouse-", "alt-", "raw-"
        ]
    );
    assert!(
        !output.0.acquire().is_empty(),
        "interactive mode must render a frame"
    );
}

#[tokio::test]
async fn interactive_non_tty_error_is_stable_actionable_and_side_effect_free() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    let mut deps = dependencies(
        provider.clone(),
        Box::new(ScriptedTerminalInput::new([])),
        terminal.clone(),
        SharedWriter::default(),
        clock,
    );
    deps.stdin_is_tty = false;
    let error = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
        .await
        .expect_err("interactive mode must reject a non-TTY endpoint");

    assert_eq!(error, AppError::Terminal(TerminalError::TtyRequired));
    assert_eq!(
        error.to_string(),
        "interactive mode requires both stdin and stdout to be terminals; run without --interactive to render a snapshot"
    );
    assert!(provider.requests.acquire().is_empty());
    assert_eq!(provider.canonicalize_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 0);
    assert!(terminal.actions().is_empty());
}

#[tokio::test]
async fn interactive_initial_size_failure_is_typed_and_restores_before_join() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(
        FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock))
            .with_trace(Arc::clone(&trace)),
    );
    let terminal = Arc::new(TerminalLog::default().with_trace(Arc::clone(&trace)));
    terminal.fail_size();
    let output = SharedWriter::default();
    let result = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            pending_input(),
            terminal.clone(),
            output.clone(),
            clock,
        ),
    )
    .await;
    assert_eq!(
        result,
        Err(AppError::Terminal(TerminalError::Setup {
            operation: "query terminal size",
            cause: fccli::error::SanitizedCause::Io,
        }))
    );
    assert!(output.0.acquire().is_empty(), "failed size must not render");
    assert_eq!(
        terminal.actions(),
        [
            "raw+", "alt+", "mouse+", "cursor-", "cursor+", "mouse-", "alt-", "raw-"
        ]
    );
    let trace = trace.acquire();
    let restored = trace.iter().position(|entry| *entry == "raw-").unwrap();
    let joined = trace
        .iter()
        .position(|entry| *entry == "producer-finished")
        .unwrap();
    assert!(
        restored < joined,
        "terminal restore must finish before producer join"
    );
}
#[tokio::test]
async fn initial_size_failure_at_clock_max_keeps_only_primary_and_live_secondaries_when_history_is_idle()
 {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(u64::MAX)));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(
        FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock))
            .with_trace(Arc::clone(&trace)),
    );
    let terminal = Arc::new(TerminalLog::default().with_trace(Arc::clone(&trace)));
    terminal.fail_size();

    let result = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            pending_input(),
            terminal,
            SharedWriter::default(),
            clock,
        ),
    )
    .await;

    assert_eq!(
        result,
        Err(AppError::PrimaryWithSecondary {
            primary: Box::new(AppError::PrimaryWithSecondary {
                primary: Box::new(AppError::Terminal(TerminalError::Setup {
                    operation: "query terminal size",
                    cause: fccli::error::SanitizedCause::Io,
                })),
                secondary: Box::new(AppError::Invariant("live producer join deadline overflow",)),
            }),
            secondary: Box::new(AppError::Invariant("live producer join deadline elapsed",)),
        })
    );
    assert!(
        !format!("{result:?}").contains("history join deadline"),
        "an idle history coordinator must not compute or report a join deadline"
    );
    let trace = trace.acquire();
    let restored = trace.iter().position(|entry| *entry == "raw-").unwrap();
    let cleaned = trace
        .iter()
        .position(|entry| *entry == "producer-cleaned")
        .unwrap();
    assert!(
        restored < cleaned,
        "terminal restore must precede feed cleanup"
    );
}

#[tokio::test]
async fn periodic_size_failure_enters_terminal_failure_bucket_before_render() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    terminal.fail_size_after_successes(1);
    let output = SharedWriter::default();
    let result = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            pending_input(),
            terminal.clone(),
            output.clone(),
            clock,
        ),
    )
    .await;
    assert_eq!(
        result,
        Err(AppError::Terminal(TerminalError::Setup {
            operation: "query terminal size",
            cause: fccli::error::SanitizedCause::Io,
        }))
    );
    assert!(
        output.0.acquire().is_empty(),
        "failed sample must precede render"
    );
    assert!(
        terminal
            .actions()
            .ends_with(&["cursor+", "mouse-", "alt-", "raw-"])
    );
}

#[tokio::test]
async fn explicit_resize_size_failure_uses_terminal_cleanup_path() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    let input = Box::new(FailingResizeInput {
        terminal: terminal.clone(),
        phase: 0,
    });
    let output = SharedWriter::default();
    let result = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(provider, input, terminal.clone(), output.clone(), clock),
    )
    .await;
    assert_eq!(
        result,
        Err(AppError::Terminal(TerminalError::Setup {
            operation: "query terminal size",
            cause: fccli::error::SanitizedCause::Io,
        }))
    );
    assert_eq!(
        terminal.actions(),
        [
            "raw+", "alt+", "mouse+", "cursor-", "cursor+", "mouse-", "alt-", "raw-"
        ]
    );
    let rendered = String::from_utf8(output.0.acquire().clone()).expect("UTF-8 frame");
    assert!(
        !rendered.contains("80x24"),
        "failed resize must never be rendered"
    );
}

#[tokio::test]
async fn renderer_cache_rebuilds_only_for_changed_live_mutations() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let generation = GapGeneration(1);
    let first_replacement = ws_candle(0, 10.6);
    let duplicate = first_replacement.clone();
    let second_replacement = ws_candle(0, 10.7);
    let provider = Arc::new(
        FakeProvider::new(
            vec![candle(0, 59_999)],
            vec![
                Ok(MarketEvent::Candle {
                    generation,
                    candle: first_replacement,
                }),
                Ok(MarketEvent::Candle {
                    generation,
                    candle: duplicate,
                }),
                Ok(MarketEvent::Candle {
                    generation,
                    candle: second_replacement,
                }),
            ],
            Arc::clone(&clock),
        )
        .with_event_delay(Duration::from_millis(10)),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(100), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );

    let observations = observations.acquire();
    let revisions_for_close = |close: f64| {
        observations
            .iter()
            .filter(|observation| {
                observation
                    .snapshot
                    .candles
                    .first()
                    .is_some_and(|candle| candle.close() == close)
            })
            .map(|observation| observation.renderer_candle_revision)
            .collect::<Vec<_>>()
    };
    let duplicate_revisions = revisions_for_close(10.6);
    assert!(duplicate_revisions.len() >= 2);
    assert!(duplicate_revisions.iter().all(|revision| *revision == 1));
    let replacement_revisions = revisions_for_close(10.7);
    assert!(!replacement_revisions.is_empty());
    assert!(replacement_revisions.iter().all(|revision| *revision == 2));
}

#[tokio::test]
async fn direct_snapshot_dispatch_uses_the_real_injected_runner() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    let output = SharedWriter::default();
    let mut deps = dependencies(
        provider.clone(),
        Box::new(ScriptedTerminalInput::new([])),
        terminal.clone(),
        output.clone(),
        clock,
    );
    deps.stdin_is_tty = false;
    deps.stdout_is_tty = false;
    deps.render_policy = fccli::chart::RenderPolicy::Color;
    let result = run_with_dependencies(["fccli", "btc", "1m"], deps).await;
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
    assert_eq!(provider.requests.acquire()[0].limit(), 500);
    assert!(terminal.actions().is_empty());
    let rendered = String::from_utf8_lossy(&output.0.acquire()).into_owned();
    assert!(rendered.contains("SNAPSHOT"));
    assert_eq!(rendered.lines().count(), 36);
    assert!(!rendered.contains('\x1b'));
    assert!(rendered.contains('█'));
}

#[tokio::test]
async fn direct_tty_snapshot_uses_injected_60_by_18_size_and_stops_before_provider_work() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::with_sizes([(60, 18)]));
    let output = SharedWriter::default();
    let result = run_with_dependencies(
        ["fccli", "btc", "1m"],
        dependencies(
            provider.clone(),
            Box::new(ScriptedTerminalInput::new([])),
            terminal.clone(),
            output.clone(),
            clock,
        ),
    )
    .await;

    assert_eq!(
        result,
        Err(AppError::Render(RenderError::InsufficientSpace))
    );
    assert!(provider.requests.acquire().is_empty());
    assert_eq!(provider.canonicalize_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider.rate_gate_calls.load(Ordering::SeqCst), 0);
    assert!(terminal.actions().is_empty());
    let rendered = String::from_utf8(output.0.acquire().clone()).expect("UTF-8 pending frame");
    let rows: Vec<_> = rendered.split("\r\n").collect();
    assert_eq!(rows.len(), 17);
    assert!(rows.iter().all(|row| row.chars().count() == 60));
    assert!(rendered.contains("60x18"));
    assert!(rendered.contains("60x17"));
}

#[tokio::test]
async fn direct_tty_snapshot_uses_injected_60_by_19_size_for_one_provider_request() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::with_sizes([(60, 19)]));
    let output = SharedWriter::default();
    let result = run_with_dependencies(
        ["fccli", "btc", "1m"],
        dependencies(
            provider.clone(),
            Box::new(ScriptedTerminalInput::new([])),
            terminal.clone(),
            output.clone(),
            clock,
        ),
    )
    .await;

    assert_eq!(result, Ok(ExitCode::SUCCESS));
    let requests = provider.requests.acquire();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0], HistoryRequest::latest(500).unwrap());
    drop(requests);
    assert_eq!(provider.canonicalize_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider.rate_gate_calls.load(Ordering::SeqCst), 1);
    assert!(terminal.actions().is_empty());
    let rendered = String::from_utf8(output.0.acquire().clone()).expect("UTF-8 snapshot frame");
    let rows: Vec<_> = rendered.split("\r\n").collect();
    assert_eq!(rows.len(), 18);
    assert!(rows.iter().all(|row| row.chars().count() == 60));
    assert!(rendered.contains("SNAPSHOT"));
    assert!(!rendered.contains('\x1b'));
    assert!(rendered.contains('█'));
}

#[tokio::test]
async fn direct_tty_snapshot_size_failure_is_typed_and_does_not_fallback() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    terminal.fail_size();
    let output = SharedWriter::default();
    let result = run_with_dependencies(
        ["fccli", "btc", "1m"],
        dependencies(
            provider.clone(),
            Box::new(ScriptedTerminalInput::new([])),
            terminal,
            output.clone(),
            clock,
        ),
    )
    .await;

    assert_eq!(
        result,
        Err(AppError::Terminal(TerminalError::Setup {
            operation: "query terminal size",
            cause: fccli::error::SanitizedCause::Io,
        }))
    );
    assert!(provider.requests.acquire().is_empty());
    assert_eq!(provider.canonicalize_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider.rate_gate_calls.load(Ordering::SeqCst), 0);
    assert!(output.0.acquire().is_empty());
}

async fn scenario_advancing_month1_reconciliation_target_and_acks() {
    let manual = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let clock: Arc<dyn Clock> = manual.clone();
    let generation = GapGeneration(7);
    let month_open = |index: i32| {
        let absolute_month = 2000_i32 * 12 + 7 + index;
        let year = absolute_month.div_euclid(12);
        let month = time::Month::try_from(u8::try_from(absolute_month.rem_euclid(12) + 1).unwrap())
            .unwrap();
        time::Date::from_calendar_date(year, month, 1)
            .unwrap()
            .midnight()
            .assume_utc()
            .unix_timestamp_nanos()
            .checked_div(1_000_000)
            .and_then(|value| i64::try_from(value).ok())
            .unwrap()
    };
    let initial = (0..100)
        .map(|index| candle(month_open(index), month_open(index + 1) - 1))
        .collect::<Vec<_>>();
    let first_target = month_open(100);
    let latest_open = month_open(101);
    let events = vec![
        Ok(MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::Connecting,
        }),
        Ok(MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::Backoff,
        }),
        Ok(MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::GapSync,
        }),
        Ok(MarketEvent::ReconcileBatch {
            generation,
            revision: ReplayRevision(1),
            target_open_time: first_target,
            candles: vec![candle(first_target, latest_open - 1)],
        }),
        Ok(MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::GapSync,
        }),
        Ok(MarketEvent::ReconcileBatch {
            generation,
            revision: ReplayRevision(2),
            target_open_time: latest_open,
            candles: vec![
                Candle::from_ws(
                    latest_open,
                    month_open(102) - 1,
                    11.0,
                    13.0,
                    10.0,
                    12.0,
                    9.0,
                    true,
                )
                .unwrap(),
            ],
        }),
        Ok(MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::Connected,
        }),
    ];
    let provider = Arc::new(
        FakeProvider::new(initial, events, Arc::clone(&clock))
            .with_initial_event_delay(Duration::from_millis(40))
            .with_event_delay(Duration::from_millis(10)),
    );
    let output = SharedWriter::default();
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut interactions = Vec::new();
    interactions.extend((0..40).map(|_| key(KeyCode::Char('a'), KeyModifiers::NONE)));
    interactions.push(key(KeyCode::Char('v'), KeyModifiers::NONE));
    interactions.push(Event::Mouse(MouseEvent {
        kind: MouseEventKind::Moved,
        column: 19,
        row: 9,
        modifiers: KeyModifiers::NONE,
    }));
    let input = ScriptedTerminalInput::with_delays(
        interactions
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                (
                    if index == 0 {
                        Duration::from_millis(5)
                    } else {
                        Duration::ZERO
                    },
                    event,
                )
            })
            .chain([
                (
                    Duration::from_millis(300),
                    Event::Mouse(MouseEvent {
                        kind: MouseEventKind::Moved,
                        column: 0,
                        row: 0,
                        modifiers: KeyModifiers::NONE,
                    }),
                ),
                (Duration::ZERO, key(KeyCode::End, KeyModifiers::NONE)),
                (
                    Duration::from_millis(100),
                    key(KeyCode::Char('q'), KeyModifiers::NONE),
                ),
            ]),
    );
    let mut deps = dependencies(
        provider.clone(),
        Box::new(input),
        Arc::new(TerminalLog::with_sizes([(100, 30)])),
        output.clone(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    let gate_provider = Arc::clone(&provider);
    let gate_manual = Arc::clone(&manual);
    let gate_transition = async move {
        tokio::time::sleep(Duration::from_millis(90)).await;
        gate_provider.publish_gate(RateGateState::TimedUntil(MonoInstant::from_nanos(9)));
        tokio::time::sleep(Duration::from_millis(40)).await;
        gate_manual.advance_to(MonoInstant::from_nanos(9)).unwrap();
        gate_provider.publish_gate(RateGateState::Open);
    };
    let (result, ()) = tokio::join!(
        run_with_dependencies(["fccli", "btc", "1M", "--interactive"], deps),
        gate_transition,
    );
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
    assert_eq!(
        provider.acknowledgements.acquire().as_slice(),
        &[
            ReconcileAck {
                generation,
                revision: ReplayRevision(1),
                through: first_target
            },
            ReconcileAck {
                generation,
                revision: ReplayRevision(2),
                through: latest_open
            },
        ]
    );
    let observations = observations.acquire();
    let inspected_index = observations
        .iter()
        .position(|observation| {
            viewport_signature(observation).is_some_and(|state| !state.2 && state.4)
        })
        .unwrap_or_else(|| {
            panic!(
                "missing inspected state: {:?}",
                observations
                    .iter()
                    .filter_map(viewport_signature)
                    .collect::<Vec<_>>()
            )
        });
    let inspected = viewport_signature(&observations[inspected_index]).unwrap();
    assert!(
        observations[inspected_index + 1..]
            .iter()
            .any(|observation| {
                observation.snapshot.display_status == fccli::chart::DisplayStatus::Backoff
            })
    );
    assert!(
        observations[inspected_index + 1..]
            .iter()
            .any(|observation| {
                observation.snapshot.rate_gate
                    == RateGateState::TimedUntil(MonoInstant::from_nanos(9))
            })
    );
    for state in observations[inspected_index + 1..]
        .iter()
        .filter_map(viewport_signature)
        .take_while(|state| state.4)
    {
        assert_eq!(state.0, inspected.0);
        assert_eq!(state.1, inspected.1);
        assert!(!state.2);
        assert_eq!(state.3, inspected.3);
    }
    let final_state = observations
        .iter()
        .rev()
        .find(|observation| observation.stop.is_none())
        .expect("running state immediately before quit");
    assert_eq!(final_state.active_generation, Some(generation));
    assert_eq!(final_state.snapshot.candles.len(), 102);
    assert_eq!(
        final_state.snapshot.display_status,
        fccli::chart::DisplayStatus::Connected
    );

    let rendered = String::from_utf8_lossy(&output.0.acquire()).into_owned();
    let final_frame = rendered
        .rsplit("\u{1b}[H")
        .next()
        .expect("cursor-home frame");
    assert!(final_frame.contains("LIVE"), "{final_frame:?}");
    assert!(final_frame.contains("BTC/USDT"), "{final_frame:?}");
    for field in ["O:11", "H:13", "L:10", "C:12", "V:9"] {
        assert!(
            final_frame.contains(field),
            "missing {field:?} in {final_frame:?}"
        );
    }
}

#[tokio::test]
async fn reconciliation_target_and_state_persistence() {
    scenario_advancing_month1_reconciliation_target_and_acks().await;
    scenario_history_coordinator_preserves_inspected_view().await;
    scenario_gap_sync_client_4xx_is_terminal_without_retry().await;
}

#[tokio::test]
async fn history_task_panic_is_a_one_shot_fatal_app_failure() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = (0..30)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let provider = Arc::new(
        FakeProvider::new(initial, vec![], Arc::clone(&clock)).with_panicking_older_history(),
    );
    let terminal = Arc::new(TerminalLog::with_sizes([(100, 30)]));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider.clone(),
        Box::new(PanThenPendingInput { pan_events: 40 }),
        terminal.clone(),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    let error = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
        .await
        .expect_err("history task panic must terminate the App");
    assert!(error.to_string().contains("history task failed"));
    assert!(
        !error
            .to_string()
            .contains("injected older-history task panic")
    );
    assert_eq!(provider.requests.acquire().len(), 2);
    assert_eq!(terminal.actions().last(), Some(&"raw-"));
    assert_eq!(
        observations
            .acquire()
            .iter()
            .filter(|observation| observation.stop == Some(EpochStop::TerminalFailure))
            .count(),
        1,
        "the fatal history progress is admitted exactly once"
    );
}

#[tokio::test]
async fn closed_rate_gate_preserves_primary_through_restore_and_producer_cleanup() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = (0..30)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let trace = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(
        FakeProvider::new(initial, vec![], Arc::clone(&clock)).with_trace(Arc::clone(&trace)),
    );
    provider.close_gate();
    let terminal = Arc::new(TerminalLog::with_sizes([(100, 30)]).with_trace(Arc::clone(&trace)));
    terminal.fail_raw_restore();

    let error = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            Box::new(PanThenPendingInput { pan_events: 40 }),
            terminal.clone(),
            SharedWriter::default(),
            clock,
        ),
    )
    .await
    .expect_err("closed rate-gate observer must terminate the App");
    let rendered = error.to_string();
    assert!(rendered.starts_with("provider invariant failed: provider rate gate closed"));
    assert!(rendered.contains("secondary failure: terminal restoration failed"));

    let trace = trace.acquire();
    let restore = trace
        .iter()
        .position(|entry| *entry == "raw-")
        .expect("raw restoration was attempted");
    let producer_cleanup = trace
        .iter()
        .position(|entry| *entry == "producer-cleaned")
        .expect("live producer was cancelled and joined");
    assert!(
        restore < producer_cleanup,
        "terminal restoration must precede bounded producer join cleanup"
    );
    assert!(
        terminal
            .actions()
            .iter()
            .filter(|action| **action == "raw-")
            .count()
            >= 2,
        "Drop retries the failed inverse after explicit aggregated cleanup"
    );
}

#[tokio::test]
async fn history_client_4xx_disables_backfill_but_app_keeps_running() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = (0..30)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let completed = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(true));
    let (started_tx, _started_rx) = mpsc::channel();
    let client_error = ProviderError::ClientStatus {
        context: ErrorContext::operation(ErrorOperation::History),
        status: 403,
        code: None,
        message: None,
    };
    let provider = Arc::new(
        FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock))
            .with_history_pages([Ok(initial), Err(client_error)])
            .control_older_history(started_tx, release, Arc::clone(&completed)),
    );
    let terminal = Arc::new(TerminalLog::with_sizes([(100, 30)]));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider.clone(),
        Box::new(PanThenCompletionQuitInput {
            pan_events: 40,
            completed,
            settle_polls: 5,
        }),
        terminal.clone(),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    let result = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps).await;
    assert_eq!(result, Ok(ExitCode::SUCCESS));
    assert_eq!(
        provider.requests.acquire().len(),
        2,
        "client 4xx disables all later history requests"
    );
    assert!(
        observations
            .acquire()
            .iter()
            .all(|observation| observation.stop != Some(EpochStop::TerminalFailure)),
        "client 4xx history disablement remains nonfatal"
    );
    assert_eq!(terminal.actions().last(), Some(&"raw-"));
}

#[tokio::test]
async fn pending_history_request_renders_backfilling_then_refreshes_after_page_ready() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(
        FakeProvider::new(vec![candle(60_000, 119_999)], vec![], Arc::clone(&clock))
            .with_history_pages([Ok(vec![candle(60_000, 119_999)]), Ok(Vec::new())])
            .with_older_history_delay(Duration::from_millis(30)),
    );
    let terminal = Arc::new(TerminalLog::default());
    let output = SharedWriter::default();
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(80), 'q'),
        terminal,
        output.clone(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation);
    }));

    let result = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps).await;
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);

    let observations = observations.acquire();
    let backfilling = observations
        .iter()
        .position(|observation| observation.snapshot.display_status == DisplayStatus::Backfilling)
        .expect("RequestStarted is reduced and observed as BACKFILLING");
    assert!(
        observations[backfilling + 1..]
            .iter()
            .any(|observation| observation.snapshot.display_status == DisplayStatus::Connecting),
        "PageReady refreshes the display after the history request completes"
    );
    drop(observations);

    let rendered = String::from_utf8_lossy(&output.0.acquire()).into_owned();
    assert!(rendered.contains("BACKFILLING"));
}

#[test]
fn binary_help_version_and_argument_errors_exit_before_valid_dispatch() {
    Command::cargo_bin("fccli")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
    Command::cargo_bin("fccli")
        .unwrap()
        .arg("--version")
        .assert()
        .success();
    let output = Command::cargo_bin("fccli")
        .unwrap()
        .args(["btc", "not-an-interval"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported timeframe"));
    assert!(stderr.contains("use one of: s, m, h, d, w, M, 1s, 1m"));
    assert!(!stderr.contains("not-an-interval"));
    assert!(!stderr.contains("valid modes require direct injected dependencies"));
}

#[test]
fn binary_semantic_instrument_error_exits_before_valid_dispatch() {
    let output = Command::cargo_bin("fccli")
        .unwrap()
        .args(["USDT", "1m"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("instrument must include a base asset"));
    assert!(!stderr.contains("valid modes require direct injected dependencies"));
}

#[test]
fn binary_known_provider_passes_main_semantic_gate() {
    let output = Command::cargo_bin("fccli")
        .unwrap()
        .args(["okx:btc", "1m"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("valid modes require direct injected dependencies"),
        "{stderr}"
    );
    assert!(!stderr.contains("has no default-quote rule"), "{stderr}");
    assert!(!stderr.contains("use lowercase `binance`"), "{stderr}");
}

#[tokio::test]
async fn direct_dispatch_known_unregistered_provider_fails_at_registry() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        Vec::new(),
        Vec::new(),
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    let mut deps = dependencies(
        provider.clone(),
        Box::new(ScriptedTerminalInput::new([])),
        terminal.clone(),
        SharedWriter::default(),
        Arc::clone(&clock),
    );
    deps.stdin_is_tty = true;
    deps.stdout_is_tty = true;

    let error = run_with_dependencies(["fccli", "bybit:btc", "1m"], deps)
        .await
        .expect_err("known unimplemented providers fail at registry");

    assert!(
        error
            .to_string()
            .contains("unsupported market-data provider"),
        "{error}"
    );
    assert_eq!(provider.canonicalize_calls.load(Ordering::SeqCst), 0);
    assert!(provider.requests.acquire().is_empty());
    assert!(terminal.actions().is_empty());
}

#[tokio::test]
async fn direct_dispatch_semantic_validation_precedes_provider_and_terminal_use() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        Vec::new(),
        Vec::new(),
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    let mut deps = dependencies(
        provider.clone(),
        Box::new(ScriptedTerminalInput::new([])),
        terminal.clone(),
        SharedWriter::default(),
        Arc::clone(&clock),
    );
    deps.stdin_is_tty = true;
    deps.stdout_is_tty = true;

    let error = run_with_dependencies(["fccli", "USDT", "1m", "--interactive"], deps)
        .await
        .expect_err("quote-only instrument must fail semantic validation");

    assert!(error.to_string().contains("instrument is not valid"));
    assert!(!error.to_string().contains("USDT"));
    assert!(provider.requests.acquire().is_empty());
    assert!(terminal.actions().is_empty());
}

#[tokio::test]
async fn esc_ctrl_c_and_ctrl_d_share_the_graceful_shutdown_path() {
    for event in [
        key(KeyCode::Esc, KeyModifiers::NONE),
        key(KeyCode::Char('c'), KeyModifiers::CONTROL),
        key(KeyCode::Char('d'), KeyModifiers::CONTROL),
    ] {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
        let provider = Arc::new(FakeProvider::new(
            vec![candle(0, 59_999)],
            vec![],
            Arc::clone(&clock),
        ));
        let terminal = Arc::new(TerminalLog::default());
        let result = run_with_dependencies(
            ["fccli", "btc", "1m", "--interactive"],
            dependencies(
                provider,
                Box::new(ScriptedTerminalInput::new([event])),
                terminal.clone(),
                SharedWriter::default(),
                clock,
            ),
        )
        .await;
        assert_eq!(result.unwrap(), ExitCode::SUCCESS);
        assert!(
            terminal
                .actions()
                .ends_with(&["cursor+", "mouse-", "alt-", "raw-"])
        );
    }
}

#[tokio::test]
async fn hung_producer_is_join_bounded_after_terminal_restoration() {
    let manual = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let clock: Arc<dyn Clock> = manual.clone();
    let provider =
        Arc::new(FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock)).hung());
    let terminal = Arc::new(TerminalLog::default());
    let run_terminal = terminal.clone();
    let run = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            key_input('q'),
            run_terminal,
            SharedWriter::default(),
            clock,
        ),
    );
    let advance = async {
        for _ in 0..10_000 {
            if terminal.actions().last() == Some(&"raw-") {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(terminal.actions().last(), Some(&"raw-"));
        manual.advance_by(Duration::from_secs(5)).unwrap();
    };
    let (result, ()) = tokio::join!(run, advance);
    let error = result.expect_err("hung producer must time out");
    assert!(error.to_string().contains("join deadline elapsed"));
}

#[tokio::test]
async fn pending_history_join_aborts_after_restoration_at_manual_deadline() {
    let manual = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let clock: Arc<dyn Clock> = manual.clone();
    let trace = Arc::new(Mutex::new(Vec::new()));
    let (history_started_tx, history_started_rx) = mpsc::channel();
    let initial = (0..20)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let provider = Arc::new(
        FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock))
            .with_history_pages([Ok(initial), Ok(Vec::new())])
            .control_older_history(
                history_started_tx,
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
            )
            .with_trace(Arc::clone(&trace)),
    );
    let terminal = Arc::new(TerminalLog::with_sizes([(100, 30)]).with_trace(Arc::clone(&trace)));
    let run = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            Box::new(PendingHistoryQuitInput {
                pan_events: 40,
                history_started: history_started_rx,
                quit_sent: false,
            }),
            terminal,
            SharedWriter::default(),
            clock,
        ),
    );
    let advance = async {
        for _ in 0..10_000 {
            let (restored, producer_joined) = {
                let trace = trace.acquire();
                (trace.contains(&"raw-"), trace.contains(&"producer-cleaned"))
            };
            if restored && producer_joined {
                break;
            }
            tokio::task::yield_now().await;
        }
        let (restored_before_deadline, producer_joined_before_deadline) = {
            let before_deadline = trace.acquire();
            (
                before_deadline.contains(&"raw-"),
                before_deadline.contains(&"producer-cleaned"),
            )
        };
        assert!(restored_before_deadline);
        assert!(producer_joined_before_deadline);
        assert!(!trace.acquire().contains(&"history-cleaned"));
        for _ in 0..20 {
            manual.advance_by(Duration::from_secs(1)).unwrap();
            tokio::task::yield_now().await;
            if trace.acquire().contains(&"history-cleaned") {
                break;
            }
        }
    };
    let (result, ()) = tokio::join!(run, advance);
    let error = result.expect_err("pending history task must hit its bounded join deadline");
    assert!(error.to_string().contains("history join deadline elapsed"));
    assert!(
        !error.to_string().contains("history join deadline overflow"),
        "the pending-history path must use its real bounded deadline"
    );
    let trace = trace.acquire();
    let restored = trace.iter().position(|entry| *entry == "raw-").unwrap();
    let history_cleaned = trace
        .iter()
        .position(|entry| *entry == "history-cleaned")
        .unwrap();
    assert!(restored < history_cleaned);
}

#[tokio::test]
async fn producer_success_and_stream_completion_exit_cleanly_once() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(
        FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock))
            .closes_successfully(),
    );
    let terminal = Arc::new(TerminalLog::default());
    let result = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            pending_input(),
            terminal.clone(),
            SharedWriter::default(),
            clock,
        ),
    )
    .await;
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
    assert_eq!(
        terminal
            .actions()
            .iter()
            .filter(|&&action| action == "raw-")
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_input_owner_is_joined_before_terminal_restore() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![Ok(MarketEvent::TerminalError(
            ProviderError::InvalidBanExpiry,
        ))],
        Arc::clone(&clock),
    ));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let terminal = Arc::new(TerminalLog::default().with_trace(Arc::clone(&trace)));
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        run_with_dependencies(
            ["fccli", "btc", "1m", "--interactive"],
            dependencies(
                provider,
                Box::new(DropObservedInput {
                    trace: Arc::clone(&trace),
                }),
                terminal,
                SharedWriter::default(),
                clock,
            ),
        ),
    )
    .await
    .expect("bounded input owner must stop and join")
    .expect_err("provider error must remain primary");
    assert_eq!(result, AppError::Provider(ProviderError::InvalidBanExpiry));
    let trace = trace.acquire();
    let input_drop = trace
        .iter()
        .position(|entry| *entry == "input-owner-dropped")
        .unwrap();
    let restore = trace.iter().position(|entry| *entry == "cursor+").unwrap();
    assert!(
        input_drop < restore,
        "input owner must be joined before restore begins"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_interactive_future_joins_input_before_terminal_restore() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        Vec::new(),
        Arc::clone(&clock),
    ));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let alive = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let terminal = Arc::new(TerminalLog::default().with_trace(Arc::clone(&trace)));
    let mut run = Box::pin(run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            Box::new(AbortObservedInput {
                trace: Arc::clone(&trace),
                started: Some(started_tx),
                alive: Arc::clone(&alive),
            }),
            terminal,
            SharedWriter::default(),
            clock,
        ),
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                result = &mut run => panic!("interactive run completed before cancellation: {result:?}"),
                () = tokio::task::yield_now() => {
                    if started_rx.try_recv().is_ok() {
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("idle input owner must start");

    drop(run);

    assert!(
        !alive.load(Ordering::SeqCst),
        "input owner thread survived future drop"
    );
    let trace = trace.acquire();
    let input_drop = trace
        .iter()
        .position(|entry| *entry == "input-owner-dropped")
        .expect("input owner must be dropped by its task guard");
    let restore = trace
        .iter()
        .position(|entry| *entry == "cursor+")
        .expect("terminal session must restore during unwind");
    assert!(
        input_drop < restore,
        "input owner must join before terminal restoration"
    );
}

#[tokio::test]
async fn stream_and_input_channel_closure_are_failures_not_success_completion() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(
        FakeProvider::new(vec![candle(0, 59_999)], vec![], Arc::clone(&clock)).closes_stream_only(),
    );
    let terminal = Arc::new(TerminalLog::default());
    let error = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            pending_input(),
            terminal.clone(),
            SharedWriter::default(),
            Arc::clone(&clock),
        ),
    )
    .await
    .expect_err("closed event stream without producer completion must fail");
    assert!(error.to_string().contains("market event stream closed"));
    assert_eq!(terminal.actions().last(), Some(&"raw-"));

    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    let error = run_with_dependencies(
        ["fccli", "btc", "1m", "--interactive"],
        dependencies(
            provider,
            Box::new(ScriptedTerminalInput::new([])),
            terminal.clone(),
            SharedWriter::default(),
            clock,
        ),
    )
    .await
    .expect_err("closed terminal input channel must fail");
    assert!(error.to_string().contains("terminal input channel closed"));
    assert_eq!(terminal.actions().last(), Some(&"raw-"));
}

#[tokio::test]
async fn gap_sync_client_4xx_is_consumed_as_terminal_without_retry() {
    scenario_gap_sync_client_4xx_is_terminal_without_retry().await;
}

async fn scenario_gap_sync_client_4xx_is_terminal_without_retry() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let terminal_error = ProviderError::ClientStatus {
        context: ErrorContext::operation(ErrorOperation::History),
        status: 403,
        code: None,
        message: None,
    };
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![Ok(MarketEvent::TerminalError(terminal_error))],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::default());
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider.clone(),
        pending_input(),
        terminal.clone(),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    let error = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
        .await
        .expect_err("terminal 4xx must shut down the App");
    assert!(error.to_string().contains("403"));
    assert_eq!(
        provider.requests.acquire().len(),
        1,
        "terminal 4xx must not start a history retry"
    );
    assert_eq!(terminal.actions().last(), Some(&"raw-"));
    let observations = observations.acquire();
    assert!(
        observations
            .iter()
            .any(|observation| observation.stop == Some(EpochStop::LiveTerminalError))
    );
    assert!(
        observations.iter().all(|observation| {
            observation.snapshot.display_status != DisplayStatus::Backoff
                && observation.active_generation.is_none()
        }),
        "terminal non-special 4xx must not emit Backoff or start a later generation"
    );
}

#[tokio::test]
async fn layout_pending_transitions_on_first_ready_and_later_resize_keeps_chart_data() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        (0..20)
            .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
            .collect(),
        vec![],
        Arc::clone(&clock),
    ));
    let terminal = Arc::new(TerminalLog::with_sizes([(59, 18)]));
    let output = SharedWriter::default();
    let (pending_observed_tx, pending_observed_rx) = mpsc::channel();
    let (pending_resize_queued_tx, pending_resize_queued_rx) = mpsc::sync_channel(0);
    let (first_ready_tx, first_ready_rx) = mpsc::channel();
    let (later_ready_tx, later_ready_rx) = mpsc::channel();
    let input = LayoutTransitionInput {
        terminal: Arc::clone(&terminal),
        pending_observed: pending_observed_rx,
        pending_resize_queued: pending_resize_queued_tx,
        first_ready_observed: first_ready_rx,
        later_ready_observed: later_ready_rx,
        phase: 0,
    };
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let first_ready_sent = Arc::new(AtomicBool::new(false));
    let observer_ready_sent = Arc::clone(&first_ready_sent);
    let mut deps = dependencies(provider, Box::new(input), terminal, output.clone(), clock);
    deps.stdout = Box::new(PendingFrameWriter::new(
        output.clone(),
        pending_observed_tx,
        pending_resize_queued_rx,
        later_ready_tx,
    ));
    deps.epoch_observer = Some(Arc::new(move |observation| {
        if viewport_signature(&observation).is_some()
            && !observer_ready_sent.swap(true, Ordering::SeqCst)
        {
            let _ = first_ready_tx.send(());
        }
        captured.acquire().push(observation)
    }));
    let result = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps).await;
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
    let rendered = String::from_utf8_lossy(&output.0.acquire()).into_owned();
    let resize_at = rendered
        .find("Resize terminal to at least 60x18")
        .expect("pending resize frame");
    let first_chart_at = rendered
        .find("BTC/USDT")
        .expect("first adequate ready frame");
    assert_eq!(
        rendered
            .matches("Resize terminal to at least 60x18")
            .count(),
        1,
        "the initial pending layout must render once before first adequate initialization"
    );
    assert!(resize_at < first_chart_at);
    let ready = observations
        .acquire()
        .iter()
        .filter_map(viewport_signature)
        .collect::<Vec<_>>();
    assert!(
        ready.len() >= 2,
        "first adequate and later adequate sizes must both pass through the actual reducer"
    );
    assert_eq!(
        ready[0].1, ready[1].1,
        "later resize must preserve the initialized right-edge anchor instead of reinitializing"
    );
    assert!(
        rendered[first_chart_at..].matches("BTC/USDT").count() >= 2,
        "later adequate resize must retain chart state and data"
    );
}

#[tokio::test]
async fn completed_history_page_survives_layout_pending_until_first_ready_resize() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = (20..50)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let older = (0..20)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let expected_right_open_time = initial.last().unwrap().open_time();
    let (history_started_tx, history_started_rx) = mpsc::channel();
    let (pending_page_tx, pending_page_rx) = mpsc::channel();
    let (ready_tx, ready_rx) = mpsc::channel();
    let release_history = Arc::new(AtomicBool::new(false));
    let history_completed = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(
        FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock))
            .with_history_pages([Ok(initial), Ok(older)])
            .control_older_history(
                history_started_tx,
                Arc::clone(&release_history),
                Arc::clone(&history_completed),
            ),
    );
    let terminal = Arc::new(TerminalLog::with_sizes([(100, 30)]));
    let output = SharedWriter::default();
    let input = HistoryThenResizeInput {
        pan_events: 40,
        history_started: history_started_rx,
        pending_page_observed: pending_page_rx,
        ready_observed: ready_rx,
        terminal: Arc::clone(&terminal),
        phase: 0,
    };
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let release = Arc::clone(&release_history);
    let completed = Arc::clone(&history_completed);
    let pending_page_tx = Mutex::new(Some(pending_page_tx));
    let ready_tx = Mutex::new(Some(ready_tx));
    let mut deps = dependencies(provider.clone(), Box::new(input), terminal, output, clock);
    deps.epoch_observer = Some(Arc::new(move |observation| {
        let is_pending_backfill = observation.layout_pending
            && observation.snapshot.display_status == DisplayStatus::Backfilling;
        if is_pending_backfill {
            release.store(true, Ordering::Release);
        }
        if is_pending_backfill
            && completed.load(Ordering::Acquire)
            && observation.source_counts[3] > 0
            && let Some(sender) = pending_page_tx.acquire().take()
        {
            let _ = sender.send(());
        }
        if observation.snapshot.candles.len() == 50
            && viewport_signature(&observation).is_some()
            && let Some(sender) = ready_tx.acquire().take()
        {
            let _ = sender.send(());
        }
        captured.acquire().push(observation);
    }));

    let result = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps).await;
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
    assert_eq!(
        provider.requests.acquire().len(),
        2,
        "history page merges once"
    );

    let observations = observations.acquire();
    let retained_page = observations
        .iter()
        .find(|observation| {
            observation.layout_pending
                && observation.snapshot.display_status == DisplayStatus::Backfilling
                && observation.source_counts[3] > 0
        })
        .expect("PageReady remains retained while the terminal is undersized");
    assert_eq!(retained_page.snapshot.candles.len(), 30);
    let ready = observations
        .iter()
        .find(|observation| {
            !observation.layout_pending
                && observation.snapshot.candles.len() == 50
                && viewport_signature(observation).is_some()
        })
        .expect("first adequate resize applies the retained page");
    assert_eq!(
        ready.snapshot.candles.len(),
        50,
        "older page is merged exactly once"
    );
    assert_eq!(
        viewport_signature(ready).unwrap().1,
        expected_right_open_time,
        "applying the retained page preserves the inspected right anchor"
    );
    assert_ne!(ready.snapshot.display_status, DisplayStatus::Backfilling);
}

#[tokio::test]
async fn retained_history_page_at_max_clock_clears_without_join_deadline_overflow() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(u64::MAX)));
    let initial = (20..50)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let older = (0..20)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let (history_started_tx, history_started_rx) = mpsc::channel();
    let (pending_page_tx, pending_page_rx) = mpsc::channel();
    let release_history = Arc::new(AtomicBool::new(false));
    let history_completed = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(
        FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock))
            .with_history_pages([Ok(initial), Ok(older)])
            .control_older_history(
                history_started_tx,
                Arc::clone(&release_history),
                Arc::clone(&history_completed),
            ),
    );
    let terminal = Arc::new(TerminalLog::with_sizes([(100, 30)]));
    let input = HistoryRetainedPageQuitInput {
        pan_events: 40,
        history_started: history_started_rx,
        pending_page_observed: pending_page_rx,
        terminal: Arc::clone(&terminal),
        resized: false,
    };
    let release = Arc::clone(&release_history);
    let completed = Arc::clone(&history_completed);
    let pending_page_tx = Mutex::new(Some(pending_page_tx));
    let mut deps = dependencies(
        provider.clone(),
        Box::new(input),
        terminal,
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        let retained = observation.layout_pending
            && observation.snapshot.display_status == DisplayStatus::Backfilling;
        if retained {
            release.store(true, Ordering::Release);
        }
        if retained
            && completed.load(Ordering::Acquire)
            && observation.source_counts[3] > 0
            && let Some(sender) = pending_page_tx.acquire().take()
        {
            let _ = sender.send(());
        }
    }));

    let result = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps).await;
    let rendered = format!("{result:?}");
    assert!(
        !rendered.contains("history join deadline overflow"),
        "a retained completed page has no owned task needing a checked deadline: {rendered}"
    );
    assert!(
        !rendered.contains("history join deadline elapsed"),
        "clearing a retained completed page must not manufacture a timeout: {rendered}"
    );
    assert_eq!(provider.requests.acquire().len(), 2);
}

#[tokio::test]
async fn history_coordinator_runs_through_app_without_losing_inspected_view() {
    scenario_history_coordinator_preserves_inspected_view().await;
}

async fn scenario_history_coordinator_preserves_inspected_view() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = (20..50)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let initial_oldest = initial[0].open_time();
    let older = (0..20)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let generation = GapGeneration(2);
    let provider = Arc::new(
        FakeProvider::new(
            initial.clone(),
            vec![
                Ok(MarketEvent::Status {
                    generation: Some(generation),
                    status: ConnectionStatus::Connecting,
                }),
                Ok(MarketEvent::Status {
                    generation: Some(generation),
                    status: ConnectionStatus::Backoff,
                }),
                Ok(MarketEvent::RecoverableError {
                    generation: Some(generation),
                    error: ProviderError::QueueSaturated,
                    rate_gate_deadline: Some(MonoInstant::from_nanos(30)),
                }),
                Ok(MarketEvent::Status {
                    generation: Some(generation),
                    status: ConnectionStatus::GapSync,
                }),
                Ok(MarketEvent::Status {
                    generation: Some(generation),
                    status: ConnectionStatus::Connected,
                }),
            ],
            Arc::clone(&clock),
        )
        .with_history_pages([Ok(initial), Ok(older)])
        .with_event_delay(Duration::from_millis(15)),
    );
    let terminal = Arc::new(TerminalLog::with_sizes([(100, 30)]));
    let output = SharedWriter::default();
    let mut events = Vec::new();
    events.extend((0..40).map(|_| key(KeyCode::Char('a'), KeyModifiers::NONE)));
    events.push(key(KeyCode::Char('v'), KeyModifiers::NONE));
    events.push(Event::Mouse(MouseEvent {
        kind: MouseEventKind::Moved,
        column: 19,
        row: 9,
        modifiers: KeyModifiers::NONE,
    }));
    let input = ScriptedTerminalInput::with_delays(
        events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                (
                    if index == 0 {
                        Duration::from_millis(30)
                    } else {
                        Duration::ZERO
                    },
                    event,
                )
            })
            .chain([(
                Duration::from_millis(120),
                key(KeyCode::Char('q'), KeyModifiers::NONE),
            )]),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider.clone(),
        Box::new(input),
        terminal,
        output.clone(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    let result = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps).await;
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
    let requests = provider.requests.acquire();
    assert_eq!(requests[0], HistoryRequest::latest(500).unwrap());
    let older_request = requests
        .iter()
        .skip(1)
        .find(|request| request.kind() == HistoryRequestKind::Older)
        .expect("App must drive the actual HistoryCoordinator after the inspected viewport reaches the threshold");
    assert_eq!(older_request.limit(), 1000);
    assert_eq!(older_request.end_time(), Some(initial_oldest - 1));
    drop(requests);
    let rendered = String::from_utf8_lossy(&output.0.acquire()).into_owned();
    assert!(rendered.contains("RECONNECTING"));
    assert!(rendered.contains("RATE LIMITED UNTIL"));
    assert!(
        rendered.matches("BTC/USDT").count() >= 2,
        "status, gate, and history activity must preserve the accepted series"
    );
    assert!(
        observations
            .acquire()
            .iter()
            .any(|observation| observation.snapshot.candles.len() == 50),
        "the actual older page must be committed through HistoryCoordinator"
    );
    let initial_cache_observations = observations
        .acquire()
        .iter()
        .filter(|observation| observation.snapshot.candles.len() == 30)
        .map(|observation| observation.renderer_candle_revision)
        .collect::<Vec<_>>();
    assert!(
        initial_cache_observations.len() > 1,
        "status and interaction redraws must expose multiple snapshots before data changes"
    );
    assert!(
        initial_cache_observations
            .iter()
            .all(|revision| *revision == 0),
        "non-data redraws and a duplicate history page must reuse the initial candle Arc"
    );
    let mutated_cache_revisions = observations
        .acquire()
        .iter()
        .filter(|observation| observation.snapshot.candles.len() == 50)
        .map(|observation| observation.renderer_candle_revision)
        .collect::<Vec<_>>();
    assert!(!mutated_cache_revisions.is_empty());
    assert!(
        mutated_cache_revisions
            .iter()
            .all(|revision| *revision == 1),
        "the accepted older-page mutation must rebuild once and later redraws must reuse it"
    );
    let observations = observations.acquire();
    assert!(
        observations
            .iter()
            .any(|observation| observation.source_counts[1] == 32),
        "real input intake must enforce the per-source epoch quota"
    );
    let inspected = observations
        .iter()
        .filter_map(viewport_signature)
        .find(|signature| !signature.2 && signature.4)
        .expect("paused manual-Y inspected viewport observation");
    assert!(
        observations
            .iter()
            .filter_map(viewport_signature)
            .skip_while(|signature| *signature != inspected)
            .skip(1)
            .all(|signature| signature.0 == inspected.0
                && signature.1 == inspected.1
                && !signature.2
                && signature.3 == inspected.3),
        "status, reconnect, rate-limit, and history epochs must preserve paused/manual-Y inspected viewport anchors"
    );
}

#[tokio::test]
async fn reducer_enforces_live_quota_and_filters_every_stale_generation_shape_before_reduction() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let current = GapGeneration(9);
    let stale = GapGeneration(8);
    let mut events = vec![
        Ok(MarketEvent::RecoverableError {
            generation: Some(stale),
            error: ProviderError::QueueSaturated,
            rate_gate_deadline: Some(MonoInstant::from_nanos(3)),
        }),
        Ok(MarketEvent::Status {
            generation: Some(stale),
            status: ConnectionStatus::Backoff,
        }),
        Ok(MarketEvent::ReconcileBatch {
            generation: stale,
            revision: ReplayRevision(1),
            target_open_time: 60_000,
            candles: vec![Candle::from_ws(60_000, 119_999, 1.0, 2.0, 0.5, 1.5, 1.0, true).unwrap()],
        }),
        Ok(MarketEvent::Candle {
            generation: stale,
            candle: Candle::from_ws(120_000, 179_999, 1.0, 2.0, 0.5, 1.5, 1.0, true).unwrap(),
        }),
        Ok(MarketEvent::Status {
            generation: Some(current),
            status: ConnectionStatus::Connected,
        }),
        Ok(MarketEvent::ReconcileBatch {
            generation: current,
            revision: ReplayRevision(2),
            target_open_time: 60_000,
            candles: vec![
                Candle::from_ws(60_000, 119_999, 10.0, 12.0, 9.0, 11.0, 2.0, true).unwrap(),
            ],
        }),
    ];
    events.extend((2..67).map(|index| {
        Ok(MarketEvent::Candle {
            generation: current,
            candle: Candle::from_ws(
                index * 60_000,
                index * 60_000 + 59_999,
                10.0,
                12.0,
                9.0,
                11.0,
                2.0,
                true,
            )
            .unwrap(),
        })
    }));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        events,
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(80), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );

    let observations = observations.acquire();
    assert!(
        observations
            .iter()
            .all(|observation| observation.source_counts[2] <= 32)
    );
    assert!(
        observations
            .iter()
            .any(|observation| observation.source_counts[2] == 32)
    );
    assert!(
        observations
            .iter()
            .any(|observation| observation.stale_generation_events == 4)
    );
    let final_snapshot = observations
        .iter()
        .rev()
        .find(|observation| observation.stop.is_none())
        .unwrap();
    assert_eq!(final_snapshot.active_generation, Some(current));
    assert_eq!(
        final_snapshot.snapshot.display_status,
        fccli::chart::DisplayStatus::Connected
    );
    assert_eq!(final_snapshot.snapshot.candles.len(), 67);
    assert_eq!(
        final_snapshot.snapshot.candles[1].open(),
        10.0,
        "stale reconciliation must not mutate the series"
    );
}

#[tokio::test]
async fn generationless_saturation_pair_is_applied_in_canonical_order() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![
            Ok(MarketEvent::RecoverableError {
                generation: None,
                error: ProviderError::QueueSaturated,
                rate_gate_deadline: None,
            }),
            Ok(MarketEvent::Status {
                generation: None,
                status: ConnectionStatus::Backoff,
            }),
        ],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(30), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );
    let observations = observations.acquire();
    let saturated = observations
        .iter()
        .position(|observation| {
            matches!(
                observation.snapshot.status_detail,
                Some(ProviderError::QueueSaturated)
            )
        })
        .expect("generationless saturation error is reduced");
    assert!(observations[saturated..].iter().any(|observation| {
        observation.snapshot.display_status == fccli::chart::DisplayStatus::Backoff
    }));
    assert!(
        observations
            .iter()
            .all(|observation| observation.active_generation.is_none())
    );
}

#[tokio::test]
async fn generationless_emergency_invalidates_same_epoch_candidate_before_any_tagged_reduction() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let candidate = GapGeneration(12);
    let provider = Arc::new(
        FakeProvider::new(
            vec![candle(0, 59_999)],
            vec![
                Ok(MarketEvent::Status {
                    generation: Some(candidate),
                    status: ConnectionStatus::Connected,
                }),
                Ok(MarketEvent::ReconcileBatch {
                    generation: candidate,
                    revision: ReplayRevision(1),
                    target_open_time: 60_000,
                    candles: vec![
                        Candle::from_ws(60_000, 119_999, 10.0, 12.0, 9.0, 11.0, 2.0, true).unwrap(),
                    ],
                }),
                Ok(MarketEvent::Candle {
                    generation: candidate,
                    candle: Candle::from_ws(120_000, 179_999, 20.0, 22.0, 19.0, 21.0, 3.0, true)
                        .unwrap(),
                }),
                Ok(MarketEvent::RecoverableError {
                    generation: None,
                    error: ProviderError::QueueSaturated,
                    rate_gate_deadline: None,
                }),
                Ok(MarketEvent::Status {
                    generation: None,
                    status: ConnectionStatus::Backoff,
                }),
            ],
            Arc::clone(&clock),
        )
        .without_reconcile_ack_wait(),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(30), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );

    let observations = observations.acquire();
    let emergency_epoch = observations
        .iter()
        .find(|observation| observation.source_counts[2] == 5)
        .expect("all no-delay live events are admitted to one epoch");
    assert_eq!(emergency_epoch.active_generation, None);
    assert_eq!(emergency_epoch.invalidated_generation, Some(candidate));
    assert_eq!(emergency_epoch.stale_generation_events, 3);
    assert_eq!(emergency_epoch.snapshot.candles.len(), 1);
    assert_eq!(emergency_epoch.snapshot.candles[0].open_time(), 0);
    assert_eq!(
        emergency_epoch.snapshot.display_status,
        DisplayStatus::Backoff
    );
    assert_eq!(
        emergency_epoch.snapshot.status_detail,
        Some(ProviderError::QueueSaturated)
    );
    assert!(observations.iter().all(|observation| {
        observation.active_generation != Some(candidate)
            && observation.snapshot.display_status != DisplayStatus::Connected
            && observation.snapshot.candles.len() == 1
    }));
}

#[tokio::test]
async fn generationless_emergency_only_invalidates_candidate_before_pair_in_same_epoch() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let invalidated = GapGeneration(5);
    let accepted = GapGeneration(6);
    let provider = Arc::new(
        FakeProvider::new(
            vec![candle(0, 59_999)],
            vec![
                Ok(MarketEvent::Candle {
                    generation: invalidated,
                    candle: Candle::from_ws(60_000, 119_999, 50.0, 52.0, 49.0, 51.0, 2.0, true)
                        .unwrap(),
                }),
                Ok(MarketEvent::RecoverableError {
                    generation: None,
                    error: ProviderError::QueueSaturated,
                    rate_gate_deadline: None,
                }),
                Ok(MarketEvent::Status {
                    generation: None,
                    status: ConnectionStatus::Backoff,
                }),
                Ok(MarketEvent::Candle {
                    generation: accepted,
                    candle: Candle::from_ws(120_000, 179_999, 60.0, 62.0, 59.0, 61.0, 3.0, true)
                        .unwrap(),
                }),
            ],
            Arc::clone(&clock),
        )
        .without_reconcile_ack_wait(),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(30), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );

    let observations = observations.acquire();
    let epoch = observations
        .iter()
        .find(|observation| observation.source_counts[2] == 4)
        .expect("all no-delay live arrivals are reduced in one epoch");
    assert_eq!(epoch.active_generation, Some(accepted));
    assert_eq!(epoch.invalidated_generation, None);
    assert_eq!(epoch.stale_generation_events, 1);
    assert_eq!(epoch.snapshot.candles.len(), 2);
    assert_eq!(epoch.snapshot.candles[0].open_time(), 0);
    assert_eq!(epoch.snapshot.candles[1].open_time(), 120_000);
    assert_eq!(epoch.snapshot.candles[1].open(), 60.0);
    assert!(
        epoch
            .snapshot
            .candles
            .iter()
            .all(|candle| candle.open_time() != 60_000)
    );
}

#[tokio::test]
async fn emergency_pair_discards_delayed_old_generation_until_higher_generation_is_accepted() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let invalidated = GapGeneration(5);
    let next = GapGeneration(6);
    let provider = Arc::new(
        FakeProvider::new(
            vec![candle(0, 59_999)],
            vec![
                Ok(MarketEvent::Status {
                    generation: Some(invalidated),
                    status: ConnectionStatus::Connected,
                }),
                Ok(MarketEvent::RecoverableError {
                    generation: None,
                    error: ProviderError::QueueSaturated,
                    rate_gate_deadline: None,
                }),
                Ok(MarketEvent::Status {
                    generation: None,
                    status: ConnectionStatus::Backoff,
                }),
                Ok(MarketEvent::Status {
                    generation: Some(invalidated),
                    status: ConnectionStatus::Connected,
                }),
                Ok(MarketEvent::RecoverableError {
                    generation: Some(invalidated),
                    error: ProviderError::QueueSaturated,
                    rate_gate_deadline: None,
                }),
                Ok(MarketEvent::ReconcileBatch {
                    generation: invalidated,
                    revision: ReplayRevision(1),
                    target_open_time: 60_000,
                    candles: vec![
                        Candle::from_ws(60_000, 119_999, 90.0, 92.0, 89.0, 91.0, 4.0, true)
                            .unwrap(),
                    ],
                }),
                Ok(MarketEvent::Candle {
                    generation: invalidated,
                    candle: Candle::from_ws(120_000, 179_999, 90.0, 92.0, 89.0, 91.0, 4.0, true)
                        .unwrap(),
                }),
                Ok(MarketEvent::Status {
                    generation: Some(next),
                    status: ConnectionStatus::Connecting,
                }),
            ],
            Arc::clone(&clock),
        )
        .with_event_delay(Duration::from_millis(2))
        .without_reconcile_ack_wait(),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(80), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );

    let observations = observations.acquire();
    let invalidation = observations
        .iter()
        .position(|observation| {
            matches!(
                observation.snapshot.status_detail,
                Some(ProviderError::QueueSaturated)
            ) && observation.active_generation.is_none()
        })
        .expect("emergency pair invalidates the active generation");
    let next_generation = observations
        .iter()
        .position(|observation| observation.active_generation == Some(next))
        .expect("a higher generation is admitted");
    assert!(invalidation < next_generation);
    assert!(
        observations[invalidation..next_generation]
            .iter()
            .all(|observation| observation.active_generation.is_none())
    );
    assert!(
        observations[invalidation..next_generation]
            .iter()
            .map(|observation| observation.stale_generation_events)
            .sum::<usize>()
            >= 4
    );
    let final_snapshot = observations
        .iter()
        .rev()
        .find(|observation| observation.stop.is_none())
        .expect("final running observation");
    assert_eq!(final_snapshot.active_generation, Some(next));
    assert_eq!(final_snapshot.snapshot.candles.len(), 1);
    assert_eq!(final_snapshot.snapshot.candles[0].open(), 10.0);
}

#[tokio::test]
async fn repeated_saturation_cycles_keep_one_marker_and_reject_all_delayed_generations() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let first = GapGeneration(21);
    let second = GapGeneration(22);
    let third = GapGeneration(23);
    let saturation = || {
        [
            Ok(MarketEvent::RecoverableError {
                generation: None,
                error: ProviderError::QueueSaturated,
                rate_gate_deadline: None,
            }),
            Ok(MarketEvent::Status {
                generation: None,
                status: ConnectionStatus::Backoff,
            }),
        ]
    };
    let delayed_candle = |generation, open_time, open| {
        Ok(MarketEvent::Candle {
            generation,
            candle: Candle::from_ws(
                open_time,
                open_time + 59_999,
                open,
                open + 2.0,
                open - 1.0,
                open + 1.0,
                4.0,
                true,
            )
            .unwrap(),
        })
    };
    let mut events = vec![Ok(MarketEvent::Status {
        generation: Some(first),
        status: ConnectionStatus::Connected,
    })];
    events.extend(saturation());
    events.push(delayed_candle(first, 60_000, 21.0));
    events.push(Ok(MarketEvent::Status {
        generation: Some(second),
        status: ConnectionStatus::Connecting,
    }));
    events.extend(saturation());
    events.push(delayed_candle(first, 120_000, 31.0));
    events.push(delayed_candle(second, 180_000, 41.0));
    events.push(Ok(MarketEvent::Status {
        generation: Some(third),
        status: ConnectionStatus::Connected,
    }));
    events.push(delayed_candle(first, 240_000, 51.0));
    events.push(delayed_candle(second, 300_000, 61.0));

    let provider = Arc::new(
        FakeProvider::new(vec![candle(0, 59_999)], events, Arc::clone(&clock))
            .with_event_delay(Duration::from_millis(2))
            .without_reconcile_ack_wait(),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(100), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );

    let observations = observations.acquire();
    assert!(observations.iter().any(|observation| {
        observation.active_generation.is_none() && observation.invalidated_generation == Some(first)
    }));
    assert!(observations.iter().any(|observation| {
        observation.active_generation.is_none()
            && observation.invalidated_generation == Some(second)
    }));
    assert!(observations.iter().all(|observation| {
        observation.invalidated_generation.is_none() || observation.active_generation.is_none()
    }));
    let final_observation = observations
        .iter()
        .rev()
        .find(|observation| observation.stop.is_none())
        .expect("final running observation");
    assert_eq!(final_observation.active_generation, Some(third));
    assert_eq!(final_observation.invalidated_generation, None);
    assert_eq!(final_observation.snapshot.candles.len(), 1);
    assert!(
        observations
            .iter()
            .map(|observation| observation.stale_generation_events)
            .sum::<usize>()
            >= 5
    );
}

#[tokio::test]
async fn live_terminal_bucket_short_circuits_recoverable_status_reconcile_candle_and_resize_buckets()
 {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let generation = GapGeneration(3);
    let terminal = ProviderError::ClientStatus {
        context: ErrorContext::operation(ErrorOperation::History),
        status: 403,
        code: None,
        message: None,
    };
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![
            Ok(MarketEvent::TerminalError(terminal)),
            Ok(MarketEvent::RecoverableError {
                generation: Some(generation),
                error: ProviderError::QueueSaturated,
                rate_gate_deadline: Some(MonoInstant::from_nanos(5)),
            }),
            Ok(MarketEvent::Status {
                generation: Some(generation),
                status: ConnectionStatus::Connected,
            }),
            Ok(MarketEvent::ReconcileBatch {
                generation,
                revision: ReplayRevision(1),
                target_open_time: 60_000,
                candles: vec![candle(60_000, 119_999)],
            }),
            Ok(MarketEvent::Candle {
                generation,
                candle: candle(120_000, 179_999),
            }),
        ],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        pending_input(),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    let error = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("403"));
    let stopped = observations
        .acquire()
        .iter()
        .find(|observation| observation.stop == Some(EpochStop::LiveTerminalError))
        .cloned()
        .unwrap();
    assert_eq!(stopped.snapshot.candles.len(), 1);
    assert_eq!(stopped.snapshot.rate_gate, RateGateState::Open);
    assert_eq!(
        stopped.snapshot.display_status,
        fccli::chart::DisplayStatus::TerminalError
    );
}

#[tokio::test]
async fn reducer_applies_reconcile_before_same_epoch_candle_without_finality_regression() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let generation = GapGeneration(4);
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![
            Ok(MarketEvent::ReconcileBatch {
                generation,
                revision: ReplayRevision(1),
                target_open_time: 60_000,
                candles: vec![
                    Candle::from_ws(60_000, 119_999, 10.0, 13.0, 9.0, 12.0, 4.0, true).unwrap(),
                ],
            }),
            Ok(MarketEvent::Candle {
                generation,
                candle: Candle::from_ws(60_000, 119_999, 20.0, 23.0, 19.0, 22.0, 5.0, false)
                    .unwrap(),
            }),
        ],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider,
        delayed_key(Duration::from_millis(30), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );
    let observations = observations.acquire();
    let candle = observations
        .iter()
        .rev()
        .find_map(|observation| observation.snapshot.candles.get(1))
        .unwrap();
    assert!(candle.is_closed());
    assert_eq!(candle.open(), 10.0);
    assert_eq!(candle.close(), 12.0);
}

#[tokio::test]
async fn unacknowledged_revision_invalidates_generation_before_backoff() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let generation = GapGeneration(12);
    let month_1 = 2_678_400_000;
    let month_2 = 5_097_600_000;
    let month_3 = 7_776_000_000;
    let events = vec![
        Ok(MarketEvent::ReconcileBatch {
            generation,
            revision: ReplayRevision(1),
            target_open_time: month_1,
            candles: vec![
                Candle::from_ws(month_1, month_2 - 1, 10.0, 12.0, 9.0, 11.0, 1.0, true).unwrap(),
            ],
        }),
        Ok(MarketEvent::ReconcileBatch {
            generation,
            revision: ReplayRevision(2),
            target_open_time: month_2,
            candles: vec![
                Candle::from_ws(month_2, month_3 - 1, 11.0, 13.0, 10.0, 12.0, 1.0, true).unwrap(),
            ],
        }),
        Ok(MarketEvent::ReconcileBatch {
            generation,
            revision: ReplayRevision(3),
            target_open_time: month_3,
            candles: vec![
                Candle::from_ws(month_3, 10_454_399_999, 12.0, 14.0, 11.0, 13.0, 1.0, true)
                    .unwrap(),
            ],
        }),
        Ok(MarketEvent::RecoverableError {
            generation: Some(generation),
            error: ProviderError::ReconcileAckTimeout {
                generation,
                revision: ReplayRevision(4),
                target_open_time: 10_454_400_000,
            },
            rate_gate_deadline: None,
        }),
        Ok(MarketEvent::Status {
            generation: None,
            status: ConnectionStatus::Backoff,
        }),
        Ok(MarketEvent::Status {
            generation: Some(GapGeneration(13)),
            status: ConnectionStatus::Connecting,
        }),
    ];
    let provider = Arc::new(
        FakeProvider::new(vec![candle(0, month_1 - 1)], events, Arc::clone(&clock))
            .with_event_delay(Duration::from_millis(3)),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider.clone(),
        delayed_key(Duration::from_millis(60), 'q'),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));
    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1M", "--interactive"], deps)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );
    assert_eq!(
        provider.acknowledgements.acquire().as_slice(),
        &[
            ReconcileAck {
                generation,
                revision: ReplayRevision(1),
                through: month_1
            },
            ReconcileAck {
                generation,
                revision: ReplayRevision(2),
                through: month_2
            },
            ReconcileAck {
                generation,
                revision: ReplayRevision(3),
                through: month_3
            },
        ]
    );
    let observations = observations.acquire();
    let timeout_index = observations
        .iter()
        .position(|observation| matches!(
            observation.snapshot.status_detail.as_ref(),
            Some(ProviderError::ReconcileAckTimeout { generation: actual_generation, revision: ReplayRevision(4), target_open_time: 10_454_400_000 }) if *actual_generation == generation
        ))
        .expect("unacknowledged revision times out");
    assert!(observations[timeout_index..].iter().any(|observation| {
        observation.snapshot.display_status == fccli::chart::DisplayStatus::Backoff
    }));
    assert!(
        observations[timeout_index..]
            .iter()
            .any(|observation| observation.active_generation == Some(GapGeneration(13)))
    );
}

#[tokio::test]
async fn direct_rate_gate_transitions_are_monotonic_quota_controlled_and_closure_is_terminal() {
    let manual = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let clock: Arc<dyn Clock> = manual.clone();
    let provider = Arc::new(FakeProvider::new(
        (0..20)
            .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
            .collect(),
        vec![],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let captured = Arc::clone(&observations);
    let mut deps = dependencies(
        provider.clone(),
        pending_input(),
        Arc::new(TerminalLog::default()),
        SharedWriter::default(),
        clock,
    );
    deps.epoch_observer = Some(Arc::new(move |observation| {
        captured.acquire().push(observation)
    }));

    let run = run_with_dependencies(["fccli", "btc", "1m", "--interactive"], deps);
    let drive_gate = async {
        async fn wait_for_after(
            observations: &Arc<Mutex<Vec<EpochObservation>>>,
            start: usize,
            expected: RateGateState,
        ) -> usize {
            for _ in 0..10_000 {
                let matching_offset = {
                    let observations = observations.acquire();
                    observations[start..]
                        .iter()
                        .position(|observation| observation.snapshot.rate_gate == expected)
                };
                if let Some(offset) = matching_offset {
                    return start + offset;
                }
                tokio::task::yield_now().await;
            }
            panic!("rate gate state was not reduced after observation {start}: {expected:?}");
        }

        let mut cursor = wait_for_after(&observations, 0, RateGateState::Open).await + 1;
        let deadline_10 = MonoInstant::from_nanos(10);
        let deadline_20 = MonoInstant::from_nanos(20);
        provider.publish_gate(RateGateState::TimedUntil(deadline_10));
        cursor = wait_for_after(
            &observations,
            cursor,
            RateGateState::TimedUntil(deadline_10),
        )
        .await
            + 1;

        manual.advance_to(MonoInstant::from_nanos(9)).unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            observations.acquire().last().unwrap().snapshot.rate_gate,
            RateGateState::TimedUntil(deadline_10)
        );

        provider.publish_gate(RateGateState::TimedUntil(deadline_20));
        cursor = wait_for_after(
            &observations,
            cursor,
            RateGateState::TimedUntil(deadline_20),
        )
        .await
            + 1;
        provider.publish_gate(RateGateState::TimedUntil(MonoInstant::from_nanos(15)));
        provider.publish_gate(RateGateState::Open);
        assert_eq!(
            provider.gate.current().unwrap(),
            RateGateState::TimedUntil(deadline_20)
        );

        manual.advance_to(deadline_10).unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            observations.acquire().last().unwrap().snapshot.rate_gate,
            RateGateState::TimedUntil(deadline_20)
        );

        manual.advance_to(deadline_20).unwrap();
        cursor = wait_for_after(&observations, cursor, RateGateState::Open).await + 1;
        assert_eq!(
            provider.gate.current().unwrap(),
            RateGateState::TimedUntil(deadline_20)
        );

        provider.publish_gate(RateGateState::ProcessBlocked(
            ProcessBlocker::InvalidBanExpiry,
        ));
        let _ = wait_for_after(
            &observations,
            cursor,
            RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry),
        )
        .await;
        provider.publish_gate(RateGateState::Open);
        assert_eq!(
            provider.gate.current().unwrap(),
            RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry)
        );
        provider.close_gate();
    };
    let (result, ()) = tokio::join!(run, drive_gate);
    let error = result.expect_err("closed provider gate must terminate the App");
    assert!(error.to_string().contains("provider rate gate closed"));

    let observations = observations.acquire();
    assert!(
        observations
            .iter()
            .all(|observation| observation.source_counts[5] <= 32)
    );
    let baseline = observations
        .iter()
        .find_map(viewport_signature)
        .expect("ready initial viewport");
    for observation in observations.iter().filter(|observation| {
        matches!(
            observation.snapshot.rate_gate,
            RateGateState::TimedUntil(_) | RateGateState::ProcessBlocked(_)
        )
    }) {
        let signature = viewport_signature(observation).expect("gate update preserves ready state");
        assert_eq!(signature.0, baseline.0);
        assert_eq!(signature.1, baseline.1);
        assert_eq!(observation.snapshot.candles.len(), 20);
    }
    assert!(
        observations
            .iter()
            .any(|observation| observation.stop == Some(EpochStop::TerminalFailure))
    );
}

#[tokio::test]
async fn colon_opens_editor_and_esc_cancels_without_switching() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let input = keys_input([
        key(KeyCode::Char(':'), KeyModifiers::NONE),
        key(KeyCode::Esc, KeyModifiers::NONE),
        key(KeyCode::Char('q'), KeyModifiers::NONE),
    ]);
    let run = run_with_observations(
        Arc::clone(&provider),
        input,
        clock,
        Arc::clone(&observations),
    );
    assert_eq!(run.await.unwrap(), ExitCode::SUCCESS);

    let observations = observations.acquire();
    assert!(
        observations
            .iter()
            .all(|observation| observation.snapshot.instrument.provider_symbol() == "BTCUSDT")
    );
    assert_eq!(
        provider.open_live_calls.load(Ordering::SeqCst),
        1,
        "no second live feed should be opened"
    );
}

#[tokio::test]
async fn same_canonical_target_is_noop_and_does_not_open_a_second_live_feed() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let mut events = switch_events("btc 1m");
    events.push(key(KeyCode::Char('q'), KeyModifiers::NONE));
    let input = keys_input(events);
    let run = run_with_observations(provider.clone(), input, clock, Arc::clone(&observations));
    assert_eq!(run.await.unwrap(), ExitCode::SUCCESS);

    let observations = observations.acquire();
    assert_eq!(
        provider.open_live_calls.load(Ordering::SeqCst),
        1,
        "same canonical target must not open a second live feed"
    );
    assert!(
        observations
            .iter()
            .all(|observation| observation.snapshot.instrument.provider_symbol() == "BTCUSDT")
    );
}

#[tokio::test]
async fn empty_interactive_target_resolves_to_default_market_and_timeframe() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let run = run_with_observations(
        provider.clone(),
        switch_then_quit_input(&[""]),
        clock,
        Arc::clone(&observations),
    );
    assert_eq!(run.await.unwrap(), ExitCode::SUCCESS);

    assert_eq!(provider.open_live_calls.load(Ordering::SeqCst), 2);
    let observations = observations.acquire();
    let switched_index = observations
        .iter()
        .position(|observation| observation.snapshot.timeframe == Timeframe::Hour1)
        .expect("empty target switches the initial BTC 1m session to default BTC 1h");
    assert_eq!(
        observations[switched_index]
            .snapshot
            .instrument
            .provider_symbol(),
        "BTCUSDT"
    );
}

#[tokio::test]
async fn known_unregistered_switch_shows_registry_error_and_preserves_old_chart() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let provider = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let mut events = switch_events("kraken:btc 1m");
    events.push(key(KeyCode::Char('q'), KeyModifiers::NONE));
    let input = keys_input(events);
    let run = run_with_observations(provider.clone(), input, clock, Arc::clone(&observations));
    assert_eq!(run.await.unwrap(), ExitCode::SUCCESS);

    let observations = observations.acquire();
    assert!(observations.iter().any(|observation| {
        matches!(&observation.snapshot.footer, FooterPresentation::Error { message } if message.contains("unsupported market-data provider"))
    }));
    assert_eq!(
        provider.open_live_calls.load(Ordering::SeqCst),
        1,
        "invalid provider must not open a second live feed"
    );
    assert!(
        observations
            .iter()
            .all(|observation| observation.snapshot.instrument.provider_symbol() == "BTCUSDT")
    );
}

#[tokio::test]
async fn ordinary_registry_switch_to_okx_is_accepted_and_commits() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let binance = Arc::new(FakeProvider::new(
        vec![candle(0, 59_999)],
        vec![],
        Arc::clone(&clock),
    ));
    let okx = Arc::new(
        FakeProvider::new(
            vec![candle(60_000, 119_999)],
            Vec::new(),
            Arc::clone(&clock),
        )
        .with_provider_id("okx"),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let terminal = Arc::new(TerminalLog::default());
    let output = SharedWriter::default();
    let providers = ProviderRegistry::new([
        Arc::clone(&binance) as Arc<dyn MarketDataProvider>,
        Arc::clone(&okx) as Arc<dyn MarketDataProvider>,
    ])
    .expect("ordinary registry with OKX");
    let captured = Arc::clone(&observations);
    let dependencies = RunDependencies {
        providers,
        clock,
        terminal,
        input: switch_then_quit_input(&["okx:btc 1m"]),
        stdout: Box::new(output),
        stderr: Box::new(SharedWriter::default()),
        stdin_is_tty: true,
        stdout_is_tty: true,
        render_policy: fccli::chart::RenderPolicy::StyleFree,
        epoch_observer: Some(Arc::new(move |observation| {
            captured.acquire().push(observation)
        })),
    };
    assert_eq!(
        run_with_dependencies(["fccli", "btc", "1m", "--interactive"], dependencies)
            .await
            .unwrap(),
        ExitCode::SUCCESS
    );
    let observations = observations.acquire();
    assert!(observations.iter().any(|observation| {
        observation.snapshot.instrument.provider().as_str() == "okx"
            && observation.snapshot.instrument.provider_symbol() == "BTCUSDT"
    }));
    assert_eq!(okx.open_live_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn successful_switch_commits_new_session_and_resets_view() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = (0..100)
        .map(|index| candle(index * 60_000, index * 60_000 + 59_999))
        .collect::<Vec<_>>();
    let switched = vec![candle(3_600_000, 3_600_000 + 59_999)];
    let provider = Arc::new(
        FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock))
            .with_history_pages([Ok(initial), Ok(switched.clone())]),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let input = switch_then_quit_input(&["eth 1h"]);
    let run = run_with_observations(provider.clone(), input, clock, Arc::clone(&observations));
    assert_eq!(run.await.unwrap(), ExitCode::SUCCESS);

    let observations = observations.acquire();
    let switched_index = wait_for_instrument_sync(&observations, 0, "ETHUSDT");
    assert_eq!(
        observations[switched_index].snapshot.timeframe,
        Timeframe::Hour1
    );
    assert_eq!(
        provider.open_live_calls.load(Ordering::SeqCst),
        2,
        "a second live feed must be opened for the new target"
    );
    assert_eq!(
        observations[switched_index].snapshot.candles.len(),
        switched.len()
    );
    assert_eq!(
        observations[switched_index].snapshot.candles[0].open_time(),
        switched[0].open_time()
    );
}

#[tokio::test]
async fn interactive_single_market_and_unit_aliases_reach_canonical_session_identity() {
    for (target, expected_timeframe) in [
        ("eth", Timeframe::Hour1),
        ("eth m", Timeframe::Minute1),
        ("eth M", Timeframe::Month1),
    ] {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
        let initial = vec![candle(0, 59_999)];
        let switched = vec![candle(3_600_000, 3_600_000 + 59_999)];
        let provider = Arc::new(
            FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock))
                .with_history_pages([Ok(initial), Ok(switched)]),
        );
        let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
        let run = run_with_observations(
            provider.clone(),
            switch_then_quit_input(&[target]),
            clock,
            Arc::clone(&observations),
        );
        assert_eq!(run.await.unwrap(), ExitCode::SUCCESS, "{target}");

        let observations = observations.acquire();
        let switched_index = wait_for_instrument_sync(&observations, 0, "ETHUSDT");
        assert_eq!(
            observations[switched_index].snapshot.timeframe, expected_timeframe,
            "{target}"
        );
        assert_eq!(
            provider.open_live_calls.load(Ordering::SeqCst),
            2,
            "{target}"
        );
    }
}

#[tokio::test]
async fn latest_submitted_command_cancels_pending_preparation() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = vec![candle(0, 59_999)];
    // Both switch preparations share the same history page content; the test only verifies
    // that the latest submitted target wins, not which page each task consumed.
    let switched = vec![candle(7_200_000, 7_200_000 + 59_999)];
    let provider = Arc::new(
        FakeProvider::new(initial, vec![], Arc::clone(&clock)).with_history_pages([
            Ok(vec![candle(0, 59_999)]),
            Ok(switched.clone()),
            Ok(switched.clone()),
        ]),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let input = switch_then_quit_input(&["eth 1h", "sol 2h"]);
    let run = run_with_observations(provider.clone(), input, clock, Arc::clone(&observations));
    assert_eq!(run.await.unwrap(), ExitCode::SUCCESS);

    let observations = observations.acquire();
    let final_index = observations
        .iter()
        .rposition(|observation| observation.stop.is_none())
        .expect("final running observation");
    assert_eq!(
        observations[final_index]
            .snapshot
            .instrument
            .provider_symbol(),
        "SOLUSDT",
        "latest submitted command must win"
    );
    assert_eq!(
        observations[final_index].snapshot.timeframe,
        Timeframe::Hour2
    );
}

#[tokio::test]
async fn preparation_failure_preserves_old_chart_and_shows_footer_error() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let initial = vec![candle(0, 59_999)];
    let provider = Arc::new(
        FakeProvider::new(initial.clone(), vec![], Arc::clone(&clock)).with_history_pages([
            Ok(initial.clone()),
            Err(ProviderError::ClientStatus {
                context: ErrorContext::operation(ErrorOperation::History),
                status: 403,
                code: None,
                message: None,
            }),
        ]),
    );
    let observations = Arc::new(Mutex::new(Vec::<EpochObservation>::new()));
    let mut events = switch_events("kraken:eth 1h")
        .into_iter()
        .map(|event| (Duration::ZERO, event))
        .collect::<Vec<_>>();
    events.push((
        Duration::from_millis(100),
        key(KeyCode::Char('q'), KeyModifiers::NONE),
    ));
    let input: Box<dyn TerminalInput> = Box::new(ScriptedTerminalInput::with_delays(events));
    let run = run_with_observations(provider.clone(), input, clock, Arc::clone(&observations));
    assert_eq!(run.await.unwrap(), ExitCode::SUCCESS);

    let observations = observations.acquire();
    assert!(observations.iter().any(|observation| {
        matches!(
            &observation.snapshot.footer,
            FooterPresentation::Error { .. }
        )
    }));
    assert!(
        observations
            .iter()
            .all(|observation| observation.snapshot.instrument.provider_symbol() == "BTCUSDT")
    );
}

fn wait_for_instrument_sync(
    observations: &[EpochObservation],
    start: usize,
    symbol: &str,
) -> usize {
    observations[start..]
        .iter()
        .position(|observation| observation.snapshot.instrument.provider_symbol() == symbol)
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("instrument {symbol} not observed after {start}"))
}
