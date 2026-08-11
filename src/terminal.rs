//! Transactional interactive-terminal setup and restoration.

use std::{
    fmt,
    io::{self, IsTerminal, Stdin, Stdout},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use crossterm::{
    cursor::{Hide, Show},
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};

use crate::error::{AppError, SanitizedCause, TerminalError};

/// State of one independently reversible terminal mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StepState {
    NotAttempted,
    AttemptedOrActive,
    Restored,
}

/// Public snapshot of all transactional setup steps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TerminalStepStates {
    pub raw: StepState,
    pub alternate: StepState,
    pub mouse: StepState,
    pub cursor: StepState,
}

impl Default for TerminalStepStates {
    fn default() -> Self {
        Self {
            raw: StepState::NotAttempted,
            alternate: StepState::NotAttempted,
            mouse: StepState::NotAttempted,
            cursor: StepState::NotAttempted,
        }
    }
}

/// Object-safe injectable boundary around terminal mutations.
///
/// Drivers are shared through [`Arc`] by the frozen runtime dependency boundary. Implementations
/// therefore expose shared-reference operations and synchronize any mutable terminal or fake state.
/// Every inverse must be safe to retry when the corresponding forward call may have changed the
/// terminal before returning an error.
pub trait TerminalDriver: Send + Sync {
    fn enable_raw(&self) -> io::Result<()>;
    fn enter_alternate(&self) -> io::Result<()>;
    fn enable_mouse(&self) -> io::Result<()>;
    fn hide_cursor(&self) -> io::Result<()>;
    fn show_cursor(&self) -> io::Result<()>;
    fn disable_mouse(&self) -> io::Result<()>;
    fn leave_alternate(&self) -> io::Result<()>;
    fn disable_raw(&self) -> io::Result<()>;
    /// Samples the current render target size. Test drivers may return scripted changes.
    fn size(&self) -> io::Result<(u16, u16)> {
        crossterm::terminal::size()
    }
}

/// Crossterm-backed production driver bound to the process standard streams.
///
/// Constructing the driver is side-effect free: it only acquires handles to process stdin and
/// stdout. Crossterm raw mode operates on stdin when it is a TTY; alternate-screen, mouse, and
/// cursor controls are written to stdout. Endpoint validation belongs to [`Self::enter_checked`].
pub struct CrosstermTerminalDriver {
    _input: Stdin,
    output: Mutex<Stdout>,
}

impl TerminalDriver for CrosstermTerminalDriver {
    fn enable_raw(&self) -> io::Result<()> {
        let _output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        enable_raw_mode()
    }
    fn enter_alternate(&self) -> io::Result<()> {
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        execute!(*output, EnterAlternateScreen)
    }
    fn enable_mouse(&self) -> io::Result<()> {
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        execute!(*output, EnableMouseCapture)
    }
    fn hide_cursor(&self) -> io::Result<()> {
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        execute!(*output, Hide)
    }
    fn show_cursor(&self) -> io::Result<()> {
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        execute!(*output, Show)
    }
    fn disable_mouse(&self) -> io::Result<()> {
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        execute!(*output, DisableMouseCapture)
    }
    fn leave_alternate(&self) -> io::Result<()> {
        let mut output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        execute!(*output, LeaveAlternateScreen)
    }
    fn disable_raw(&self) -> io::Result<()> {
        let _output = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        disable_raw_mode()
    }
}

impl CrosstermTerminalDriver {
    /// Constructs the production terminal dependency without checking TTY capabilities or
    /// mutating either endpoint.
    pub fn production_driver() -> Arc<dyn TerminalDriver> {
        Arc::new(Self {
            _input: io::stdin(),
            output: Mutex::new(io::stdout()),
        })
    }

    /// Validates both actual process endpoints, then enters the supplied production handle.
    pub fn enter_checked(
        driver: Arc<dyn TerminalDriver>,
    ) -> Result<TerminalSession, TerminalLifecycleError> {
        let stdin_is_terminal = io::stdin().is_terminal();
        let stdout_is_terminal = io::stdout().is_terminal();
        enter_with_tty_preflight(stdin_is_terminal, stdout_is_terminal, || driver)
    }
}

/// Applies terminal setup only when both injected endpoint capabilities are TTYs.
///
/// The setup closure is deliberately lazy so rejected capability combinations cannot construct a
/// driver, mutate terminal state, or emit control sequences. Production calls this seam with the
/// actual [`IsTerminal`] results for stdin and stdout.
pub fn enter_with_tty_preflight<F>(
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
    setup: F,
) -> Result<TerminalSession, TerminalLifecycleError>
where
    F: FnOnce() -> Arc<dyn TerminalDriver>,
{
    if !stdin_is_terminal || !stdout_is_terminal {
        return Err(TerminalLifecycleError::Primary(
            TerminalError::TtyRequired.into(),
        ));
    }
    TerminalSession::enter(setup())
}

/// One failed best-effort inverse operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupFailure {
    pub operation: &'static str,
    pub cause: SanitizedCause,
}

/// Ordered failures from a complete reverse restoration pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CleanupFailures(Vec<CleanupFailure>);

impl CleanupFailures {
    #[must_use]
    pub fn failures(&self) -> &[CleanupFailure] {
        &self.0
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for CleanupFailures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "terminal cleanup failed in {} step(s)", self.0.len())?;
        for failure in &self.0 {
            write!(f, "; {}: {}", failure.operation, failure.cause)?;
        }
        Ok(())
    }
}

impl std::error::Error for CleanupFailures {}

/// Returned-error precedence with cleanup retained as secondary evidence.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TerminalLifecycleError {
    #[error("{primary}; secondary cleanup failure: {cleanup}")]
    PrimaryWithCleanup {
        primary: AppError,
        cleanup: CleanupFailures,
    },
    #[error(transparent)]
    Primary(#[from] AppError),
    #[error(transparent)]
    Cleanup(#[from] CleanupFailures),
}

/// An entered or partially entered terminal transaction.
///
/// The session owns the same shared driver shape used by future runtime dependencies.
pub struct TerminalSession {
    driver: Arc<dyn TerminalDriver>,
    states: TerminalStepStates,
}

impl TerminalSession {
    /// Applies raw → alternate → mouse → cursor, marking uncertainty before every call.
    pub fn enter(driver: Arc<dyn TerminalDriver>) -> Result<Self, TerminalLifecycleError> {
        let mut session = Self {
            driver,
            states: TerminalStepStates::default(),
        };
        if let Err(primary) = session.setup() {
            let cleanup = session.restore().err();
            return Err(match cleanup {
                Some(cleanup) => TerminalLifecycleError::PrimaryWithCleanup { primary, cleanup },
                None => TerminalLifecycleError::Primary(primary),
            });
        }
        Ok(session)
    }

    fn setup(&mut self) -> Result<(), AppError> {
        self.states.raw = StepState::AttemptedOrActive;
        self.driver
            .enable_raw()
            .map_err(|_| setup_error("enable raw mode"))?;
        self.states.alternate = StepState::AttemptedOrActive;
        self.driver
            .enter_alternate()
            .map_err(|_| setup_error("enter alternate screen"))?;
        self.states.mouse = StepState::AttemptedOrActive;
        self.driver
            .enable_mouse()
            .map_err(|_| setup_error("enable mouse capture"))?;
        self.states.cursor = StepState::AttemptedOrActive;
        self.driver
            .hide_cursor()
            .map_err(|_| setup_error("hide cursor"))?;
        Ok(())
    }

    #[must_use]
    pub const fn states(&self) -> TerminalStepStates {
        self.states
    }

    pub fn driver(&self) -> &Arc<dyn TerminalDriver> {
        &self.driver
    }

    /// Attempts cursor → mouse → alternate → raw without short-circuiting.
    pub fn restore(&mut self) -> Result<(), CleanupFailures> {
        let mut failures = CleanupFailures::default();
        restore_step(
            &mut self.states.cursor,
            "show cursor",
            &mut failures,
            || self.driver.show_cursor(),
        );
        restore_step(
            &mut self.states.mouse,
            "disable mouse capture",
            &mut failures,
            || self.driver.disable_mouse(),
        );
        restore_step(
            &mut self.states.alternate,
            "leave alternate screen",
            &mut failures,
            || self.driver.leave_alternate(),
        );
        restore_step(
            &mut self.states.raw,
            "disable raw mode",
            &mut failures,
            || self.driver.disable_raw(),
        );
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }

    /// Restores the terminal and applies the locked primary/secondary error precedence.
    pub fn finish(mut self, primary: Option<AppError>) -> Result<(), TerminalLifecycleError> {
        let cleanup = self.restore().err();
        match (primary, cleanup) {
            (None, None) => Ok(()),
            (Some(primary), None) => Err(TerminalLifecycleError::Primary(primary)),
            (None, Some(cleanup)) => Err(TerminalLifecycleError::Cleanup(cleanup)),
            (Some(primary), Some(cleanup)) => {
                Err(TerminalLifecycleError::PrimaryWithCleanup { primary, cleanup })
            }
        }
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        // `restore` isolates every driver inverse so later undos still run. Keep an outer unwind
        // boundary as the final Drop guarantee: cleanup must never replace an in-flight panic or
        // start a double-panic abort if bookkeeping itself unexpectedly panics.
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _ = self.restore();
        }));
    }
}

/// Shutdown-order seam: input closes and cancellation fires, then every terminal step must be
/// restored before a possibly non-cooperating producer join is entered. If cleanup fails, the
/// join closure is not called; dropping the session can retry any unresolved inverse operation.
pub fn restore_before_join<C, X, J, T>(
    session: &mut TerminalSession,
    close_input: C,
    cancel_producer: X,
    join_producer: J,
) -> Result<T, CleanupFailures>
where
    C: FnOnce(),
    X: FnOnce(),
    J: FnOnce() -> T,
{
    close_input();
    cancel_producer();
    session.restore()?;
    if session.states
        != (TerminalStepStates {
            raw: StepState::Restored,
            alternate: StepState::Restored,
            mouse: StepState::Restored,
            cursor: StepState::Restored,
        })
    {
        return Err(CleanupFailures(vec![CleanupFailure {
            operation: "verify terminal restoration",
            cause: SanitizedCause::Other,
        }]));
    }
    Ok(join_producer())
}

fn setup_error(operation: &'static str) -> AppError {
    TerminalError::Setup {
        operation,
        cause: SanitizedCause::Io,
    }
    .into()
}

fn restore_step<F>(
    state: &mut StepState,
    operation: &'static str,
    failures: &mut CleanupFailures,
    action: F,
) where
    F: FnOnce() -> io::Result<()>,
{
    if *state != StepState::AttemptedOrActive {
        return;
    }
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(())) => *state = StepState::Restored,
        Ok(Err(_)) => failures.0.push(CleanupFailure {
            operation,
            cause: SanitizedCause::Io,
        }),
        Err(_) => failures.0.push(CleanupFailure {
            operation,
            cause: SanitizedCause::Other,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Arc, Mutex},
    };

    use super::{CleanupFailures, StepState, TerminalDriver, TerminalSession};
    use crate::error::SanitizedCause;

    #[derive(Default)]
    struct PanicTrace {
        calls: Vec<&'static str>,
        panic_once: Vec<&'static str>,
    }

    struct PanickingDriver(Arc<Mutex<PanicTrace>>);

    impl PanickingDriver {
        fn call(&self, operation: &'static str) -> io::Result<()> {
            let should_panic = {
                let mut trace = self
                    .0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                trace.calls.push(operation);
                trace
                    .panic_once
                    .iter()
                    .position(|candidate| *candidate == operation)
                    .map(|index| trace.panic_once.remove(index))
                    .is_some()
            };
            if should_panic {
                panic!("injected terminal inverse panic")
            }
            Ok(())
        }
    }

    impl TerminalDriver for PanickingDriver {
        fn enable_raw(&self) -> io::Result<()> {
            self.call("raw+")
        }
        fn enter_alternate(&self) -> io::Result<()> {
            self.call("alternate+")
        }
        fn enable_mouse(&self) -> io::Result<()> {
            self.call("mouse+")
        }
        fn hide_cursor(&self) -> io::Result<()> {
            self.call("cursor-")
        }
        fn show_cursor(&self) -> io::Result<()> {
            self.call("cursor+")
        }
        fn disable_mouse(&self) -> io::Result<()> {
            self.call("mouse-")
        }
        fn leave_alternate(&self) -> io::Result<()> {
            self.call("alternate-")
        }
        fn disable_raw(&self) -> io::Result<()> {
            self.call("raw-")
        }
    }

    fn driver(panic_once: Vec<&'static str>) -> (Arc<dyn TerminalDriver>, Arc<Mutex<PanicTrace>>) {
        let trace = Arc::new(Mutex::new(PanicTrace {
            panic_once,
            ..PanicTrace::default()
        }));
        (Arc::new(PanickingDriver(Arc::clone(&trace))), trace)
    }

    fn operations(failures: &CleanupFailures) -> Vec<(&'static str, SanitizedCause)> {
        failures
            .failures()
            .iter()
            .map(|failure| (failure.operation, failure.cause))
            .collect()
    }

    #[test]
    fn explicit_restore_aggregates_every_inverse_panic_and_retries_unresolved_steps() {
        let (driver, trace) = driver(vec!["cursor+", "mouse-", "alternate-", "raw-"]);
        let mut session = TerminalSession::enter(driver).unwrap();
        let failures = session.restore().unwrap_err();
        assert_eq!(
            operations(&failures),
            [
                ("show cursor", SanitizedCause::Other),
                ("disable mouse capture", SanitizedCause::Other),
                ("leave alternate screen", SanitizedCause::Other),
                ("disable raw mode", SanitizedCause::Other),
            ]
        );
        assert_eq!(session.states().cursor, StepState::AttemptedOrActive);
        assert_eq!(session.states().mouse, StepState::AttemptedOrActive);
        assert_eq!(session.states().alternate, StepState::AttemptedOrActive);
        assert_eq!(session.states().raw, StepState::AttemptedOrActive);
        session.restore().unwrap();
        assert_eq!(
            &trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .calls[4..],
            [
                "cursor+",
                "mouse-",
                "alternate-",
                "raw-",
                "cursor+",
                "mouse-",
                "alternate-",
                "raw-"
            ]
        );
    }

    #[test]
    fn drop_continues_after_each_inverse_panic_without_propagating() {
        for panicking_inverse in ["cursor+", "mouse-", "alternate-", "raw-"] {
            let (driver, trace) = driver(vec![panicking_inverse]);
            let result = catch_unwind(AssertUnwindSafe(|| {
                drop(TerminalSession::enter(driver).unwrap())
            }));
            assert!(result.is_ok(), "Drop propagated {panicking_inverse}");
            assert_eq!(
                &trace
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .calls[4..],
                ["cursor+", "mouse-", "alternate-", "raw-"]
            );
        }
    }

    #[test]
    fn drop_never_double_panics_during_an_existing_unwind() {
        let (driver, trace) = driver(vec!["cursor+", "mouse-", "alternate-", "raw-"]);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _session = TerminalSession::enter(driver).unwrap();
            panic!("outer application panic");
        }));
        assert!(result.is_err());
        assert_eq!(
            &trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .calls[4..],
            ["cursor+", "mouse-", "alternate-", "raw-"]
        );
    }
}
