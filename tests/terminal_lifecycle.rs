use std::{
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use fccli::{
    error::{AppError, ProviderError, RenderError, SanitizedCause, TerminalError},
    terminal::{
        CleanupFailures, StepState, TerminalDriver, TerminalLifecycleError, TerminalSession,
        enter_with_tty_preflight, restore_before_join,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailMode {
    Before,
    After,
}

#[derive(Default, Debug)]
struct Trace {
    calls: Vec<&'static str>,
    active: [bool; 4],
    failures: Vec<(&'static str, FailMode)>,
}

#[derive(Clone)]
struct FakeDriver(Arc<Mutex<Trace>>);

impl FakeDriver {
    fn new(failures: Vec<(&'static str, FailMode)>) -> (Self, Arc<Mutex<Trace>>) {
        let trace = Arc::new(Mutex::new(Trace {
            failures,
            ..Trace::default()
        }));
        (Self(Arc::clone(&trace)), trace)
    }

    fn call(&self, name: &'static str, slot: usize, value: bool) -> io::Result<()> {
        let mut trace = self.0.lock().unwrap();
        trace.calls.push(name);
        let failure = trace
            .failures
            .iter()
            .position(|entry| entry.0 == name)
            .map(|index| trace.failures.remove(index).1);
        if failure == Some(FailMode::Before) {
            return Err(io::Error::other("injected"));
        }
        trace.active[slot] = value;
        if failure == Some(FailMode::After) {
            return Err(io::Error::other("injected"));
        }
        Ok(())
    }
}

impl TerminalDriver for FakeDriver {
    fn enable_raw(&self) -> io::Result<()> {
        self.call("raw+", 0, true)
    }
    fn enter_alternate(&self) -> io::Result<()> {
        self.call("alt+", 1, true)
    }
    fn enable_mouse(&self) -> io::Result<()> {
        self.call("mouse+", 2, true)
    }
    fn hide_cursor(&self) -> io::Result<()> {
        self.call("cursor-", 3, true)
    }
    fn show_cursor(&self) -> io::Result<()> {
        self.call("cursor+", 3, false)
    }
    fn disable_mouse(&self) -> io::Result<()> {
        self.call("mouse-", 2, false)
    }
    fn leave_alternate(&self) -> io::Result<()> {
        self.call("alt-", 1, false)
    }
    fn disable_raw(&self) -> io::Result<()> {
        self.call("raw-", 0, false)
    }
}

fn cleanup_operations(error: &CleanupFailures) -> Vec<&'static str> {
    error
        .failures()
        .iter()
        .map(|failure| failure.operation)
        .collect()
}

#[test]
fn tty_capability_matrix_rejects_before_setup_or_control_writes() {
    for (stdin_is_terminal, stdout_is_terminal) in
        [(true, true), (true, false), (false, true), (false, false)]
    {
        let setup_calls = Arc::new(Mutex::new(0_usize));
        let setup_calls_at_entry = Arc::clone(&setup_calls);
        let trace = Arc::new(Mutex::new(Trace::default()));
        let trace_at_entry = Arc::clone(&trace);

        let result = enter_with_tty_preflight(stdin_is_terminal, stdout_is_terminal, move || {
            *setup_calls_at_entry.lock().unwrap() += 1;
            Arc::new(FakeDriver(trace_at_entry))
        });

        if stdin_is_terminal && stdout_is_terminal {
            let mut session = result.expect("TTY/TTY must proceed to the setup seam");
            assert_eq!(*setup_calls.lock().unwrap(), 1);
            assert_eq!(
                trace.lock().unwrap().calls,
                ["raw+", "alt+", "mouse+", "cursor-"]
            );
            session.restore().unwrap();
        } else {
            assert!(matches!(
                result,
                Err(TerminalLifecycleError::Primary(AppError::Terminal(
                    TerminalError::TtyRequired
                )))
            ));
            assert_eq!(*setup_calls.lock().unwrap(), 0);
            assert!(
                trace.lock().unwrap().calls.is_empty(),
                "rejected capability pair must emit no mutation/control calls"
            );
        }
    }
}

#[test]
fn setup_and_restore_use_exact_transactional_order_and_states() {
    let (driver, trace) = FakeDriver::new(vec![]);
    let mut session = TerminalSession::enter(Arc::new(driver)).unwrap();
    assert_eq!(session.states().raw, StepState::AttemptedOrActive);
    assert_eq!(session.states().alternate, StepState::AttemptedOrActive);
    assert_eq!(session.states().mouse, StepState::AttemptedOrActive);
    assert_eq!(session.states().cursor, StepState::AttemptedOrActive);
    session.restore().unwrap();
    assert_eq!(session.states().raw, StepState::Restored);
    assert_eq!(session.states().alternate, StepState::Restored);
    assert_eq!(session.states().mouse, StepState::Restored);
    assert_eq!(session.states().cursor, StepState::Restored);
    assert_eq!(
        trace.lock().unwrap().calls,
        [
            "raw+", "alt+", "mouse+", "cursor-", "cursor+", "mouse-", "alt-", "raw-"
        ]
    );
}

#[test]
fn caller_owned_arc_survives_pre_entry_work_and_enters_the_session_unchanged() {
    let (driver, trace) = FakeDriver::new(vec![]);
    let driver: Arc<dyn TerminalDriver> = Arc::new(driver);
    let dependency_driver = Arc::clone(&driver);

    trace.lock().unwrap().calls.push("pre-entry");
    let mut session = enter_with_tty_preflight(true, true, || dependency_driver).unwrap();
    assert!(
        Arc::ptr_eq(session.driver(), &driver),
        "checked entry must retain the caller-owned RunDependencies handle"
    );
    session.restore().unwrap();
    assert_eq!(
        trace.lock().unwrap().calls,
        [
            "pre-entry",
            "raw+",
            "alt+",
            "mouse+",
            "cursor-",
            "cursor+",
            "mouse-",
            "alt-",
            "raw-"
        ]
    );
}

#[test]
fn every_setup_failure_before_or_after_side_effect_rolls_back_every_uncertain_step() {
    for (forward, inverse) in [
        ("raw+", "raw-"),
        ("alt+", "alt-"),
        ("mouse+", "mouse-"),
        ("cursor-", "cursor+"),
    ] {
        for mode in [FailMode::Before, FailMode::After] {
            let (driver, trace) = FakeDriver::new(vec![(forward, mode)]);
            assert!(TerminalSession::enter(Arc::new(driver)).is_err());
            let trace = trace.lock().unwrap();
            assert!(trace.calls.contains(&inverse), "{forward:?} {mode:?}");
            assert!(
                !trace.active.iter().any(|active| *active),
                "{forward:?} {mode:?}"
            );
        }
    }
}

#[test]
fn teardown_is_non_short_circuiting_and_failed_steps_are_retryable() {
    let (driver, trace) = FakeDriver::new(vec![
        ("cursor+", FailMode::Before),
        ("alt-", FailMode::After),
    ]);
    let mut session = TerminalSession::enter(Arc::new(driver)).unwrap();
    let error = session.restore().unwrap_err();
    assert_eq!(
        cleanup_operations(&error),
        ["show cursor", "leave alternate screen"]
    );
    assert_eq!(session.states().cursor, StepState::AttemptedOrActive);
    assert_eq!(session.states().mouse, StepState::Restored);
    assert_eq!(session.states().alternate, StepState::AttemptedOrActive);
    assert_eq!(session.states().raw, StepState::Restored);
    session.restore().unwrap();
    assert_eq!(
        &trace.lock().unwrap().calls[4..],
        ["cursor+", "mouse-", "alt-", "raw-", "cursor+", "alt-"]
    );
}

#[test]
fn primary_provider_or_render_failure_keeps_precedence_and_cleanup_is_secondary() {
    for primary in [
        ProviderError::Configuration("provider primary").into(),
        RenderError::InsufficientSpace.into(),
    ] {
        let (driver, _) = FakeDriver::new(vec![("mouse-", FailMode::Before)]);
        let error = TerminalSession::enter(Arc::new(driver))
            .unwrap()
            .finish(Some(primary))
            .unwrap_err();
        match error {
            TerminalLifecycleError::PrimaryWithCleanup { primary, cleanup } => {
                assert!(matches!(
                    primary,
                    AppError::Provider(ProviderError::Configuration("provider primary"))
                        | AppError::Render(RenderError::InsufficientSpace)
                ));
                assert_eq!(cleanup_operations(&cleanup), ["disable mouse capture"]);
            }
            other => panic!("wrong precedence: {other:?}"),
        }
    }
}

#[test]
fn successful_finish_with_cleanup_failure_returns_cleanup_as_primary() {
    let (driver, _) = FakeDriver::new(vec![("raw-", FailMode::Before)]);
    let error = TerminalSession::enter(Arc::new(driver))
        .unwrap()
        .finish(None)
        .unwrap_err();
    assert!(matches!(error, TerminalLifecycleError::Cleanup(_)));
}

#[test]
fn drop_retries_unresolved_steps_without_panicking() {
    let (driver, trace) = FakeDriver::new(vec![("cursor+", FailMode::Before)]);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut session = TerminalSession::enter(Arc::new(driver)).unwrap();
        assert!(session.restore().is_err());
    }));
    assert!(result.is_ok());
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|call| **call == "cursor+")
            .count(),
        2
    );
}

#[test]
fn terminal_finish_restores_independent_of_shutdown_cause() {
    let (driver, trace) = FakeDriver::new(vec![]);
    TerminalSession::enter(Arc::new(driver))
        .unwrap()
        .finish(None)
        .unwrap();
    assert!(!trace.lock().unwrap().active.iter().any(|active| *active));
}

#[test]
fn drop_catches_panicking_inverse_and_never_panics() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct PanicOnceDriver {
        inner: FakeDriver,
        panic_once: Arc<AtomicBool>,
    }
    impl TerminalDriver for PanicOnceDriver {
        fn enable_raw(&self) -> io::Result<()> {
            self.inner.enable_raw()
        }
        fn enter_alternate(&self) -> io::Result<()> {
            self.inner.enter_alternate()
        }
        fn enable_mouse(&self) -> io::Result<()> {
            self.inner.enable_mouse()
        }
        fn hide_cursor(&self) -> io::Result<()> {
            self.inner.hide_cursor()
        }
        fn show_cursor(&self) -> io::Result<()> {
            if self.panic_once.swap(false, Ordering::SeqCst) {
                panic!("injected inverse panic")
            }
            self.inner.show_cursor()
        }
        fn disable_mouse(&self) -> io::Result<()> {
            self.inner.disable_mouse()
        }
        fn leave_alternate(&self) -> io::Result<()> {
            self.inner.leave_alternate()
        }
        fn disable_raw(&self) -> io::Result<()> {
            self.inner.disable_raw()
        }
    }
    let (inner, trace) = FakeDriver::new(vec![]);
    let driver = PanicOnceDriver {
        inner,
        panic_once: Arc::new(AtomicBool::new(true)),
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        drop(TerminalSession::enter(Arc::new(driver)).unwrap())
    }));
    assert!(result.is_ok());
    assert_eq!(trace.lock().unwrap().calls.last(), Some(&"raw-"));
}

#[test]
fn panic_unwind_restores_via_drop() {
    let (driver, trace) = FakeDriver::new(vec![]);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _session = TerminalSession::enter(Arc::new(driver)).unwrap();
        panic!("render panic");
    }));
    assert!(result.is_err());
    assert!(!trace.lock().unwrap().active.iter().any(|active| *active));
}

#[test]
fn synchronous_restore_close_cancel_join_order_is_exact() {
    let (driver, trace) = FakeDriver::new(vec![]);
    let mut session = TerminalSession::enter(Arc::new(driver)).unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let close_order = Arc::clone(&order);
    let cancel_order = Arc::clone(&order);
    let join_order = Arc::clone(&order);
    let trace_at_join = Arc::clone(&trace);
    let joined = restore_before_join(
        &mut session,
        move || close_order.lock().unwrap().push("input closed"),
        move || cancel_order.lock().unwrap().push("cancelled"),
        move || {
            assert!(
                !trace_at_join
                    .lock()
                    .unwrap()
                    .active
                    .iter()
                    .any(|active| *active)
            );
            join_order.lock().unwrap().push("join entered");
            "synchronous join result"
        },
    )
    .unwrap();
    assert_eq!(joined, "synchronous join result");
    assert_eq!(
        *order.lock().unwrap(),
        ["input closed", "cancelled", "join entered"]
    );
}

#[test]
fn cleanup_failure_returns_before_join_and_drop_retries_unresolved_step() {
    let (driver, trace) = FakeDriver::new(vec![("cursor+", FailMode::Before)]);
    {
        let mut session = TerminalSession::enter(Arc::new(driver)).unwrap();
        let error = restore_before_join(
            &mut session,
            || {},
            || {},
            || -> () { panic!("producer join must not be entered after cleanup failure") },
        )
        .unwrap_err();
        assert_eq!(cleanup_operations(&error), ["show cursor"]);
        assert_eq!(session.states().cursor, StepState::AttemptedOrActive);
    }

    let trace = trace.lock().unwrap();
    assert_eq!(
        trace
            .calls
            .iter()
            .filter(|call| **call == "cursor+")
            .count(),
        2,
        "Drop must retry the unresolved inverse without entering producer join"
    );
    assert!(!trace.active.iter().any(|active| *active));
}
#[cfg(target_os = "linux")]
fn read_pty_output(mut master: std::fs::File) -> Vec<u8> {
    use std::io::Read;

    let mut output = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match master.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => output.extend_from_slice(&buffer[..read]),
            Err(error) if error.raw_os_error() == Some(nix::libc::EIO) => break,
            Err(error) => panic!("failed to read PTY master: {error}"),
        }
    }
    output
}

#[cfg(target_os = "linux")]
fn assert_inverse_sequences(output: &[u8]) {
    fn position(output: &[u8], sequence: &[u8]) -> usize {
        output
            .windows(sequence.len())
            .position(|window| window == sequence)
            .unwrap_or_else(|| panic!("missing terminal sequence {sequence:?} in {output:?}"))
    }

    let enter_alternate = position(output, b"\x1b[?1049h");
    let enable_mouse = position(output, b"\x1b[?1000h");
    let hide_cursor = position(output, b"\x1b[?25l");
    let show_cursor = position(output, b"\x1b[?25h");
    let disable_mouse = position(output, b"\x1b[?1000l");
    let leave_alternate = position(output, b"\x1b[?1049l");
    assert!(enter_alternate < enable_mouse);
    assert!(enable_mouse < hide_cursor);
    assert!(hide_cursor < show_cursor);
    assert!(show_cursor < disable_mouse);
    assert!(disable_mouse < leave_alternate);
}

#[cfg(target_os = "linux")]
fn current_termios() -> nix::sys::termios::Termios {
    use std::os::fd::BorrowedFd;

    // SAFETY: standard input remains open for the duration of this immediate query.
    let stdin = unsafe { BorrowedFd::borrow_raw(nix::libc::STDIN_FILENO) };
    nix::sys::termios::tcgetattr(stdin).expect("stdin must be a real TTY")
}

#[cfg(target_os = "linux")]
fn assert_manual_restoration<F>(scenario: F)
where
    F: FnOnce() + std::panic::UnwindSafe,
{
    let before = current_termios();
    scenario();
    let after = current_termios();
    assert_eq!(
        after, before,
        "production session did not restore complete termios state"
    );
}

#[test]
fn production_checked_entry_accepts_the_frozen_arc_boundary() {
    use fccli::terminal::CrosstermTerminalDriver;

    let _: fn(Arc<dyn TerminalDriver>) -> Result<TerminalSession, TerminalLifecycleError> =
        CrosstermTerminalDriver::enter_checked;
    let driver = CrosstermTerminalDriver::production_driver();
    let dependency_driver = Arc::clone(&driver);
    assert!(Arc::ptr_eq(&driver, &dependency_driver));
}

#[cfg(target_os = "linux")]
#[test]
fn linux_openpty_restores_kernel_state() {
    use std::{
        fs::File,
        os::{fd::AsRawFd, unix::process::CommandExt},
        process::Command,
    };

    use fccli::terminal::CrosstermTerminalDriver;
    use nix::{pty::openpty, sys::termios};

    const HELPER_ENV: &str = "FCCLI_TERMINAL_PTY_HELPER";
    if std::env::var_os(HELPER_ENV).is_some() {
        let before = current_termios();
        let driver: Arc<dyn TerminalDriver> = CrosstermTerminalDriver::production_driver();
        let dependency_driver = Arc::clone(&driver);
        let pre_entry_probe = current_termios();
        assert_eq!(
            pre_entry_probe, before,
            "simulated pre-entry work must leave terminal state untouched"
        );
        let session = CrosstermTerminalDriver::enter_checked(dependency_driver)
            .expect("production driver must accept PTY stdin/stdout");
        assert!(
            Arc::ptr_eq(session.driver(), &driver),
            "checked entry must use the already-constructed RunDependencies handle after pre-entry work"
        );
        let active = current_termios();
        assert!(
            !active
                .local_flags
                .intersects(termios::LocalFlags::ECHO | termios::LocalFlags::ICANON),
            "production session must disable kernel ECHO and ICANON"
        );
        assert_ne!(
            active, before,
            "production session must change complete kernel termios state"
        );
        let primary: AppError = ProviderError::Configuration("PTY provider probe").into();
        let error = session
            .finish(Some(primary))
            .expect_err("provider failure must be returned");
        assert!(matches!(
            error,
            TerminalLifecycleError::Primary(AppError::Provider(ProviderError::Configuration(
                "PTY provider probe"
            )))
        ));
        return;
    }

    let pty = openpty(None, None).expect("openpty");
    let initial = termios::tcgetattr(&pty.slave).expect("initial slave termios");
    let slave_fd = pty.slave.as_raw_fd();
    let mut child = Command::new(std::env::current_exe().expect("current test executable"));
    child
        .arg("--exact")
        .arg("linux_openpty_restores_kernel_state")
        .arg("--nocapture")
        .env(HELPER_ENV, "1");
    // SAFETY: the closure only invokes async-signal-safe dup2 calls before exec. The owned slave
    // descriptor remains alive in the parent until spawn returns.
    unsafe {
        child.pre_exec(move || {
            if nix::libc::dup2(slave_fd, nix::libc::STDIN_FILENO) == -1
                || nix::libc::dup2(slave_fd, nix::libc::STDOUT_FILENO) == -1
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = child.spawn().expect("spawn PTY helper");
    let status = child.wait().expect("wait for PTY helper");
    let restored = termios::tcgetattr(&pty.slave).expect("restored slave termios");
    assert!(status.success(), "PTY helper exited with {status}");
    assert_eq!(
        restored, initial,
        "helper did not restore complete kernel termios state"
    );
    drop(pty.slave);
    let master: File = pty.master.into();
    assert_inverse_sequences(&read_pty_output(master));
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "manual real TTY provider-error restoration probe"]
fn manual_real_tty_provider_error_restores() {
    use fccli::terminal::CrosstermTerminalDriver;

    assert_manual_restoration(|| {
        let before_raw = current_termios();
        let session =
            CrosstermTerminalDriver::enter_checked(CrosstermTerminalDriver::production_driver())
                .unwrap();
        assert_ne!(
            current_termios(),
            before_raw,
            "session did not enter raw mode"
        );
        let error = session
            .finish(Some(
                ProviderError::Configuration("manual provider probe").into(),
            ))
            .unwrap_err();
        assert!(matches!(
            error,
            TerminalLifecycleError::Primary(AppError::Provider(_))
        ));
    });
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "manual real TTY render-error restoration probe"]
fn manual_real_tty_render_error_restores() {
    use fccli::terminal::CrosstermTerminalDriver;

    assert_manual_restoration(|| {
        let before_raw = current_termios();
        let session =
            CrosstermTerminalDriver::enter_checked(CrosstermTerminalDriver::production_driver())
                .unwrap();
        assert_ne!(
            current_termios(),
            before_raw,
            "session did not enter raw mode"
        );
        let error = session
            .finish(Some(RenderError::InsufficientSpace.into()))
            .unwrap_err();
        assert!(matches!(
            error,
            TerminalLifecycleError::Primary(AppError::Render(_))
        ));
    });
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "manual real TTY panic restoration probe"]
fn manual_real_tty_panic_restores() {
    use fccli::terminal::CrosstermTerminalDriver;

    assert_manual_restoration(|| {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let before_raw = current_termios();
            let _session = CrosstermTerminalDriver::enter_checked(
                CrosstermTerminalDriver::production_driver(),
            )
            .unwrap();
            assert_ne!(
                current_termios(),
                before_raw,
                "session did not enter raw mode"
            );
            panic!("manual panic restoration probe");
        }));
        assert!(result.is_err(), "panic scenario did not unwind");
    });
}

#[test]
fn cleanup_failures_are_sanitized() {
    let (driver, _) = FakeDriver::new(vec![("raw-", FailMode::Before)]);
    let error = TerminalSession::enter(Arc::new(driver))
        .unwrap()
        .finish(None)
        .unwrap_err();
    let TerminalLifecycleError::Cleanup(cleanup) = error else {
        panic!("expected cleanup")
    };
    assert_eq!(cleanup.failures()[0].cause, SanitizedCause::Io);
    assert!(!cleanup.to_string().contains("injected"));
}
