use std::{
    ffi::OsString,
    io::{self, Write},
    process::ExitCode,
};

use fccli::cli::{Cli, canonicalize_instrument};

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
use fccli::{
    app::{CrosstermTerminalInput, RunDependencies, run_with_dependencies},
    chart::{detect_render_policy, no_color_present},
    clock::{Clock, SystemClock},
    provider::{ProviderRegistry, binance::BinanceProvider},
    terminal::CrosstermTerminalDriver,
};
#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
use std::{io::IsTerminal, sync::Arc};

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().collect();
    let cli = match parse_cli(&args) {
        Ok(cli) => cli,
        Err(code) => return code,
    };
    if let Err(error) = canonicalize_instrument(cli.instrument()) {
        eprintln!("fccli: {error}");
        return ExitCode::FAILURE;
    }
    #[cfg(any(
        all(feature = "production-transport", not(feature = "test-transport")),
        all(feature = "test-transport", not(feature = "production-transport"))
    ))]
    {
        run_valid(args)
    }
    #[cfg(all(feature = "production-transport", feature = "test-transport"))]
    {
        let _ = args;
        ExitCode::FAILURE
    }
}

fn parse_cli(args: &[OsString]) -> Result<Cli, ExitCode> {
    Cli::try_parse_from(args.iter().cloned()).map_err(|error| {
        let mut target: Box<dyn Write> = if error.use_stderr() {
            Box::new(io::stderr())
        } else {
            Box::new(io::stdout())
        };
        let _ = target.write_all(error.to_string().as_bytes());
        ExitCode::from(if error.use_stderr() { 2 } else { 0 })
    })
}

#[cfg(all(feature = "production-transport", not(feature = "test-transport")))]
fn run_valid(args: Vec<OsString>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("fccli: failed to start async runtime");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        let stdin_is_tty = io::stdin().is_terminal();
        let stdout_is_tty = io::stdout().is_terminal();
        let no_color = std::env::var_os("NO_COLOR");
        let render_policy =
            detect_render_policy(stdout_is_tty, no_color_present(no_color.as_deref()));
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let provider = match BinanceProvider::new(Arc::clone(&clock)) {
            Ok(provider) => Arc::new(provider),
            Err(error) => {
                eprintln!("fccli: {error}");
                return ExitCode::FAILURE;
            }
        };
        let dependencies = RunDependencies {
            providers: ProviderRegistry::new(provider),
            clock,
            terminal: CrosstermTerminalDriver::production_driver(),
            input: Box::new(CrosstermTerminalInput::new()),
            stdout: Box::new(io::stdout()),
            stderr: Box::new(io::stderr()),
            stdin_is_tty,
            stdout_is_tty,
            render_policy,
        };
        match run_with_dependencies(args, dependencies).await {
            Ok(code) => code,
            Err(error) => {
                eprintln!("fccli: {error}");
                ExitCode::FAILURE
            }
        }
    })
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
fn run_valid(_args: Vec<OsString>) -> ExitCode {
    // Valid modes are integration-tested through `run_with_dependencies` with explicit local
    // dependencies. The test-feature binary exists only for parse-time help/version/error exits.
    eprintln!("fccli: valid modes require direct injected dependencies in test builds");
    ExitCode::FAILURE
}
