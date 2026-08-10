//! Command-line parsing and local, side-effect-free instrument canonicalization.

use std::{error::Error as _, ffi::OsString};

use clap::{CommandFactory, Parser, error::ErrorKind};

use crate::model::{
    Instrument, InstrumentSpec, MAX_PROVIDER_SYMBOL_LEN, Market, ProviderId, Timeframe,
};

const DEFAULT_PROVIDER: &str = "binance";
const DEFAULT_QUOTE: &str = "USDT";

/// The output mode selected by the command line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Snapshot,
    Interactive,
}

/// Parsed command-line input. Parsing performs no terminal, runtime, registry, or network work.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "fccli",
    version,
    about = "Render Binance Spot candlestick charts",
    after_help = "Examples:\n  fccli btc 1m\n  fccli binance:btc/usdc 1h\n  fccli BTCUSDT 1M --interactive"
)]
pub struct Cli {
    /// Instrument as ASSET, BASE/QUOTE, BASE-QUOTE, or PROVIDER:INSTRUMENT
    #[arg(value_name = "INSTRUMENT", value_parser = parse_instrument_spec)]
    instrument: InstrumentSpec,

    /// Candle interval (case-sensitive; for example 1m, 1h, or 1M)
    #[arg(value_name = "TIMEFRAME", value_parser = parse_timeframe)]
    timeframe: Timeframe,

    /// Run the interactive terminal UI instead of rendering one snapshot
    #[arg(short = 'i', long)]
    interactive: bool,
}

impl Cli {
    #[must_use]
    pub const fn instrument(&self) -> &InstrumentSpec {
        &self.instrument
    }

    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    #[must_use]
    pub const fn interactive(&self) -> bool {
        self.interactive
    }

    #[must_use]
    pub const fn mode(&self) -> Mode {
        if self.interactive {
            Mode::Interactive
        } else {
            Mode::Snapshot
        }
    }

    /// Render the Clap command without executing any application behavior.
    #[must_use]
    pub fn command() -> clap::Command {
        <Self as CommandFactory>::command()
    }

    /// Parse an argument iterator before any application side effects are created.
    pub fn try_parse_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        <Self as Parser>::try_parse_from(args).map_err(sanitize_parse_error)
    }
}

/// A stable, actionable failure from Binance-local canonicalization.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CanonicalizationError {
    #[error(
        "unsupported provider `{provider}`; use lowercase `binance` (for example `binance:btc`)"
    )]
    UnsupportedProvider { provider: ProviderId },

    #[error(
        "instrument must include a base asset; `USDT` alone is only a quote (try `btc` or `btc/usdt`)"
    )]
    QuoteOnly,

    #[error("instrument could not be canonicalized; try `btc`, `BTCUSDT`, or `btc/usdt`")]
    InvalidInstrument,
}

/// Resolve a provider-neutral specification to Binance Spot identifiers without I/O.
pub fn canonicalize_binance(
    specification: &InstrumentSpec,
) -> Result<Instrument, CanonicalizationError> {
    if specification.provider().as_str() != DEFAULT_PROVIDER {
        return Err(CanonicalizationError::UnsupportedProvider {
            provider: specification.provider().clone(),
        });
    }

    let base = specification.base();
    let quote = specification.quote();
    validate_symbol_lengths(base, quote).map_err(|()| CanonicalizationError::InvalidInstrument)?;

    let normalized_base = base.to_ascii_uppercase();
    let normalized_quote = quote.map(str::to_ascii_uppercase);
    let (base, quote) = match normalized_quote.as_deref() {
        Some(quote) => (normalized_base.as_str(), quote),
        None => {
            if let Some(base) = normalized_base.strip_suffix(DEFAULT_QUOTE) {
                if base.is_empty() {
                    return Err(CanonicalizationError::QuoteOnly);
                }
                (base, DEFAULT_QUOTE)
            } else {
                (normalized_base.as_str(), DEFAULT_QUOTE)
            }
        }
    };

    let provider_symbol = format!("{base}{quote}");
    Instrument::new(
        specification.provider().clone(),
        Market::Spot,
        base,
        quote,
        provider_symbol,
    )
    .map_err(|_| CanonicalizationError::InvalidInstrument)
}

fn parse_timeframe(value: &str) -> Result<Timeframe, String> {
    value.parse().map_err(|_| {
        "unsupported timeframe; use one of: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M (case-sensitive)".to_owned()
    })
}

fn parse_instrument_spec(value: &str) -> Result<InstrumentSpec, String> {
    let (provider, pair) = split_provider(value)?;
    let (base, quote) = split_pair(pair)?;
    validate_symbol_lengths(base, quote).map_err(|()| invalid_instrument_message())?;

    InstrumentSpec::new(
        provider,
        base.to_ascii_uppercase(),
        quote.map(str::to_ascii_uppercase),
    )
    .map_err(|_| invalid_instrument_message())
}

fn split_provider(value: &str) -> Result<(ProviderId, &str), String> {
    let mut parts = value.split(':');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() {
        return Err(
            "invalid provider prefix; use at most one `:` (for example `binance:btc`)".to_owned(),
        );
    }

    match second {
        Some(pair) => {
            if first.len() > MAX_PROVIDER_SYMBOL_LEN {
                return Err("invalid provider; provider names must be at most 256 ASCII letters or digits (for example `binance`)".to_owned());
            }
            let provider = ProviderId::new(first).map_err(|_| {
                "invalid provider; provider names must be nonempty ASCII letters or digits (for example `binance`)".to_owned()
            })?;
            if pair.is_empty() {
                return Err(
                    "missing instrument after provider prefix; try `binance:btc`".to_owned(),
                );
            }
            Ok((provider, pair))
        }
        None => Ok((
            ProviderId::new(DEFAULT_PROVIDER).expect("locked default provider is valid"),
            first,
        )),
    }
}

fn split_pair(value: &str) -> Result<(&str, Option<&str>), String> {
    let slash_count = value.bytes().filter(|byte| *byte == b'/').count();
    let dash_count = value.bytes().filter(|byte| *byte == b'-').count();

    if slash_count > 0 && dash_count > 0 {
        return Err(
            "invalid instrument; do not mix `/` and `-` separators (try `btc/usdt`)".to_owned(),
        );
    }
    if slash_count > 1 || dash_count > 1 {
        return Err(
            "invalid instrument; use at most one pair separator (try `btc/usdt`)".to_owned(),
        );
    }

    let separator = if slash_count == 1 {
        Some('/')
    } else if dash_count == 1 {
        Some('-')
    } else {
        None
    };

    let (base, quote) = match separator {
        Some(separator) => {
            let (base, quote) = value
                .split_once(separator)
                .expect("counted separator must be present");
            (base, Some(quote))
        }
        None => (value, None),
    };

    if base.is_empty() || quote.is_some_and(str::is_empty) {
        return Err(
            "invalid instrument; base and quote must be nonempty (try `btc/usdt`)".to_owned(),
        );
    }
    if !base.bytes().all(|byte| byte.is_ascii_alphanumeric())
        || quote.is_some_and(|quote| !quote.bytes().all(|byte| byte.is_ascii_alphanumeric()))
    {
        return Err(
            "invalid instrument; components must contain only ASCII letters and digits (try `btc/usdt`)".to_owned(),
        );
    }

    Ok((base, quote))
}

fn validate_symbol_lengths(base: &str, quote: Option<&str>) -> Result<(), ()> {
    let projected_quote_len = match quote {
        Some(quote) => quote.len(),
        None if base
            .get(base.len().saturating_sub(DEFAULT_QUOTE.len())..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(DEFAULT_QUOTE)) =>
        {
            0
        }
        None => DEFAULT_QUOTE.len(),
    };

    if base.len() > MAX_PROVIDER_SYMBOL_LEN
        || quote.is_some_and(|quote| quote.len() > MAX_PROVIDER_SYMBOL_LEN)
        || base
            .len()
            .checked_add(projected_quote_len)
            .is_none_or(|length| length > MAX_PROVIDER_SYMBOL_LEN)
    {
        return Err(());
    }
    Ok(())
}

fn invalid_instrument_message() -> String {
    "invalid instrument; components must be nonempty ASCII letters or digits and the combined symbol must be at most 256 bytes (for example `btc`, `BTCUSDT`, or `binance:btc/usdt`)".to_owned()
}

fn sanitize_parse_error(error: clap::Error) -> clap::Error {
    let kind = error.kind();
    if matches!(kind, ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
        return error;
    }

    let parser_message = error.source().map(ToString::to_string).unwrap_or_default();
    let message = match kind {
        ErrorKind::UnknownArgument => {
            "unknown argument; use `--help` to list options (for example `--interactive`)"
        }
        ErrorKind::InvalidValue | ErrorKind::ValueValidation
            if parser_message.contains("unsupported timeframe") =>
        {
            "unsupported timeframe; use one of: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M (case-sensitive)"
        }
        ErrorKind::InvalidValue | ErrorKind::ValueValidation
            if parser_message.contains("invalid provider") =>
        {
            "invalid provider; use nonempty ASCII letters or digits (for example `binance`)"
        }
        ErrorKind::InvalidValue | ErrorKind::ValueValidation
            if parser_message.contains("missing instrument") =>
        {
            "missing instrument after provider prefix; try `binance:btc`"
        }
        ErrorKind::InvalidValue | ErrorKind::ValueValidation => {
            "invalid instrument; use `btc`, `BTCUSDT`, or `binance:btc/usdt`"
        }
        ErrorKind::InvalidUtf8 => {
            "argument is not valid UTF-8; use ASCII instrument and timeframe values"
        }
        ErrorKind::MissingRequiredArgument | ErrorKind::TooFewValues => {
            "missing required arguments <INSTRUMENT> <TIMEFRAME>; use `fccli btc 1m`"
        }
        ErrorKind::ArgumentConflict => {
            "conflicting arguments; use `--help` to list the accepted command form"
        }
        ErrorKind::TooManyValues | ErrorKind::WrongNumberOfValues => {
            "unexpected extra argument; use `fccli btc 1m`"
        }
        _ => "invalid command line; use `fccli --help` for accepted arguments",
    };
    clap::Error::raw(kind, message)
}
