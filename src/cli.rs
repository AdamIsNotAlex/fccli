//! Command-line parsing and local, side-effect-free instrument canonicalization.

use std::ffi::OsString;

use clap::{CommandFactory, Parser, error::ErrorKind};

use crate::model::{
    Instrument, InstrumentSpec, MAX_PROVIDER_SYMBOL_LEN, Market, ProviderId, Timeframe,
};

const DEFAULT_PROVIDER: &str = "binance";
const DEFAULT_INSTRUMENT: &str = "binance:btc";
const DEFAULT_TIMEFRAME: &str = "1h";
const DEFAULT_QUOTE: &str = "USDT";
const EXTRA_ARGUMENT_ERROR: &str =
    "unexpected extra argument; use up to an instrument and timeframe";

/// A provider-neutral interactive market/timeframe target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketTarget {
    pub instrument: InstrumentSpec,
    pub timeframe: Timeframe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TargetParseError {
    ExtraArgument,
    UnsupportedTimeframe,
    InvalidProvider,
    MissingInstrument,
    InvalidInstrument,
}

impl std::fmt::Display for TargetParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExtraArgument => formatter.write_str(EXTRA_ARGUMENT_ERROR),
            Self::UnsupportedTimeframe => formatter.write_str(&timeframe_error_message()),
            Self::InvalidProvider => formatter.write_str(
                "invalid provider; use nonempty ASCII letters or digits (for example `binance`)",
            ),
            Self::MissingInstrument => {
                formatter.write_str("missing instrument after provider prefix; try `binance:btc`")
            }
            Self::InvalidInstrument => formatter
                .write_str("invalid instrument; use `btc`, `BTCUSDT`, or `binance:btc/usdt`"),
        }
    }
}

/// Parse the same instrument and timeframe grammar used by startup CLI arguments.
pub fn parse_market_target(value: &str) -> Result<MarketTarget, String> {
    let mut fields = value.split_whitespace();
    let instrument = fields.next();
    let timeframe = fields.next();
    if fields.next().is_some() {
        return Err(TargetParseError::ExtraArgument.to_string());
    }
    resolve_market_target(instrument, timeframe).map_err(|error| error.to_string())
}

fn resolve_market_target(
    instrument: Option<&str>,
    timeframe: Option<&str>,
) -> Result<MarketTarget, TargetParseError> {
    Ok(MarketTarget {
        instrument: parse_instrument_spec(instrument.unwrap_or(DEFAULT_INSTRUMENT))?,
        timeframe: parse_timeframe(timeframe.unwrap_or(DEFAULT_TIMEFRAME))?,
    })
}

/// The output mode selected by the command line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Snapshot,
    Interactive,
}

/// Parsed command-line input. Parsing performs no terminal, runtime, registry, or network work.
#[derive(Clone, Debug)]
pub struct Cli {
    instrument: InstrumentSpec,
    timeframe: Timeframe,
    interactive: bool,
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "fccli",
    version,
    about = "Render Binance Spot candlestick charts",
    after_help = "Examples:\n  fccli\n  fccli eth\n  fccli btc h\n  fccli binance:btc/usdc 1h\n  fccli BTCUSDT M --interactive"
)]
struct RawCli {
    /// Instrument as ASSET, BASE/QUOTE, BASE-QUOTE, or PROVIDER:INSTRUMENT (default: binance:btc)
    #[arg(value_name = "INSTRUMENT")]
    instrument: Option<String>,

    /// Candle interval; unit-only s/m/h/d/w/M means 1 (default: 1h; case-sensitive)
    #[arg(value_name = "TIMEFRAME")]
    timeframe: Option<String>,

    /// Run the interactive terminal UI instead of rendering one snapshot
    #[arg(short = 'i', long)]
    interactive: bool,

    #[arg(hide = true)]
    extra: Vec<String>,
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
        <RawCli as CommandFactory>::command()
    }

    /// Parse an argument iterator before any application side effects are created.
    pub fn try_parse_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let raw = <RawCli as Parser>::try_parse_from(args).map_err(sanitize_parse_error)?;
        if !raw.extra.is_empty() {
            return Err(clap::Error::raw(
                ErrorKind::TooManyValues,
                TargetParseError::ExtraArgument.to_string(),
            ));
        }
        let target = resolve_market_target(raw.instrument.as_deref(), raw.timeframe.as_deref())
            .map_err(|error| clap::Error::raw(ErrorKind::ValueValidation, error.to_string()))?;
        Ok(Self {
            instrument: target.instrument,
            timeframe: target.timeframe,
            interactive: raw.interactive,
        })
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

fn timeframe_error_message() -> String {
    let spellings = Timeframe::INPUT_SPELLINGS
        .map(|(spelling, _)| spelling)
        .join(", ");
    format!(
        "unsupported timeframe; use one of: {spellings} (unit-only values mean 1; case-sensitive)"
    )
}

fn parse_timeframe(value: &str) -> Result<Timeframe, TargetParseError> {
    value
        .parse()
        .map_err(|_| TargetParseError::UnsupportedTimeframe)
}

fn parse_instrument_spec(value: &str) -> Result<InstrumentSpec, TargetParseError> {
    let (provider, pair) = split_provider(value)?;
    let (base, quote) = split_pair(pair)?;
    validate_symbol_lengths(base, quote).map_err(|()| TargetParseError::InvalidInstrument)?;

    InstrumentSpec::new(
        provider,
        base.to_ascii_uppercase(),
        quote.map(str::to_ascii_uppercase),
    )
    .map_err(|_| TargetParseError::InvalidInstrument)
}

fn split_provider(value: &str) -> Result<(ProviderId, &str), TargetParseError> {
    let mut parts = value.split(':');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() {
        return Err(TargetParseError::InvalidProvider);
    }

    match second {
        Some(pair) => {
            if first.len() > MAX_PROVIDER_SYMBOL_LEN {
                return Err(TargetParseError::InvalidProvider);
            }
            let provider = ProviderId::new(first).map_err(|_| TargetParseError::InvalidProvider)?;
            if pair.is_empty() {
                return Err(TargetParseError::MissingInstrument);
            }
            Ok((provider, pair))
        }
        None => Ok((
            ProviderId::new(DEFAULT_PROVIDER).expect("locked default provider is valid"),
            first,
        )),
    }
}

fn split_pair(value: &str) -> Result<(&str, Option<&str>), TargetParseError> {
    let slash_count = value.bytes().filter(|byte| *byte == b'/').count();
    let dash_count = value.bytes().filter(|byte| *byte == b'-').count();

    if slash_count > 0 && dash_count > 0 || slash_count > 1 || dash_count > 1 {
        return Err(TargetParseError::InvalidInstrument);
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

    if base.is_empty()
        || quote.is_some_and(str::is_empty)
        || !base.bytes().all(|byte| byte.is_ascii_alphanumeric())
        || quote.is_some_and(|quote| !quote.bytes().all(|byte| byte.is_ascii_alphanumeric()))
    {
        return Err(TargetParseError::InvalidInstrument);
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

fn sanitize_parse_error(error: clap::Error) -> clap::Error {
    let kind = error.kind();
    if matches!(kind, ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
        return error;
    }

    let rendered = error.to_string();
    let message = match kind {
        ErrorKind::UnknownArgument if !rendered.contains("--") => EXTRA_ARGUMENT_ERROR,
        ErrorKind::UnknownArgument => {
            "unknown argument; use `--help` to list options (for example `--interactive`)"
        }
        ErrorKind::InvalidUtf8 => {
            "argument is not valid UTF-8; use ASCII instrument and timeframe values"
        }
        ErrorKind::MissingRequiredArgument | ErrorKind::TooFewValues => {
            "missing required option value; use `fccli --help` for accepted arguments"
        }
        ErrorKind::ArgumentConflict => {
            "conflicting arguments; use `--help` to list the accepted command form"
        }
        ErrorKind::TooManyValues | ErrorKind::WrongNumberOfValues => EXTRA_ARGUMENT_ERROR,
        _ => "invalid command line; use `fccli --help` for accepted arguments",
    };
    clap::Error::raw(kind, message)
}
