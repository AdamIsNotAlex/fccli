//! Command-line parsing and local, side-effect-free instrument canonicalization.

use std::ffi::OsString;

use clap::{CommandFactory, Parser};

use crate::model::{Instrument, InstrumentSpec, Market, ProviderId, Timeframe};

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
    #[arg(
        value_name = "INSTRUMENT",
        value_parser = parse_instrument_spec,
        allow_hyphen_values = true
    )]
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
        <Self as Parser>::try_parse_from(args)
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

    let (base, quote) = match specification.quote() {
        Some(quote) => (specification.base(), quote),
        None => {
            let token = specification.base();
            if let Some(base) = token.strip_suffix(DEFAULT_QUOTE) {
                if base.is_empty() {
                    return Err(CanonicalizationError::QuoteOnly);
                }
                (base, DEFAULT_QUOTE)
            } else {
                (token, DEFAULT_QUOTE)
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
        format!(
            "unsupported timeframe `{value}`; use one of: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M (case-sensitive)"
        )
    })
}

fn parse_instrument_spec(value: &str) -> Result<InstrumentSpec, String> {
    let (provider, pair) = split_provider(value)?;
    let (base, quote) = split_pair(pair)?;

    InstrumentSpec::new(provider, base.to_ascii_uppercase(), quote.map(str::to_ascii_uppercase))
        .map_err(|_| {
            format!(
                "invalid instrument `{value}`; components must be nonempty ASCII letters or digits (for example `btc`, `BTCUSDT`, or `binance:btc/usdt`)"
            )
        })
}

fn split_provider(value: &str) -> Result<(ProviderId, &str), String> {
    let mut parts = value.split(':');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() {
        return Err(format!(
            "invalid provider prefix in `{value}`; use at most one `:` (for example `binance:btc`)"
        ));
    }

    match second {
        Some(pair) => {
            let provider = ProviderId::new(first).map_err(|_| {
                format!(
                    "invalid provider `{first}`; provider names must be nonempty ASCII letters or digits (for example `binance`)"
                )
            })?;
            if pair.is_empty() {
                return Err(format!(
                    "missing instrument after provider prefix in `{value}`; try `binance:btc`"
                ));
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
        return Err(format!(
            "invalid instrument `{value}`; do not mix `/` and `-` separators (try `btc/usdt`)"
        ));
    }
    if slash_count > 1 || dash_count > 1 {
        return Err(format!(
            "invalid instrument `{value}`; use at most one pair separator (try `btc/usdt`)"
        ));
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
        return Err(format!(
            "invalid instrument `{value}`; base and quote must be nonempty (try `btc/usdt`)"
        ));
    }
    if !base.bytes().all(|byte| byte.is_ascii_alphanumeric())
        || quote.is_some_and(|quote| !quote.bytes().all(|byte| byte.is_ascii_alphanumeric()))
    {
        return Err(format!(
            "invalid instrument `{value}`; components must contain only ASCII letters and digits (try `btc/usdt`)"
        ));
    }

    Ok((base, quote))
}
