//! Command-line parsing and local, side-effect-free instrument canonicalization.

use std::ffi::OsString;

use clap::{CommandFactory, Parser, error::ErrorKind};

use crate::model::{
    Instrument, InstrumentSpec, MAX_PROVIDER_SYMBOL_LEN, Market, ProviderId, Timeframe,
    is_spot_index_token,
};

const DEFAULT_PROVIDER: &str = "binance";
const DEFAULT_INSTRUMENT: &str = "binance:btc";
const DEFAULT_TIMEFRAME: &str = "1h";
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
    Hip3RequiresPerpetual,
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
            Self::InvalidInstrument => formatter.write_str(
                "invalid instrument; use `btc`, `BTCUSDT`, `btc.p`, `binance:btc/usdt`, or `hyperliquid:btc.p`",
            ),
            Self::Hip3RequiresPerpetual => formatter.write_str(
                "HIP-3 builder DEX markets are perpetual-only; use `hyperliquid:<dex>:<coin>.p`",
            ),
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
    about = "Render Binance and Hyperliquid Spot and Perpetual candlestick charts",
    after_help = "Examples:\n  fccli\n  fccli eth\n  fccli btc h\n  fccli btc.p\n  fccli binance:btc/usdc 1h\n  fccli hyperliquid:btc.p 1h\n  fccli BTCUSDT.p M --interactive"
)]
struct RawCli {
    /// Instrument as ASSET, BASE/QUOTE, BASE-QUOTE, PROVIDER:INSTRUMENT, or hyperliquid:<dex>:<coin>.p; trailing .p selects perpetual (default: binance:btc)
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

/// A stable, actionable failure from local instrument canonicalization.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CanonicalizationError {
    #[error(
        "provider `{provider}` has no default-quote rule; use one of `binance`, `okx`, `bybit`, `coinbase`, `kraken`, or `hyperliquid` (lowercase)"
    )]
    UnsupportedProvider { provider: ProviderId },

    #[error(
        "instrument must include a base asset; the selected provider's default quote alone is only a quote (try `btc` or an explicit pair such as `btc/usdt`)"
    )]
    QuoteOnly,

    #[error(
        "instrument could not be canonicalized; try an asset such as `btc` or an explicit pair such as `btc/usdt`"
    )]
    InvalidInstrument,
}

/// Canonicalization metadata only. Presence here is not a registered transport.
fn default_quote_for(provider: &ProviderId) -> Option<&'static str> {
    match provider.as_str() {
        "binance" | "okx" | "bybit" => Some("USDT"),
        "coinbase" | "kraken" => Some("USD"),
        "hyperliquid" => Some("USDC"),
        _ => None,
    }
}

/// Resolve a provider-neutral specification to locked identifiers without I/O.
pub fn canonicalize_instrument(
    specification: &InstrumentSpec,
) -> Result<Instrument, CanonicalizationError> {
    let Some(default_quote) = default_quote_for(specification.provider()) else {
        return Err(CanonicalizationError::UnsupportedProvider {
            provider: specification.provider().clone(),
        });
    };

    let base = specification.base();
    let quote = specification.quote();
    validate_symbol_lengths(base, quote, Some(default_quote))
        .map_err(|()| CanonicalizationError::InvalidInstrument)?;

    let normalized_base = base.to_ascii_uppercase();
    let normalized_quote = quote.map(str::to_ascii_uppercase);
    let (base, quote) = match normalized_quote.as_deref() {
        Some(quote) => (normalized_base.as_str(), quote),
        None => {
            if let Some(base) = normalized_base.strip_suffix(default_quote) {
                if base.is_empty() {
                    return Err(CanonicalizationError::QuoteOnly);
                }
                (base, default_quote)
            } else {
                (normalized_base.as_str(), default_quote)
            }
        }
    };

    let provider_symbol = format!("{base}{quote}");
    Instrument::new(
        specification.provider().clone(),
        specification.market(),
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
    let (pair, market) = strip_perpetual_suffix(pair)?;
    if provider.as_str() == "hyperliquid" {
        if let Some((dex, coin)) = pair.split_once(':') {
            if market != Market::Perpetual {
                return Err(TargetParseError::Hip3RequiresPerpetual);
            }
            if dex.is_empty()
                || coin.is_empty()
                || !dex.bytes().all(|byte| byte.is_ascii_alphanumeric())
                || !coin.bytes().all(|byte| byte.is_ascii_alphanumeric())
            {
                return Err(TargetParseError::InvalidInstrument);
            }
            validate_symbol_lengths(coin, None, default_quote_for(&provider))
                .map_err(|()| TargetParseError::InvalidInstrument)?;
            return InstrumentSpec::new_with_market_and_venue(
                provider,
                market,
                coin.to_ascii_uppercase(),
                None::<String>,
                Some(dex.to_ascii_lowercase()),
            )
            .map_err(|_| TargetParseError::InvalidInstrument);
        }
        if is_spot_index_token(pair) {
            if market != Market::Spot {
                return Err(TargetParseError::InvalidInstrument);
            }
            validate_symbol_lengths(pair, None, default_quote_for(&provider))
                .map_err(|()| TargetParseError::InvalidInstrument)?;
            return InstrumentSpec::new_with_market(provider, market, pair, None::<String>)
                .map_err(|_| TargetParseError::InvalidInstrument);
        }
    }
    let (base, quote) = split_pair(pair)?;
    validate_symbol_lengths(base, quote, default_quote_for(&provider))
        .map_err(|()| TargetParseError::InvalidInstrument)?;

    InstrumentSpec::new_with_market(
        provider,
        market,
        base.to_ascii_uppercase(),
        quote.map(str::to_ascii_uppercase),
    )
    .map_err(|_| TargetParseError::InvalidInstrument)
}

fn strip_perpetual_suffix(value: &str) -> Result<(&str, Market), TargetParseError> {
    if let Some(stripped) = value
        .strip_suffix(".p")
        .or_else(|| value.strip_suffix(".P"))
    {
        if stripped.is_empty()
            || stripped.ends_with(".p")
            || stripped.ends_with(".P")
            || stripped.ends_with('.')
        {
            return Err(TargetParseError::InvalidInstrument);
        }
        return Ok((stripped, Market::Perpetual));
    }
    if value.ends_with('.') {
        return Err(TargetParseError::InvalidInstrument);
    }
    Ok((value, Market::Spot))
}

fn split_provider(value: &str) -> Result<(ProviderId, &str), TargetParseError> {
    let Some((first, rest)) = value.split_once(':') else {
        return Ok((
            ProviderId::new(DEFAULT_PROVIDER).expect("locked default provider is valid"),
            value,
        ));
    };
    if first.len() > MAX_PROVIDER_SYMBOL_LEN {
        return Err(TargetParseError::InvalidProvider);
    }
    let provider = ProviderId::new(first).map_err(|_| TargetParseError::InvalidProvider)?;
    if rest.is_empty() {
        return Err(TargetParseError::MissingInstrument);
    }
    if first == "hyperliquid" {
        if rest.bytes().filter(|byte| *byte == b':').count() > 1 {
            return Err(TargetParseError::InvalidInstrument);
        }
        return Ok((provider, rest));
    }
    if rest.contains(':') {
        return Err(TargetParseError::InvalidProvider);
    }
    Ok((provider, rest))
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

fn validate_symbol_lengths(
    base: &str,
    quote: Option<&str>,
    default_quote: Option<&str>,
) -> Result<(), ()> {
    let projected_quote_len = match quote {
        Some(quote) => quote.len(),
        None => match default_quote {
            Some(default_quote)
                if base
                    .get(base.len().saturating_sub(default_quote.len())..)
                    .is_some_and(|suffix| suffix.eq_ignore_ascii_case(default_quote)) =>
            {
                0
            }
            Some(default_quote) => default_quote.len(),
            None => 0,
        },
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
