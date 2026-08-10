use clap::error::ErrorKind;
use fccli::{
    cli::{CanonicalizationError, Cli, Mode, canonicalize_binance},
    model::{Market, Timeframe},
};

fn parse(instrument: &str, timeframe: &str) -> Cli {
    Cli::try_parse_from(["fccli", instrument, timeframe]).expect("valid CLI arguments")
}

#[test]
fn valid_symbol_forms_canonicalize_to_locked_binance_spot_identifiers() {
    let cases = [
        ("btc", "BTC", None, "BTC", "USDT", "BTCUSDT"),
        ("binance:btc", "BTC", None, "BTC", "USDT", "BTCUSDT"),
        ("btc/usdc", "BTC", Some("USDC"), "BTC", "USDC", "BTCUSDC"),
        ("btc-usdt", "BTC", Some("USDT"), "BTC", "USDT", "BTCUSDT"),
        ("BTCUSDT", "BTCUSDT", None, "BTC", "USDT", "BTCUSDT"),
        ("aUSDT", "AUSDT", None, "A", "USDT", "AUSDT"),
        ("BTCUSDC", "BTCUSDC", None, "BTCUSDC", "USDT", "BTCUSDCUSDT"),
        (
            "binance:eth2/btc1",
            "ETH2",
            Some("BTC1"),
            "ETH2",
            "BTC1",
            "ETH2BTC1",
        ),
    ];

    for (input, spec_base, spec_quote, base, quote, provider_symbol) in cases {
        let cli = parse(input, "1h");
        assert_eq!(cli.instrument().provider().as_str(), "binance", "{input}");
        assert_eq!(cli.instrument().base(), spec_base, "{input}");
        assert_eq!(cli.instrument().quote(), spec_quote, "{input}");

        let instrument = canonicalize_binance(cli.instrument()).expect("Binance canonicalization");
        assert_eq!(instrument.provider().as_str(), "binance", "{input}");
        assert_eq!(instrument.market(), Market::Spot, "{input}");
        assert_eq!(instrument.base(), base, "{input}");
        assert_eq!(instrument.quote(), quote, "{input}");
        assert_eq!(
            instrument.display_pair(),
            format!("{base}/{quote}"),
            "{input}"
        );
        assert_eq!(instrument.provider_symbol(), provider_symbol, "{input}");
    }
}

#[test]
fn all_sixteen_intervals_parse_exactly_and_minute_is_distinct_from_month() {
    let spellings = [
        "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d",
        "1w", "1M",
    ];

    for (expected, spelling) in Timeframe::ALL.into_iter().zip(spellings) {
        assert_eq!(parse("btc", spelling).timeframe(), expected, "{spelling}");
    }
    assert_eq!(parse("btc", "1m").timeframe(), Timeframe::Minute1);
    assert_eq!(parse("btc", "1M").timeframe(), Timeframe::Month1);

    for invalid in ["1S", "1month", "60m", "1 m", "1m "] {
        let error = Cli::try_parse_from(["fccli", "btc", invalid]).expect_err(invalid);
        assert_eq!(error.kind(), ErrorKind::ValueValidation, "{invalid}");
        let rendered = error.to_string();
        assert!(rendered.contains("unsupported timeframe"), "{rendered}");
        assert!(rendered.contains("case-sensitive"), "{rendered}");
    }
}

#[test]
fn provider_defaults_to_binance_and_explicit_provider_is_preserved_until_canonicalization() {
    let defaulted = parse("btc", "1m");
    assert_eq!(defaulted.instrument().provider().as_str(), "binance");

    let explicit = parse("binance:btc", "1m");
    assert_eq!(explicit.instrument().provider().as_str(), "binance");

    let unknown = parse("kraken:btc/usdt", "1m");
    assert_eq!(unknown.instrument().provider().as_str(), "kraken");
    assert_eq!(
        canonicalize_binance(unknown.instrument()),
        Err(CanonicalizationError::UnsupportedProvider {
            provider: unknown.instrument().provider().clone(),
        })
    );

    let wrong_case = parse("Binance:btc", "1m");
    assert!(matches!(
        canonicalize_binance(wrong_case.instrument()),
        Err(CanonicalizationError::UnsupportedProvider { .. })
    ));
}

#[test]
fn snapshot_is_default_and_both_interactive_flags_select_interactive_mode() {
    let snapshot = parse("btc", "1m");
    assert!(!snapshot.interactive());
    assert_eq!(snapshot.mode(), Mode::Snapshot);

    for flag in ["-i", "--interactive"] {
        let cli = Cli::try_parse_from(["fccli", "btc", "1m", flag]).expect(flag);
        assert!(cli.interactive(), "{flag}");
        assert_eq!(cli.mode(), Mode::Interactive, "{flag}");
    }
}

#[test]
fn whitespace_non_ascii_empty_and_malformed_components_are_rejected() {
    let invalid = [
        "",
        " btc",
        "btc ",
        "bt c",
        "btc/us dt",
        "btc/ usdt",
        "btç",
        "比特币",
        ":btc",
        "binance:",
        "/usdt",
        "btc/",
        "-usdt",
        "btc-",
        "btc/usdt/eth",
        "btc-usdt-eth",
        "btc/usdt/",
        "btc-usdt-",
        "btc/usdt-eth",
        "btc-usdt/eth",
        "binance:btc:usdt",
        "binance::btc",
        "binance:btc/usdt:extra",
    ];

    for value in invalid {
        let error = Cli::try_parse_from(["fccli", value, "1m"]).expect_err(value);
        assert_eq!(error.kind(), ErrorKind::ValueValidation, "{value:?}");
        let rendered = error.to_string();
        assert!(
            rendered.contains("invalid instrument")
                || rendered.contains("invalid provider")
                || rendered.contains("missing instrument"),
            "unexpected error for {value:?}: {rendered}"
        );
    }
}

#[test]
fn quote_only_tokens_fail_during_local_canonicalization() {
    for value in ["USDT", "usdt", "binance:USDT"] {
        let cli = parse(value, "1m");
        assert_eq!(
            canonicalize_binance(cli.instrument()),
            Err(CanonicalizationError::QuoteOnly),
            "{value}"
        );
    }
}

#[test]
fn parser_error_precedence_is_stable_and_actionable() {
    let instrument_first = Cli::try_parse_from(["fccli", "btc/usdt/eth", "60m"])
        .expect_err("both arguments are invalid");
    assert_eq!(instrument_first.kind(), ErrorKind::ValueValidation);
    let rendered = instrument_first.to_string();
    assert!(rendered.contains("instrument"), "{rendered}");
    assert!(!rendered.contains("unsupported timeframe"), "{rendered}");

    let timeframe_second =
        Cli::try_parse_from(["fccli", "btc", "60m"]).expect_err("invalid timeframe");
    assert_eq!(timeframe_second.kind(), ErrorKind::ValueValidation);
    assert!(
        timeframe_second
            .to_string()
            .contains("unsupported timeframe"),
        "{timeframe_second}"
    );

    let missing = Cli::try_parse_from(["fccli"]).expect_err("missing positionals");
    assert_eq!(missing.kind(), ErrorKind::MissingRequiredArgument);
    let rendered = missing.to_string();
    assert!(rendered.contains("<INSTRUMENT>"), "{rendered}");
    assert!(rendered.contains("<TIMEFRAME>"), "{rendered}");
}

#[test]
fn help_version_and_command_rendering_are_library_only_and_stable() {
    let help = Cli::try_parse_from(["fccli", "--help"]).expect_err("help is rendered");
    assert_eq!(help.kind(), ErrorKind::DisplayHelp);
    let help = help.to_string();
    for expected in [
        "Render Binance Spot candlestick charts",
        "Usage: fccli [OPTIONS] <INSTRUMENT> <TIMEFRAME>",
        "-i, --interactive",
        "case-sensitive",
        "fccli binance:btc/usdc 1h",
    ] {
        assert!(help.contains(expected), "missing {expected:?} in:\n{help}");
    }

    let version = Cli::try_parse_from(["fccli", "--version"]).expect_err("version is rendered");
    assert_eq!(version.kind(), ErrorKind::DisplayVersion);
    assert_eq!(version.to_string().trim(), "fccli 0.1.0");

    let command = Cli::command();
    assert_eq!(command.get_name(), "fccli");
    assert_eq!(command.get_version(), Some("0.1.0"));
    assert!(
        command
            .get_arguments()
            .any(|argument| argument.get_id() == "instrument")
    );
    assert!(
        command
            .get_arguments()
            .any(|argument| argument.get_id() == "timeframe")
    );
    assert!(
        command
            .get_arguments()
            .any(|argument| argument.get_id() == "interactive")
    );
}
