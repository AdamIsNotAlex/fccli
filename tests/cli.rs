use clap::error::ErrorKind;
use fccli::{
    cli::{CanonicalizationError, Cli, Mode, canonicalize_instrument, parse_market_target},
    model::{InstrumentSpec, Market, ProviderId, Timeframe},
};

fn parse(instrument: &str, timeframe: &str) -> Cli {
    Cli::try_parse_from(["fccli", instrument, timeframe]).expect("valid CLI arguments")
}

fn parse_args(arguments: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("fccli").chain(arguments.iter().copied()))
        .expect("valid CLI arguments")
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

        let instrument =
            canonicalize_instrument(cli.instrument()).expect("Binance canonicalization");
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
fn trailing_p_suffix_selects_binance_perpetual_without_changing_identifiers() {
    let cases = [
        ("btc.p", "BTC", None, "BTC", "USDT", "BTCUSDT"),
        ("BTC.P", "BTC", None, "BTC", "USDT", "BTCUSDT"),
        ("binance:btc.p", "BTC", None, "BTC", "USDT", "BTCUSDT"),
        ("btc/usdt.p", "BTC", Some("USDT"), "BTC", "USDT", "BTCUSDT"),
        ("btc-usdt.p", "BTC", Some("USDT"), "BTC", "USDT", "BTCUSDT"),
        ("BTCUSDT.p", "BTCUSDT", None, "BTC", "USDT", "BTCUSDT"),
        ("btc/usdc.p", "BTC", Some("USDC"), "BTC", "USDC", "BTCUSDC"),
        ("btc-usd.p", "BTC", Some("USD"), "BTC", "USD", "BTCUSD"),
    ];

    for (input, spec_base, spec_quote, base, quote, provider_symbol) in cases {
        let cli = parse(input, "1h");
        assert_eq!(cli.instrument().provider().as_str(), "binance", "{input}");
        assert_eq!(cli.instrument().market(), Market::Perpetual, "{input}");
        assert_eq!(cli.instrument().base(), spec_base, "{input}");
        assert_eq!(cli.instrument().quote(), spec_quote, "{input}");

        let instrument =
            canonicalize_instrument(cli.instrument()).expect("Binance perpetual canonicalization");
        assert_eq!(instrument.provider().as_str(), "binance", "{input}");
        assert_eq!(instrument.market(), Market::Perpetual, "{input}");
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
fn direct_instrument_specs_are_canonicalized_independently_of_cli_preprocessing() {
    let provider = ProviderId::new("binance").expect("valid provider");
    let cases = [
        ("btc", None, "BTC", "USDT", "BTCUSDT"),
        ("bTcUsDt", None, "BTC", "USDT", "BTCUSDT"),
        ("eTh", Some("uSdC"), "ETH", "USDC", "ETHUSDC"),
    ];

    for (base, quote, expected_base, expected_quote, expected_symbol) in cases {
        let specification = InstrumentSpec::new(provider.clone(), base, quote)
            .expect("valid direct instrument specification");
        let instrument = canonicalize_instrument(&specification).expect("canonical instrument");
        assert_eq!(instrument.base(), expected_base, "{base:?}/{quote:?}");
        assert_eq!(instrument.quote(), expected_quote, "{base:?}/{quote:?}");
        assert_eq!(
            instrument.display_pair(),
            format!("{expected_base}/{expected_quote}"),
            "{base:?}/{quote:?}"
        );
        assert_eq!(
            instrument.provider_symbol(),
            expected_symbol,
            "{base:?}/{quote:?}"
        );
    }

    let quote_only = InstrumentSpec::new(provider, "uSdT", None::<String>)
        .expect("valid provider-neutral quote token");
    assert_eq!(
        canonicalize_instrument(&quote_only),
        Err(CanonicalizationError::QuoteOnly)
    );
}

#[test]
fn canonical_and_unit_only_intervals_parse_with_distinct_minutes_and_months() {
    for expected in Timeframe::ALL {
        let spelling = expected.as_str();
        assert_eq!(parse("btc", spelling).timeframe(), expected, "{spelling}");
    }
    for (alias, expected) in [
        ("s", Timeframe::Second1),
        ("m", Timeframe::Minute1),
        ("h", Timeframe::Hour1),
        ("d", Timeframe::Day1),
        ("w", Timeframe::Week1),
        ("M", Timeframe::Month1),
    ] {
        assert_eq!(parse("btc", alias).timeframe(), expected, "{alias}");
    }
    assert_eq!(parse("btc", "m").timeframe(), Timeframe::Minute1);
    assert_eq!(parse("btc", "M").timeframe(), Timeframe::Month1);

    for invalid in ["S", "H", "1S", "1month", "60m", "1 m", "1m "] {
        let error = Cli::try_parse_from(["fccli", "btc", invalid]).expect_err(invalid);
        assert_eq!(error.kind(), ErrorKind::ValueValidation, "{invalid}");
        let rendered = error.to_string();
        assert!(rendered.contains("unsupported timeframe"), "{rendered}");
        assert!(rendered.contains("unit-only values mean 1"), "{rendered}");
        assert!(rendered.contains("case-sensitive"), "{rendered}");
    }
}

#[test]
fn provider_defaults_to_binance_and_explicit_provider_is_preserved_until_canonicalization() {
    let defaulted = parse("btc", "1m");
    assert_eq!(defaulted.instrument().provider().as_str(), "binance");

    let explicit = parse("binance:btc", "1m");
    assert_eq!(explicit.instrument().provider().as_str(), "binance");

    for provider in [
        "binance",
        "okx",
        "bybit",
        "coinbase",
        "kraken",
        "hyperliquid",
    ] {
        let cli = parse(&format!("{provider}:btc"), "1m");
        assert_eq!(cli.instrument().provider().as_str(), provider);
        assert!(
            canonicalize_instrument(cli.instrument()).is_ok(),
            "{provider}"
        );
    }

    let unknown = parse("gemini:btc/usdt", "1m");
    assert_eq!(unknown.instrument().provider().as_str(), "gemini");
    assert_eq!(
        canonicalize_instrument(unknown.instrument()),
        Err(CanonicalizationError::UnsupportedProvider {
            provider: unknown.instrument().provider().clone(),
        })
    );
    let rendered = CanonicalizationError::UnsupportedProvider {
        provider: unknown.instrument().provider().clone(),
    }
    .to_string();
    assert!(rendered.contains("has no default-quote rule"), "{rendered}");
    assert!(rendered.contains("binance"), "{rendered}");
    assert!(rendered.contains("hyperliquid"), "{rendered}");
    assert!(rendered.contains("lowercase"), "{rendered}");

    let wrong_case = parse("Binance:btc", "1m");
    assert!(matches!(
        canonicalize_instrument(wrong_case.instrument()),
        Err(CanonicalizationError::UnsupportedProvider { .. })
    ));
}

#[test]
fn omitted_targets_use_defaults_and_one_token_is_always_an_instrument() {
    for (arguments, expected_base) in [
        (&[][..], "BTC"),
        (&["eth"][..], "ETH"),
        (&["h"][..], "H"),
        (&["60m"][..], "60M"),
    ] {
        let cli = parse_args(arguments);
        assert_eq!(
            cli.instrument().provider().as_str(),
            "binance",
            "{arguments:?}"
        );
        assert_eq!(cli.instrument().base(), expected_base, "{arguments:?}");
        assert_eq!(cli.instrument().quote(), None, "{arguments:?}");
        assert_eq!(cli.timeframe(), Timeframe::Hour1, "{arguments:?}");
    }
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
fn unknown_short_and_long_options_use_clap_unknown_argument_errors() {
    for arguments in [
        ["fccli", "--interactiv", "btc", "1m"],
        ["fccli", "-x", "btc", "1m"],
        ["fccli", "-usdt", "btc", "1m"],
    ] {
        let error = Cli::try_parse_from(arguments).expect_err("unknown option");
        assert_eq!(error.kind(), ErrorKind::UnknownArgument, "{arguments:?}");
    }

    let dashed_pair = Cli::try_parse_from(["fccli", "btc-usdt", "1m"])
        .expect("an embedded dash remains a valid pair separator");
    assert_eq!(dashed_pair.instrument().base(), "BTC");
    assert_eq!(dashed_pair.instrument().quote(), Some("USDT"));
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
        ".p",
        "btc.p.p",
        "btc.",
        "btc.perp",
        "btc.p/",
        "btc/.p",
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
fn rejected_arguments_are_bounded_and_never_echo_terminal_control_payloads() {
    let malicious = [
        ("btc\ninjected", "injected"),
        ("btc\rspoofed", "spoofed"),
        ("btc\u{1b}[31mred", "red"),
        ("btc\u{1b}]0;owned\u{7}", "owned"),
        ("btc\u{202e}txt", "txt"),
        ("btc\0hidden", "hidden"),
    ];

    for (value, marker) in malicious {
        for arguments in [["fccli", value, "1m"], ["fccli", "btc", value]] {
            let error = Cli::try_parse_from(arguments).expect_err("unsafe argument");
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{value:?}");
            let rendered = error.to_string();
            assert!(
                !rendered.contains(value),
                "unsafe input was echoed: {rendered:?}"
            );
            assert!(
                !rendered.contains(marker),
                "unsafe payload marker was echoed: {rendered:?}"
            );
            assert!(!rendered.contains('\u{1b}'), "ESC survived: {rendered:?}");
            assert!(
                !rendered.contains('\u{202e}'),
                "bidi survived: {rendered:?}"
            );
            assert!(!rendered.contains('\0'), "NUL survived: {rendered:?}");
        }
    }

    let oversized = "a".repeat(1_000_000);
    for arguments in [
        ["fccli", oversized.as_str(), "1m"],
        ["fccli", "btc", oversized.as_str()],
    ] {
        let error = Cli::try_parse_from(arguments).expect_err("oversized programmatic argument");
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        let rendered = error.to_string();
        assert!(!rendered.contains(&oversized));
        assert!(rendered.len() < 4_096, "error rendering was not bounded");
    }
}

#[test]
fn provider_symbol_length_accepts_exact_limit_and_rejects_one_over() {
    const PROVIDER_SYMBOL_LIMIT: usize = 256;
    const DEFAULT_QUOTE: &str = "USDT";

    let exact = format!(
        "{}{}",
        "a".repeat(PROVIDER_SYMBOL_LIMIT - DEFAULT_QUOTE.len()),
        DEFAULT_QUOTE
    );
    assert_eq!(exact.len(), PROVIDER_SYMBOL_LIMIT);
    let expected_provider_symbol = exact.to_ascii_uppercase();
    let cli = Cli::try_parse_from(["fccli", exact.as_str(), "1m"])
        .expect("exact-limit separator-free USDT-suffixed instrument specification");
    assert_eq!(cli.instrument().base(), expected_provider_symbol);
    assert_eq!(cli.instrument().quote(), None);
    let instrument = canonicalize_instrument(cli.instrument())
        .expect("exact-limit separator-free USDT-suffixed canonicalization");
    assert_eq!(
        instrument.base().len(),
        PROVIDER_SYMBOL_LIMIT - DEFAULT_QUOTE.len()
    );
    assert_eq!(instrument.quote(), DEFAULT_QUOTE);
    assert_eq!(instrument.provider_symbol().len(), PROVIDER_SYMBOL_LIMIT);
    assert_eq!(instrument.provider_symbol(), expected_provider_symbol);

    let one_over = format!(
        "{}{}",
        "a".repeat(PROVIDER_SYMBOL_LIMIT - DEFAULT_QUOTE.len() + 1),
        DEFAULT_QUOTE
    );
    assert_eq!(one_over.len(), PROVIDER_SYMBOL_LIMIT + 1);
    let error = Cli::try_parse_from(["fccli", one_over.as_str(), "1m"])
        .expect_err("one-over-limit separator-free USDT-suffixed instrument specification");
    assert_eq!(error.kind(), ErrorKind::ValueValidation);
    assert!(!error.to_string().contains(&one_over));

    let provider = ProviderId::new("binance").expect("valid provider");
    let exact_specification = InstrumentSpec::new(provider.clone(), exact, None::<String>)
        .expect("exact-limit direct instrument specification");
    let instrument = canonicalize_instrument(&exact_specification)
        .expect("exact-limit direct separator-free USDT-suffixed canonicalization");
    assert_eq!(
        instrument.base().len(),
        PROVIDER_SYMBOL_LIMIT - DEFAULT_QUOTE.len()
    );
    assert_eq!(instrument.quote(), DEFAULT_QUOTE);
    assert_eq!(instrument.provider_symbol().len(), PROVIDER_SYMBOL_LIMIT);
    assert_eq!(instrument.provider_symbol(), expected_provider_symbol);

    let one_over_specification = InstrumentSpec::new(provider, one_over, None::<String>)
        .expect("one-over-limit direct instrument specification");
    assert_eq!(
        canonicalize_instrument(&one_over_specification),
        Err(CanonicalizationError::InvalidInstrument)
    );
}

#[test]
fn quote_only_tokens_fail_during_local_canonicalization() {
    for value in [
        "USDT",
        "usdt",
        "binance:USDT",
        "okx:USDT",
        "bybit:USDT",
        "coinbase:USD",
        "kraken:USD",
        "hyperliquid:USDC",
    ] {
        let cli = parse(value, "1m");
        assert_eq!(
            canonicalize_instrument(cli.instrument()),
            Err(CanonicalizationError::QuoteOnly),
            "{value}"
        );
    }

    let coinbase_usdt = parse("coinbase:USDT", "1m");
    let instrument = canonicalize_instrument(coinbase_usdt.instrument())
        .expect("USDT is not Coinbase's default quote");
    assert_eq!(instrument.base(), "USDT");
    assert_eq!(instrument.quote(), "USD");
    assert_eq!(instrument.provider_symbol(), "USDTUSD");
}

#[test]
fn known_providers_apply_locked_default_quotes_and_suffixes() {
    let cases = [
        ("binance", "USDT"),
        ("okx", "USDT"),
        ("bybit", "USDT"),
        ("coinbase", "USD"),
        ("kraken", "USD"),
        ("hyperliquid", "USDC"),
    ];

    for (provider, quote) in cases {
        for input in [format!("{provider}:btc"), format!("{provider}:BTC{quote}")] {
            let cli = parse(&input, "1h");
            assert_eq!(cli.instrument().provider().as_str(), provider, "{input}");
            let instrument = canonicalize_instrument(cli.instrument())
                .expect("known providers canonicalize locally");
            assert_eq!(instrument.provider().as_str(), provider, "{input}");
            assert_eq!(instrument.market(), Market::Spot, "{input}");
            assert_eq!(instrument.base(), "BTC", "{input}");
            assert_eq!(instrument.quote(), quote, "{input}");
            assert_eq!(instrument.display_pair(), format!("BTC/{quote}"), "{input}");
            assert_eq!(
                instrument.provider_symbol(),
                format!("BTC{quote}"),
                "{input}"
            );
        }
    }
}

#[test]
fn suffix_split_is_scoped_to_selected_provider_default() {
    let binance = canonicalize_instrument(parse("binance:BTCUSDC", "1h").instrument())
        .expect("Binance keeps USDC as base when no USDT suffix");
    assert_eq!(binance.base(), "BTCUSDC");
    assert_eq!(binance.quote(), "USDT");
    assert_eq!(binance.provider_symbol(), "BTCUSDCUSDT");

    let coinbase = canonicalize_instrument(parse("coinbase:BTCUSDT", "1h").instrument())
        .expect("Coinbase keeps USDT as base when no USD suffix");
    assert_eq!(coinbase.base(), "BTCUSDT");
    assert_eq!(coinbase.quote(), "USD");
    assert_eq!(coinbase.provider_symbol(), "BTCUSDTUSD");

    let explicit = canonicalize_instrument(parse("coinbase:btc/usdt", "1h").instrument())
        .expect("explicit quote is preserved");
    assert_eq!(explicit.base(), "BTC");
    assert_eq!(explicit.quote(), "USDT");
    assert_eq!(explicit.provider_symbol(), "BTCUSDT");
}

#[test]
fn provider_symbol_length_projection_uses_selected_default_quote() {
    const PROVIDER_SYMBOL_LIMIT: usize = 256;

    for (provider, default_quote) in [("coinbase", "USD"), ("hyperliquid", "USDC")] {
        let exact = format!(
            "{}{}",
            "a".repeat(PROVIDER_SYMBOL_LIMIT - default_quote.len()),
            default_quote
        );
        assert_eq!(exact.len(), PROVIDER_SYMBOL_LIMIT, "{provider}");
        let input = format!("{provider}:{exact}");
        let cli = Cli::try_parse_from(["fccli", input.as_str(), "1m"])
            .expect("exact-limit separator-free default-quote suffix");
        assert_eq!(cli.instrument().quote(), None, "{provider}");
        let instrument = canonicalize_instrument(cli.instrument())
            .expect("exact-limit suffix projection uses the selected default");
        assert_eq!(
            instrument.base().len(),
            PROVIDER_SYMBOL_LIMIT - default_quote.len(),
            "{provider}"
        );
        assert_eq!(instrument.quote(), default_quote, "{provider}");
        assert_eq!(
            instrument.provider_symbol().len(),
            PROVIDER_SYMBOL_LIMIT,
            "{provider}"
        );

        let one_over = format!(
            "{}{}",
            "a".repeat(PROVIDER_SYMBOL_LIMIT - default_quote.len() + 1),
            default_quote
        );
        let one_over_input = format!("{provider}:{one_over}");
        let error = Cli::try_parse_from(["fccli", one_over_input.as_str(), "1m"])
            .expect_err("one-over-limit separator-free default-quote suffix");
        assert_eq!(error.kind(), ErrorKind::ValueValidation, "{provider}");
        assert!(!error.to_string().contains(&one_over), "{provider}");

        let unsuffixed = "a".repeat(PROVIDER_SYMBOL_LIMIT - default_quote.len());
        let unsuffixed_input = format!("{provider}:{unsuffixed}");
        let unsuffixed_cli = Cli::try_parse_from(["fccli", unsuffixed_input.as_str(), "1m"])
            .expect("exact-limit unsuffixed projection appends the selected default");
        let unsuffixed_instrument = canonicalize_instrument(unsuffixed_cli.instrument())
            .expect("unsuffixed exact-limit projection");
        assert_eq!(
            unsuffixed_instrument.base(),
            unsuffixed.to_ascii_uppercase()
        );
        assert_eq!(unsuffixed_instrument.quote(), default_quote, "{provider}");
        assert_eq!(
            unsuffixed_instrument.provider_symbol().len(),
            PROVIDER_SYMBOL_LIMIT,
            "{provider}"
        );

        let unsuffixed_one_over = "a".repeat(PROVIDER_SYMBOL_LIMIT - default_quote.len() + 1);
        let unsuffixed_one_over_input = format!("{provider}:{unsuffixed_one_over}");
        let unsuffixed_error =
            Cli::try_parse_from(["fccli", unsuffixed_one_over_input.as_str(), "1m"])
                .expect_err("one-over unsuffixed projection rejects at parse");
        assert_eq!(
            unsuffixed_error.kind(),
            ErrorKind::ValueValidation,
            "{provider}"
        );
    }
}

#[test]
fn unknown_provider_without_quote_does_not_project_an_append() {
    const PROVIDER_SYMBOL_LIMIT: usize = 256;
    let exact = "a".repeat(PROVIDER_SYMBOL_LIMIT);
    let input = format!("gemini:{exact}");
    let cli = Cli::try_parse_from(["fccli", input.as_str(), "1m"])
        .expect("unknown provider projects no default-quote append");
    assert_eq!(cli.instrument().provider().as_str(), "gemini");
    assert_eq!(cli.instrument().base().len(), PROVIDER_SYMBOL_LIMIT);
    assert_eq!(cli.instrument().quote(), None);
    assert_eq!(
        canonicalize_instrument(cli.instrument()),
        Err(CanonicalizationError::UnsupportedProvider {
            provider: cli.instrument().provider().clone(),
        })
    );

    let one_over = "a".repeat(PROVIDER_SYMBOL_LIMIT + 1);
    let one_over_input = format!("gemini:{one_over}");
    let error = Cli::try_parse_from(["fccli", one_over_input.as_str(), "1m"])
        .expect_err("257-byte unknown-provider base still fails parser length validation");
    assert_eq!(error.kind(), ErrorKind::ValueValidation);
    assert!(!error.to_string().contains(&one_over));
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

    let defaulted = Cli::try_parse_from(["fccli"]).expect("positionals have defaults");
    assert_eq!(defaulted.instrument().base(), "BTC");
    assert_eq!(defaulted.timeframe(), Timeframe::Hour1);

    let extra = Cli::try_parse_from(["fccli", "btc", "1h", "secret-extra"])
        .expect_err("third positional is rejected");
    assert_eq!(extra.kind(), ErrorKind::TooManyValues);
    let rendered = extra.to_string();
    assert!(rendered.contains("unexpected extra argument"), "{rendered}");
    assert!(!rendered.contains("secret-extra"), "{rendered}");
}

#[test]
fn help_version_and_command_rendering_are_library_only_and_stable() {
    let help = Cli::try_parse_from(["fccli", "--help"]).expect_err("help is rendered");
    assert_eq!(help.kind(), ErrorKind::DisplayHelp);
    let help = help.to_string();
    for expected in [
        "Render Binance and Hyperliquid Spot and Perpetual candlestick charts",
        "Usage: fccli [OPTIONS] [INSTRUMENT] [TIMEFRAME]",
        "-i, --interactive",
        "default: binance:btc",
        "unit-only s/m/h/d/w/M means 1",
        "fccli binance:btc/usdc 1h",
        "fccli btc.p",
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

#[test]
fn parse_market_target_matches_startup_grammar_for_zero_one_and_two_fields() {
    for fields in [
        &[][..],
        &["eth"][..],
        &["h"][..],
        &["60m"][..],
        &["btc", "m"][..],
        &["btc/usdc", "M"][..],
        &["binance:btc", "1h"][..],
    ] {
        let cli = parse_args(fields);
        let target = parse_market_target(&fields.join(" ")).expect("valid target");
        assert_eq!(target.instrument, *cli.instrument(), "{fields:?}");
        assert_eq!(target.timeframe, cli.timeframe(), "{fields:?}");
    }
}

#[test]
fn parse_market_target_defaults_missing_fields_and_rejects_extra_or_invalid_tokens() {
    let defaulted = parse_market_target("").expect("empty target uses defaults");
    assert_eq!(defaulted.instrument.base(), "BTC");
    assert_eq!(defaulted.timeframe, Timeframe::Hour1);

    let market_only = parse_market_target("h").expect("one field is an instrument");
    assert_eq!(market_only.instrument.base(), "H");
    assert_eq!(market_only.timeframe, Timeframe::Hour1);

    assert!(parse_market_target("btc 1m 1h").is_err());
    assert!(parse_market_target("btc/usdt/eth 1m").is_err());
    assert!(parse_market_target("btc 60m").is_err());
    assert!(parse_market_target("btç 1m").is_err());
}

#[test]
fn parse_market_target_collapses_internal_whitespace() {
    let target = parse_market_target("   btc/usdt   1m   ").expect("collapsed whitespace");
    let cli = Cli::try_parse_from(["fccli", "btc/usdt", "1m"]).expect("valid CLI");
    assert_eq!(target.instrument, *cli.instrument());
    assert_eq!(target.timeframe, cli.timeframe());
}

#[test]
fn parse_market_target_defaults_absent_provider_to_binance() {
    let target = parse_market_target("btc 1m").expect("valid target");
    assert_eq!(target.instrument.provider().as_str(), "binance");
    let explicit = parse_market_target("binance:btc 1m").expect("valid explicit target");
    assert_eq!(explicit.instrument.provider().as_str(), "binance");
    assert_eq!(target.instrument, explicit.instrument);
}

#[test]
fn parse_market_target_preserves_unknown_provider_until_canonicalization() {
    let target = parse_market_target("gemini:btc/usdt 1m").expect("unknown provider parses");
    assert_eq!(target.instrument.provider().as_str(), "gemini");
    assert_eq!(
        canonicalize_instrument(&target.instrument),
        Err(CanonicalizationError::UnsupportedProvider {
            provider: target.instrument.provider().clone(),
        })
    );

    let known = parse_market_target("kraken:btc 1m").expect("known unimplemented provider parses");
    assert_eq!(known.instrument.provider().as_str(), "kraken");
    let instrument = canonicalize_instrument(&known.instrument)
        .expect("known providers canonicalize before registry lookup");
    assert_eq!(instrument.base(), "BTC");
    assert_eq!(instrument.quote(), "USD");
    assert_eq!(instrument.provider_symbol(), "BTCUSD");
}

#[test]
fn parse_market_target_rejects_unsafe_payloads_without_echo() {
    for (value, marker) in [
        ("btc\ninjected 1m", "injected"),
        ("btc\u{1b}[31mred 1m", "red"),
        ("btc\0hidden 1m", "hidden"),
    ] {
        let error = parse_market_target(value).expect_err("unsafe target");
        let rendered = error.to_string();
        assert!(!rendered.contains(value), "echoed: {rendered:?}");
        assert!(!rendered.contains(marker), "marker echoed: {rendered:?}");
        assert!(!rendered.contains('\u{1b}'), "ESC survived: {rendered:?}");
        assert!(!rendered.contains('\0'), "NUL survived: {rendered:?}");
    }
}

#[test]
fn parse_market_target_rejects_oversized_input_boundedly() {
    let oversized = format!("{} 1m", "a".repeat(1_000_000));
    let error = parse_market_target(&oversized).expect_err("oversized target");
    let rendered = error.to_string();
    assert!(!rendered.contains(&oversized));
    assert!(rendered.len() < 4_096, "error rendering was not bounded");
}

#[test]
fn market_target_is_clone_debug_eq() {
    let target = parse_market_target("btc/usdt 1m").expect("valid target");
    let cloned = target.clone();
    assert_eq!(target, cloned);
    assert!(format!("{target:?}").contains("MarketTarget"));
}

#[test]
fn hyperliquid_spot_index_and_hip3_parse_without_changing_local_canonicalize() {
    let index = parse("hyperliquid:@107", "1h");
    assert_eq!(index.instrument().provider().as_str(), "hyperliquid");
    assert_eq!(index.instrument().market(), Market::Spot);
    assert_eq!(index.instrument().base(), "@107");
    assert_eq!(index.instrument().quote(), None);
    assert_eq!(index.instrument().venue(), None);
    let local = canonicalize_instrument(index.instrument()).expect("local canonicalize");
    assert_eq!(local.base(), "@107");
    assert_eq!(local.quote(), "USDC");
    assert_eq!(local.provider_symbol(), "@107USDC");

    let hip3 = parse("hyperliquid:xyz:XYZ100.p", "1h");
    assert_eq!(hip3.instrument().provider().as_str(), "hyperliquid");
    assert_eq!(hip3.instrument().market(), Market::Perpetual);
    assert_eq!(hip3.instrument().base(), "XYZ100");
    assert_eq!(hip3.instrument().venue(), Some("xyz"));
    let local = canonicalize_instrument(hip3.instrument()).expect("HIP-3 local canonicalize");
    assert_eq!(local.base(), "XYZ100");
    assert_eq!(local.quote(), "USDC");
    assert_eq!(local.provider_symbol(), "XYZ100USDC");
}

#[test]
fn hyperliquid_hip3_without_perp_suffix_is_rejected() {
    let error = Cli::try_parse_from(["fccli", "hyperliquid:xyz:XYZ100", "1h"])
        .expect_err("HIP-3 requires .p");
    assert_eq!(error.kind(), ErrorKind::ValueValidation);
    let rendered = error.to_string();
    assert!(rendered.contains("perpetual-only"), "{rendered}");
    assert!(
        rendered.contains("hyperliquid:<dex>:<coin>.p"),
        "{rendered}"
    );
    assert!(!rendered.contains("XYZ100"), "{rendered}");
}
