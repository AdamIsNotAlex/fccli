use std::str::FromStr;

use fccli::{
    error::{
        ErrorContext, ErrorOperation, ModelError, ProviderError, SanitizedCause, SanitizedMessage,
    },
    model::{
        CHART_PRICE_MAX, Candle, ConnectionStatus, FinalityAuthority, GapGeneration,
        HistoryRequest, HistoryRequestKind, Instrument, InstrumentSpec, MAX_HISTORY_LIMIT,
        MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS, Market, MarketEvent, MonoInstant, ProviderId,
        RateGateState, ReplayRevision, Timeframe,
    },
};

fn candle(authority: FinalityAuthority) -> Candle {
    match authority {
        FinalityAuthority::RestProvisionalOpen => Candle::from_rest(
            1_700_000_000_000,
            1_700_000_059_999,
            10.0,
            12.0,
            9.0,
            11.0,
            42.5,
        ),
        FinalityAuthority::WsAuthoritativeOpen => Candle::from_ws(
            1_700_000_000_000,
            1_700_000_059_999,
            10.0,
            12.0,
            9.0,
            11.0,
            42.5,
            false,
        ),
        FinalityAuthority::WsAuthoritativeClosed => Candle::from_ws(
            1_700_000_000_000,
            1_700_000_059_999,
            10.0,
            12.0,
            9.0,
            11.0,
            42.5,
            true,
        ),
        FinalityAuthority::RestProvisionalClosed => {
            panic!("REST closure is intentionally sealed behind CandleSeries")
        }
    }
    .expect("valid candle")
}

#[test]
fn timeframes_accept_canonical_and_unit_only_spellings() {
    let canonical_spellings = Timeframe::INPUT_SPELLINGS
        .into_iter()
        .filter(|(spelling, timeframe)| *spelling == timeframe.as_str());

    assert_eq!(Timeframe::ALL.len(), canonical_spellings.clone().count());
    for (timeframe, (spelling, mapped_timeframe)) in
        Timeframe::ALL.into_iter().zip(canonical_spellings)
    {
        assert_eq!(mapped_timeframe, timeframe);
        assert_eq!(timeframe.as_str(), spelling);
        assert_eq!(timeframe.to_string(), spelling);
        assert_eq!(Timeframe::from_str(spelling), Ok(timeframe));
    }

    for (alias, expected) in [
        ("s", Timeframe::Second1),
        ("m", Timeframe::Minute1),
        ("h", Timeframe::Hour1),
        ("d", Timeframe::Day1),
        ("w", Timeframe::Week1),
        ("M", Timeframe::Month1),
    ] {
        assert_eq!(Timeframe::from_str(alias), Ok(expected), "{alias}");
        assert_eq!(expected.to_string(), format!("1{alias}"), "{alias}");
    }

    assert_ne!(Timeframe::from_str("m"), Timeframe::from_str("M"));
    for invalid in ["S", "H", "D", "W", "1S", "1month", "60m", "", " 1m"] {
        assert_eq!(
            Timeframe::from_str(invalid),
            Err(ModelError::InvalidTimeframe)
        );
    }
}

#[test]
fn validated_instrument_values_expose_only_immutable_derived_views() {
    let provider = ProviderId::new("binance").expect("valid provider");
    let spec = InstrumentSpec::new(provider.clone(), "BTC", Some("USDT"))
        .expect("valid instrument specification");
    assert_eq!(spec.provider(), &provider);
    assert_eq!(spec.base(), "BTC");
    assert_eq!(spec.quote(), Some("USDT"));

    let instrument = Instrument::new(
        provider.clone(),
        Market::Spot,
        "BTC",
        "USDT",
        "BTC-USDT/spot",
    )
    .expect("provider-neutral symbol punctuation is allowed");
    assert_eq!(instrument.provider(), &provider);
    assert_eq!(instrument.market(), Market::Spot);
    assert_eq!(instrument.base(), "BTC");
    assert_eq!(instrument.quote(), "USDT");
    assert_eq!(instrument.display_pair(), "BTC/USDT");
    assert_eq!(instrument.provider_symbol(), "BTC-USDT/spot");
}

#[test]
fn market_labels_and_specs_preserve_perpetual_selection() {
    assert_eq!(Market::Spot.display_label(), "Spot");
    assert_eq!(Market::Perpetual.display_label(), "Perp");

    let provider = ProviderId::new("binance").expect("valid provider");
    let spec =
        InstrumentSpec::new_with_market(provider.clone(), Market::Perpetual, "BTC", Some("USDT"))
            .expect("valid perpetual specification");
    assert_eq!(spec.market(), Market::Perpetual);
    assert_eq!(spec.base(), "BTC");
    assert_eq!(spec.quote(), Some("USDT"));

    let instrument = Instrument::new(provider, Market::Perpetual, "BTC", "USDT", "BTCUSDT")
        .expect("valid perpetual instrument");
    assert_eq!(instrument.market(), Market::Perpetual);
    assert_eq!(instrument.display_pair(), "BTC/USDT");
    assert_eq!(instrument.provider_symbol(), "BTCUSDT");
}

#[test]
fn candle_accepts_formatter_and_chart_safe_boundaries() {
    for open_time in [MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS - 1] {
        let value = Candle::from_rest(
            open_time,
            open_time + 1,
            -CHART_PRICE_MAX,
            CHART_PRICE_MAX,
            -CHART_PRICE_MAX,
            CHART_PRICE_MAX,
            0.0,
        )
        .expect("boundary candle must remain formatter- and chart-safe");
        assert_eq!(value.open_time(), open_time);
        assert_eq!(value.close_time(), open_time + 1);
    }
}

#[test]
fn candle_rejects_every_invalid_numeric_timestamp_and_body_shape() {
    let valid = (1_000, 1_999, 10.0, 12.0, 9.0, 11.0, 1.0);
    let new = Candle::from_rest;

    assert!(matches!(
        new(
            MIN_TIMESTAMP_MS - 1,
            valid.1,
            valid.2,
            valid.3,
            valid.4,
            valid.5,
            valid.6
        ),
        Err(ModelError::TimestampOutOfRange { .. })
    ));
    assert!(matches!(
        new(
            valid.0,
            MAX_TIMESTAMP_MS + 1,
            valid.2,
            valid.3,
            valid.4,
            valid.5,
            valid.6
        ),
        Err(ModelError::TimestampOutOfRange { .. })
    ));
    assert_eq!(
        new(2_000, 1_999, valid.2, valid.3, valid.4, valid.5, valid.6),
        Err(ModelError::InvalidTimestampOrder)
    );

    for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(matches!(
            new(
                valid.0, valid.1, non_finite, valid.3, valid.4, valid.5, valid.6
            ),
            Err(ModelError::NonFinite { field: "open" })
        ));
    }
    assert!(matches!(
        new(
            valid.0,
            valid.1,
            CHART_PRICE_MAX.next_up(),
            CHART_PRICE_MAX.next_up(),
            valid.4,
            valid.5,
            valid.6,
        ),
        Err(ModelError::PriceOutOfRange { .. })
    ));
    assert!(matches!(
        new(
            valid.0,
            valid.1,
            valid.2,
            valid.3,
            valid.4,
            valid.5,
            f64::NAN
        ),
        Err(ModelError::NonFinite {
            field: "base_volume"
        })
    ));
    assert_eq!(
        new(valid.0, valid.1, valid.2, valid.3, valid.4, valid.5, -0.01),
        Err(ModelError::NegativeVolume)
    );
    assert_eq!(
        new(valid.0, valid.1, 10.0, 9.0, 12.0, 11.0, valid.6),
        Err(ModelError::InvalidOhlc)
    );
    assert_eq!(
        new(valid.0, valid.1, 13.0, 12.0, 9.0, 11.0, valid.6),
        Err(ModelError::InvalidBodyBounds)
    );
}

#[test]
fn candle_closed_state_is_derived_only_from_finality_authority() {
    let rest = candle(FinalityAuthority::RestProvisionalOpen);
    let ws_open = candle(FinalityAuthority::WsAuthoritativeOpen);
    let ws_closed = candle(FinalityAuthority::WsAuthoritativeClosed);

    for (value, authority, closed, authoritative) in [
        (&rest, FinalityAuthority::RestProvisionalOpen, false, false),
        (
            &ws_open,
            FinalityAuthority::WsAuthoritativeOpen,
            false,
            true,
        ),
        (
            &ws_closed,
            FinalityAuthority::WsAuthoritativeClosed,
            true,
            true,
        ),
    ] {
        assert_eq!(value.is_closed(), closed);
        assert_eq!(authority.is_closed(), closed);
        assert_eq!(authority.is_authoritative(), authoritative);
        assert_eq!(value.authority(), authority);
        assert_eq!(value.open_time(), 1_700_000_000_000);
        assert_eq!(value.close_time(), 1_700_000_059_999);
        assert_eq!(value.open(), 10.0);
        assert_eq!(value.high(), 12.0);
        assert_eq!(value.low(), 9.0);
        assert_eq!(value.close(), 11.0);
        assert_eq!(value.base_volume(), 42.5);
    }
}

#[test]
fn history_requests_validate_latest_older_and_inclusive_gap_arithmetic() {
    let latest = HistoryRequest::latest(500).expect("valid latest request");
    assert_eq!(latest.kind(), HistoryRequestKind::Latest);
    assert_eq!(
        (latest.start_time(), latest.end_time(), latest.limit()),
        (None, None, 500)
    );

    let older = HistoryRequest::older(10_000, MAX_HISTORY_LIMIT).expect("checked oldest minus one");
    assert_eq!(older.kind(), HistoryRequestKind::Older);
    assert_eq!((older.start_time(), older.end_time()), (None, Some(9_999)));

    let gap = HistoryRequest::gap(20_000, 30_000, 1_000).expect("inclusive gap");
    assert_eq!(gap.kind(), HistoryRequestKind::Gap);
    assert_eq!(
        (gap.start_time(), gap.end_time(), gap.limit()),
        (Some(20_000), Some(30_000), 1_000)
    );
    assert_eq!(HistoryRequest::next_inclusive_start(30_000), Ok(30_001));

    assert!(matches!(
        HistoryRequest::latest(0),
        Err(ModelError::InvalidLimit { limit: 0 })
    ));
    assert!(matches!(
        HistoryRequest::latest(MAX_HISTORY_LIMIT + 1),
        Err(ModelError::InvalidLimit { .. })
    ));
    assert_eq!(HistoryRequest::gap(2, 1, 1), Err(ModelError::InvalidRange));
    assert!(matches!(
        HistoryRequest::older(MIN_TIMESTAMP_MS, 1),
        Err(ModelError::TimestampArithmetic | ModelError::TimestampOutOfRange { .. })
    ));
    assert!(matches!(
        HistoryRequest::next_inclusive_start(MAX_TIMESTAMP_MS),
        Err(ModelError::TimestampArithmetic | ModelError::TimestampOutOfRange { .. })
    ));
}

#[test]
fn monotonic_deadline_and_event_values_preserve_exact_tags_and_payloads() {
    let deadline = MonoInstant::from_millis(10_000).expect("deadline conversion");
    assert_eq!(deadline.as_millis(), 10_000);
    assert_eq!(deadline.as_nanos(), 10_000_000_000);
    assert_eq!(
        RateGateState::TimedUntil(deadline),
        RateGateState::TimedUntil(deadline)
    );

    let generation = GapGeneration(7);
    let revision = ReplayRevision(11);
    let item = candle(FinalityAuthority::WsAuthoritativeOpen);
    let events = [
        MarketEvent::Status {
            generation: Some(generation),
            status: ConnectionStatus::GapSync,
        },
        MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time: item.open_time(),
            candles: vec![item.clone()],
        },
        MarketEvent::Candle {
            generation,
            candle: item.clone(),
        },
        MarketEvent::RecoverableError {
            generation: Some(generation),
            error: ProviderError::RateLimited {
                context: ErrorContext::operation(ErrorOperation::History),
                status: 429,
            },
            rate_gate_deadline: Some(deadline),
        },
        MarketEvent::TerminalError(ProviderError::Configuration("invalid limits")),
    ];

    assert!(matches!(
        &events[0],
        MarketEvent::Status {
            generation: Some(GapGeneration(7)),
            status: ConnectionStatus::GapSync
        }
    ));
    assert!(matches!(
        &events[1],
        MarketEvent::ReconcileBatch {
            generation: GapGeneration(7),
            revision: ReplayRevision(11),
            target_open_time: 1_700_000_000_000,
            candles,
        } if candles == std::slice::from_ref(&item)
    ));
    assert!(matches!(
        &events[2],
        MarketEvent::Candle {
            generation: GapGeneration(7),
            ..
        }
    ));
    assert!(
        matches!(&events[3], MarketEvent::RecoverableError { rate_gate_deadline: Some(value), .. } if *value == deadline)
    );
    assert!(matches!(
        &events[4],
        MarketEvent::TerminalError(ProviderError::Configuration("invalid limits"))
    ));
}

#[test]
fn typed_errors_never_retain_untrusted_context_or_provider_messages() {
    let provider = ProviderId::new("binance").expect("validated provider");
    let instrument = Instrument::new(provider, Market::Spot, "BTC", "USDT", "BTCUSDT")
        .expect("validated instrument");
    let context = ErrorContext::operation(ErrorOperation::WebSocket)
        .with_market(&instrument, Timeframe::Minute1);

    let malicious = [
        "https://user:secret@example.invalid/private?token=secret",
        "Authorization: Bearer top-secret",
        "X-API-Key=top-secret",
        "unknown provider payload top-secret\n\r\u{1b}[31m\u{202e}",
        "api_key_ABC123",
        "Basic dXNlcjpwYXNz",
    ];

    for raw in malicious {
        let invalid_symbol = ProviderError::InvalidSymbol {
            context: context.clone(),
            code: -1121,
            message: SanitizedMessage::new(raw),
        };
        let client_status = ProviderError::ClientStatus {
            context: context.clone(),
            status: 400,
            code: Some(-1),
            message: Some(SanitizedMessage::new(raw)),
        };

        for error in [invalid_symbol, client_status] {
            let display = error.to_string();
            let debug = format!("{error:?}");
            for rendered in [&display, &debug] {
                assert!(!rendered.contains(raw));
                assert!(!rendered.contains("top-secret"));
                assert!(!rendered.contains("user:secret"));
                assert!(!rendered.contains("token=secret"));
                assert!(!rendered.contains("Authorization"));
                assert!(!rendered.contains("X-API-Key"));
                assert!(!rendered.contains("api_key_ABC123"));
                assert!(!rendered.contains("Basic dXNlcjpwYXNz"));
                assert!(!rendered.contains('\n'));
                assert!(!rendered.contains('\r'));
                assert!(!rendered.contains('\u{1b}'));
                assert!(!rendered.contains('\u{202e}'));
            }
        }
    }

    assert_eq!(
        ProviderId::new("api_key_ABC123"),
        Err(ModelError::InvalidProvider)
    );
    assert_eq!(
        ProviderId::new("Basic dXNlcjpwYXNz"),
        Err(ModelError::InvalidProvider)
    );

    let transport = ProviderError::Transport {
        context,
        cause: SanitizedCause::Connection,
    };
    assert!(transport.to_string().contains("binance"));
    assert!(transport.to_string().contains("BTC/USDT"));
    assert!(format!("{transport:?}").contains("Minute1"));
}

#[test]
fn reconcile_ack_timeout_preserves_the_exact_failed_expectation() {
    let timeout = ProviderError::ReconcileAckTimeout {
        generation: GapGeneration(7),
        revision: ReplayRevision(11),
        target_open_time: 1_700_000_000_000,
    };

    assert!(matches!(
        &timeout,
        ProviderError::ReconcileAckTimeout {
            generation: GapGeneration(7),
            revision: ReplayRevision(11),
            target_open_time: 1_700_000_000_000,
        }
    ));
    let rendered = timeout.to_string();
    assert!(rendered.contains('7'));
    assert!(rendered.contains("11"));
    assert!(rendered.contains("1700000000000"));
}
