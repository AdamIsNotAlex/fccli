use std::str::FromStr;

use fccli::{
    error::{ErrorContext, ModelError, ProviderError, SanitizedCause, SanitizedMessage},
    model::{
        CHART_PRICE_MAX, Candle, ConnectionStatus, FinalityAuthority, GapGeneration,
        HistoryRequest, HistoryRequestKind, MAX_HISTORY_LIMIT, MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS,
        MarketEvent, MonoInstant, RateGateState, ReplayRevision, Timeframe,
    },
};

fn candle(authority: FinalityAuthority) -> Candle {
    Candle::new(
        1_700_000_000_000,
        1_700_000_059_999,
        10.0,
        12.0,
        9.0,
        11.0,
        42.5,
        authority,
    )
    .expect("valid candle")
}

#[test]
fn all_sixteen_timeframes_round_trip_and_minutes_are_distinct_from_months() {
    let spellings = [
        "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d",
        "1w", "1M",
    ];

    assert_eq!(Timeframe::ALL.len(), spellings.len());
    for (timeframe, spelling) in Timeframe::ALL.into_iter().zip(spellings) {
        assert_eq!(timeframe.as_str(), spelling);
        assert_eq!(timeframe.to_string(), spelling);
        assert_eq!(Timeframe::from_str(spelling), Ok(timeframe));
    }

    assert_ne!(Timeframe::from_str("1m"), Timeframe::from_str("1M"));
    for invalid in ["1S", "1month", "60m", "", " 1m"] {
        assert_eq!(
            Timeframe::from_str(invalid),
            Err(ModelError::InvalidTimeframe)
        );
    }
}

#[test]
fn candle_accepts_formatter_and_chart_safe_boundaries() {
    for open_time in [MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS - 1] {
        let value = Candle::new(
            open_time,
            open_time + 1,
            -CHART_PRICE_MAX,
            CHART_PRICE_MAX,
            -CHART_PRICE_MAX,
            CHART_PRICE_MAX,
            0.0,
            FinalityAuthority::RestProvisionalOpen,
        )
        .expect("boundary candle must remain formatter- and chart-safe");
        assert_eq!(value.open_time, open_time);
        assert_eq!(value.close_time, open_time + 1);
    }
}

#[test]
fn candle_rejects_every_invalid_numeric_timestamp_and_body_shape() {
    let valid = (1_000, 1_999, 10.0, 12.0, 9.0, 11.0, 1.0);
    let authority = FinalityAuthority::RestProvisionalOpen;

    assert!(matches!(
        Candle::new(
            MIN_TIMESTAMP_MS - 1,
            valid.1,
            valid.2,
            valid.3,
            valid.4,
            valid.5,
            valid.6,
            authority
        ),
        Err(ModelError::TimestampOutOfRange { .. })
    ));
    assert!(matches!(
        Candle::new(
            valid.0,
            MAX_TIMESTAMP_MS + 1,
            valid.2,
            valid.3,
            valid.4,
            valid.5,
            valid.6,
            authority
        ),
        Err(ModelError::TimestampOutOfRange { .. })
    ));
    assert_eq!(
        Candle::new(
            2_000, 1_999, valid.2, valid.3, valid.4, valid.5, valid.6, authority
        ),
        Err(ModelError::InvalidTimestampOrder)
    );

    for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(matches!(
            Candle::new(
                valid.0, valid.1, non_finite, valid.3, valid.4, valid.5, valid.6, authority
            ),
            Err(ModelError::NonFinite { field: "open" })
        ));
    }
    assert!(matches!(
        Candle::new(
            valid.0,
            valid.1,
            CHART_PRICE_MAX.next_up(),
            CHART_PRICE_MAX.next_up(),
            valid.4,
            valid.5,
            valid.6,
            authority
        ),
        Err(ModelError::PriceOutOfRange { .. })
    ));
    assert!(matches!(
        Candle::new(
            valid.0,
            valid.1,
            valid.2,
            valid.3,
            valid.4,
            valid.5,
            f64::NAN,
            authority
        ),
        Err(ModelError::NonFinite {
            field: "base_volume"
        })
    ));
    assert_eq!(
        Candle::new(
            valid.0, valid.1, valid.2, valid.3, valid.4, valid.5, -0.01, authority
        ),
        Err(ModelError::NegativeVolume)
    );
    assert_eq!(
        Candle::new(valid.0, valid.1, 10.0, 9.0, 12.0, 11.0, valid.6, authority),
        Err(ModelError::InvalidOhlc)
    );
    assert_eq!(
        Candle::new(valid.0, valid.1, 13.0, 12.0, 9.0, 11.0, valid.6, authority),
        Err(ModelError::InvalidBodyBounds)
    );
}

#[test]
fn candle_closed_state_is_derived_only_from_finality_authority() {
    let cases = [
        (FinalityAuthority::RestProvisionalOpen, false, false),
        (FinalityAuthority::RestProvisionalClosed, true, false),
        (FinalityAuthority::WsAuthoritativeOpen, false, true),
        (FinalityAuthority::WsAuthoritativeClosed, true, true),
    ];

    for (authority, closed, authoritative) in cases {
        let candle = candle(authority);
        assert_eq!(candle.is_closed(), closed);
        assert_eq!(authority.is_closed(), closed);
        assert_eq!(authority.is_authoritative(), authoritative);
        assert_eq!(candle.authority, authority);
    }
}

#[test]
fn history_requests_validate_latest_older_and_inclusive_gap_arithmetic() {
    let latest = HistoryRequest::latest(500).expect("valid latest request");
    assert_eq!(latest.kind, HistoryRequestKind::Latest);
    assert_eq!(
        (latest.start_time, latest.end_time, latest.limit),
        (None, None, 500)
    );

    let older = HistoryRequest::older(10_000, MAX_HISTORY_LIMIT).expect("checked oldest minus one");
    assert_eq!(older.kind, HistoryRequestKind::Older);
    assert_eq!((older.start_time, older.end_time), (None, Some(9_999)));

    let gap = HistoryRequest::gap(20_000, 30_000, 1_000).expect("inclusive gap");
    assert_eq!(gap.kind, HistoryRequestKind::Gap);
    assert_eq!(
        (gap.start_time, gap.end_time, gap.limit),
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
            target_open_time: item.open_time,
            candles: vec![item.clone()],
        },
        MarketEvent::Candle {
            generation,
            candle: item.clone(),
        },
        MarketEvent::RecoverableError {
            generation: Some(generation),
            error: ProviderError::RateLimited {
                context: ErrorContext::operation("history"),
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
    assert!(
        matches!(&events[1], MarketEvent::ReconcileBatch { generation: GapGeneration(7), revision: ReplayRevision(11), target_open_time: 1_700_000_000_000, candles } if candles == std::slice::from_ref(&item))
    );
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
fn typed_errors_expose_sanitized_context_without_raw_payload_or_control_sequences() {
    let secret = "bad\nmessage\r\u{1b}[31m";
    let message = SanitizedMessage::new(secret);
    assert!(!message.as_str().chars().any(char::is_control));
    assert!(message.as_str().len() <= SanitizedMessage::MAX_CHARS);

    let context = ErrorContext::operation("decode").with_market("binance", "BTCUSDT", "1m");
    let invalid_symbol = ProviderError::InvalidSymbol {
        context,
        code: -1121,
        message,
    };
    let rendered = invalid_symbol.to_string();
    assert!(rendered.contains("invalid symbol"));
    assert!(rendered.contains("binance"));
    assert!(rendered.contains("BTCUSDT"));
    assert!(rendered.contains("1m"));
    assert!(!rendered.contains('\n'));
    assert!(!rendered.contains('\r'));
    assert!(!rendered.contains('\u{1b}'));

    let raw_url = "https://user:secret@example.invalid/private?token=secret";
    let transport = ProviderError::Transport {
        context: ErrorContext::operation("websocket"),
        cause: SanitizedCause::Connection,
    };
    let display = transport.to_string();
    let debug = format!("{transport:?}");
    assert!(!display.contains(raw_url));
    assert!(!debug.contains(raw_url));
    assert!(matches!(
        transport,
        ProviderError::Transport {
            cause: SanitizedCause::Connection,
            ..
        }
    ));
}
