#![cfg(feature = "test-transport")]

use std::sync::Arc;

use fccli::{
    clock::ManualClock,
    error::{ModelError, PayloadError, ProviderError, SanitizedMessage},
    model::{
        FinalityAuthority, HistoryRequest, Instrument, Market, MonoInstant, ProcessBlocker,
        ProviderId, RateGateState, Timeframe,
    },
    provider::binance::BinanceProvider,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

const VALID: &str = include_str!("fixtures/binance_klines.json");
const EMPTY: &str = "[]";

fn single_kline_fixture() -> serde_json::Value {
    let mut payload =
        serde_json::from_str::<serde_json::Value>(VALID).expect("valid kline fixture JSON");
    payload
        .as_array_mut()
        .expect("kline fixture root array")
        .truncate(1);
    payload
}

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("binance").expect("provider"),
        Market::Spot,
        "BTC",
        "USDT",
        "BTCUSDT",
    )
    .expect("instrument")
}

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(MonoInstant::ZERO))
}

fn provider(server: &MockServer, clock: Arc<ManualClock>) -> BinanceProvider {
    BinanceProvider::new_test(server.uri(), clock).expect("loopback provider")
}

async fn history(
    provider: &BinanceProvider,
    request: HistoryRequest,
) -> Result<Vec<fccli::model::Candle>, ProviderError> {
    provider
        .history(
            &instrument(),
            Timeframe::Minute1,
            request,
            CancellationToken::new(),
        )
        .await
}

fn payload_source(error: ProviderError) -> PayloadError {
    match error {
        ProviderError::Payload { source, .. } => source,
        other => panic!("expected payload error, got {other:?}"),
    }
}

#[tokio::test]
async fn latest_query_uses_exact_endpoint_utc_defaults_user_agent_and_no_extra_parameters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "1m"))
        .and(query_param("limit", "500"))
        .and(header(
            "user-agent",
            concat!("fccli/", env!("CARGO_PKG_VERSION")),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_raw(VALID, "application/json"))
        .expect(1)
        .mount(&server)
        .await;

    let candles = history(
        &provider(&server, clock()),
        HistoryRequest::latest(500).expect("latest"),
    )
    .await
    .expect("valid klines");
    assert_eq!(candles.len(), 2);
    assert_eq!(candles[0].open_time(), 1_704_067_200_000);
    assert_eq!(candles[1].open_time(), 1_704_067_260_000);
    assert_eq!(
        candles[0].authority(),
        FinalityAuthority::RestProvisionalOpen
    );
    assert!(!candles[0].is_closed());
    assert_eq!(
        (candles[0].open(), candles[0].high(), candles[0].low()),
        (42_000.10, 42_125.50, 41_950.25)
    );
    assert_eq!(
        (candles[0].close(), candles[0].base_volume()),
        (42_075.75, 123.456)
    );

    let requests = server.received_requests().await.expect("requests");
    let query: Vec<_> = requests[0].url.query_pairs().collect();
    assert_eq!(
        query.len(),
        3,
        "latest must omit startTime, endTime, and timeZone"
    );
}

#[tokio::test]
async fn perpetual_history_uses_fapi_path_on_the_injected_loopback_host() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/fapi/v1/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "1m"))
        .and(query_param("limit", "500"))
        .and(header(
            "user-agent",
            concat!("fccli/", env!("CARGO_PKG_VERSION")),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_raw(VALID, "application/json"))
        .expect(1)
        .mount(&server)
        .await;

    let perpetual = Instrument::new(
        ProviderId::new("binance").expect("provider"),
        Market::Perpetual,
        "BTC",
        "USDT",
        "BTCUSDT",
    )
    .expect("perpetual instrument");
    let candles = provider(&server, clock())
        .history(
            &perpetual,
            Timeframe::Minute1,
            HistoryRequest::latest(500).expect("latest"),
            CancellationToken::new(),
        )
        .await
        .expect("valid perpetual klines");
    assert_eq!(candles.len(), 2);

    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/fapi/v1/klines");
    let query: Vec<_> = requests[0].url.query_pairs().collect();
    assert_eq!(
        query.len(),
        3,
        "perpetual latest must omit startTime, endTime, and timeZone"
    );
}

#[tokio::test]
async fn older_and_gap_queries_use_checked_exact_cursors_limits_and_no_timezone() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "1m"))
        .and(query_param("limit", "1000"))
        .and(query_param("endTime", "1704067199999"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(EMPTY, "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "1M"))
        .and(query_param("limit", "17"))
        .and(query_param("startTime", "1704067200001"))
        .and(query_param("endTime", "1706745600000"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(EMPTY, "application/json"))
        .expect(1)
        .mount(&server)
        .await;

    history(
        &provider(&server, clock()),
        HistoryRequest::older(1_704_067_200_000, 1000).expect("older"),
    )
    .await
    .expect("older response");
    provider(&server, clock())
        .history(
            &instrument(),
            Timeframe::Month1,
            HistoryRequest::gap(1_704_067_200_001, 1_706_745_600_000, 17).expect("gap"),
            CancellationToken::new(),
        )
        .await
        .expect("gap response");

    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 2);
    for request in requests {
        let mut query: Vec<_> = request
            .url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        query.sort();
        if query.iter().any(|(key, _)| key == "startTime") {
            assert_eq!(
                query,
                vec![
                    ("endTime".to_owned(), "1706745600000".to_owned()),
                    ("interval".to_owned(), "1M".to_owned()),
                    ("limit".to_owned(), "17".to_owned()),
                    ("startTime".to_owned(), "1704067200001".to_owned()),
                    ("symbol".to_owned(), "BTCUSDT".to_owned()),
                ]
            );
        } else {
            assert_eq!(
                query,
                vec![
                    ("endTime".to_owned(), "1704067199999".to_owned()),
                    ("interval".to_owned(), "1m".to_owned()),
                    ("limit".to_owned(), "1000".to_owned()),
                    ("symbol".to_owned(), "BTCUSDT".to_owned()),
                ]
            );
        }
    }
}

#[test]
fn test_constructor_rejects_every_public_or_unsafe_base() {
    for base in [
        "https://127.0.0.1:1234",
        "http://example.com",
        "http://8.8.8.8",
        "http://localhost:1234",
        "http://user:secret@127.0.0.1:1234",
        "http://127.0.0.1:1234?next=https://example.com",
    ] {
        assert!(
            matches!(
                BinanceProvider::new_test(base, clock()),
                Err(ProviderError::Configuration(_))
            ),
            "accepted unsafe test base {base}"
        );
    }
}

#[tokio::test]
async fn structural_payload_failures_are_typed() {
    let cases = [
        ("{", PayloadError::MalformedJson),
        ("{}", PayloadError::ExpectedArray),
        (
            "[null]",
            PayloadError::WrongArity {
                expected: 12,
                actual: 0,
            },
        ),
        (
            "[[1,2]]",
            PayloadError::WrongArity {
                expected: 12,
                actual: 2,
            },
        ),
        (
            "[[null,\"1\",\"1\",\"1\",\"1\",\"1\",2,0,0,0,0,0]]",
            PayloadError::InvalidField { field: "open_time" },
        ),
        (
            "[[1,1,\"1\",\"1\",\"1\",\"1\",2,0,0,0,0,0]]",
            PayloadError::InvalidField { field: "open" },
        ),
    ];
    for (body, expected) in cases {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/klines"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let error = history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest"),
        )
        .await
        .expect_err("invalid payload");
        assert_eq!(payload_source(error), expected);
    }
}

#[tokio::test]
async fn invalid_numbers_nonfinite_values_and_candle_domain_errors_are_distinct() {
    let cases = [("nope", "open"), ("NaN", "open"), ("inf", "open")];
    for (invalid_open, expected_field) in cases {
        let server = MockServer::start().await;
        let mut payload = single_kline_fixture();
        payload[0][1] = serde_json::Value::String(invalid_open.to_owned());
        let body = serde_json::to_string(&payload).expect("serializable kline fixture JSON");
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let error = history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest"),
        )
        .await
        .expect_err("invalid number");
        match error {
            ProviderError::Payload {
                source: PayloadError::InvalidField { field },
                ..
            } => assert_eq!(field, expected_field),
            ProviderError::Domain {
                source: ModelError::NonFinite { field },
                ..
            } => assert_eq!(field, "open"),
            other => panic!("unexpected numeric error {other:?}"),
        }
    }

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            "[[1,\"2\",\"1\",\"0\",\"1\",\"1\",2,\"0\",0,\"0\",\"0\",\"0\"]]",
            "application/json",
        ))
        .mount(&server)
        .await;
    assert!(matches!(
        history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest")
        )
        .await,
        Err(ProviderError::Domain {
            source: ModelError::InvalidBodyBounds,
            ..
        })
    ));
}

#[tokio::test]
async fn invalid_symbol_and_generic_client_statuses_are_sanitized_and_nonretryable() {
    for (status, body, invalid_symbol) in [
        (400, r#"{"code":-1121,"msg":"Invalid symbol."}"#, true),
        (400, r#"{"code":-1100,"msg":"api_key_ABC123"}"#, false),
        (403, r#"{"code":-2015,"msg":"Basic dXNlcjpwYXNz"}"#, false),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let error = history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest"),
        )
        .await
        .expect_err("client error");
        if invalid_symbol {
            assert!(matches!(
                &error,
                ProviderError::InvalidSymbol {
                    code: -1121,
                    message: SanitizedMessage::InvalidSymbol,
                    ..
                }
            ));
        } else {
            assert!(
                matches!(&error, ProviderError::ClientStatus { status: actual, .. } if *actual == status)
            );
            assert!(!error.is_recoverable_for_history());
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains("ABC123") && !rendered.contains("dXNlcjpwYXNz"));
        }
    }
}

#[tokio::test]
async fn retry_after_matrix_extends_shared_gate_and_429_falls_back() {
    for (status, header_value, expected_ms) in [
        (429, Some("0"), 0),
        (429, Some("7"), 7_000),
        (429, None, 30_000),
        (429, Some("invalid"), 30_000),
        (429, Some("+1"), 30_000),
        (429, Some("-1"), 30_000),
        (429, Some("18446744073709551615"), 30_000),
        (418, Some("0"), 0),
        (418, Some("11"), 11_000),
    ] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(status);
        if let Some(value) = header_value {
            response = response.insert_header("retry-after", value);
        }
        Mock::given(method("GET"))
            .respond_with(response)
            .mount(&server)
            .await;
        let provider = provider(&server, clock());
        let error = history(&provider, HistoryRequest::latest(1).expect("latest"))
            .await
            .expect_err("rate limit");
        assert!(
            matches!(error, ProviderError::RateLimited { status: actual, .. } if actual == status)
        );
        assert_eq!(
            provider.rate_gate().current(),
            Ok(RateGateState::TimedUntil(
                MonoInstant::from_millis(expected_ms).expect("deadline")
            ))
        );
    }
}

#[tokio::test]
async fn missing_invalid_negative_or_overflowing_418_absorbs_the_shared_process_gate() {
    for value in [
        None,
        Some("invalid"),
        Some("-1"),
        Some("18446744073709551615"),
    ] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(418);
        if let Some(value) = value {
            response = response.insert_header("retry-after", value);
        }
        Mock::given(method("GET"))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
        let provider = provider(&server, clock());
        assert!(matches!(
            history(&provider, HistoryRequest::latest(1).expect("latest")).await,
            Err(ProviderError::InvalidBanExpiry)
        ));
        assert_eq!(
            provider.rate_gate().current(),
            Ok(RateGateState::ProcessBlocked(
                ProcessBlocker::InvalidBanExpiry
            ))
        );
        assert!(matches!(
            history(&provider, HistoryRequest::latest(1).expect("latest")).await,
            Err(ProviderError::InvalidBanExpiry)
        ));
    }
}

#[tokio::test]
async fn ignored_numeric_fields_are_finite_and_nonnegative() {
    for (index, value, field) in [
        (
            7,
            serde_json::Value::String("NaN".to_owned()),
            "quote_volume",
        ),
        (8, serde_json::json!(-1), "trade_count"),
        (9, serde_json::json!("-0.1"), "taker_buy_base_volume"),
        (
            10,
            serde_json::Value::String("inf".to_owned()),
            "taker_buy_quote_volume",
        ),
    ] {
        let server = MockServer::start().await;
        let mut payload = single_kline_fixture();
        payload[0][index] = value;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(payload))
            .mount(&server)
            .await;
        assert_eq!(
            payload_source(
                history(
                    &provider(&server, clock()),
                    HistoryRequest::latest(1).expect("latest"),
                )
                .await
                .expect_err("invalid ignored field")
            ),
            PayloadError::InvalidField { field }
        );
    }
}
