#![cfg(all(feature = "test-transport", not(feature = "production-transport")))]

use fccli::{
    clock::ManualClock,
    error::ProviderError,
    model::{
        HistoryRequest, Instrument, Market, MonoInstant, ProviderId, RateGateState, Timeframe,
    },
    provider::okx::{OkxProvider, OkxTestConfig},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const NOW_MS: i64 = 1_800_000_000_000;

fn instrument(market: Market) -> Instrument {
    Instrument::new(
        ProviderId::new("okx").unwrap(),
        market,
        "BTC",
        "USDT",
        if market == Market::Spot {
            "BTC-USDT"
        } else {
            "BTC-USDT-SWAP"
        },
    )
    .unwrap()
}

fn provider(server: &MockServer) -> OkxProvider {
    let mut config = OkxTestConfig::loopback(server.uri());
    config.now_ms = Some(NOW_MS);
    OkxProvider::new_test_with_config_and_clock(
        config,
        Arc::new(ManualClock::new(MonoInstant::ZERO)),
    )
    .unwrap()
}

fn envelope(rows: serde_json::Value) -> String {
    json!({"code":"0","msg":"","data":rows}).to_string()
}

fn row(open: i64) -> serde_json::Value {
    json!([
        open.to_string(),
        "10",
        "12",
        "9",
        "11",
        "5",
        "0.005",
        "55",
        "1"
    ])
}

async fn history(
    provider: &OkxProvider,
    market: Market,
    timeframe: Timeframe,
    request: HistoryRequest,
) -> Result<Vec<fccli::model::Candle>, ProviderError> {
    provider
        .history(
            &instrument(market),
            timeframe,
            request,
            CancellationToken::new(),
        )
        .await
}

#[tokio::test]
async fn latest_uses_exact_native_request_and_decodes_strict_newest_first() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/market/candles"))
        .and(query_param("instId", "BTC-USDT"))
        .and(query_param("bar", "1m"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([
            row(1_700_000_100_000),
            row(1_700_000_040_000)
        ]))))
        .expect(1)
        .mount(&server)
        .await;
    let candles = history(
        &provider(&server),
        Market::Spot,
        Timeframe::Minute1,
        HistoryRequest::latest(2).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        candles.iter().map(|c| c.open_time()).collect::<Vec<_>>(),
        [1_700_000_040_000, 1_700_000_100_000]
    );
    assert_eq!(candles[0].base_volume(), 5.0);
    assert_eq!(candles[0].close_time(), 1_700_000_099_999);
}

#[tokio::test]
async fn older_and_gap_cursors_are_strict_and_gap_page_is_forward_bounded() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .and(query_param("after", "1700000039999"))
        .and(query_param("limit", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([
            row(1_699_999_980_000),
            row(1_699_999_920_000)
        ]))))
        .expect(1)
        .mount(&server)
        .await;
    let older = history(
        &provider(&server),
        Market::Spot,
        Timeframe::Minute1,
        HistoryRequest::older(1_700_000_040_000, 3).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        older
            .iter()
            .map(|candle| candle.open_time())
            .collect::<Vec<_>>(),
        [1_699_999_920_000, 1_699_999_980_000]
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .and(query_param("before", "1700000039999"))
        .and(query_param("after", "1700000160001"))
        .and(query_param("limit", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([
            row(1_700_000_160_000),
            row(1_700_000_100_000),
            row(1_700_000_040_000)
        ]))))
        .expect(1)
        .mount(&server)
        .await;
    let candles = history(
        &provider(&server),
        Market::Spot,
        Timeframe::Minute1,
        HistoryRequest::gap(1_700_000_040_000, 1_799_999_940_000, 3).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(candles.last().unwrap().open_time(), 1_700_000_160_000);
}

#[tokio::test]
async fn older_rejects_a_candle_equal_to_the_exclusive_end_boundary() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .and(query_param("after", "1700000039999"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(envelope(json!([row(1_700_000_040_000)]))),
        )
        .expect(1)
        .mount(&server)
        .await;

    let error = history(
        &provider(&server),
        Market::Spot,
        Timeframe::Minute1,
        HistoryRequest::older(1_700_000_040_000, 3).unwrap(),
    )
    .await
    .expect_err("boundary candle must be rejected");
    assert!(matches!(
        error,
        ProviderError::Payload {
            source: fccli::error::PayloadError::MalformedProtocol,
            ..
        }
    ));
}

#[tokio::test]
async fn calendar_month_gap_uses_three_aligned_bars_not_the_distant_newest_tail() {
    let server = MockServer::start().await;
    let january = 1_704_067_200_000;
    let march = 1_709_251_200_000;
    Mock::given(method("GET"))
        .and(query_param("bar", "1Mutc"))
        .and(query_param("before", (january - 1).to_string()))
        .and(query_param("after", (march + 1).to_string()))
        .and(query_param("limit", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([
            row(march),
            row(1_706_745_600_000),
            row(january)
        ]))))
        .expect(1)
        .mount(&server)
        .await;
    let candles = history(
        &provider(&server),
        Market::Spot,
        Timeframe::Month1,
        HistoryRequest::gap(january, 1_800_000_000_000, 3).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(candles.len(), 3);
}

#[tokio::test]
async fn every_rest_bar_spelling_is_exact_and_preflight_rejects_8h_and_over_300_without_io() {
    let cases = [
        (Timeframe::Second1, "1s"),
        (Timeframe::Minute1, "1m"),
        (Timeframe::Minute3, "3m"),
        (Timeframe::Minute5, "5m"),
        (Timeframe::Minute15, "15m"),
        (Timeframe::Minute30, "30m"),
        (Timeframe::Hour1, "1H"),
        (Timeframe::Hour2, "2H"),
        (Timeframe::Hour4, "4H"),
        (Timeframe::Hour6, "6Hutc"),
        (Timeframe::Hour12, "12Hutc"),
        (Timeframe::Day1, "1Dutc"),
        (Timeframe::Day3, "3Dutc"),
        (Timeframe::Week1, "1Wutc"),
        (Timeframe::Month1, "1Mutc"),
    ];
    for (timeframe, spelling) in cases {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param("bar", spelling))
            .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([]))))
            .expect(1)
            .mount(&server)
            .await;
        history(
            &provider(&server),
            Market::Spot,
            timeframe,
            HistoryRequest::latest(1).unwrap(),
        )
        .await
        .unwrap();
    }
    let server = MockServer::start().await;
    let provider = provider(&server);
    assert!(
        history(
            &provider,
            Market::Spot,
            Timeframe::Hour8,
            HistoryRequest::latest(1).unwrap()
        )
        .await
        .is_err()
    );
    assert!(
        history(
            &provider,
            Market::Spot,
            Timeframe::Minute1,
            HistoryRequest::latest(301).unwrap()
        )
        .await
        .is_err()
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn swap_selects_vol_ccy_but_all_volume_fields_must_be_finite_and_nonnegative() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(envelope(json!([row(1_700_000_040_000)]))),
        )
        .mount(&server)
        .await;
    let candles = history(
        &provider(&server),
        Market::Perpetual,
        Timeframe::Minute1,
        HistoryRequest::latest(1).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(candles[0].base_volume(), 0.005);

    for fields in [
        json!(["1700000040000", "10", "12", "9", "11", "-1", "1", "55", "1"]),
        json!([
            "1700000040000",
            "10",
            "12",
            "9",
            "11",
            "5",
            "NaN",
            "55",
            "1"
        ]),
        json!(["1700000040000", "10", "12", "9", "11", "5", "1", "-1", "1"]),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([fields]))))
            .mount(&server)
            .await;
        assert!(
            history(
                &provider(&server),
                Market::Spot,
                Timeframe::Minute1,
                HistoryRequest::latest(1).unwrap()
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn rows_require_nine_strings_confirm_grid_window_order_and_future_bound() {
    let invalid_rows = [
        json!(["1700000040000", "10", "12", "9", "11", "5", "1", "55"]),
        json!([
            1700000040000_i64,
            "10",
            "12",
            "9",
            "11",
            "5",
            "1",
            "55",
            "1"
        ]),
        json!(["1700000040000", "10", "12", "9", "11", "5", "1", "55", "2"]),
        json!(["1700000040001", "10", "12", "9", "11", "5", "1", "55", "1"]),
        json!(["1800000360000", "10", "12", "9", "11", "5", "1", "55", "1"]),
    ];
    for invalid in invalid_rows {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([invalid]))))
            .mount(&server)
            .await;
        assert!(
            history(
                &provider(&server),
                Market::Spot,
                Timeframe::Minute1,
                HistoryRequest::latest(1).unwrap()
            )
            .await
            .is_err()
        );
    }

    for rows in [
        json!([row(1_700_000_040_000), row(1_700_000_100_000)]),
        json!([row(1_700_000_220_000)]),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(envelope(rows)))
            .mount(&server)
            .await;
        assert!(
            history(
                &provider(&server),
                Market::Spot,
                Timeframe::Minute1,
                HistoryRequest::gap(1_700_000_040_000, 1_700_000_160_000, 2).unwrap()
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn weekly_grid_is_monday_and_month_grid_is_first_day() {
    for (timeframe, open) in [
        (Timeframe::Week1, 1_704_067_200_000),
        (Timeframe::Month1, 1_704_067_200_000),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(envelope(json!([row(open)]))))
            .mount(&server)
            .await;
        assert!(
            history(
                &provider(&server),
                Market::Spot,
                timeframe,
                HistoryRequest::latest(1).unwrap()
            )
            .await
            .is_ok()
        );
    }
}

#[tokio::test]
async fn business_codes_classify_symbol_and_publish_timed_rate_gate_without_leaking_message() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(json!({"code":"51001","msg":"bad\nsecret","data":[]}).to_string()),
        )
        .mount(&server)
        .await;
    let error = history(
        &provider(&server),
        Market::Spot,
        Timeframe::Minute1,
        HistoryRequest::latest(1).unwrap(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ProviderError::InvalidSymbol { code: 51001, .. }
    ));
    assert!(!error.to_string().contains('\n'));

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(
                json!({"code":"51000","msg":"parameter error","data":[]}).to_string(),
            ),
        )
        .mount(&server)
        .await;
    assert!(matches!(
        history(
            &provider(&server),
            Market::Spot,
            Timeframe::Minute1,
            HistoryRequest::latest(1).unwrap()
        )
        .await
        .unwrap_err(),
        ProviderError::ClientStatus {
            code: Some(51000),
            ..
        }
    ));

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(json!({"code":"50011","msg":"rate limit","data":[]}).to_string()),
        )
        .mount(&server)
        .await;
    let mut config = OkxTestConfig::loopback(server.uri());
    config.now_ms = Some(NOW_MS);
    config.rate_limit_fallback = Duration::from_secs(7);
    let provider = OkxProvider::new_test_with_config_and_clock(
        config,
        Arc::new(ManualClock::new(MonoInstant::ZERO)),
    )
    .unwrap();
    assert!(
        history(
            &provider,
            Market::Spot,
            Timeframe::Minute1,
            HistoryRequest::latest(1).unwrap()
        )
        .await
        .is_err()
    );
    assert_eq!(
        provider.rate_gate().current(),
        Ok(RateGateState::TimedUntil(
            MonoInstant::from_millis(7_000).unwrap()
        ))
    );
}
