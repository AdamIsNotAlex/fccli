#![cfg(feature = "test-transport")]

use std::sync::Arc;

use fccli::{
    clock::ManualClock,
    error::ProviderError,
    model::{
        HistoryRequest, HistoryRequestKind, Instrument, InstrumentSpec, Market, MonoInstant,
        ProviderId, RateGateState, Timeframe,
    },
    provider::hyperliquid::{HyperliquidProvider, HyperliquidTestConfig},
};
fn perpetual_btc() -> Instrument {
    Instrument::new(
        ProviderId::new("hyperliquid").expect("provider"),
        Market::Perpetual,
        "BTC",
        "USDC",
        "BTC",
    )
    .expect("instrument")
}

fn candle_rows(count: usize) -> String {
    let rows = (0..count)
        .map(|index| {
            let open = 1_700_000_040_000_i64 + i64::try_from(index).expect("index") * 60_000;
            serde_json::json!({
                "T": open + 59_999,
                "c": "101.0",
                "h": "102.0",
                "i": "1m",
                "l": "99.0",
                "n": index,
                "o": "100.0",
                "s": "BTC",
                "t": open,
                "v": "1.0"
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&rows).expect("rows")
}

fn history_request(kind: HistoryRequestKind, limit: u16) -> HistoryRequest {
    match kind {
        HistoryRequestKind::Latest => HistoryRequest::latest(limit).expect("latest"),
        HistoryRequestKind::Older => {
            HistoryRequest::older(1_700_060_100_000, limit).expect("older")
        }
        HistoryRequestKind::Gap => {
            HistoryRequest::gap(1_700_000_040_000, 1_800_000_000_000, limit).expect("gap")
        }
    }
}

#[tokio::test]
async fn every_request_kind_enforces_retention_and_absolute_row_boundaries() {
    const LIMIT: u16 = 3;
    for kind in [
        HistoryRequestKind::Latest,
        HistoryRequestKind::Older,
        HistoryRequestKind::Gap,
    ] {
        for count in [usize::from(LIMIT), usize::from(LIMIT) + 1, 1001, 1002] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/info"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_raw(candle_rows(count), "application/json"),
                )
                .mount(&server)
                .await;
            let result = provider(&server, 1_700_060_100_000)
                .history(
                    &perpetual_btc(),
                    Timeframe::Minute1,
                    history_request(kind, LIMIT),
                    CancellationToken::new(),
                )
                .await;
            if count == 1002 {
                assert!(
                    matches!(result, Err(ProviderError::Payload { .. })),
                    "{kind:?}: {result:?}"
                );
                continue;
            }
            let candles = result.expect("bounded response");
            assert_eq!(candles.len(), usize::from(LIMIT), "{kind:?} count {count}");
            let first_index = match kind {
                HistoryRequestKind::Latest | HistoryRequestKind::Older => {
                    count - usize::from(LIMIT)
                }
                HistoryRequestKind::Gap => 0,
            };
            assert_eq!(
                candles[0].open_time(),
                1_700_000_040_000 + i64::try_from(first_index).expect("index") * 60_000,
                "{kind:?} count {count}"
            );
        }
    }
}

#[tokio::test]
async fn requested_limit_1000_accepts_the_1001_row_overlap() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(candle_rows(1001), "application/json"),
        )
        .mount(&server)
        .await;
    let candles = provider(&server, 1_700_060_100_000)
        .history(
            &perpetual_btc(),
            Timeframe::Minute1,
            HistoryRequest::latest(1000).expect("latest"),
            CancellationToken::new(),
        )
        .await
        .expect("1001-row overlap");
    assert_eq!(candles.len(), 1000);
    assert_eq!(candles[0].open_time(), 1_700_000_100_000);
}

#[tokio::test]
async fn trade_count_is_required_and_must_be_a_non_negative_integer() {
    for invalid_n in [
        None,
        Some(serde_json::Value::Null),
        Some(serde_json::json!(-1)),
        Some(serde_json::json!(1.5)),
        Some(serde_json::json!("1")),
    ] {
        let server = MockServer::start().await;
        let mut row = serde_json::from_str::<Vec<serde_json::Value>>(&candle_rows(1))
            .expect("row")
            .remove(0);
        if let Some(invalid_n) = invalid_n {
            row["n"] = invalid_n;
        } else {
            row.as_object_mut().expect("object").remove("n");
        }
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![row]))
            .mount(&server)
            .await;
        let error = provider(&server, 1_800_000_000_000)
            .history(
                &perpetual_btc(),
                Timeframe::Minute1,
                HistoryRequest::latest(1).expect("latest"),
                CancellationToken::new(),
            )
            .await
            .expect_err("invalid n");
        assert!(matches!(error, ProviderError::Payload { .. }), "{error}");
    }
}

#[tokio::test]
async fn rate_limit_uses_retry_after_or_local_fallback_without_process_blocking() {
    for (retry_after, expected_deadline) in [
        (
            Some("7"),
            MonoInstant::from_millis(7_000).expect("deadline"),
        ),
        (None, MonoInstant::from_millis(30_000).expect("deadline")),
        (
            Some("invalid"),
            MonoInstant::from_millis(30_000).expect("deadline"),
        ),
    ] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(429);
        if let Some(value) = retry_after {
            response = response.insert_header("Retry-After", value);
        }
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(response)
            .mount(&server)
            .await;
        let manual_clock = clock();
        let mut config = HyperliquidTestConfig::loopback(server.uri());
        config.now_ms = Some(1_800_000_000_000);
        let provider = HyperliquidProvider::new_test_with_config_and_clock(config, manual_clock)
            .expect("provider");
        let error = provider
            .history(
                &perpetual_btc(),
                Timeframe::Minute1,
                HistoryRequest::latest(1).expect("latest"),
                CancellationToken::new(),
            )
            .await
            .expect_err("rate limited");
        assert!(matches!(
            error,
            ProviderError::RateLimited { status: 429, .. }
        ));
        assert_eq!(
            provider.rate_gate().current(),
            Ok(RateGateState::TimedUntil(expected_deadline))
        );
    }
}

async fn malformed_rows_are_rejected(rows: Vec<serde_json::Value>, limit: u16) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rows))
        .mount(&server)
        .await;
    let error = provider(&server, 1_800_000_000_000)
        .history(
            &perpetual_btc(),
            Timeframe::Minute1,
            HistoryRequest::gap(1_700_000_040_000, 1_800_000_000_000, limit).expect("gap"),
            CancellationToken::new(),
        )
        .await
        .expect_err("malformed row");
    assert!(matches!(error, ProviderError::Payload { .. }), "{error}");
}

fn candle_row(open: i64) -> serde_json::Value {
    let mut row = serde_json::from_str::<Vec<serde_json::Value>>(&candle_rows(1))
        .expect("row")
        .remove(0);
    row["t"] = serde_json::json!(open);
    row["T"] = serde_json::json!(open + 59_999);
    row
}

#[tokio::test]
async fn rest_rejects_off_grid_candle_open() {
    malformed_rows_are_rejected(vec![candle_row(1_700_000_040_001)], 1).await;
}

#[tokio::test]
async fn rest_rejects_inconsistent_candle_close() {
    let mut row = candle_row(1_700_000_040_000);
    row["T"] = serde_json::json!(1_700_000_099_998_i64);
    malformed_rows_are_rejected(vec![row], 1).await;
}

#[tokio::test]
async fn rest_rejects_candle_outside_response_window() {
    malformed_rows_are_rejected(vec![candle_row(1_699_999_980_000)], 1).await;
}

#[tokio::test]
async fn rest_rejects_duplicate_and_regressive_rows() {
    for rows in [
        vec![candle_row(1_700_000_040_000), candle_row(1_700_000_040_000)],
        vec![candle_row(1_700_000_160_000), candle_row(1_700_000_100_000)],
    ] {
        malformed_rows_are_rejected(rows, 2).await;
    }
}

#[tokio::test]
async fn gap_validates_discarded_rows_after_retention_limit() {
    let mut discarded = candle_row(1_700_000_100_000);
    discarded["T"] = serde_json::json!(1_700_000_159_998_i64);
    malformed_rows_are_rejected(vec![candle_row(1_700_000_040_000), discarded], 1).await;
}

use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_partial_json, method, path},
};

const VALID: &str = include_str!("fixtures/hyperliquid_candles.json");

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(MonoInstant::ZERO))
}

fn spec(base: &str, market: Market) -> InstrumentSpec {
    InstrumentSpec::new_with_market(
        ProviderId::new("hyperliquid").expect("provider"),
        market,
        base,
        None::<String>,
    )
    .expect("spec")
}

fn provider(server: &MockServer, now_ms: i64) -> HyperliquidProvider {
    let mut config = HyperliquidTestConfig::loopback(server.uri());
    config.now_ms = Some(now_ms);
    HyperliquidProvider::new_test_with_config_and_clock(config, clock()).expect("provider")
}

#[test]
fn canonicalize_remaps_locked_aliases_and_wire_coins() {
    let provider = HyperliquidProvider::new_test("http://127.0.0.1:9", clock()).expect("provider");

    let spot_btc = provider
        .canonicalize(&spec("btc", Market::Spot))
        .expect("spot btc");
    assert_eq!(spot_btc.base(), "UBTC");
    assert_eq!(spot_btc.quote(), "USDC");
    assert_eq!(spot_btc.display_pair(), "UBTC/USDC");
    assert_eq!(spot_btc.provider_symbol(), "@142");

    let perp_btc = provider
        .canonicalize(&spec("btc", Market::Perpetual))
        .expect("perp btc");
    assert_eq!(perp_btc.base(), "BTC");
    assert_eq!(perp_btc.provider_symbol(), "BTC");

    let hype = provider
        .canonicalize(&spec("hype", Market::Spot))
        .expect("hype");
    assert_eq!(hype.provider_symbol(), "@107");

    let purr = provider
        .canonicalize(&spec("purr", Market::Spot))
        .expect("purr");
    assert_eq!(purr.provider_symbol(), "PURR/USDC");

    let hip3 = InstrumentSpec::new_with_market_and_venue(
        ProviderId::new("hyperliquid").expect("provider"),
        Market::Perpetual,
        "XYZ100",
        None::<String>,
        Some("xyz"),
    )
    .expect("hip3 spec");
    let hip3 = provider.canonicalize(&hip3).expect("hip3");
    assert_eq!(hip3.provider_symbol(), "xyz:XYZ100");
    assert_eq!(hip3.display_pair(), "XYZ100/USDC");
}

#[tokio::test]
async fn latest_posts_candle_snapshot_with_wire_coin_and_window() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(body_partial_json(serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": "@142",
                "interval": "1m",
                "startTime": 1_704_067_140_001_i64,
                "endTime": 1_704_067_200_000_i64,
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_raw(VALID, "application/json"))
        .mount(&server)
        .await;

    let provider = provider(&server, 1_704_067_200_000);
    let instrument = provider
        .canonicalize(&spec("btc", Market::Spot))
        .expect("canonical");
    let candles = provider
        .history(
            &instrument,
            Timeframe::Minute1,
            HistoryRequest::latest(1).expect("latest"),
            CancellationToken::new(),
        )
        .await
        .expect("history");
    assert_eq!(candles.len(), 1);
    assert_eq!(candles[0].open_time(), 1_704_067_200_000);
}

#[tokio::test]
async fn unsupported_timeframes_fail_before_network() {
    let server = MockServer::start().await;
    let provider = provider(&server, 1_704_067_200_000);
    let instrument = Instrument::new(
        ProviderId::new("hyperliquid").expect("provider"),
        Market::Perpetual,
        "BTC",
        "USDC",
        "BTC",
    )
    .expect("instrument");
    for timeframe in [Timeframe::Second1, Timeframe::Hour6] {
        let error = provider
            .history(
                &instrument,
                timeframe,
                HistoryRequest::latest(1).expect("latest"),
                CancellationToken::new(),
            )
            .await
            .expect_err("unsupported");
        assert!(
            matches!(error, ProviderError::Configuration(message) if message.contains("1s or 6h")),
            "{error}"
        );
    }
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );
}

fn extra_row_fixture() -> String {
    let newer = serde_json::json!({
        "T": 1_704_067_319_999_i64,
        "c": "42100.00",
        "h": "42150.00",
        "i": "1m",
        "l": "42000.00",
        "n": 10,
        "o": "42075.75",
        "s": "@142",
        "t": 1_704_067_260_000_i64,
        "v": "1.0"
    });
    let mut rows = serde_json::from_str::<Vec<serde_json::Value>>(VALID).expect("fixture");
    rows.push(newer);
    serde_json::to_string(&rows).expect("extra-row fixture")
}

#[tokio::test]
async fn latest_truncates_extra_rows_to_newest_limit() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(extra_row_fixture(), "application/json"),
        )
        .mount(&server)
        .await;

    let provider = provider(&server, 1_704_067_260_000);
    let instrument = provider
        .canonicalize(&spec("btc", Market::Spot))
        .expect("canonical");
    let candles = provider
        .history(
            &instrument,
            Timeframe::Minute1,
            HistoryRequest::latest(1).expect("latest"),
            CancellationToken::new(),
        )
        .await
        .expect("history");
    assert_eq!(candles.len(), 1);
    assert_eq!(candles[0].open_time(), 1_704_067_260_000);
}

#[tokio::test]
async fn gap_window_is_capped_to_limit_intervals() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(body_partial_json(serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": "BTC",
                "interval": "1m",
                "startTime": 1_704_067_200_000_i64,
                "endTime": 1_704_067_259_999_i64,
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
        .mount(&server)
        .await;

    let provider = provider(&server, 1_704_067_200_000);
    let instrument = Instrument::new(
        ProviderId::new("hyperliquid").expect("provider"),
        Market::Perpetual,
        "BTC",
        "USDC",
        "BTC",
    )
    .expect("instrument");
    let candles = provider
        .history(
            &instrument,
            Timeframe::Minute1,
            HistoryRequest::gap(1_704_067_200_000, 1_704_067_400_000, 1).expect("gap"),
            CancellationToken::new(),
        )
        .await
        .expect("history");
    assert!(candles.is_empty());
}

#[tokio::test]
async fn info_error_object_is_invalid_symbol() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(r#"{"error":"unknown coin"}"#, "application/json"),
        )
        .mount(&server)
        .await;

    let provider = provider(&server, 1_704_067_200_000);
    let instrument = Instrument::new(
        ProviderId::new("hyperliquid").expect("provider"),
        Market::Perpetual,
        "BTC",
        "USDC",
        "BTC",
    )
    .expect("instrument");
    let error = provider
        .history(
            &instrument,
            Timeframe::Minute1,
            HistoryRequest::latest(1).expect("latest"),
            CancellationToken::new(),
        )
        .await
        .expect_err("error object");
    assert!(
        matches!(error, ProviderError::InvalidSymbol { message, .. } if message.as_str() == "invalid symbol"),
        "{error}"
    );
}

#[tokio::test]
async fn binance_ban_status_without_metadata_cannot_block_hyperliquid() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(ResponseTemplate::new(418).set_body_json(serde_json::json!({
            "code": -1003,
            "msg": "banned"
        })))
        .mount(&server)
        .await;
    let provider = provider(&server, 1_800_000_000_000);
    let error = provider
        .history(
            &perpetual_btc(),
            Timeframe::Minute1,
            HistoryRequest::latest(1).expect("latest"),
            CancellationToken::new(),
        )
        .await
        .expect_err("client status");
    assert!(matches!(
        error,
        ProviderError::ClientStatus { status: 418, .. }
    ));
    assert_eq!(provider.rate_gate().current(), Ok(RateGateState::Open));
}

#[test]
fn loopback_constructor_rejects_public_hosts() {
    match HyperliquidProvider::new_test("https://api.hyperliquid.xyz", clock()) {
        Ok(_) => panic!("public host"),
        Err(error) => assert!(matches!(error, ProviderError::Configuration(_))),
    }
}
