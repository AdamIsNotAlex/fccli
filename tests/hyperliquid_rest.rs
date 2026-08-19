#![cfg(feature = "test-transport")]

use std::sync::Arc;

use fccli::{
    clock::ManualClock,
    error::ProviderError,
    model::{
        HistoryRequest, Instrument, InstrumentSpec, Market, MonoInstant, ProviderId, Timeframe,
    },
    provider::hyperliquid::{HyperliquidProvider, HyperliquidTestConfig},
};
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
    assert!(server.received_requests().await.unwrap_or_default().is_empty());
}

fn extra_row_fixture() -> String {
    let newer = serde_json::json!({
        "T": 1_704_067_265_999_i64,
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
        .respond_with(ResponseTemplate::new(200).set_body_raw(extra_row_fixture(), "application/json"))
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
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"error":"unknown coin"}"#,
            "application/json",
        ))
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

#[test]
fn loopback_constructor_rejects_public_hosts() {
    match HyperliquidProvider::new_test("https://api.hyperliquid.xyz", clock()) {
        Ok(_) => panic!("public host"),
        Err(error) => assert!(matches!(error, ProviderError::Configuration(_))),
    }
}
