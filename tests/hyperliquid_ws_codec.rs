#![cfg(feature = "test-transport")]

use fccli::{
    model::{Instrument, Market, ProviderId, Timeframe},
    provider::hyperliquid::{DecodedFrame, WsConfig, decode_ws_frame, test_websocket_url},
};
use tokio_tungstenite::tungstenite::Message;

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("hyperliquid").expect("provider"),
        Market::Spot,
        "UBTC",
        "USDC",
        "@142",
    )
    .expect("instrument")
}

#[test]
fn test_websocket_url_has_no_stream_path() {
    let url = test_websocket_url("ws://127.0.0.1:9", &instrument(), Timeframe::Minute1)
        .expect("loopback url");
    assert_eq!(url.as_str(), "ws://127.0.0.1:9/");
}

#[test]
fn decode_open_and_closed_from_close_time() {
    let open = include_str!("fixtures/hyperliquid_candle_open.json");
    let closed = include_str!("fixtures/hyperliquid_candle_closed.json");
    let config = WsConfig::production();
    match decode_ws_frame(
        Message::Text(open.into()),
        &instrument(),
        Timeframe::Minute1,
        &config,
    ) {
        DecodedFrame::Candle(candle) => assert!(!candle.is_closed()),
        other => panic!("expected open candle, got {other:?}"),
    }
    match decode_ws_frame(
        Message::Text(closed.into()),
        &instrument(),
        Timeframe::Minute1,
        &config,
    ) {
        DecodedFrame::Candle(candle) => assert!(candle.is_closed()),
        other => panic!("expected closed candle, got {other:?}"),
    }
}

#[test]
fn subscription_response_is_ignored() {
    let config = WsConfig::production();
    assert_eq!(
        decode_ws_frame(
            Message::Text(r#"{"channel":"subscriptionResponse","data":{}}"#.into()),
            &instrument(),
            Timeframe::Minute1,
            &config,
        ),
        DecodedFrame::Ignored
    );
}
