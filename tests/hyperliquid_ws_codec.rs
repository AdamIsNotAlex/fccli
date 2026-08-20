#![cfg(feature = "test-transport")]

use std::collections::VecDeque;

use fccli::{
    error::{PayloadError, ProviderError},
    model::{FinalityAuthority, Instrument, Market, ProviderId, Timeframe},
    provider::hyperliquid::{
        DecodedFrame, HyperliquidWsCodec, WsConfig, decode_ws_frame, test_websocket_url,
    },
};
use serde_json::{Value, json};
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

fn decode(codec: &mut HyperliquidWsCodec, value: Value) -> Vec<DecodedFrame> {
    let mut outcomes = VecDeque::new();
    decode_ws_frame(
        codec,
        Message::Text(value.to_string().into()),
        &instrument(),
        Timeframe::Minute1,
        &WsConfig::production(),
        &mut outcomes,
    );
    outcomes.into_iter().collect()
}

fn candle(open_time: i64, close: &str) -> Value {
    json!({
        "channel": "candle",
        "data": {
            "t": open_time,
            "T": open_time + 59_999,
            "s": "@142",
            "i": "1m",
            "o": "42000.10",
            "c": close,
            "h": "42125.50",
            "l": "41950.25",
            "v": "123.456",
            "n": 12
        }
    })
}

fn subscribe_ack() -> Value {
    json!({
        "channel": "subscriptionResponse",
        "data": {
            "method": "subscribe",
            "subscription": {"type": "candle", "coin": "@142", "interval": "1m"}
        }
    })
}

#[test]
fn test_websocket_url_has_no_stream_path() {
    let url = test_websocket_url("ws://127.0.0.1:9", &instrument(), Timeframe::Minute1)
        .expect("loopback url");
    assert_eq!(url.as_str(), "ws://127.0.0.1:9/");
}

#[test]
fn matching_subscribe_ack_is_accepted() {
    assert_eq!(
        decode(&mut HyperliquidWsCodec::new(), subscribe_ack()),
        vec![DecodedFrame::SubscribeAccepted]
    );
}

#[test]
fn mismatched_subscribe_ack_fields_are_rejected() {
    for (path, replacement) in [
        (("data", "method"), json!("unsubscribe")),
        (("subscription", "type"), json!("trades")),
        (("subscription", "coin"), json!("BTC")),
        (("subscription", "interval"), json!("5m")),
    ] {
        let mut ack = subscribe_ack();
        if path.0 == "data" {
            ack["data"][path.1] = replacement;
        } else {
            ack["data"]["subscription"][path.1] = replacement;
        }
        assert!(matches!(
            decode(&mut HyperliquidWsCodec::new(), ack).as_slice(),
            [DecodedFrame::ProviderError(ProviderError::Payload {
                source: PayloadError::MalformedProtocol,
                ..
            })]
        ));
    }
}

#[test]
fn application_pong_is_distinct_from_transport_control_frames() {
    let mut codec = HyperliquidWsCodec::new();
    assert_eq!(
        decode(&mut codec, json!({"channel": "pong"})),
        vec![DecodedFrame::ApplicationPong]
    );
    for frame in [
        Message::Ping(vec![1, 2, 3].into()),
        Message::Pong(vec![4, 5, 6].into()),
    ] {
        let mut outcomes = VecDeque::new();
        decode_ws_frame(
            &mut codec,
            frame,
            &instrument(),
            Timeframe::Minute1,
            &WsConfig::production(),
            &mut outcomes,
        );
        assert_eq!(outcomes, VecDeque::from([DecodedFrame::Ignored]));
    }
}

#[test]
fn successor_finality_state_machine_is_exact() {
    let mut codec = HyperliquidWsCodec::new();
    let first = decode(&mut codec, candle(1_704_067_200_000, "42075.75"));
    let [DecodedFrame::Candle(first)] = first.as_slice() else {
        panic!("expected first open candle: {first:?}");
    };
    assert_eq!(first.authority(), FinalityAuthority::WsAuthoritativeOpen);

    let changed = decode(&mut codec, candle(1_704_067_200_000, "42080.00"));
    let [DecodedFrame::Candle(changed)] = changed.as_slice() else {
        panic!("expected changed open candle: {changed:?}");
    };
    assert_eq!(changed.close(), 42_080.0);
    assert_eq!(changed.authority(), FinalityAuthority::WsAuthoritativeOpen);
    assert!(decode(&mut codec, candle(1_704_067_200_000, "42080.00")).is_empty());

    let successor = decode(&mut codec, candle(1_704_067_260_000, "42090.00"));
    let [DecodedFrame::Candle(closed), DecodedFrame::Candle(open)] = successor.as_slice() else {
        panic!("expected close then open: {successor:?}");
    };
    assert_eq!(closed.open_time(), 1_704_067_200_000);
    assert_eq!(closed.close(), 42_080.0);
    assert_eq!(closed.authority(), FinalityAuthority::WsAuthoritativeClosed);
    assert_eq!(open.open_time(), 1_704_067_260_000);
    assert_eq!(open.authority(), FinalityAuthority::WsAuthoritativeOpen);

    let skipped = decode(&mut codec, candle(1_704_067_380_000, "42100.00"));
    let [DecodedFrame::Candle(closed), DecodedFrame::Candle(open)] = skipped.as_slice() else {
        panic!("expected skipped successor close then open: {skipped:?}");
    };
    assert_eq!(closed.open_time(), 1_704_067_260_000);
    assert_eq!(open.open_time(), 1_704_067_380_000);
    assert!(decode(&mut codec, candle(1_704_067_320_000, "42095.00")).is_empty());
}

#[test]
fn local_wall_clock_and_wire_close_time_do_not_prove_finality() {
    let mut codec = HyperliquidWsCodec::new();
    let payload: Value =
        serde_json::from_str(include_str!("fixtures/hyperliquid_candle_closed.json"))
            .expect("fixture");
    let decoded = decode(&mut codec, payload);
    let [DecodedFrame::Candle(candle)] = decoded.as_slice() else {
        panic!("expected candle");
    };
    assert_eq!(candle.authority(), FinalityAuthority::WsAuthoritativeOpen);
}

#[test]
fn malformed_payload_and_market_echoes_are_rejected() {
    let mut codec = HyperliquidWsCodec::new();
    for payload in [
        json!({"channel": "candle", "data": {}}),
        {
            let mut value = candle(1_704_067_200_000, "42075.75");
            value["data"]["s"] = json!("BTC");
            value
        },
        {
            let mut value = candle(1_704_067_200_000, "42075.75");
            value["data"]["i"] = json!("5m");
            value
        },
    ] {
        assert!(matches!(
            decode(&mut codec, payload).as_slice(),
            [DecodedFrame::ProviderError(_)]
        ));
    }
}
