#![cfg(feature = "test-transport")]

use fccli::{
    model::{Instrument, Market, ProviderId, Timeframe},
    provider::{
        okx::{OkxWsCodec, decode_ws_frame, test_subscribe_message, test_websocket_url},
        test_transport::{DecodedFrame, OkxDecoded, WsConfig},
    },
};
use serde_json::{Value, json};
use std::collections::VecDeque;
use tokio_tungstenite::tungstenite::Message;

const NOW_MS: i64 = 1_800_000_000_000;

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("okx").unwrap(),
        Market::Spot,
        "BTC",
        "USDT",
        "BTC-USDT",
    )
    .unwrap()
}

fn decode(codec: &mut OkxWsCodec, value: impl Into<Message>) -> Vec<DecodedFrame<OkxDecoded>> {
    let mut output = VecDeque::new();
    decode_ws_frame(
        codec,
        value.into(),
        &instrument(),
        Timeframe::Minute1,
        &WsConfig::production(),
        &mut output,
    );
    output.into_iter().collect()
}

fn ack(id: &str, channel: &str) -> String {
    json!({"id":id,"event":"subscribe","arg":{"channel":channel,"instId":"BTC-USDT"}}).to_string()
}

fn candle_message(open_time: i64, close: &str) -> String {
    json!({
        "arg":{"channel":"candle1m","instId":"BTC-USDT"},
        "data":[[open_time.to_string(),"10","12","9",close,"5","0.005","55","0"]]
    })
    .to_string()
}

#[test]
fn loopback_url_exact_ack_and_subscription_id_are_enforced() {
    assert_eq!(
        test_websocket_url("ws://127.0.0.1:9", &instrument(), Timeframe::Minute1)
            .unwrap()
            .as_str(),
        "ws://127.0.0.1:9/"
    );
    let payload: Value =
        serde_json::from_str(&test_subscribe_message(&instrument(), Timeframe::Minute1)).unwrap();
    let id = payload["id"].as_str().unwrap();
    assert!(!id.is_empty() && id.bytes().all(|byte| byte.is_ascii_alphanumeric()));
    assert_eq!(payload["op"], "subscribe");
    assert_eq!(payload["args"][0]["channel"], "candle1m");
    assert_eq!(payload["args"][0]["instId"], "BTC-USDT");

    assert_eq!(
        decode(
            &mut OkxWsCodec::with_now_ms(NOW_MS),
            Message::Text(ack(id, "candle1m").into()),
        ),
        vec![DecodedFrame::Provider(OkxDecoded::SubscribeAccepted {
            buffered: Vec::new()
        })]
    );
    for invalid in [
        ack("wrong", "candle1m"),
        ack(id, "candle5m"),
        json!({"id":id,"event":"subscribe","arg":{"channel":"candle1m","instId":"ETH-USDT"}})
            .to_string(),
        json!({"id":id,"event":"subscribed","arg":{"channel":"candle1m","instId":"BTC-USDT"}})
            .to_string(),
    ] {
        assert!(matches!(
            decode(
                &mut OkxWsCodec::with_now_ms(NOW_MS),
                Message::Text(invalid.into())
            )
            .as_slice(),
            [DecodedFrame::ProviderError(_)]
        ));
    }
}

#[test]
fn every_supported_channel_spelling_is_exact_and_8h_is_rejected() {
    let cases = [
        (Timeframe::Second1, "candle1s"),
        (Timeframe::Minute1, "candle1m"),
        (Timeframe::Minute3, "candle3m"),
        (Timeframe::Minute5, "candle5m"),
        (Timeframe::Minute15, "candle15m"),
        (Timeframe::Minute30, "candle30m"),
        (Timeframe::Hour1, "candle1H"),
        (Timeframe::Hour2, "candle2H"),
        (Timeframe::Hour4, "candle4H"),
        (Timeframe::Hour6, "candle6Hutc"),
        (Timeframe::Hour12, "candle12Hutc"),
        (Timeframe::Day1, "candle1Dutc"),
        (Timeframe::Day3, "candle3Dutc"),
        (Timeframe::Week1, "candle1Wutc"),
        (Timeframe::Month1, "candle1Mutc"),
    ];
    for (timeframe, expected) in cases {
        let payload: Value =
            serde_json::from_str(&test_subscribe_message(&instrument(), timeframe)).unwrap();
        assert_eq!(payload["args"][0]["channel"], expected);
    }
    assert!(test_websocket_url("ws://127.0.0.1:9", &instrument(), Timeframe::Hour8).is_err());
}

#[test]
fn candle_before_ack_is_coalesced_and_transferred_in_single_ready_outcome() {
    let mut codec = OkxWsCodec::with_now_ms(NOW_MS);
    assert!(
        decode(
            &mut codec,
            Message::Text(candle_message(1_700_000_040_000, "11").into())
        )
        .is_empty()
    );
    assert!(
        decode(
            &mut codec,
            Message::Text(candle_message(1_700_000_040_000, "11.5").into())
        )
        .is_empty()
    );
    let output = decode(&mut codec, Message::Text(ack("fccli1", "candle1m").into()));
    match output.as_slice() {
        [DecodedFrame::Provider(OkxDecoded::SubscribeAccepted { buffered })] => {
            assert_eq!(buffered.len(), 1);
            assert_eq!(buffered[0].close(), 11.5);
        }
        other => panic!("unexpected readiness output: {other:?}"),
    }
}

#[test]
fn pre_ack_buffer_overflow_errors_without_evicting_and_duplicate_ack_is_invalid() {
    let mut codec = OkxWsCodec::with_now_ms(NOW_MS);
    for index in 0..16 {
        let open = 1_700_000_040_000 + index * 60_000;
        assert!(decode(&mut codec, Message::Text(candle_message(open, "11").into())).is_empty());
    }
    assert!(matches!(
        decode(
            &mut codec,
            Message::Text(candle_message(1_700_001_000_000, "11").into())
        )
        .as_slice(),
        [DecodedFrame::ProviderError(_)]
    ));

    let mut ready = OkxWsCodec::with_now_ms(NOW_MS);
    assert!(matches!(
        decode(&mut ready, Message::Text(ack("fccli1", "candle1m").into())).as_slice(),
        [DecodedFrame::Provider(OkxDecoded::SubscribeAccepted { .. })]
    ));
    assert!(matches!(
        decode(&mut ready, Message::Text(ack("fccli1", "candle1m").into())).as_slice(),
        [DecodedFrame::ProviderError(_)]
    ));
}

#[test]
fn pong_and_only_service_notice_64008_have_special_meaning() {
    assert_eq!(
        decode(
            &mut OkxWsCodec::with_now_ms(NOW_MS),
            Message::Text("pong".into())
        ),
        vec![DecodedFrame::Provider(OkxDecoded::ApplicationPong)]
    );
    let reconnect = json!({"event":"notice","code":"64008","msg":"service upgrade"}).to_string();
    assert_eq!(
        decode(
            &mut OkxWsCodec::with_now_ms(NOW_MS),
            Message::Text(reconnect.into())
        ),
        vec![DecodedFrame::ReconnectRequested]
    );
    let ordinary = json!({"event":"notice","code":"64009","msg":"ordinary"}).to_string();
    assert_eq!(
        decode(
            &mut OkxWsCodec::with_now_ms(NOW_MS),
            Message::Text(ordinary.into())
        ),
        vec![DecodedFrame::Ignored]
    );
}

#[test]
fn live_rows_enforce_grid_volume_confirm_and_future_skew() {
    for row in [
        json!({"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1700000040001","10","12","9","11","5","1","55","0"]]}),
        json!({"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1700000040000","10","12","9","11","-1","1","55","0"]]}),
        json!({"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1700000040000","10","12","9","11","5","1","NaN","0"]]}),
        json!({"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1700000040000","10","12","9","11","5","1","55","2"]]}),
        json!({"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1800000360000","10","12","9","11","5","1","55","0"]]}),
    ] {
        assert!(matches!(
            decode(
                &mut OkxWsCodec::with_now_ms(NOW_MS),
                Message::Text(row.to_string().into())
            )
            .as_slice(),
            [DecodedFrame::ProviderError(_)]
        ));
    }
}
