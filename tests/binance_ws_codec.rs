#![cfg(feature = "test-transport")]

use std::time::Duration;

use fccli::{
    error::{PayloadError, ProviderError, SanitizedCause, SanitizedMessage, TimeoutKind},
    model::{FinalityAuthority, Instrument, Market, ProviderId, Timeframe},
    provider::{
        binance::{decode_ws_frame, test_websocket_url},
        test_transport::{
            BinanceDecoded, DecodedFrame, WS_FRAME_SIZE, WS_MAX_WRITE_BUFFER_SIZE,
            WS_MESSAGE_INACTIVITY_TIMEOUT, WS_MESSAGE_SIZE, WS_READ_BUFFER_SIZE,
            WS_STALLED_WRITE_TIMEOUT, WS_WRITE_BUFFER_SIZE, WsConfig, read_raw_websocket,
            send_raw_websocket,
        },
    },
};
use futures_util::{SinkExt, StreamExt};
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{
        Message,
        protocol::{
            CloseFrame,
            frame::{
                Frame,
                coding::{CloseCode, Data, OpCode},
            },
        },
    },
};

const OPEN: &str = include_str!("fixtures/binance_kline_open.json");
const CLOSED: &str = include_str!("fixtures/binance_kline_closed.json");

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

fn payload_error(frame: DecodedFrame<BinanceDecoded>) -> PayloadError {
    match frame {
        DecodedFrame::ProviderError(ProviderError::Payload { source, .. }) => source,
        other => panic!("expected payload error, got {other:?}"),
    }
}

#[test]
fn all_timeframe_stream_paths_are_exact() {
    for timeframe in Timeframe::ALL {
        let url =
            test_websocket_url("ws://127.0.0.1:32123", &instrument(), timeframe).expect("test URL");
        assert_eq!(
            url.as_str(),
            format!(
                "ws://127.0.0.1:32123/ws/btcusdt@kline_{}",
                timeframe.as_str()
            )
        );
    }
}

#[test]
fn loopback_stream_urls_append_only_the_binance_path() {
    let expected_path = "/ws/btcusdt@kline_1m";
    for base in ["ws://127.0.0.1:32123", "ws://[::1]:32123"] {
        let url = test_websocket_url(base, &instrument(), Timeframe::Minute1)
            .expect("literal loopback URL");
        assert_eq!(url.path(), expected_path);
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
    }
}

#[test]
fn open_and_closed_fixtures_decode_to_authoritative_candles() {
    let config = WsConfig::production();
    let open = match decode_ws_frame(
        Message::Text(OPEN.into()),
        &instrument(),
        Timeframe::Minute1,
        &config,
    ) {
        DecodedFrame::Provider(BinanceDecoded::Candle(candle)) => candle,
        other => panic!("expected open candle, got {other:?}"),
    };
    assert_eq!(open.open_time(), 1_700_000_040_000);
    assert_eq!(open.close_time(), 1_700_000_099_999);
    assert_eq!(
        (
            open.open(),
            open.high(),
            open.low(),
            open.close(),
            open.base_volume()
        ),
        (37_000.0, 37_050.0, 36_975.25, 37_025.5, 12.5)
    );
    assert_eq!(open.authority(), FinalityAuthority::WsAuthoritativeOpen);

    let closed = match decode_ws_frame(
        Message::Binary(CLOSED.as_bytes().to_vec().into()),
        &instrument(),
        Timeframe::Minute1,
        &config,
    ) {
        DecodedFrame::Provider(BinanceDecoded::Candle(candle)) => candle,
        other => panic!("expected closed candle, got {other:?}"),
    };
    assert_eq!(closed.open_time(), open.open_time());
    assert_eq!(closed.authority(), FinalityAuthority::WsAuthoritativeClosed);
    assert!(closed.is_closed());
}

#[test]
fn malformed_oversized_mismatched_and_provider_frames_are_typed() {
    let config = WsConfig::production();
    assert_eq!(
        payload_error(decode_ws_frame(
            Message::Text("{".into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        )),
        PayloadError::MalformedProtocol
    );

    let mut tiny = config;
    tiny.max_message_size = 32;
    tiny.max_frame_size = 32;
    assert_eq!(
        payload_error(decode_ws_frame(
            Message::Binary(vec![b'x'; 33].into()),
            &instrument(),
            Timeframe::Minute1,
            &tiny
        )),
        PayloadError::OverBudget { limit_bytes: 32 }
    );

    let parsed: serde_json::Value = serde_json::from_str(OPEN).expect("fixture JSON");
    for (label, pointer, replacement) in [
        ("outer symbol missing", "/s", None),
        ("outer symbol mismatch", "/s", Some("ETHUSDT")),
        ("nested symbol missing", "/k/s", None),
        ("nested symbol mismatch", "/k/s", Some("ETHUSDT")),
    ] {
        let mut value = parsed.clone();
        let (parent_pointer, key) = pointer.rsplit_once('/').expect("JSON pointer");
        let parent = value
            .pointer_mut(parent_pointer)
            .expect("symbol parent")
            .as_object_mut()
            .expect("symbol object");
        if let Some(symbol) = replacement {
            parent.insert(key.to_owned(), serde_json::Value::String(symbol.to_owned()));
        } else {
            parent.remove(key);
        }
        assert!(
            matches!(
                decode_ws_frame(
                    Message::Text(serde_json::to_string(&value).expect("encode").into()),
                    &instrument(),
                    Timeframe::Minute1,
                    &config
                ),
                DecodedFrame::ProviderError(ProviderError::Protocol { .. })
                    | DecodedFrame::ProviderError(ProviderError::Payload {
                        source: PayloadError::MalformedProtocol,
                        ..
                    })
            ),
            "accepted {label}"
        );
    }
    let invalid_symbol = decode_ws_frame(
        Message::Text(r#"{"code":-1121,"msg":"Invalid symbol; api_key_SECRET"}"#.into()),
        &instrument(),
        Timeframe::Minute1,
        &config,
    );
    let DecodedFrame::ProviderError(error) = invalid_symbol else {
        panic!("expected typed provider error, got {invalid_symbol:?}");
    };
    assert!(matches!(
        &error,
        ProviderError::InvalidSymbol {
            code: -1121,
            message: SanitizedMessage::InvalidSymbol,
            ..
        }
    ));
    assert_eq!(
        error.to_string(),
        "invalid symbol (operation websocket, provider binance, instrument BTC/USDT, timeframe 1m); provider code -1121: invalid symbol"
    );
    assert_eq!(
        format!("{error:?}"),
        "InvalidSymbol { context: ErrorContext { provider: Some(ProviderId(\"binance\")), instrument: Some(\"BTC/USDT\"), timeframe: Some(Minute1), operation: WebSocket }, code: -1121, message: InvalidSymbol }"
    );
    assert!(!error.to_string().contains("api_key_SECRET"));
    assert!(!format!("{error:?}").contains("api_key_SECRET"));

    assert!(matches!(
        decode_ws_frame(
            Message::Text(r#"{"code":-1000,"msg":"generic provider failure"}"#.into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::ProviderError(ProviderError::Protocol {
            detail: "provider reported a WebSocket error",
            ..
        })
    ));
    assert_eq!(
        decode_ws_frame(
            Message::Text(r#"{"e":"serverShutdown"}"#.into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::ReconnectRequested
    );
    assert_eq!(
        decode_ws_frame(
            Message::Text(r#"{"e":"subscriptionResponse"}"#.into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::Ignored
    );
}
