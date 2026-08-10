#![cfg(feature = "test-transport")]

use std::time::Duration;

use fccli::{
    error::{PayloadError, ProviderError, SanitizedCause, SanitizedMessage, TimeoutKind},
    model::{FinalityAuthority, Instrument, Market, ProviderId, Timeframe},
    provider::binance::{
        DecodedFrame, WS_FRAME_SIZE, WS_MAX_WRITE_BUFFER_SIZE, WS_MESSAGE_INACTIVITY_TIMEOUT,
        WS_MESSAGE_SIZE, WS_READ_BUFFER_SIZE, WS_STALLED_WRITE_TIMEOUT, WS_WRITE_BUFFER_SIZE,
        WsConfig, decode_ws_frame, read_raw_websocket, test_websocket_url,
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

fn payload_error(frame: DecodedFrame) -> PayloadError {
    match frame {
        DecodedFrame::ProviderError(ProviderError::Payload { source, .. }) => source,
        other => panic!("expected payload error, got {other:?}"),
    }
}

#[test]
fn production_config_and_all_timeframe_stream_paths_are_exact() {
    let config = WsConfig::production();
    assert_eq!(config.read_buffer_size, WS_READ_BUFFER_SIZE);
    assert_eq!(config.max_message_size, WS_MESSAGE_SIZE);
    assert_eq!(config.max_frame_size, WS_FRAME_SIZE);
    assert_eq!(config.write_buffer_size, WS_WRITE_BUFFER_SIZE);
    assert_eq!(config.max_write_buffer_size, WS_MAX_WRITE_BUFFER_SIZE);
    assert_eq!(config.stalled_write_timeout, WS_STALLED_WRITE_TIMEOUT);
    assert_eq!(
        config.message_inactivity_timeout,
        WS_MESSAGE_INACTIVITY_TIMEOUT
    );
    assert_eq!(config.validate(), Ok(config));

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
fn websocket_config_rejects_every_invalid_boundary_before_connect() {
    let production = WsConfig::production();
    for field in 0..5 {
        for value in [0, 16 * 1024 * 1024 + 1] {
            let mut config = production;
            match field {
                0 => config.read_buffer_size = value,
                1 => config.max_message_size = value,
                2 => config.max_frame_size = value,
                3 => config.write_buffer_size = value,
                4 => config.max_write_buffer_size = value,
                _ => unreachable!(),
            }
            assert!(matches!(
                config.validate(),
                Err(ProviderError::Configuration(_))
            ));
        }
    }

    let mut frame_larger_than_message = production;
    frame_larger_than_message.max_frame_size = 2;
    frame_larger_than_message.max_message_size = 1;
    assert!(matches!(
        frame_larger_than_message.validate(),
        Err(ProviderError::Configuration(_))
    ));

    let mut equal_write_limits = production;
    equal_write_limits.write_buffer_size = 4096;
    equal_write_limits.max_write_buffer_size = 4096;

    let mut minimum_headroom = production;
    minimum_headroom.write_buffer_size = 1024;
    minimum_headroom.max_write_buffer_size = 1024 + 131;
    assert_eq!(minimum_headroom.validate(), Ok(minimum_headroom));
    minimum_headroom.max_write_buffer_size -= 1;
    assert!(matches!(
        minimum_headroom.validate(),
        Err(ProviderError::Configuration(_))
    ));
    assert!(matches!(
        equal_write_limits.validate(),
        Err(ProviderError::Configuration(_))
    ));

    for invalid in [
        Duration::ZERO,
        Duration::from_secs(60) + Duration::from_nanos(1),
    ] {
        let mut config = production;
        config.stalled_write_timeout = invalid;
        assert!(matches!(
            config.validate(),
            Err(ProviderError::Configuration(_))
        ));
    }

    for invalid in [
        Duration::ZERO,
        Duration::from_secs(120) + Duration::from_nanos(1),
    ] {
        let mut config = production;
        config.message_inactivity_timeout = invalid;
        assert!(matches!(
            config.validate(),
            Err(ProviderError::Configuration(_))
        ));
    }

    for valid in [Duration::from_millis(1), Duration::from_secs(120)] {
        let mut config = production;
        config.message_inactivity_timeout = valid;
        assert_eq!(config.validate(), Ok(config));
    }

    for valid in [Duration::from_millis(1), Duration::from_secs(60)] {
        let mut config = production;
        config.stalled_write_timeout = valid;
        assert_eq!(config.validate(), Ok(config));
    }
}

#[tokio::test]
async fn equal_write_limits_fail_before_tcp_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let mut config = WsConfig::production();
    config.write_buffer_size = 4096;
    config.max_write_buffer_size = 4096;

    let result = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await;
    assert!(matches!(
        result,
        Err(ProviderError::WebSocketConfiguration { .. })
    ));
    assert!(
        timeout(Duration::from_millis(75), listener.accept())
            .await
            .is_err(),
        "invalid config reached TCP connect"
    );
}

#[test]
fn loopback_test_urls_are_exact_and_public_hosts_are_rejected() {
    let expected_path = "/ws/btcusdt@kline_1m";
    for base in ["ws://127.0.0.1:32123", "ws://[::1]:32123"] {
        let url = test_websocket_url(base, &instrument(), Timeframe::Minute1)
            .expect("literal loopback URL");
        assert_eq!(url.path(), expected_path);
        assert!(url.query().is_none());
    }

    for unsafe_base in [
        "wss://data-stream.binance.vision",
        "ws://example.com",
        "ws://192.0.2.1:80",
        "http://127.0.0.1:80",
        "ws://127.0.0.1:80?token=secret",
        "ws://127.0.0.1:80/#fragment",
    ] {
        assert!(
            matches!(
                test_websocket_url(unsafe_base, &instrument(), Timeframe::Minute1),
                Err(ProviderError::WebSocketConfiguration { .. })
            ),
            "accepted unsafe test WebSocket base {unsafe_base}"
        );
    }
}

#[tokio::test]
async fn test_connector_rejects_public_hosts_before_network_io() {
    let result = fccli::provider::binance::connect_test_websocket(
        "ws://192.0.2.1:80",
        &instrument(),
        Timeframe::Minute1,
        WsConfig::production(),
    )
    .await;
    assert!(matches!(
        result,
        Err(ProviderError::WebSocketConfiguration { .. })
    ));
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
        DecodedFrame::Candle(candle) => candle,
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
        DecodedFrame::Candle(candle) => candle,
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
        DecodedFrame::ServerShutdown
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

#[test]
fn close_and_control_frames_have_closed_internal_outcomes() {
    let config = WsConfig::production();
    let close = CloseFrame {
        code: CloseCode::Away,
        reason: "maintenance".into(),
    };
    assert_eq!(
        decode_ws_frame(
            Message::Close(Some(close)),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::Close(Some(CloseCode::Away))
    );
    assert_eq!(
        decode_ws_frame(
            Message::Ping(vec![1, 2, 3].into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::Ignored
    );
    assert_eq!(
        decode_ws_frame(
            Message::Pong(vec![1, 2, 3].into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::Ignored
    );
}

#[tokio::test]
async fn fragmented_message_over_configured_budget_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Frame(Frame::message(
                vec![b'a'; 40],
                OpCode::Data(Data::Text),
                false,
            )))
            .await
            .expect("first fragment");
        socket
            .send(Message::Frame(Frame::message(
                vec![b'b'; 40],
                OpCode::Data(Data::Continue),
                true,
            )))
            .await
            .expect("final fragment");
    });

    let mut config = WsConfig::production();
    config.max_message_size = 64;
    config.max_frame_size = 64;
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");
    let error = read_raw_websocket(&mut socket)
        .await
        .expect_err("fragmented message exceeds aggregate budget");
    assert!(matches!(
        error,
        ProviderError::Payload {
            source: PayloadError::OverBudget { limit_bytes: 64 },
            ..
        }
    ));
    server.await.expect("server task");
}

#[tokio::test]
async fn local_raw_socket_flushes_exactly_one_automatic_pong_for_one_ping() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Ping(vec![7, 8, 9].into()))
            .await
            .expect("send ping");
        let first = timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("automatic pong timeout")
            .expect("socket open")
            .expect("pong frame");
        assert_eq!(first, Message::Pong(vec![7, 8, 9].into()));
        assert!(
            timeout(Duration::from_millis(75), socket.next())
                .await
                .is_err(),
            "duplicate Pong or unexpected client frame"
        );
    });

    let base_url = format!("ws://{address}");
    let config = WsConfig::production();
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &base_url,
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");
    assert_eq!(
        read_raw_websocket(&mut socket).await.expect("read ping"),
        DecodedFrame::Ignored
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn healthy_ping_read_waits_for_automatic_pong_flush() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let (pong_received_tx, pong_received_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Ping(vec![1, 3, 3, 7].into()))
            .await
            .expect("send ping");
        let reply = timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("automatic pong timeout")
            .expect("socket open")
            .expect("valid pong");
        assert_eq!(reply, Message::Pong(vec![1, 3, 3, 7].into()));
        pong_received_tx.send(()).expect("report flushed pong");
    });

    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        WsConfig::production(),
    )
    .await
    .expect("client handshake");

    assert_eq!(
        timeout(Duration::from_secs(1), read_raw_websocket(&mut socket))
            .await
            .expect("healthy Ping read must complete after control flush")
            .expect("healthy Ping outcome"),
        DecodedFrame::Ignored
    );
    timeout(Duration::from_secs(1), pong_received_rx)
        .await
        .expect("peer must observe the Pong immediately after the read completes")
        .expect("peer reported Pong");
    server.await.expect("server task");
}

#[tokio::test]
async fn decoded_queue_at_capacity_recovers_after_temporary_write_backpressure() {
    const DECODED_CAPACITY: usize = 64;
    const LARGE_WRITE: usize = 8 * 1024 * 1024;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        for _ in 0..DECODED_CAPACITY {
            socket
                .send(Message::Text(OPEN.into()))
                .await
                .expect("fill decoded queue");
        }
        socket
            .send(Message::Ping(vec![6, 4].into()))
            .await
            .expect("Ping behind full decoded queue");

        tokio::time::sleep(Duration::from_millis(75)).await;
        let mut saw_large_write = false;
        let mut saw_pong = false;
        while !saw_large_write || !saw_pong {
            let message = timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("temporary backpressure must recover")
                .expect("socket open")
                .expect("valid client frame");
            match message {
                Message::Binary(bytes) => {
                    assert_eq!(bytes.len(), LARGE_WRITE);
                    saw_large_write = true;
                }
                Message::Pong(bytes) => {
                    assert_eq!(bytes.as_ref(), &[6, 4]);
                    saw_pong = true;
                }
                other => panic!("unexpected client frame during recovery: {other:?}"),
            }
        }
    });

    let mut config = WsConfig::production();
    config.write_buffer_size = 64 * 1024;
    config.max_write_buffer_size = LARGE_WRITE + 64 * 1024;
    config.stalled_write_timeout = Duration::from_secs(2);
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");

    timeout(
        Duration::from_secs(3),
        fccli::provider::binance::send_raw_websocket(
            &mut socket,
            Message::Binary(vec![0; LARGE_WRITE].into()),
        ),
    )
    .await
    .expect("write must resume after peer starts draining")
    .expect("temporary backpressure is recoverable");

    for index in 0..DECODED_CAPACITY {
        assert!(matches!(
            timeout(Duration::from_secs(1), read_raw_websocket(&mut socket))
                .await
                .unwrap_or_else(|_| panic!("retained candle {index} timed out"))
                .unwrap_or_else(|error| panic!("retained candle {index} failed: {error}")),
            DecodedFrame::Candle(_)
        ));
    }
    assert_eq!(
        timeout(Duration::from_secs(1), read_raw_websocket(&mut socket))
            .await
            .expect("Ping behind the capacity boundary must resume")
            .expect("Ping outcome"),
        DecodedFrame::Ignored
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn raw_socket_reports_stalled_application_write() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let _socket = accept_async(stream).await.expect("server handshake");
        tokio::time::sleep(Duration::from_secs(3)).await;
    });

    let mut config = WsConfig::production();
    config.write_buffer_size = 64 * 1024;
    config.max_write_buffer_size = 128 * 1024;
    config.stalled_write_timeout = Duration::from_millis(1);
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");

    let outcome = timeout(Duration::from_secs(2), async {
        loop {
            match fccli::provider::binance::send_raw_websocket(
                &mut socket,
                Message::Binary(vec![0; 64 * 1024].into()),
            )
            .await
            {
                Err(error) => break error,
                Ok(()) => {}
            }
        }
    })
    .await
    .expect("writes should eventually stall");
    assert!(matches!(
        outcome,
        ProviderError::Timeout {
            kind: TimeoutKind::StalledWrite,
            ..
        }
    ));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn failed_connect_carries_sanitized_market_context_in_display_and_debug() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    drop(listener);

    let error = match fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        WsConfig::production(),
    )
    .await
    {
        Ok(_) => panic!("closed listener must reject connect"),
        Err(error) => error,
    };
    assert!(matches!(
        &error,
        ProviderError::Transport {
            cause: SanitizedCause::Io,
            ..
        }
    ));
    let display = error.to_string();
    assert_eq!(
        display,
        "transport failed (operation websocket, provider binance, instrument BTC/USDT, timeframe 1m): I/O failure"
    );
    assert!(!display.contains(&address.to_string()));
    assert!(!display.contains("secret"));

    let debug = format!("{error:?}");
    assert_eq!(
        debug,
        "Transport { context: ErrorContext { provider: Some(ProviderId(\"binance\")), instrument: Some(\"BTC/USDT\"), timeframe: Some(Minute1), operation: WebSocket }, cause: Io }"
    );
    assert!(!debug.contains(&address.to_string()));
    assert!(!debug.contains("secret"));
}

#[tokio::test]
async fn socket_binds_immutable_validated_config_and_minimum_headroom_flushes_pong() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Ping(vec![0; 125].into()))
            .await
            .expect("largest control payload");
        let pong = timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("pong timeout")
            .expect("socket open")
            .expect("pong");
        assert_eq!(pong, Message::Pong(vec![0; 125].into()));
    });

    let mut config = WsConfig::production();
    config.write_buffer_size = 1024;
    config.max_write_buffer_size = 1024 + 131;
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");
    assert_eq!(socket.config(), &config);
    assert_eq!(
        read_raw_websocket(&mut socket).await.expect("read ping"),
        DecodedFrame::Ignored
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn stalled_write_still_flushes_one_pong_and_retains_data_and_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Ping(vec![4, 5, 6].into()))
            .await
            .expect("ping");
        socket.send(Message::Text(OPEN.into())).await.expect("data");
        tokio::time::sleep(Duration::from_millis(50)).await;
        socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "maintenance".into(),
            })))
            .await
            .expect("close");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut pong_count = 0;
        while let Ok(Some(Ok(message))) = timeout(Duration::from_millis(250), socket.next()).await {
            if message == Message::Pong(vec![4, 5, 6].into()) {
                pong_count += 1;
            }
        }
        assert_eq!(pong_count, 1, "automatic Pong must be emitted exactly once");
    });

    let mut config = WsConfig::production();
    config.write_buffer_size = 64 * 1024;
    config.max_write_buffer_size = 128 * 1024;
    config.stalled_write_timeout = Duration::from_millis(10);
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");

    let error = timeout(Duration::from_secs(2), async {
        loop {
            if let Err(error) = fccli::provider::binance::send_raw_websocket(
                &mut socket,
                Message::Binary(vec![0; 64 * 1024].into()),
            )
            .await
            {
                break error;
            }
        }
    })
    .await
    .expect("ordinary writes should stall");
    assert!(matches!(
        &error,
        ProviderError::Timeout {
            kind: TimeoutKind::StalledWrite,
            ..
        }
    ));
    let display = error.to_string();
    assert_eq!(
        display,
        "stalled write timed out (operation websocket, provider binance, instrument BTC/USDT, timeframe 1m)"
    );
    assert!(!display.contains(&address.to_string()));
    assert!(!display.contains("secret"));

    let debug = format!("{error:?}");
    assert_eq!(
        debug,
        "Timeout { context: ErrorContext { provider: Some(ProviderId(\"binance\")), instrument: Some(\"BTC/USDT\"), timeframe: Some(Minute1), operation: WebSocket }, kind: StalledWrite }"
    );
    assert!(!debug.contains(&address.to_string()));
    assert!(!debug.contains("secret"));

    assert_eq!(
        read_raw_websocket(&mut socket)
            .await
            .expect("retained ping"),
        DecodedFrame::Ignored
    );
    assert!(matches!(
        read_raw_websocket(&mut socket)
            .await
            .expect("retained candle"),
        DecodedFrame::Candle(_)
    ));
    assert_eq!(
        read_raw_websocket(&mut socket)
            .await
            .expect("retained close"),
        DecodedFrame::Close(Some(CloseCode::Away))
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn invalid_utf8_and_protocol_raw_frames_are_typed_and_contextual() {
    for (label, bytes, detail) in [
        (
            "invalid UTF-8",
            vec![0x81, 0x01, 0xff],
            "invalid WebSocket UTF-8",
        ),
        (
            "masked server frame",
            vec![0x81, 0x80, 0x00, 0x00, 0x00, 0x00],
            "invalid WebSocket framing",
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(stream).await.expect("server handshake");
            socket
                .get_mut()
                .write_all(&bytes)
                .await
                .expect("raw invalid frame");
        });
        let mut socket = fccli::provider::binance::connect_test_websocket(
            &format!("ws://{address}"),
            &instrument(),
            Timeframe::Minute1,
            WsConfig::production(),
        )
        .await
        .expect("client handshake");
        let error = read_raw_websocket(&mut socket).await.expect_err(label);
        assert!(matches!(
            &error,
            ProviderError::Protocol {
                detail: actual_detail,
                ..
            } if *actual_detail == detail
        ));
        let display = error.to_string();
        assert_eq!(
            display,
            format!(
                "protocol failure (operation websocket, provider binance, instrument BTC/USDT, timeframe 1m): {detail}"
            ),
            "{label}"
        );
        assert!(!display.contains(&address.to_string()), "{label}");
        assert!(!display.contains("secret"), "{label}");

        let debug = format!("{error:?}");
        assert_eq!(
            debug,
            format!(
                "Protocol {{ context: ErrorContext {{ provider: Some(ProviderId(\"binance\")), instrument: Some(\"BTC/USDT\"), timeframe: Some(Minute1), operation: WebSocket }}, detail: \"{detail}\" }}"
            ),
            "{label}"
        );
        assert!(!debug.contains(&address.to_string()), "{label}");
        assert!(!debug.contains("secret"), "{label}");
        server.await.expect("server task");
    }
}

#[tokio::test]
async fn unfinished_fragment_with_control_activity_hits_nonresetting_inactivity_bound() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Frame(Frame::message(
                Vec::new(),
                OpCode::Data(Data::Text),
                false,
            )))
            .await
            .expect("empty initial fragment");
        for value in 0..8_u8 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if socket
                .send(Message::Ping(vec![value].into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut config = WsConfig::production();
    config.message_inactivity_timeout = Duration::from_millis(40);
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");
    let error = timeout(Duration::from_secs(1), async {
        loop {
            match read_raw_websocket(&mut socket).await {
                Ok(DecodedFrame::Ignored) => {}
                Ok(other) => panic!("unexpected decoded frame before inactivity: {other:?}"),
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("inactivity deadline");
    assert!(matches!(
        error,
        ProviderError::Timeout {
            kind: TimeoutKind::WebSocketInactivity,
            ..
        }
    ));
    server.await.expect("server task");
}

#[tokio::test]
async fn continuous_inbound_data_cannot_extend_a_stalled_write_deadline_or_lose_frames() {
    const FRAME_COUNT: usize = 64;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        for _ in 0..FRAME_COUNT {
            socket
                .send(Message::Text(OPEN.into()))
                .await
                .expect("continuous candle");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut config = WsConfig::production();
    config.write_buffer_size = 64 * 1024;
    config.max_write_buffer_size = 128 * 1024;
    config.stalled_write_timeout = Duration::from_millis(15);
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");

    let error = timeout(Duration::from_secs(2), async {
        loop {
            if let Err(error) = fccli::provider::binance::send_raw_websocket(
                &mut socket,
                Message::Binary(vec![0; 64 * 1024].into()),
            )
            .await
            {
                break error;
            }
        }
    })
    .await
    .expect("continuous inbound traffic must not starve stalled-write timeout");
    assert!(matches!(
        error,
        ProviderError::Timeout {
            kind: TimeoutKind::StalledWrite,
            ..
        }
    ));

    let mut candles = 0;
    while candles < FRAME_COUNT {
        match timeout(Duration::from_secs(1), read_raw_websocket(&mut socket))
            .await
            .expect("retained candle timeout")
            .expect("retained candle")
        {
            DecodedFrame::Candle(_) => candles += 1,
            DecodedFrame::Ignored => {}
            other => panic!("unexpected retained outcome {other:?}"),
        }
    }
    assert_eq!(
        candles, FRAME_COUNT,
        "every consumed candle is retained once"
    );
    server.await.expect("server task");
}

#[tokio::test]
async fn continuously_buffered_pings_cannot_starve_message_inactivity() {
    const PING_COUNT: usize = 4096;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Frame(Frame::message(
                Vec::new(),
                OpCode::Data(Data::Text),
                false,
            )))
            .await
            .expect("empty initial fragment");
        for value in 0..PING_COUNT {
            if socket
                .send(Message::Ping(vec![(value & 0xff) as u8].into()))
                .await
                .is_err()
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut config = WsConfig::production();
    config.message_inactivity_timeout = Duration::from_millis(10);
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");

    let error = timeout(Duration::from_secs(1), async {
        loop {
            match read_raw_websocket(&mut socket).await {
                Ok(DecodedFrame::Ignored) => {}
                Ok(other) => panic!("unexpected decoded frame before inactivity: {other:?}"),
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("buffered controls must not starve inactivity deadline");
    assert!(matches!(
        error,
        ProviderError::Timeout {
            kind: TimeoutKind::WebSocketInactivity,
            ..
        }
    ));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn deferred_terminal_error_rejects_subsequent_send_without_consuming_read_order() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Text(CLOSED.into()))
            .await
            .expect("candle");
        socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "maintenance".into(),
            })))
            .await
            .expect("close");
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        WsConfig::production(),
    )
    .await
    .expect("client handshake");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = fccli::provider::binance::send_raw_websocket(
        &mut socket,
        Message::Text("application-write".into()),
    )
    .await;
    let subsequent_send = timeout(
        Duration::from_secs(1),
        fccli::provider::binance::send_raw_websocket(
            &mut socket,
            Message::Text("subsequent-application-write".into()),
        ),
    )
    .await
    .expect("subsequent send must not spin")
    .expect_err("terminal socket rejects subsequent send");
    assert!(matches!(
        subsequent_send,
        ProviderError::Transport { .. } | ProviderError::Protocol { .. }
    ));

    assert!(matches!(
        read_raw_websocket(&mut socket)
            .await
            .expect("retained candle"),
        DecodedFrame::Candle(_)
    ));
    assert_eq!(
        read_raw_websocket(&mut socket)
            .await
            .expect("retained close"),
        DecodedFrame::Close(Some(CloseCode::Away))
    );
    let terminal = read_raw_websocket(&mut socket)
        .await
        .expect_err("terminal sink error follows retained outcomes");
    assert!(matches!(
        terminal,
        ProviderError::Transport { .. } | ProviderError::Protocol { .. }
    ));
    // The failed subsequent send must not consume or reorder the terminal read error:
    // both retained outcomes still precede that error, and the error is delivered once.
    server.await.expect("server task");
}

#[tokio::test]
async fn peer_close_racing_ordinary_outbound_replies_before_termination_and_is_delivered_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let (close_sent_tx, close_sent_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "maintenance".into(),
            })))
            .await
            .expect("send peer close");
        close_sent_tx.send(()).expect("signal peer close sent");

        let reply = timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("automatic close reply must arrive promptly")
            .expect("close reply frame")
            .expect("valid close reply");
        assert!(
            matches!(reply, Message::Close(_)),
            "ordinary outbound won the close race: {reply:?}"
        );
    });

    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        WsConfig::production(),
    )
    .await
    .expect("client handshake");
    close_sent_rx.await.expect("peer close sent");

    timeout(
        Duration::from_secs(1),
        fccli::provider::binance::send_raw_websocket(
            &mut socket,
            Message::Text("ordinary-outbound".into()),
        ),
    )
    .await
    .expect("outbound racing peer Close must terminate promptly")
    .expect_err("peer Close prevents ordinary outbound completion");

    assert_eq!(
        read_raw_websocket(&mut socket)
            .await
            .expect("retained peer Close"),
        DecodedFrame::Close(Some(CloseCode::Away))
    );
    let terminal = timeout(Duration::from_secs(1), read_raw_websocket(&mut socket))
        .await
        .expect("terminal read after Close must return promptly")
        .expect_err("peer Close outcome is delivered only once");
    assert!(matches!(
        terminal,
        ProviderError::Transport { .. } | ProviderError::Protocol { .. }
    ));

    server.await.expect("server task");
}

#[tokio::test]
async fn stalled_drain_flushes_automatic_close_reply_before_close_outcome_and_error() {
    const LARGE_WRITE: usize = 8 * 1024 * 1024;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let (start_close_tx, start_close_rx) = tokio::sync::oneshot::channel();
    let (close_reply_tx, close_reply_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = accept_async(stream).await.expect("server handshake");
        start_close_rx.await.expect("client entered stalled drain");
        socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "maintenance".into(),
            })))
            .await
            .expect("send peer Close");

        tokio::time::sleep(Duration::from_millis(75)).await;
        loop {
            let message = timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("automatic Close reply must flush after drain resumes")
                .expect("socket open until Close reply")
                .expect("valid client frame");
            if matches!(message, Message::Close(_)) {
                close_reply_tx.send(()).expect("report Close reply");
                break;
            }
        }
    });

    let mut config = WsConfig::production();
    config.write_buffer_size = 64 * 1024;
    config.max_write_buffer_size = LARGE_WRITE + 64 * 1024;
    config.stalled_write_timeout = Duration::from_millis(20);
    let mut socket = fccli::provider::binance::connect_test_websocket(
        &format!("ws://{address}"),
        &instrument(),
        Timeframe::Minute1,
        config,
    )
    .await
    .expect("client handshake");

    let stalled = timeout(Duration::from_secs(2), async {
        loop {
            if let Err(error) = fccli::provider::binance::send_raw_websocket(
                &mut socket,
                Message::Binary(vec![0; LARGE_WRITE].into()),
            )
            .await
            {
                break error;
            }
        }
    })
    .await
    .expect("non-draining peer must trigger stalled-write mode");
    assert!(matches!(
        stalled,
        ProviderError::Timeout {
            kind: TimeoutKind::StalledWrite,
            ..
        }
    ));
    start_close_tx.send(()).expect("request peer Close");

    assert_eq!(
        timeout(Duration::from_secs(2), read_raw_websocket(&mut socket))
            .await
            .expect("Close read must recover when peer resumes draining")
            .expect("Close outcome precedes terminal error"),
        DecodedFrame::Close(Some(CloseCode::Away))
    );
    timeout(Duration::from_secs(1), close_reply_rx)
        .await
        .expect("peer must observe automatic Close reply")
        .expect("peer reported Close reply");

    let terminal = timeout(Duration::from_secs(1), read_raw_websocket(&mut socket))
        .await
        .expect("terminal error after Close must return promptly")
        .expect_err("stalled-write error follows the retained Close outcome");
    assert!(matches!(
        terminal,
        ProviderError::Timeout {
            kind: TimeoutKind::StalledWrite,
            ..
        }
    ));
    server.await.expect("server task");
}
