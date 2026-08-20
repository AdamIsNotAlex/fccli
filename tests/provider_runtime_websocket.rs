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

#[test]
fn production_websocket_config_defaults_are_exact() {
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
        send_raw_websocket(&mut socket, Message::Binary(vec![0; LARGE_WRITE].into())),
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
            DecodedFrame::Provider(BinanceDecoded::Candle(_))
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
            if let Err(error) =
                send_raw_websocket(&mut socket, Message::Binary(vec![0; 64 * 1024].into())).await
            {
                break error;
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
            if let Err(error) =
                send_raw_websocket(&mut socket, Message::Binary(vec![0; 64 * 1024].into())).await
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
        DecodedFrame::Provider(BinanceDecoded::Candle(_))
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
            if let Err(error) =
                send_raw_websocket(&mut socket, Message::Binary(vec![0; 64 * 1024].into())).await
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
            DecodedFrame::Provider(BinanceDecoded::Candle(_)) => candles += 1,
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
    let _ = send_raw_websocket(&mut socket, Message::Text("application-write".into())).await;
    let subsequent_send = timeout(
        Duration::from_secs(1),
        send_raw_websocket(
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
        DecodedFrame::Provider(BinanceDecoded::Candle(_))
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
        send_raw_websocket(&mut socket, Message::Text("ordinary-outbound".into())),
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
            if let Err(error) =
                send_raw_websocket(&mut socket, Message::Binary(vec![0; LARGE_WRITE].into())).await
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

mod emitter_contracts {
    #![cfg(feature = "test-transport")]

    use std::{sync::Arc, time::Duration};

    use fccli::{
        clock::{Clock, ManualClock},
        error::{
            ErrorContext, ErrorOperation, ModelError, PayloadError, ProviderError, SanitizedCause,
            SanitizedMessage, TimeoutKind,
        },
        model::{
            Candle, ConnectionStatus, GapGeneration, HistoryRequest, Instrument, InstrumentSpec,
            Market, MarketEvent, MonoInstant, ProviderId, RateGateState, ReplayRevision, Timeframe,
        },
        provider::binance::{
            BinanceProvider, BinanceTestConfig, CONTROL_CAPACITY, EMERGENCY_CONTROL_CAPACITY,
            FIRST_KLINE_HANDSHAKE_TIMEOUT, KEYED_CANDLE_CAPACITY, LiveCompletionDisposition,
            LiveErrorDisposition, LiveInBandEventDisposition, LiveInputClassification,
            LiveSupervisorConfig, MARKET_EVENT_CHANNEL_CAPACITY, MAX_CONNECTION_AGE,
            RECONCILE_ACK_TIMEOUT, classify_live_error_for_test, classify_live_input_for_test,
        },
        provider::test_transport::{BinanceDecoded, DecodedFrame, EventEmitterTestFacade},
        provider::{
            CancellationToken, LiveRequest, MarketDataProvider, ProducerCompletion,
            ProviderRegistry, ReconcileAck, ReconcileAckPublishError, accepted_watermark_channel,
            reconcile_ack_channel,
        },
    };
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
        time::timeout,
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, query_param},
    };

    const OPEN_TIME: i64 = 1_700_000_040_000;
    fn candle(open_time: i64, close: f64) -> Candle {
        Candle::from_ws(
            open_time,
            open_time + 59_999,
            close,
            close,
            close,
            close,
            1.0,
            false,
        )
        .expect("valid candle")
    }

    #[tokio::test]
    async fn same_key_replacement_preserves_distinct_capacity_and_event_order() {
        let mut facade = EventEmitterTestFacade::new(2);

        facade
            .queue_candle(candle(OPEN_TIME, 37_000.0))
            .expect("first keyed slot");
        facade
            .queue_candle(candle(OPEN_TIME, 37_111.0))
            .expect("same-key replacement must not consume a second slot");
        facade
            .queue_candle(candle(OPEN_TIME + 60_000, 37_222.0))
            .expect("distinct key retains the second keyed slot");
        assert_eq!(
            facade.queue_candle(candle(OPEN_TIME + 120_000, 37_333.0)),
            Err(ProviderError::QueueSaturated),
            "only a third distinct key exhausts keyed capacity"
        );

        facade.flush().await.expect("flush queued candles");
        let first = timeout(Duration::from_secs(1), facade.recv())
            .await
            .expect("first event timeout")
            .expect("first event")
            .expect("first event result");
        let second = timeout(Duration::from_secs(1), facade.recv())
            .await
            .expect("second event timeout")
            .expect("second event")
            .expect("second event result");

        match first {
            MarketEvent::Candle { generation, candle } => {
                assert_eq!(generation, GapGeneration(1));
                assert_eq!(candle.open_time(), OPEN_TIME);
                assert_eq!(candle.close(), 37_111.0, "queued value must be replaced");
            }
            other => panic!("expected first keyed candle, got {other:?}"),
        }
        match second {
            MarketEvent::Candle { generation, candle } => {
                assert_eq!(generation, GapGeneration(1));
                assert_eq!(candle.open_time(), OPEN_TIME + 60_000);
                assert_eq!(candle.close(), 37_222.0);
            }
            other => panic!("expected second keyed candle, got {other:?}"),
        }
        assert!(
            timeout(Duration::from_millis(25), facade.recv())
                .await
                .is_err(),
            "replacement must not enqueue an extra event"
        );
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

    fn rest_row(open_time: i64) -> serde_json::Value {
        json!([
            open_time,
            "37000.00",
            "37050.00",
            "36975.25",
            "37025.50",
            "12.5",
            open_time + 59_999,
            "462812.5",
            50,
            "6.25",
            "231406.25",
            "0"
        ])
    }

    fn ws_kline(open_time: i64, closed: bool, close: &str) -> String {
        ws_kline_for(open_time, closed, close, "1m")
    }

    fn ws_kline_for(open_time: i64, closed: bool, close: &str, interval: &str) -> String {
        json!({
            "e": "kline",
            "E": open_time + 60_001,
            "s": "BTCUSDT",
            "k": {
                "t": open_time,
                "T": open_time + 59_999,
                "s": "BTCUSDT",
                "i": interval,
                "o": "37000.00",
                "c": close,
                "h": "37200.00",
                "l": "36975.25",
                "v": "12.5",
                "x": closed
            }
        })
        .to_string()
    }

    async fn rest_server(response: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(response)
            .mount(&server)
            .await;
        server
    }

    async fn websocket_listener() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("WS listener");
        let address = listener.local_addr().expect("WS address");
        (listener, format!("ws://{address}"))
    }

    async fn held_rest_listener(
        body: serde_json::Value,
    ) -> (
        String,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        held_rest_listener_with_followup(body, None).await
    }

    async fn held_rest_listener_with_followup(
        body: serde_json::Value,
        followup_body: Option<serde_json::Value>,
    ) -> (
        String,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("REST listener");
        let address = listener.local_addr().expect("REST address");
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut started_tx = Some(started_tx);
            let mut release_rx = Some(release_rx);
            let mut request_count = 0_usize;
            loop {
                let (mut stream, _) = listener.accept().await.expect("REST accept");
                let mut request = Vec::new();
                let mut chunk = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut chunk).await.expect("REST request read");
                    assert!(read != 0, "REST request closed before headers");
                    request.extend_from_slice(&chunk[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                if let Some(started_tx) = started_tx.take() {
                    started_tx.send(()).ok();
                    if release_rx
                        .take()
                        .expect("first REST release receiver")
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let response_body = if request_count == 0 {
                    &body
                } else {
                    followup_body.as_ref().unwrap_or(&body)
                };
                request_count += 1;
                let body = serde_json::to_vec(response_body).expect("REST JSON");
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                if stream.write_all(response.as_bytes()).await.is_ok() {
                    let _ = stream.write_all(&body).await;
                    let _ = stream.shutdown().await;
                }
            }
        });
        (format!("http://{address}"), started_rx, release_tx, server)
    }

    fn provider(
        rest_uri: &str,
        websocket_uri: &str,
        clock: Arc<dyn Clock>,
        live: LiveSupervisorConfig,
    ) -> Arc<BinanceProvider> {
        let mut config = BinanceTestConfig::loopback(rest_uri).with_websocket_base(websocket_uri);
        config.live = live;
        Arc::new(BinanceProvider::new_test_live(config, clock).expect("test provider"))
    }

    fn request(
        startup_watermark: Option<i64>,
    ) -> (
        LiveRequest,
        fccli::provider::AcceptedWatermarkSender,
        fccli::provider::ReconcileAckSender,
    ) {
        request_for(startup_watermark, Timeframe::Minute1)
    }

    fn request_for(
        startup_watermark: Option<i64>,
        timeframe: Timeframe,
    ) -> (
        LiveRequest,
        fccli::provider::AcceptedWatermarkSender,
        fccli::provider::ReconcileAckSender,
    ) {
        let (watermark_tx, watermark_rx) = accepted_watermark_channel(startup_watermark);
        let (ack_tx, ack_rx) = reconcile_ack_channel();
        let cancellation = fccli::provider::CancellationToken::new();
        (
            LiveRequest {
                instrument: instrument(),
                timeframe,
                startup_watermark,
                accepted_watermark_rx: watermark_rx,
                reconcile_ack_rx: ack_rx,
                cancellation,
            },
            watermark_tx,
            ack_tx,
        )
    }

    async fn next_event(feed: &mut fccli::provider::LiveFeed) -> MarketEvent {
        timeout(Duration::from_secs(2), feed.events.next())
            .await
            .expect("event timeout")
            .expect("event stream closed")
            .expect("event error")
    }

    fn assert_status(event: MarketEvent, generation: Option<u64>, expected: ConnectionStatus) {
        assert_eq!(
            event,
            MarketEvent::Status {
                generation: generation.map(GapGeneration),
                status: expected,
            }
        );
    }

    async fn next_after_optional_startup_statuses(
        feed: &mut fccli::provider::LiveFeed,
    ) -> MarketEvent {
        let mut saw_connecting = false;
        let mut saw_gap_sync = false;
        loop {
            let event = next_event(feed).await;
            match event {
                MarketEvent::Status {
                    generation: Some(GapGeneration(1)),
                    status: ConnectionStatus::Connecting,
                } if !saw_connecting && !saw_gap_sync => saw_connecting = true,
                MarketEvent::Status {
                    generation: Some(GapGeneration(1)),
                    status: ConnectionStatus::GapSync,
                } if saw_connecting && !saw_gap_sync => saw_gap_sync = true,
                other @ MarketEvent::Status {
                    generation: None,
                    status: ConnectionStatus::Stopped,
                } => return other,
                other @ MarketEvent::Status { .. } => {
                    panic!("non-canonical startup status before immediate fault: {other:?}")
                }
                other => return other,
            }
        }
    }

    async fn next_batch(
        feed: &mut fccli::provider::LiveFeed,
    ) -> (
        GapGeneration,
        fccli::model::ReplayRevision,
        i64,
        Vec<fccli::model::Candle>,
    ) {
        match next_event(feed).await {
            MarketEvent::ReconcileBatch {
                generation,
                revision,
                target_open_time,
                candles,
            } => (generation, revision, target_open_time, candles),
            other => panic!("expected reconcile batch, got {other:?}"),
        }
    }

    fn acknowledge(
        sender: &fccli::provider::ReconcileAckSender,
        generation: GapGeneration,
        revision: fccli::model::ReplayRevision,
        through: i64,
    ) {
        sender
            .publish(ReconcileAck {
                generation,
                revision,
                through,
            })
            .expect("reconciliation acknowledgement");
    }

    #[tokio::test]
    async fn saturated_app_channel_keeps_pong_progress_and_uses_emergency_pair_before_retry() {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let (release_tx, release_rx) = oneshot::channel();
        let (pongs_tx, pongs_rx) = oneshot::channel();
        let (second_release_tx, second_release_rx) = oneshot::channel();
        let (second_accept_tx, mut second_accept_rx) = oneshot::channel();
        let (third_accept_tx, mut third_accept_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                .await
                .expect("first candle");
            release_rx.await.expect("connected release");
            for payload in [vec![1], vec![2], vec![3]] {
                websocket
                    .send(Message::Ping(payload.into()))
                    .await
                    .expect("ping");
            }
            for (offset, close) in [
                (60_000, "37030.00"),
                (120_000, "37035.00"),
                (180_000, "37040.00"),
            ] {
                websocket
                    .send(Message::Text(
                        ws_kline(OPEN_TIME + offset, false, close).into(),
                    ))
                    .await
                    .expect("queued candle");
            }
            let mut pongs = Vec::new();
            while pongs.len() < 3 {
                match timeout(Duration::from_secs(1), websocket.next()).await {
                    Ok(Some(Ok(Message::Pong(payload)))) => pongs.push(payload.to_vec()),
                    Ok(Some(Ok(_))) => {}
                    _ => break,
                }
            }
            pongs_tx.send(pongs).ok();
            let (stream, _) = listener.accept().await.expect("second accept");
            second_accept_tx.send(true).ok();
            let mut websocket = accept_async(stream).await.expect("second upgrade");
            websocket
                .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                .await
                .expect("second-generation first candle");
            second_release_rx.await.expect("second connected release");
            for (offset, close) in [
                (60_000, "37030.00"),
                (120_000, "37035.00"),
                (180_000, "37040.00"),
            ] {
                websocket
                    .send(Message::Text(
                        ws_kline(OPEN_TIME + offset, false, close).into(),
                    ))
                    .await
                    .expect("second-generation saturating candle");
            }
            let third_connection = listener.accept().await;
            third_accept_tx.send(third_connection.is_ok()).ok();
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual.clone();
        let live = LiveSupervisorConfig {
            keyed_candle_capacity: 1,
            control_capacity: 1,
            market_event_capacity: 1,
            ..LiveSupervisorConfig::default()
        };
        let provider = provider(&rest.uri(), &ws_uri, clock, live);
        let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
        let mut feed = provider.open_live(request).await.expect("feed");
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Connecting,
        );
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::GapSync,
        );
        let (generation, revision, target, _) = next_batch(&mut feed).await;
        acknowledge(&ack_tx, generation, revision, target);
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Connected,
        );
        release_tx.send(()).expect("release connected traffic");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            pongs_rx.await.expect("pong report"),
            vec![vec![1], vec![2], vec![3]]
        );
        manual
            .advance_by(Duration::from_secs(1))
            .expect("first backoff elapsed while output remains saturated");
        assert!(
            timeout(Duration::from_millis(50), &mut second_accept_rx)
                .await
                .is_err(),
            "the backoff clock starts while the emergency pair remains queued, but the next generation cannot start before dequeue"
        );

        assert_eq!(
            next_event(&mut feed).await,
            MarketEvent::RecoverableError {
                generation: None,
                error: ProviderError::QueueSaturated,
                rate_gate_deadline: None,
            },
            "saturation logically purges the already queued generation-1 candle"
        );
        assert!(
            timeout(Duration::from_millis(50), &mut second_accept_rx)
                .await
                .is_err(),
            "dequeueing only the first half of the pair must not release the next generation"
        );

        assert_status(next_event(&mut feed).await, None, ConnectionStatus::Backoff);
        assert_status(
            next_event(&mut feed).await,
            Some(2),
            ConnectionStatus::Connecting,
        );
        assert!(
            timeout(Duration::from_secs(1), &mut second_accept_rx)
                .await
                .expect("second-generation connection timeout")
                .expect("second-generation accept report"),
            "the second generation must connect after the full emergency pair is consumed"
        );
        assert_status(
            next_event(&mut feed).await,
            Some(2),
            ConnectionStatus::GapSync,
        );
        let (generation, revision, target, _) = next_batch(&mut feed).await;
        acknowledge(&ack_tx, generation, revision, target);
        assert_status(
            next_event(&mut feed).await,
            Some(2),
            ConnectionStatus::Connected,
        );
        second_release_tx
            .send(())
            .expect("release second-generation traffic");
        tokio::time::sleep(Duration::from_millis(100)).await;
        manual
            .advance_by(Duration::from_secs(2))
            .expect("second backoff elapsed while pair remains queued");
        assert!(
            timeout(Duration::from_millis(50), &mut third_accept_rx)
                .await
                .is_err(),
            "repeated saturation must reuse the reserved pair and still wait for dequeue"
        );
        assert_eq!(
            next_event(&mut feed).await,
            MarketEvent::RecoverableError {
                generation: None,
                error: ProviderError::QueueSaturated,
                rate_gate_deadline: None,
            }
        );
        assert_status(next_event(&mut feed).await, None, ConnectionStatus::Backoff);
        assert_status(
            next_event(&mut feed).await,
            Some(3),
            ConnectionStatus::Connecting,
        );
        assert!(
            timeout(Duration::from_secs(1), &mut third_accept_rx)
                .await
                .expect("third-generation connection timeout")
                .expect("third-generation accept report"),
            "a second saturation cycle must not exhaust or allocate a one-shot emergency path"
        );
        feed.request_shutdown();
        server.abort();
    }

    #[tokio::test]
    async fn cancellation_while_saturation_pair_is_queued_emits_only_stopped() {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let (release_tx, release_rx) = oneshot::channel();
        let (saturated_connection_closed_tx, saturated_connection_closed_rx) = oneshot::channel();
        let (second_accept_tx, mut second_accept_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                .await
                .expect("first candle");
            release_rx.await.expect("release saturating traffic");
            for (offset, close) in [
                (60_000, "37030.00"),
                (120_000, "37035.00"),
                (180_000, "37040.00"),
            ] {
                websocket
                    .send(Message::Text(
                        ws_kline(OPEN_TIME + offset, false, close).into(),
                    ))
                    .await
                    .expect("saturating candle");
            }
            while let Some(message) = websocket.next().await {
                if message.is_err() || matches!(message, Ok(Message::Close(_))) {
                    break;
                }
            }
            saturated_connection_closed_tx.send(()).ok();
            let second_connection = listener.accept().await;
            second_accept_tx.send(second_connection.is_ok()).ok();
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual.clone();
        let live = LiveSupervisorConfig {
            keyed_candle_capacity: 1,
            control_capacity: 1,
            market_event_capacity: 1,
            ..LiveSupervisorConfig::default()
        };
        let provider = provider(&rest.uri(), &ws_uri, clock, live);
        let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
        let cancellation = request.cancellation.clone();
        let mut feed = provider.open_live(request).await.expect("feed");
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Connecting,
        );
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::GapSync,
        );
        let (generation, revision, target, _) = next_batch(&mut feed).await;
        acknowledge(&ack_tx, generation, revision, target);
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Connected,
        );
        release_tx.send(()).expect("release saturating traffic");
        saturated_connection_closed_rx
            .await
            .expect("saturated generation closed");
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        manual
            .advance_by(Duration::from_secs(1))
            .expect("emergency-pair backoff deadline");
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert!(
            timeout(Duration::from_millis(50), &mut second_accept_rx)
                .await
                .is_err(),
            "elapsed backoff cannot start generation 2 while the queued emergency pair is not dequeued"
        );
        cancellation.cancel();
        assert_status(next_event(&mut feed).await, None, ConnectionStatus::Stopped);
        match timeout(Duration::from_millis(50), feed.events.next()).await {
            Ok(Some(Ok(event))) => panic!("unexpected event after cancellation stop: {event:?}"),
            Ok(Some(Err(error))) => {
                panic!("unexpected stream error after cancellation stop: {error:?}")
            }
            Ok(None) | Err(_) => {}
        }
        assert!(matches!(
            feed.producer_completion.changed().await,
            Ok(ProducerCompletion::Finished(Ok(())))
        ));
        assert!(
            timeout(Duration::from_millis(50), &mut second_accept_rx)
                .await
                .is_err(),
            "cancellation must suppress the next generation"
        );
        server.abort();
    }

    #[tokio::test]
    async fn dropping_saturated_event_receiver_unblocks_the_producer() {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let (traffic_sent_tx, traffic_sent_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                .await
                .expect("first candle");
            tokio::time::sleep(Duration::from_millis(25)).await;
            for offset in [60_000, 120_000, 180_000] {
                websocket
                    .send(Message::Text(
                        ws_kline(OPEN_TIME + offset, false, "37030.00").into(),
                    ))
                    .await
                    .expect("saturating candle");
            }
            traffic_sent_tx.send(()).ok();
            futures_util::future::pending::<()>().await;
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual;
        let live = LiveSupervisorConfig {
            keyed_candle_capacity: 1,
            control_capacity: 1,
            market_event_capacity: 1,
            ..LiveSupervisorConfig::default()
        };
        let provider = provider(&rest.uri(), &ws_uri, clock, live);
        let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
        let mut feed = provider.open_live(request).await.expect("feed");
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Connecting,
        );
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::GapSync,
        );
        let (generation, revision, target, _) = next_batch(&mut feed).await;
        acknowledge(&ack_tx, generation, revision, target);
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Connected,
        );

        traffic_sent_rx.await.expect("saturating traffic sent");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = std::mem::replace(&mut feed.events, Box::pin(futures_util::stream::empty()));
        drop(events);

        assert!(matches!(
            timeout(Duration::from_secs(1), feed.producer_completion.changed())
                .await
                .expect("receiver drop must unblock producer"),
            Ok(ProducerCompletion::Finished(Err(
                ProviderError::ChannelClosed { .. }
            )))
        ));
        server.abort();
    }
}
