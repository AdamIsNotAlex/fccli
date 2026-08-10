#![cfg(feature = "test-transport")]

use std::time::Duration;

use fccli::{
    error::{PayloadError, ProviderError, TimeoutKind},
    model::{FinalityAuthority, Instrument, Market, ProviderId, Timeframe},
    provider::binance::{
        DecodedFrame, WS_FRAME_SIZE, WS_MAX_WRITE_BUFFER_SIZE, WS_MESSAGE_SIZE,
        WS_READ_BUFFER_SIZE, WS_STALLED_WRITE_TIMEOUT, WS_WRITE_BUFFER_SIZE, WsConfig,
        decode_ws_frame, read_raw_websocket, test_websocket_url,
    },
};
use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpListener, time::timeout};
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
    assert!(matches!(result, Err(ProviderError::Configuration(_))));
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
                Err(ProviderError::Configuration(_))
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
    assert!(matches!(result, Err(ProviderError::Configuration(_))));
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

    let wrong_market = OPEN.replace("BTCUSDT", "ETHUSDT");
    assert!(matches!(
        decode_ws_frame(
            Message::Text(wrong_market.into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::ProviderError(ProviderError::Protocol { .. })
    ));
    assert!(matches!(
        decode_ws_frame(
            Message::Text(r#"{"code":-1121,"msg":"Invalid symbol; api_key_SECRET"}"#.into()),
            &instrument(),
            Timeframe::Minute1,
            &config
        ),
        DecodedFrame::ProviderError(ProviderError::Protocol { .. })
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
    let error = read_raw_websocket(&mut socket, &instrument(), Timeframe::Minute1, &config)
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
        read_raw_websocket(&mut socket, &instrument(), Timeframe::Minute1, &config)
            .await
            .expect("read ping"),
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
                &config,
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
