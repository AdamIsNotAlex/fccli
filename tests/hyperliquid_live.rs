#![cfg(feature = "test-transport")]

use std::{sync::Arc, time::Duration};

use fccli::{
    clock::ManualClock,
    error::{PayloadError, ProviderError, TimeoutKind},
    model::{
        ConnectionStatus, Instrument, Market, MarketEvent, MonoInstant, ProviderId, Timeframe,
    },
    provider::{
        LiveRequest, MarketDataProvider, accepted_watermark_channel,
        hyperliquid::{
            HyperliquidProvider, HyperliquidTestConfig, LiveSupervisorConfig, WsConfig,
            connect_test_websocket,
        },
        reconcile_ack_channel,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(MonoInstant::ZERO))
}

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

fn request(cancellation: CancellationToken) -> LiveRequest {
    let (watermark_tx, watermark_rx) = accepted_watermark_channel(None);
    let (ack_tx, ack_rx) = reconcile_ack_channel();
    let control_lifetime = cancellation.clone();
    tokio::spawn(async move {
        control_lifetime.cancelled().await;
        drop((watermark_tx, ack_tx));
    });
    LiveRequest {
        instrument: instrument(),
        timeframe: Timeframe::Minute1,
        startup_watermark: None,
        accepted_watermark_rx: watermark_rx,
        reconcile_ack_rx: ack_rx,
        cancellation,
    }
}

async fn websocket_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    (listener, format!("ws://{address}"))
}

fn provider(
    ws_uri: &str,
    clock: Arc<ManualClock>,
    live: LiveSupervisorConfig,
) -> HyperliquidProvider {
    let mut config =
        HyperliquidTestConfig::loopback("http://127.0.0.1:9").with_websocket_base(ws_uri);
    config.live = live;
    HyperliquidProvider::new_test_live(config, clock).expect("provider")
}

fn subscribe_ack() -> String {
    json!({
        "channel": "subscriptionResponse",
        "data": {
            "method": "subscribe",
            "subscription": {"type": "candle", "coin": "@142", "interval": "1m"}
        }
    })
    .to_string()
}

async fn next_event(feed: &mut fccli::provider::LiveFeed) -> MarketEvent {
    timeout(Duration::from_secs(2), feed.events.next())
        .await
        .expect("event timeout")
        .expect("event stream")
        .expect("event")
}

async fn next_event_while_advancing(
    feed: &mut fccli::provider::LiveFeed,
    clock: &ManualClock,
) -> MarketEvent {
    timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                biased;
                event = feed.events.next() => {
                    return event.expect("event stream").expect("event");
                }
                () = tokio::time::sleep(Duration::from_millis(1)) => {
                    clock.advance_by(Duration::from_millis(1)).expect("advance clock");
                }
            }
        }
    })
    .await
    .expect("event timeout")
}

async fn await_signal(receiver: oneshot::Receiver<()>, context: &'static str) {
    timeout(Duration::from_secs(2), receiver)
        .await
        .unwrap_or_else(|_| panic!("{context} timeout"))
        .unwrap_or_else(|_| panic!("{context} sender dropped"));
}

async fn await_server(mut server: tokio::task::JoinHandle<()>) {
    match timeout(Duration::from_secs(2), &mut server).await {
        Ok(result) => result.expect("server"),
        Err(_) => {
            server.abort();
            panic!("server timeout");
        }
    }
}

#[tokio::test]
async fn unsupported_timeframe_open_live_fails_before_connect() {
    let provider = HyperliquidProvider::new_test_live(
        HyperliquidTestConfig::loopback("http://127.0.0.1:9")
            .with_websocket_base("ws://127.0.0.1:9"),
        clock(),
    )
    .expect("provider");
    let mut request = request(CancellationToken::new());
    request.timeframe = Timeframe::Second1;
    let error = match provider.open_live(request).await {
        Ok(_) => panic!("1s is unsupported"),
        Err(error) => error,
    };
    assert!(
        matches!(error, ProviderError::Configuration(message) if message.contains("1s or 6h")),
        "{error}"
    );
}

#[test]
fn subscribe_ack_timeout_configuration_bounds_are_enforced() {
    for timeout in [Duration::ZERO, Duration::from_secs(61)] {
        let live = LiveSupervisorConfig {
            subscribe_ack_timeout: timeout,
            ..LiveSupervisorConfig::default()
        };
        assert!(matches!(
            live.validate(),
            Err(ProviderError::Configuration(_))
        ));
    }
    for timeout in [Duration::from_millis(1), Duration::from_secs(60)] {
        let live = LiveSupervisorConfig {
            subscribe_ack_timeout: timeout,
            ..LiveSupervisorConfig::default()
        };
        live.validate().expect("boundary is valid");
    }
}

#[tokio::test]
async fn gap_sync_waits_for_matching_subscribe_ack() {
    let (listener, ws_uri) = websocket_listener().await;
    let (release_tx, release_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let subscribe = websocket
            .next()
            .await
            .expect("subscribe frame")
            .expect("subscribe");
        let Message::Text(subscribe) = subscribe else {
            panic!("expected subscribe text")
        };
        let value: serde_json::Value = serde_json::from_str(&subscribe).expect("subscribe json");
        assert_eq!(value["method"], "subscribe");
        assert_eq!(value["subscription"]["coin"], "@142");
        release_rx.await.expect("release ack");
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        finish_rx.await.expect("finish server");
    });
    let manual = clock();
    let provider = provider(
        &ws_uri,
        Arc::clone(&manual),
        LiveSupervisorConfig::default(),
    );
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connecting,
            ..
        }
    ));
    assert!(
        timeout(Duration::from_millis(25), feed.events.next())
            .await
            .is_err()
    );
    release_tx.send(()).expect("release");
    let event = next_event(&mut feed).await;
    assert!(
        matches!(
            event,
            MarketEvent::Status {
                status: ConnectionStatus::GapSync,
                ..
            }
        ),
        "unexpected event: {event:?}"
    );
    cancellation.cancel();
    finish_tx.send(()).expect("finish");
    await_server(server).await;
}

#[tokio::test]
async fn subscribe_ack_expiry_is_recoverable_and_never_first_kline() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        std::future::pending::<()>().await;
    });
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    assert!(matches!(
        next_event_while_advancing(&mut feed, &manual).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Timeout {
                kind: TimeoutKind::SubscribeAck,
                ..
            },
            ..
        }
    ));
    cancellation.cancel();
    server.abort();
}

#[tokio::test]
async fn malformed_ack_precedes_its_simultaneous_deadline() {
    let (listener, ws_uri) = websocket_listener().await;
    let (sent_tx, sent_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        websocket
            .send(Message::Text(
                json!({"channel":"subscriptionResponse","data":{}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("malformed ack");
        sent_tx.send(()).ok();
    });
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    await_signal(sent_rx, "malformed ack queued").await;
    manual
        .advance_by(Duration::from_millis(1))
        .expect("deadline");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Payload {
                source: PayloadError::MalformedProtocol,
                ..
            },
            ..
        }
    ));
    cancellation.cancel();
    await_server(server).await;
}

#[tokio::test]
async fn application_json_ping_is_disabled_until_ack_acceptance_enables_it() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let message = websocket
            .next()
            .await
            .expect("heartbeat frame")
            .expect("heartbeat");
        assert_eq!(message, Message::Text(r#"{"method":"ping"}"#.into()));
        websocket
            .send(Message::Text(r#"{"channel":"pong"}"#.into()))
            .await
            .expect("pong");
    });
    let mut socket = connect_test_websocket(
        &ws_uri,
        &instrument(),
        Timeframe::Minute1,
        WsConfig::production(),
    )
    .await
    .expect("socket");
    assert!(!socket.application_heartbeat_started());
    socket.start_application_heartbeat();
    assert!(socket.application_heartbeat_started());
    assert_eq!(
        fccli::provider::hyperliquid::APPLICATION_HEARTBEAT_INTERVAL,
        Duration::from_secs(50)
    );
    socket.force_application_heartbeat_due_for_test();
    assert!(matches!(
        socket.read().await,
        Ok(fccli::provider::hyperliquid::DecodedFrame::ApplicationPong)
    ));
    await_server(server).await;
}

#[tokio::test]
async fn matching_ack_already_received_at_deadline_wins_the_tie() {
    let (listener, ws_uri) = websocket_listener().await;
    let (sent_tx, sent_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        sent_tx.send(()).ok();
        finish_rx.await.expect("finish server");
    });
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    await_signal(sent_rx, "matching ack queued").await;
    manual
        .advance_by(Duration::from_millis(1))
        .expect("deadline");
    let event = next_event(&mut feed).await;
    assert!(
        matches!(
            event,
            MarketEvent::Status {
                status: ConnectionStatus::GapSync,
                ..
            }
        ),
        "unexpected event: {event:?}"
    );
    cancellation.cancel();
    finish_tx.send(()).expect("finish");
    await_server(server).await;
}

#[tokio::test]
async fn peer_close_precedes_simultaneous_subscribe_ack_deadline() {
    let (listener, ws_uri) = websocket_listener().await;
    let (sent_tx, sent_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        websocket.close(None).await.expect("close");
        sent_tx.send(()).ok();
    });
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    await_signal(sent_rx, "close queued").await;
    manual
        .advance_by(Duration::from_millis(1))
        .expect("deadline");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Protocol { .. } | ProviderError::Transport { .. },
            ..
        }
    ));
    cancellation.cancel();
    await_server(server).await;
}

#[tokio::test]
async fn cancellation_precedes_pending_readiness_events() {
    let (listener, ws_uri) = websocket_listener().await;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        ready_tx.send(()).ok();
        release_rx.await.expect("release ack");
        let _ = websocket.send(Message::Text(subscribe_ack().into())).await;
    });
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    await_signal(ready_rx, "readiness events prepared").await;
    cancellation.cancel();
    release_tx.send(()).expect("release ack");
    manual
        .advance_by(Duration::from_millis(1))
        .expect("deadline");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Stopped,
            ..
        }
    ));
    await_server(server).await;
}

#[tokio::test]
async fn abrupt_eof_precedes_simultaneous_subscribe_ack_deadline() {
    let (listener, ws_uri) = websocket_listener().await;
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        drop(websocket);
        dropped_tx.send(()).ok();
    });
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connecting,
            ..
        }
    ));
    await_signal(dropped_rx, "abrupt EOF queued").await;
    manual
        .advance_by(Duration::from_millis(1))
        .expect("deadline");
    let error = next_event(&mut feed).await;
    assert!(
        matches!(
            error,
            MarketEvent::RecoverableError {
                error: ProviderError::Protocol { .. },
                ..
            }
        ),
        "unexpected precedence winner: {error:?}"
    );
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Backoff,
            ..
        }
    ));
    cancellation.cancel();
    await_server(server).await;
}

#[tokio::test]
async fn stalled_transport_write_precedes_subscribe_ack_deadline() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let _websocket = accept_async(stream).await.expect("upgrade");
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let manual = clock();
    let mut live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        stalled_write_probe_frames: 256,
        ..LiveSupervisorConfig::default()
    };
    live.ws_config.stalled_write_timeout = Duration::from_millis(20);
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    manual
        .advance_by(Duration::from_millis(1))
        .expect("deadline");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Timeout {
                kind: TimeoutKind::StalledWrite,
                ..
            },
            ..
        }
    ));
    cancellation.cancel();
    server.abort();
}

#[tokio::test]
async fn subscribe_ack_deadline_precedes_simultaneous_websocket_inactivity() {
    let (listener, ws_uri) = websocket_listener().await;
    let (subscribed_tx, subscribed_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        subscribed_tx.send(()).ok();
        finish_rx.await.expect("finish server");
    });
    let manual = clock();
    let mut live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    live.ws_config.message_inactivity_timeout = Duration::from_millis(1);
    let inactivity_timeout = live.ws_config.message_inactivity_timeout;
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connecting,
            ..
        }
    ));
    await_signal(subscribed_rx, "subscribe sent before deadlines").await;
    let definitely_inactive_at = tokio::time::Instant::now() + inactivity_timeout;
    while tokio::time::Instant::now() < definitely_inactive_at {
        std::thread::yield_now();
    }
    manual
        .advance_by(Duration::from_millis(1))
        .expect("subscribe-ack deadline");
    let error = next_event(&mut feed).await;
    assert!(
        matches!(
            error,
            MarketEvent::RecoverableError {
                error: ProviderError::Timeout {
                    kind: TimeoutKind::SubscribeAck,
                    ..
                },
                ..
            }
        ),
        "unexpected precedence winner: {error:?}"
    );
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Backoff,
            ..
        }
    ));
    cancellation.cancel();
    finish_tx.send(()).expect("finish");
    await_server(server).await;
}

#[tokio::test]
async fn hyperliquid_json_heartbeat_is_not_a_binance_default() {
    let (hyperliquid_listener, hyperliquid_uri) = websocket_listener().await;
    let hyperliquid_server = tokio::spawn(async move {
        let (stream, _) = hyperliquid_listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        assert_eq!(
            websocket.next().await.expect("frame").expect("heartbeat"),
            Message::Text(r#"{"method":"ping"}"#.into())
        );
    });
    let mut hyperliquid = connect_test_websocket(
        &hyperliquid_uri,
        &instrument(),
        Timeframe::Minute1,
        WsConfig::production(),
    )
    .await
    .expect("Hyperliquid socket");
    hyperliquid.start_application_heartbeat();
    hyperliquid.force_application_heartbeat_due_for_test();
    let _ = timeout(Duration::from_secs(1), hyperliquid.read()).await;
    await_server(hyperliquid_server).await;

    let (binance_listener, binance_uri) = websocket_listener().await;
    let binance_server = tokio::spawn(async move {
        let (stream, _) = binance_listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Ping(b"transport".to_vec().into()))
            .await
            .expect("transport Ping");
        assert_eq!(
            timeout(Duration::from_secs(1), websocket.next())
                .await
                .expect("Pong timeout")
                .expect("frame")
                .expect("Pong"),
            Message::Pong(b"transport".to_vec().into())
        );
        assert!(
            timeout(Duration::from_millis(75), websocket.next())
                .await
                .is_err(),
            "Binance emitted an unexpected application JSON heartbeat"
        );
    });
    let binance_instrument = Instrument::new(
        ProviderId::new("binance").expect("provider"),
        Market::Spot,
        "BTC",
        "USDT",
        "BTCUSDT",
    )
    .expect("instrument");
    let mut binance = fccli::provider::binance::connect_test_websocket(
        &binance_uri,
        &binance_instrument,
        Timeframe::Minute1,
        fccli::provider::binance::WsConfig::production(),
    )
    .await
    .expect("Binance socket");
    assert!(matches!(
        timeout(
            Duration::from_secs(1),
            fccli::provider::binance::read_raw_websocket(&mut binance)
        )
        .await
        .expect("transport Ping read"),
        Ok(fccli::provider::binance::DecodedFrame::Ignored)
    ));
    await_server(binance_server).await;
}

#[tokio::test]
async fn transport_control_frames_do_not_reset_hyperliquid_inactivity() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        for value in 0..8_u8 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if websocket
                .send(if value % 2 == 0 {
                    Message::Ping(vec![value].into())
                } else {
                    Message::Pong(vec![value].into())
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let mut config = WsConfig::production();
    config.message_inactivity_timeout = Duration::from_millis(40);
    let mut socket = connect_test_websocket(&ws_uri, &instrument(), Timeframe::Minute1, config)
        .await
        .expect("socket");
    let error = timeout(Duration::from_secs(1), async {
        loop {
            match socket.read().await {
                Ok(fccli::provider::hyperliquid::DecodedFrame::Ignored) => {}
                Ok(other) => panic!("unexpected frame before inactivity: {other:?}"),
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
    await_server(server).await;
}
