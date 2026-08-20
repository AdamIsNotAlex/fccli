#![cfg(feature = "test-transport")]

use std::{sync::Arc, time::Duration};

use fccli::{
    clock::ManualClock,
    error::{ErrorContext, ErrorOperation, ProviderError, TimeoutKind},
    model::{
        ConnectionStatus, Instrument, Market, MarketEvent, MonoInstant, ProviderId, Timeframe,
    },
    provider::{
        LiveRequest, MarketDataProvider, accepted_watermark_channel,
        hyperliquid::{
            APPLICATION_HEARTBEAT_INTERVAL, HeartbeatTestHook, HyperliquidProvider,
            HyperliquidTestConfig, LiveSupervisorConfig, ReadinessDecodedAckTestHook,
            SubscribeFlushTestHook, WsConfig, connect_test_websocket,
        },
        reconcile_ack_channel,
    },
};
use futures_util::{FutureExt, SinkExt, StreamExt};
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

fn candle_message(open_time: i64) -> String {
    json!({
        "channel": "candle",
        "data": {
            "t": open_time,
            "T": open_time + 59_999,
            "s": "@142",
            "i": "1m",
            "o": "42000.10",
            "c": "42075.75",
            "h": "42125.50",
            "l": "41950.25",
            "v": "123.456",
            "n": 12
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
    assert_eq!(
        LiveSupervisorConfig::default().subscribe_ack_timeout,
        Duration::from_secs(10)
    );
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
    let event = next_event_while_advancing(&mut feed, &manual).await;
    let expected_context = ErrorContext::operation(ErrorOperation::WebSocket)
        .with_market(&instrument(), Timeframe::Minute1);
    assert!(matches!(
        event,
        MarketEvent::RecoverableError {
            error: ProviderError::Timeout {
                context,
                kind: TimeoutKind::SubscribeAck,
            },
            ..
        } if context == expected_context
    ));
    cancellation.cancel();
    server.abort();
}

#[tokio::test]
async fn far_future_first_candle_reconnects_before_gap_history_work() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket
            .next()
            .await
            .expect("subscribe")
            .expect("subscribe");
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        websocket
            .send(Message::Text(candle_message(4_102_444_800_000).into()))
            .await
            .expect("future candle");
        std::future::pending::<()>().await;
    });
    let provider = provider(&ws_uri, clock(), LiveSupervisorConfig::default());
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    let _ = next_event(&mut feed).await;
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Payload { .. },
            ..
        }
    ));
    cancellation.cancel();
    server.abort();
}

fn malformed_subscribe_ack() -> String {
    json!({
        "channel": "subscriptionResponse",
        "data": {
            "method": "unsubscribe",
            "subscription": {"type": "candle", "coin": "@142", "interval": "1m"}
        }
    })
    .to_string()
}

async fn readiness_terminal_after_malformed_flood(close_cleanly: bool) -> MarketEvent {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket
            .next()
            .await
            .expect("subscribe")
            .expect("subscribe");
        for _ in 0..80 {
            websocket
                .send(Message::Text(malformed_subscribe_ack().into()))
                .await
                .expect("malformed ack");
        }
        if close_cleanly {
            websocket.close(None).await.expect("close");
        }
    });
    let provider = provider(&ws_uri, clock(), LiveSupervisorConfig::default());
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    let event = next_event(&mut feed).await;
    cancellation.cancel();
    await_server(server).await;
    event
}

#[tokio::test]
async fn buffered_close_wins_after_more_than_readiness_retention_limit_malformed_acks() {
    assert!(matches!(
        readiness_terminal_after_malformed_flood(true).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Protocol {
                detail: "WebSocket peer requested reconnect",
                ..
            },
            ..
        }
    ));
}

#[tokio::test]
async fn buffered_eof_or_read_error_wins_after_more_than_readiness_retention_limit_malformed_acks()
{
    assert!(matches!(
        readiness_terminal_after_malformed_flood(false).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Transport { .. } | ProviderError::Protocol { .. },
            ..
        }
    ));
}

#[tokio::test]
async fn decoded_ack_wins_deterministic_deadline_tie_before_arbitration() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket
            .next()
            .await
            .expect("subscribe")
            .expect("subscribe");
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        std::future::pending::<()>().await;
    });
    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        readiness_decoded_ack_test_hook: Some(ReadinessDecodedAckTestHook {
            observed: Arc::clone(&observed),
            release: Arc::clone(&release),
        }),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    timeout(Duration::from_secs(2), observed.notified())
        .await
        .expect("decoded ack");
    manual
        .advance_by(Duration::from_millis(1))
        .expect("deadline");
    release.notify_one();
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    cancellation.cancel();
    server.abort();
}

#[tokio::test]
async fn ack_deadline_wins_actual_readiness_loop_tie_with_inactivity() {
    let (listener, ws_uri) = websocket_listener().await;
    let (subscribed_tx, subscribed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket
            .next()
            .await
            .expect("subscribe")
            .expect("subscribe");
        subscribed_tx.send(()).expect("subscribed");
        std::future::pending::<()>().await;
    });
    let inactivity = Arc::new(tokio::sync::Notify::new());
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        readiness_inactivity_test_hook: Some(Arc::clone(&inactivity)),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    await_signal(subscribed_rx, "subscribe received").await;
    inactivity.notify_one();
    manual
        .advance_by(Duration::from_millis(1))
        .expect("advance to ack deadline");
    assert!(matches!(
        next_event(&mut feed).await,
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
async fn cancellation_precedes_simultaneously_queued_ack() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        std::future::pending::<()>().await;
    });
    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let live = LiveSupervisorConfig {
        readiness_decoded_ack_test_hook: Some(ReadinessDecodedAckTestHook {
            observed: Arc::clone(&observed),
            release: Arc::clone(&release),
        }),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, clock(), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    timeout(Duration::from_secs(2), observed.notified())
        .await
        .expect("queued ack");
    cancellation.cancel();
    release.notify_one();
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Stopped,
            ..
        }
    ));
    server.abort();
}

#[tokio::test]
async fn stalled_write_outranks_buffered_malformed_ack_and_ack_deadline() {
    for advance_deadline in [false, true] {
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            let _ = websocket.next().await;
            websocket
                .send(Message::Text(malformed_subscribe_ack().into()))
                .await
                .expect("malformed ack");
            std::future::pending::<()>().await;
        });
        let manual = clock();
        let live = LiveSupervisorConfig {
            subscribe_ack_timeout: Duration::from_millis(1),
            force_stalled_write_after_readiness_frame: true,
            ..LiveSupervisorConfig::default()
        };
        let provider = provider(&ws_uri, Arc::clone(&manual), live);
        let cancellation = CancellationToken::new();
        let mut feed = provider
            .open_live(request(cancellation.clone()))
            .await
            .expect("feed");
        let _ = next_event(&mut feed).await;
        if advance_deadline {
            manual
                .advance_by(Duration::from_millis(1))
                .expect("deadline");
        }
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
}

#[tokio::test]
async fn blocked_subscribe_flush_does_not_start_ack_deadline() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        std::future::pending::<()>().await;
    });
    let blocked = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        subscribe_flush_test_hook: Some(SubscribeFlushTestHook {
            blocked: Arc::clone(&blocked),
            release: Arc::clone(&release),
        }),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    timeout(Duration::from_secs(2), blocked.notified())
        .await
        .expect("flush blocked");
    manual
        .advance_by(Duration::from_secs(1))
        .expect("advance before flush");
    assert!(
        feed.events.next().now_or_never().is_none(),
        "ack timeout started before subscribe flush"
    );
    release.notify_one();
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    cancellation.cancel();
    server.abort();
}

#[tokio::test]
async fn ignored_json_cannot_starve_absolute_subscribe_ack_deadline() {
    let (listener, ws_uri) = websocket_listener().await;
    let (subscribed_tx, subscribed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        subscribed_tx.send(()).expect("signal subscribe");
        loop {
            if websocket
                .send(Message::Text(r#"{"channel":"pong"}"#.into()))
                .await
                .is_err()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
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
    await_signal(subscribed_rx, "subscribe received").await;

    let event = async {
        for _ in 0..1_000 {
            tokio::select! {
                biased;
                event = feed.events.next() => {
                    return event.expect("event stream").expect("event");
                }
                () = tokio::task::yield_now() => {
                    manual
                        .advance_by(Duration::from_millis(1))
                        .expect("advance across deadline");
                }
            }
        }
        panic!("subscribe-ack deadline remained starved by ignored frames");
    }
    .await;
    assert!(matches!(
        event,
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
async fn application_heartbeat_schedule_begins_only_after_ack() {
    assert_eq!(APPLICATION_HEARTBEAT_INTERVAL, Duration::from_secs(50));
    let (listener, ws_uri) = websocket_listener().await;
    let (subscribed_tx, subscribed_rx) = oneshot::channel();
    let (ack_tx, ack_rx) = oneshot::channel();
    let started = Arc::new(tokio::sync::Notify::new());
    let due = Arc::new(tokio::sync::Notify::new());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket
            .next()
            .await
            .expect("subscribe")
            .expect("subscribe");
        subscribed_tx.send(()).ok();
        ack_rx.await.expect("release ack");
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        assert_eq!(
            websocket
                .next()
                .await
                .expect("heartbeat stream")
                .expect("heartbeat"),
            Message::Text(r#"{"method":"ping"}"#.into())
        );
    });
    let live = LiveSupervisorConfig {
        heartbeat_test_hook: Some(HeartbeatTestHook {
            started: Arc::clone(&started),
            due: Arc::clone(&due),
        }),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&ws_uri, clock(), live);
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    let _ = next_event(&mut feed).await;
    await_signal(subscribed_rx, "subscribe received").await;
    due.notify_one();
    assert!(started.notified().now_or_never().is_none());
    ack_tx.send(()).expect("release ack");
    started.notified().await;
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    await_server(server).await;
    cancellation.cancel();
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
    hyperliquid.start_application_heartbeat(APPLICATION_HEARTBEAT_INTERVAL);
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
