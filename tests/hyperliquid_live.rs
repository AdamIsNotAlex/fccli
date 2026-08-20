#![cfg(feature = "test-transport")]

use std::{sync::Arc, time::Duration};

use fccli::{
    clock::ManualClock,
    error::{ErrorContext, ErrorOperation, ProviderError, TimeoutKind},
    model::{
        ConnectionStatus, GapGeneration, Instrument, Market, MarketEvent, MonoInstant, ProviderId,
        Timeframe,
    },
    provider::{
        LiveRequest, MarketDataProvider, ProducerCompletion, ReconcileAck,
        accepted_watermark_channel,
        hyperliquid::{
            APPLICATION_HEARTBEAT_INTERVAL, HyperliquidProvider, HyperliquidTestConfig,
            LiveSupervisorConfig, connect_test_websocket,
            gap_target_within_generation_span_for_test,
            reconciliation_distinct_key_allowed_for_test, reconciliation_page_guard_for_test,
        },
        reconcile_ack_channel,
        test_transport::{
            CloseFlushTestHook, DecodedFrame, HeartbeatTestHook, HyperliquidDecoded,
            ReadinessDecodedAckTestHook, ReadinessDrainBudgetTestHook, SubscribeFlushTestHook,
            WsConfig, read_raw_websocket,
        },
    },
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

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

fn provider_with_http(
    http_uri: &str,
    ws_uri: &str,
    clock: Arc<ManualClock>,
    live: LiveSupervisorConfig,
) -> HyperliquidProvider {
    let mut config = HyperliquidTestConfig::loopback(http_uri).with_websocket_base(ws_uri);
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
async fn decoded_candle_saturation_invalidates_generation_and_blocks_retry_until_pair_dequeue() {
    const OPEN: i64 = 1_700_000_040_000;
    let rest = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
            "T": OPEN + 59_999,
            "c": "42075.75",
            "h": "42125.50",
            "i": "1m",
            "l": "41950.25",
            "n": 12,
            "o": "42000.10",
            "s": "@142",
            "t": OPEN,
            "v": "123.456"
        }])))
        .mount(&rest)
        .await;
    let (listener, ws_uri) = websocket_listener().await;
    let (release_candles_tx, release_candles_rx) = oneshot::channel();
    let saturation = Arc::new(tokio::sync::Notify::new());
    let (retry_accepted_tx, mut retry_accepted_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("first accept");
        let mut websocket = accept_async(stream).await.expect("first upgrade");
        assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("subscribe ack");
        websocket
            .send(Message::Text(candle_message(OPEN).into()))
            .await
            .expect("startup candle");
        release_candles_rx
            .await
            .expect("release saturation candles");
        for successor in 1..=3_i64 {
            websocket
                .feed(Message::Text(
                    candle_message(OPEN + successor * 60_000).into(),
                ))
                .await
                .expect("decoded saturation candle");
        }
        websocket.flush().await.expect("saturation candle batch");
        let (retry, _) = listener.accept().await.expect("retry accept");
        retry_accepted_tx.send(()).expect("retry accepted");
        let mut retry = accept_async(retry).await.expect("retry upgrade");
        let _ = retry.next().await;
        std::future::pending::<()>().await;
    });

    let manual = clock();
    let live = LiveSupervisorConfig {
        keyed_candle_capacity: 1,
        market_event_capacity: 1,
        saturation_test_hook: Some(Arc::clone(&saturation)),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider_with_http(&rest.uri(), &ws_uri, Arc::clone(&manual), live);
    let cancellation = CancellationToken::new();
    let (watermark_tx, watermark_rx) = accepted_watermark_channel(Some(OPEN));
    let (ack_tx, ack_rx) = reconcile_ack_channel();
    let mut feed = provider
        .open_live(LiveRequest {
            instrument: instrument(),
            timeframe: Timeframe::Minute1,
            startup_watermark: Some(OPEN),
            accepted_watermark_rx: watermark_rx,
            reconcile_ack_rx: ack_rx,
            cancellation: cancellation.clone(),
        })
        .await
        .expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(GapGeneration(1)),
            status: ConnectionStatus::Connecting
        }
    ));
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(GapGeneration(1)),
            status: ConnectionStatus::GapSync
        }
    ));
    let (generation, revision, target) = match next_event(&mut feed).await {
        MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time,
            ..
        } => (generation, revision, target_open_time),
        other => panic!("expected reconcile batch, got {other:?}"),
    };
    ack_tx
        .publish(ReconcileAck {
            generation,
            revision,
            through: target,
        })
        .expect("reconcile ack");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            generation: Some(GapGeneration(1)),
            status: ConnectionStatus::Connected
        }
    ));

    release_candles_tx
        .send(())
        .expect("release saturation candles");
    timeout(Duration::from_secs(2), saturation.notified())
        .await
        .expect("production saturation transition timeout");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: None,
            error: ProviderError::QueueSaturated,
            ..
        }
    ));
    manual
        .advance_by(Duration::from_secs(1))
        .expect("backoff deadline");
    assert_eq!(
        feed.producer_completion.current(),
        Ok(ProducerCompletion::Running)
    );
    assert!(
        timeout(Duration::ZERO, &mut retry_accepted_rx)
            .await
            .is_err(),
        "retry must remain blocked until the emergency pair is dequeued"
    );
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            generation: None,
            status: ConnectionStatus::Backoff
        }
    ));
    timeout(Duration::from_secs(2), &mut retry_accepted_rx)
        .await
        .expect("retry accept timeout")
        .expect("retry accept sender");

    cancellation.cancel();
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            generation: None,
            status: ConnectionStatus::Stopped
        }
    ));
    assert!(matches!(
        feed.producer_completion.changed().await,
        Ok(ProducerCompletion::Finished(Ok(())))
    ));
    assert_eq!(
        timeout(Duration::from_secs(2), feed.events.next())
            .await
            .expect("receiver completion timeout"),
        None
    );
    drop(watermark_tx);
    server.abort();
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
async fn received_close_wins_while_automatic_close_response_flush_is_held() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
        websocket
            .feed(Message::Text(malformed_subscribe_ack().into()))
            .await
            .expect("malformed ack");
        websocket.feed(Message::Close(None)).await.expect("close");
        websocket.flush().await.expect("readiness batch");
        assert!(matches!(
            websocket.next().await,
            Some(Ok(Message::Close(_)))
        ));
    });
    let blocked = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let live = LiveSupervisorConfig {
        close_flush_test_hook: Some(CloseFlushTestHook {
            blocked: Arc::clone(&blocked),
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
    timeout(Duration::from_secs(2), blocked.notified())
        .await
        .expect("close flush was not held");
    release.notify_one();
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Protocol {
                detail: "WebSocket peer requested reconnect",
                ..
            },
            ..
        }
    ));
    cancellation.cancel();
    await_server(server).await;
}

#[tokio::test]
async fn cancellation_drops_blocked_pre_subscription_close_finalization() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
        websocket.feed(Message::Close(None)).await.expect("close");
        websocket.flush().await.expect("close flush");
        let _ = websocket.next().await;
    });
    let blocked = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let live = LiveSupervisorConfig {
        close_flush_test_hook: Some(CloseFlushTestHook {
            blocked: Arc::clone(&blocked),
            release,
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
    timeout(Duration::from_secs(2), blocked.notified())
        .await
        .expect("close finalization was not held");

    cancellation.cancel();
    assert!(matches!(
        timeout(Duration::from_secs(2), feed.producer_completion.changed())
            .await
            .expect("cancellation did not stop blocked close finalization")
            .expect("producer completion"),
        fccli::provider::ProducerCompletion::Finished(Ok(()))
    ));
    await_server(server).await;
}

#[test]
fn reconciliation_page_and_distinct_buffer_bounds_include_exact_boundary() {
    assert!(reconciliation_page_guard_for_test(63).is_ok());
    assert!(reconciliation_page_guard_for_test(64).is_ok());
    assert!(matches!(
        reconciliation_page_guard_for_test(65),
        Err(ProviderError::Protocol {
            detail: "Hyperliquid gap reconciliation exceeded the per-generation page limit",
            ..
        })
    ));

    assert!(reconciliation_distinct_key_allowed_for_test(64_000, false));
    assert!(reconciliation_distinct_key_allowed_for_test(64_001, true));
    assert!(!reconciliation_distinct_key_allowed_for_test(64_001, false));
}

#[test]
fn reconciliation_span_arithmetic_covers_exact_limit_and_overflow() {
    const START: i64 = 1_699_999_980_000;
    assert!(gap_target_within_generation_span_for_test(
        Timeframe::Minute1,
        START,
        START + 64_000 * 60_000,
    ));
    assert!(!gap_target_within_generation_span_for_test(
        Timeframe::Minute1,
        START,
        START + 64_001 * 60_000,
    ));
    assert!(!gap_target_within_generation_span_for_test(
        Timeframe::Minute1,
        START,
        i64::MAX,
    ));

    const JANUARY_1970: i64 = 0;
    const MONTH_SUCCESSOR_64_000: i64 = 168_303_571_200_000; // 7303-05-01T00:00:00Z
    const MONTH_SUCCESSOR_64_001: i64 = 168_306_249_600_000; // 7303-06-01T00:00:00Z
    assert!(gap_target_within_generation_span_for_test(
        Timeframe::Month1,
        JANUARY_1970,
        MONTH_SUCCESSOR_64_000,
    ));
    assert!(!gap_target_within_generation_span_for_test(
        Timeframe::Month1,
        JANUARY_1970,
        MONTH_SUCCESSOR_64_001,
    ));
    assert!(!gap_target_within_generation_span_for_test(
        Timeframe::Month1,
        JANUARY_1970,
        i64::MAX,
    ));
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

#[derive(Clone, Copy)]
enum ReadinessBoundaryOutcome {
    Close,
    MalformedAck,
    MatchingAck,
}

async fn readiness_budget_boundary_event(outcome: ReadinessBoundaryOutcome) -> MarketEvent {
    let (listener, ws_uri) = websocket_listener().await;
    let (frames_sent_tx, frames_sent_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
        for value in 0..255_u16 {
            websocket
                .feed(Message::Pong(value.to_be_bytes().to_vec().into()))
                .await
                .expect("lower-priority frame");
        }
        match outcome {
            ReadinessBoundaryOutcome::Close => {
                websocket.feed(Message::Close(None)).await.expect("close")
            }
            ReadinessBoundaryOutcome::MalformedAck => websocket
                .feed(Message::Text(malformed_subscribe_ack().into()))
                .await
                .expect("malformed ack"),
            ReadinessBoundaryOutcome::MatchingAck => websocket
                .feed(Message::Text(subscribe_ack().into()))
                .await
                .expect("matching ack"),
        }
        websocket.flush().await.expect("readiness frame batch");
        frames_sent_tx.send(()).expect("frames sent");
        std::future::pending::<()>().await;
    });
    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let subscribe_blocked = Arc::new(tokio::sync::Notify::new());
    let subscribe_release = Arc::new(tokio::sync::Notify::new());
    let manual = clock();
    let live = LiveSupervisorConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        readiness_drain_budget_test_hook: Some(ReadinessDrainBudgetTestHook {
            observed: Arc::clone(&observed),
            release: Arc::clone(&release),
        }),
        subscribe_flush_test_hook: Some(SubscribeFlushTestHook {
            blocked: Arc::clone(&subscribe_blocked),
            release: Arc::clone(&subscribe_release),
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
    timeout(Duration::from_secs(2), subscribe_blocked.notified())
        .await
        .expect("subscribe flush blocked");
    subscribe_release.notify_one();
    await_signal(frames_sent_rx, "buffered readiness frames").await;
    timeout(Duration::from_secs(2), observed.notified())
        .await
        .expect("readiness budget reached");
    manual
        .advance_by(Duration::from_millis(1))
        .expect("simultaneous deadline");
    release.notify_one();
    let event = next_event(&mut feed).await;
    cancellation.cancel();
    server.abort();
    event
}

#[tokio::test]
async fn close_on_readiness_budget_boundary_wins_simultaneous_ack_deadline() {
    assert!(matches!(
        readiness_budget_boundary_event(ReadinessBoundaryOutcome::Close).await,
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
async fn malformed_ack_on_readiness_budget_boundary_wins_simultaneous_ack_deadline() {
    assert!(matches!(
        readiness_budget_boundary_event(ReadinessBoundaryOutcome::MalformedAck).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Protocol { .. } | ProviderError::Payload { .. },
            ..
        }
    ));
}

#[tokio::test]
async fn matching_ack_on_readiness_budget_boundary_wins_simultaneous_ack_deadline() {
    assert!(matches!(
        readiness_budget_boundary_event(ReadinessBoundaryOutcome::MatchingAck).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
}

#[tokio::test]
async fn reconciliation_span_bound_reconnects_while_rest_page_is_pending() {
    let (http_listener, http_ws_uri) = websocket_listener().await;
    let http_uri = http_ws_uri.replacen("ws://", "http://", 1);
    let (ws_listener, ws_uri) = websocket_listener().await;
    let (history_started_tx, history_started_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (stream, _) = http_listener.accept().await.expect("HTTP accept");
        let mut request = [0_u8; 2048];
        stream.readable().await.expect("HTTP request readiness");
        let bytes = stream.try_read(&mut request).expect("HTTP request");
        assert!(bytes > 0, "history request must reach the pending server");
        history_started_tx.send(()).expect("history started");
        std::future::pending::<()>().await;
    });
    let ws_server = tokio::spawn(async move {
        let (stream, _) = ws_listener.accept().await.expect("WebSocket accept");

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
        const START: i64 = 1_699_999_980_000;
        websocket
            .send(Message::Text(candle_message(START).into()))
            .await
            .expect("first candle");
        await_signal(history_started_rx, "pending history request").await;
        websocket
            .send(Message::Text(candle_message(START + 4 * 60_000).into()))
            .await
            .expect("out-of-span reconciliation successor");
        std::future::pending::<()>().await;
    });
    let live = LiveSupervisorConfig {
        max_gap_reconciliation_candles_for_test: 3,
        ..LiveSupervisorConfig::default()
    };
    assert_eq!(
        LiveSupervisorConfig::default().max_gap_reconciliation_candles_for_test,
        64_000,
        "the test override must default to the production generation bound"
    );
    let provider = provider_with_http(&http_uri, &ws_uri, clock(), live);
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
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Protocol {
                detail: "Hyperliquid gap reconciliation target exceeds the per-generation span limit",
                ..
            },
            ..
        }
    ));
    cancellation.cancel();
    http_server.abort();
    ws_server.abort();
}
#[tokio::test]
async fn large_in_bound_pending_rest_sequence_remains_promptly_cancellable() {
    let (http_listener, http_ws_uri) = websocket_listener().await;
    let http_uri = http_ws_uri.replacen("ws://", "http://", 1);
    let (ws_listener, ws_uri) = websocket_listener().await;
    let (history_started_tx, history_started_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (stream, _) = http_listener.accept().await.expect("HTTP accept");
        let mut request = [0_u8; 2048];
        stream.readable().await.expect("HTTP request readiness");
        assert!(stream.try_read(&mut request).expect("HTTP request") > 0);
        history_started_tx.send(()).expect("history started");
        std::future::pending::<()>().await;
    });
    let (sequence_sent_tx, sequence_sent_rx) = oneshot::channel();
    let ws_server = tokio::spawn(async move {
        let (stream, _) = ws_listener.accept().await.expect("WebSocket accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
        websocket
            .send(Message::Text(subscribe_ack().into()))
            .await
            .expect("ack");
        const START: i64 = 1_699_999_980_000;
        websocket
            .send(Message::Text(candle_message(START).into()))
            .await
            .expect("first candle");
        await_signal(history_started_rx, "pending history request").await;
        for successor in 1..=8_192_i64 {
            websocket
                .feed(Message::Text(
                    candle_message(START + successor * 60_000).into(),
                ))
                .await
                .expect("in-bound reconciliation candle");
        }
        websocket
            .flush()
            .await
            .expect("large reconciliation sequence");
        sequence_sent_tx.send(()).expect("sequence sent");
        std::future::pending::<()>().await;
    });
    let provider = provider_with_http(&http_uri, &ws_uri, clock(), LiveSupervisorConfig::default());
    let cancellation = CancellationToken::new();
    let mut feed = provider
        .open_live(request(cancellation.clone()))
        .await
        .expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status { .. }
    ));
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    await_signal(sequence_sent_rx, "large sequence").await;
    cancellation.cancel();
    timeout(Duration::from_secs(2), async {
        while feed.events.next().await.is_some() {}
    })
    .await
    .expect("cancellation must promptly stop reconciliation");
    http_server.abort();
    ws_server.abort();
}

#[tokio::test]
async fn accepted_watermark_span_bound_reconnects_while_rest_page_is_pending() {
    let (http_listener, http_ws_uri) = websocket_listener().await;
    let http_uri = http_ws_uri.replacen("ws://", "http://", 1);
    let (ws_listener, ws_uri) = websocket_listener().await;
    let (history_started_tx, history_started_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (stream, _) = http_listener.accept().await.expect("HTTP accept");
        let mut request = [0_u8; 2048];
        stream.readable().await.expect("HTTP request readiness");
        let bytes = stream.try_read(&mut request).expect("HTTP request");
        assert!(bytes > 0, "history request must reach the pending server");
        history_started_tx.send(()).expect("history started");
        std::future::pending::<()>().await;
    });
    const START: i64 = 1_699_999_980_000;
    let ws_server = tokio::spawn(async move {
        let (stream, _) = ws_listener.accept().await.expect("WebSocket accept");
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
            .send(Message::Text(candle_message(START).into()))
            .await
            .expect("first candle");
        std::future::pending::<()>().await;
    });
    let live = LiveSupervisorConfig {
        max_gap_reconciliation_candles_for_test: 3,
        ..LiveSupervisorConfig::default()
    };
    let provider = provider_with_http(&http_uri, &ws_uri, clock(), live);
    let cancellation = CancellationToken::new();
    let (watermark_tx, watermark_rx) = accepted_watermark_channel(None);
    let (ack_tx, ack_rx) = reconcile_ack_channel();
    let mut feed = provider
        .open_live(LiveRequest {
            instrument: instrument(),
            timeframe: Timeframe::Minute1,
            startup_watermark: None,
            accepted_watermark_rx: watermark_rx,
            reconcile_ack_rx: ack_rx,
            cancellation: cancellation.clone(),
        })
        .await
        .expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connecting,
            ..
        }
    ));
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    await_signal(history_started_rx, "pending history request").await;
    watermark_tx
        .publish(Some(START + 3 * 60_000))
        .expect("valid watermark advance");
    tokio::task::yield_now().await;
    assert!(
        feed.events.next().now_or_never().is_none(),
        "a watermark at the configured successor limit must remain valid"
    );
    watermark_tx
        .publish(Some(START + 4 * 60_000))
        .expect("out-of-bound watermark advance");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            error: ProviderError::Protocol {
                context,
                detail: "Hyperliquid gap reconciliation target exceeds the per-generation span limit",
            },
            ..
        } if context == ErrorContext::operation(ErrorOperation::Reconciliation)
    ));
    cancellation.cancel();
    drop(ack_tx);
    http_server.abort();
    ws_server.abort();
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
        WsConfig::production(),
    )
    .await
    .expect("Binance socket");
    assert!(matches!(
        timeout(Duration::from_secs(1), read_raw_websocket(&mut binance))
            .await
            .expect("transport Ping read"),
        Ok(DecodedFrame::Ignored)
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
                Ok(DecodedFrame::Ignored) => {}
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
