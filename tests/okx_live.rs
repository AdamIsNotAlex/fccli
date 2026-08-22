#![cfg(all(feature = "test-transport", not(feature = "production-transport")))]

use std::{sync::Arc, time::Duration};

use fccli::{
    clock::{Clock, ManualClock, checked_deadline},
    model::{
        ConnectionStatus, GapGeneration, Instrument, InstrumentSpec, Market, MarketEvent,
        MonoInstant, ProviderId, Timeframe,
    },
    provider::{
        LiveRequest, MarketDataProvider, ProducerCompletion, ReconcileAck,
        accepted_watermark_channel,
        okx::{
            APPLICATION_HEARTBEAT_INTERVAL, MESSAGE_INACTIVITY_TIMEOUT, OkxLiveConfig, OkxProvider,
            OkxTestConfig, test_websocket_url,
        },
        reconcile_ack_channel,
        test_transport::{HeartbeatTestHook, SubscribeFlushTestHook},
    },
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const OPEN_TIME: i64 = 1_700_000_040_000;
const NOW_MS: i64 = 1_800_000_000_000;

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(MonoInstant::ZERO))
}

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("okx").expect("provider"),
        Market::Spot,
        "BTC",
        "USDT",
        "BTC-USDT",
    )
    .expect("instrument")
}

fn request(startup_watermark: Option<i64>) -> (LiveRequest, fccli::provider::ReconcileAckSender) {
    let (watermark_tx, watermark_rx) = accepted_watermark_channel(startup_watermark);
    let (ack_tx, ack_rx) = reconcile_ack_channel();
    let cancellation = fccli::provider::CancellationToken::new();
    let control_lifetime = cancellation.clone();
    let ack_lifetime = ack_tx.clone();
    tokio::spawn(async move {
        control_lifetime.cancelled().await;
        drop((watermark_tx, ack_lifetime));
    });
    (
        LiveRequest {
            instrument: instrument(),
            timeframe: Timeframe::Minute1,
            startup_watermark,
            accepted_watermark_rx: watermark_rx,
            reconcile_ack_rx: ack_rx,
            cancellation,
        },
        ack_tx,
    )
}

async fn websocket_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    (listener, format!("ws://{address}"))
}

fn provider(
    rest_uri: &str,
    ws_uri: &str,
    clock: Arc<ManualClock>,
    live: OkxLiveConfig,
) -> Arc<OkxProvider> {
    let mut config = OkxTestConfig::loopback(rest_uri).with_websocket_base(ws_uri);
    config.rest.now_ms = Some(NOW_MS);
    config.live = live;
    Arc::new(OkxProvider::new_test_live(config, clock).expect("provider"))
}

fn ack() -> String {
    json!({
        "id": "fccli1",
        "event": "subscribe",
        "arg": {"channel": "candle1m", "instId": "BTC-USDT"}
    })
    .to_string()
}

fn candle(open_time: i64, confirmed: bool) -> String {
    json!({
        "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
        "data": [[
            open_time.to_string(), "10", "12", "9", "11", "5", "6", "7",
            if confirmed { "1" } else { "0" }
        ]]
    })
    .to_string()
}

fn rest_envelope(open_time: i64) -> serde_json::Value {
    json!({"code":"0","msg":"","data":[[
        open_time.to_string(), "10", "12", "9", "11", "5", "6", "7", "1"
    ]]})
}

async fn next_event(feed: &mut fccli::provider::LiveFeed) -> MarketEvent {
    timeout(Duration::from_secs(2), feed.events.next())
        .await
        .expect("event timeout")
        .expect("event stream")
        .expect("event")
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

#[test]
fn live_test_constructor_is_loopback_only_and_capabilities_are_exact() {
    let clock = clock();
    assert!(OkxProvider::new_test("https://openapi.okx.com", clock.clone()).is_err());
    let provider = OkxProvider::new_test("http://127.0.0.1:9", clock).unwrap();
    let capabilities = provider.capabilities();
    assert_eq!(capabilities.history_page_limit, 300);
    assert!(capabilities.markets.contains(&Market::Spot));
    assert!(capabilities.markets.contains(&Market::Perpetual));
    assert!(capabilities.timeframes.contains(&Timeframe::Second1));
    assert!(capabilities.timeframes.contains(&Timeframe::Hour6));
    assert!(!capabilities.timeframes.contains(&Timeframe::Hour8));
}

#[test]
fn live_defaults_match_okx_heartbeat_and_inactivity_contract() {
    assert_eq!(APPLICATION_HEARTBEAT_INTERVAL, Duration::from_secs(20));
    assert_eq!(MESSAGE_INACTIVITY_TIMEOUT, Duration::from_secs(30));
    let live = OkxLiveConfig::default();
    assert_eq!(
        live.supervisor.ws_config.message_inactivity_timeout,
        MESSAGE_INACTIVITY_TIMEOUT
    );
    assert_eq!(
        live.application_heartbeat_interval_for_test,
        APPLICATION_HEARTBEAT_INTERVAL
    );
}

#[test]
fn canonicalization_uses_native_spot_and_swap_ids() {
    let provider = OkxProvider::new_test("http://127.0.0.1:9", clock()).unwrap();
    let id = ProviderId::new("okx").unwrap();
    let spot =
        InstrumentSpec::new_with_market(id.clone(), Market::Spot, "btc", None::<String>).unwrap();
    let swap =
        InstrumentSpec::new_with_market(id, Market::Perpetual, "btc", None::<String>).unwrap();
    assert_eq!(
        provider.canonicalize(&spot).unwrap().provider_symbol(),
        "BTC-USDT"
    );
    assert_eq!(
        provider.canonicalize(&swap).unwrap().provider_symbol(),
        "BTC-USDT-SWAP"
    );
}

#[test]
fn websocket_test_url_rejects_non_loopback_hosts() {
    assert!(
        test_websocket_url(
            "wss://ws.okx.com:8443/ws/v5/business",
            &instrument(),
            Timeframe::Minute1,
        )
        .is_err()
    );
}

#[tokio::test]
async fn subscribe_payload_is_exact_and_ack_deadline_starts_only_after_flush() {
    let (listener, ws_uri) = websocket_listener().await;
    let blocked = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (received_tx, mut received_rx) = oneshot::channel();
    let (release_server_tx, release_server_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let frame = websocket.next().await.expect("frame").expect("subscribe");
        let Message::Text(payload) = frame else {
            panic!("subscribe must be text")
        };
        let value: serde_json::Value = serde_json::from_str(&payload).expect("json");
        assert_eq!(
            value,
            json!({
                "id":"fccli1", "op":"subscribe",
                "args":[{"channel":"candle1m","instId":"BTC-USDT"}]
            })
        );
        received_tx.send(()).ok();
        websocket
            .send(Message::Text(ack().into()))
            .await
            .expect("ack");
        release_server_rx.await.expect("release server");
    });
    let manual = clock();
    let live = OkxLiveConfig {
        subscribe_ack_timeout: Duration::from_millis(1),
        subscribe_flush_test_hook: Some(SubscribeFlushTestHook {
            blocked: blocked.clone(),
            release: release.clone(),
        }),
        ..OkxLiveConfig::default()
    };
    let provider = provider("http://127.0.0.1:9", &ws_uri, manual.clone(), live);
    let (request, _ack_tx) = request(None);
    let mut feed = provider.open_live(request).await.expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connecting,
            ..
        }
    ));
    timeout(Duration::from_secs(2), blocked.notified())
        .await
        .expect("flush blocked");
    manual.advance_by(Duration::from_secs(1)).expect("advance");
    assert!(
        (&mut received_rx).now_or_never().is_none(),
        "subscribe reached peer before flush release"
    );
    assert!(
        feed.events.next().now_or_never().is_none(),
        "ack timeout began before flush"
    );
    release.notify_one();
    timeout(Duration::from_secs(2), &mut received_rx)
        .await
        .expect("subscribe timeout")
        .expect("subscribe signal");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    release_server_tx.send(()).expect("release server");
    feed.request_shutdown();
    await_server(server).await;
}

#[tokio::test]
async fn pre_ack_candle_reconciles_then_connects_and_heartbeat_accepts_text_pong() {
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rest_envelope(OPEN_TIME)))
        .expect(1)
        .mount(&rest)
        .await;
    let (listener, ws_uri) = websocket_listener().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let due = Arc::new(tokio::sync::Notify::new());
    let (release_live_tx, release_live_rx) = oneshot::channel();
    let (release_ack_tx, release_ack_rx) = oneshot::channel();
    let (release_server_tx, release_server_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket
            .next()
            .await
            .expect("subscribe")
            .expect("subscribe");
        websocket
            .send(Message::Text(candle(OPEN_TIME, false).into()))
            .await
            .expect("pre-ack candle");
        release_ack_rx.await.expect("release ack");
        websocket
            .send(Message::Text(ack().into()))
            .await
            .expect("ack");
        assert_eq!(
            websocket.next().await.expect("ping").expect("ping"),
            Message::Text("ping".into())
        );
        websocket
            .send(Message::Text("pong".into()))
            .await
            .expect("pong");
        release_live_rx.await.expect("release live");
        websocket
            .send(Message::Text(candle(OPEN_TIME, true).into()))
            .await
            .expect("live candle");
        release_server_rx.await.expect("release server");
    });
    let live = OkxLiveConfig {
        heartbeat_test_hook: Some(HeartbeatTestHook {
            started: started.clone(),
            due: due.clone(),
        }),
        ..OkxLiveConfig::default()
    };
    let provider = provider(&rest.uri(), &ws_uri, clock(), live);
    let (request, ack_tx) = request(None);
    let mut feed = provider.open_live(request).await.expect("feed");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Connecting,
            ..
        }
    ));
    assert!(
        started.notified().now_or_never().is_none(),
        "heartbeat started before ack"
    );
    release_ack_tx.send(()).expect("release ack");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::GapSync,
            ..
        }
    ));
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("heartbeat start");
    due.notify_one();
    let (generation, revision, target, candles) = match next_event(&mut feed).await {
        MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time,
            candles,
        } => (generation, revision, target_open_time, candles),
        other => panic!("expected reconciliation batch, got {other:?}"),
    };
    assert_eq!(generation, GapGeneration(1));
    assert_eq!(target, OPEN_TIME);
    assert_eq!(
        candles.first().map(|item| item.open_time()),
        Some(OPEN_TIME)
    );
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
            status: ConnectionStatus::Connected,
            ..
        }
    ));
    release_live_tx.send(()).expect("release live");
    match next_event(&mut feed).await {
        MarketEvent::Candle { candle, .. } => {
            assert_eq!(candle.open_time(), OPEN_TIME);
            assert!(candle.is_closed());
        }
        other => panic!("expected live candle, got {other:?}"),
    }
    release_server_tx.send(()).expect("release server");
    feed.request_shutdown();
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::Status {
            status: ConnectionStatus::Stopped,
            ..
        }
    ));
    assert!(matches!(
        feed.producer_completion.changed().await,
        Ok(ProducerCompletion::Finished(Ok(())))
    ));
    await_server(server).await;
}

async fn reconnect_case(first_frame: Message) {
    let (listener, ws_uri) = websocket_listener().await;
    let (second_tx, second_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            let _ = websocket
                .next()
                .await
                .expect("subscribe")
                .expect("subscribe");
            if attempt == 0 {
                websocket
                    .send(Message::Text(ack().into()))
                    .await
                    .expect("ack");
                websocket
                    .send(first_frame.clone())
                    .await
                    .expect("reconnect frame");
            } else {
                second_tx.send(()).ok();
                let _ = websocket.close(None).await;
                break;
            }
        }
    });
    let manual = clock();
    let provider = provider(
        "http://127.0.0.1:9",
        &ws_uri,
        manual.clone(),
        OkxLiveConfig::default(),
    );
    let (request, _ack_tx) = request(None);
    let feed = provider.open_live(request).await.expect("feed");
    let advance = tokio::spawn(async move {
        for _ in 0..2_000 {
            manual
                .advance_by(Duration::from_millis(1))
                .expect("advance reconnect clock");
            tokio::task::yield_now().await;
        }
    });
    timeout(Duration::from_secs(2), second_rx)
        .await
        .expect("reconnect timeout")
        .expect("second connection");
    feed.request_shutdown();
    advance.abort();
    await_server(server).await;
}

#[tokio::test]
async fn peer_close_and_notice_64008_each_reconnect() {
    reconnect_case(Message::Close(None)).await;
    reconnect_case(Message::Text(
        json!({"event":"notice","code":"64008"}).to_string().into(),
    ))
    .await;
}

#[tokio::test]
async fn cancellation_requests_shutdown_and_join_completes_before_injected_deadline() {
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        let _ = websocket.next().await;
        websocket
            .send(Message::Text(ack().into()))
            .await
            .expect("ack");
        let _ = websocket.next().await;
    });
    let manual = clock();
    let provider = provider(
        "http://127.0.0.1:9",
        &ws_uri,
        manual.clone(),
        OkxLiveConfig::default(),
    );
    let (request, _ack_tx) = request(None);
    let mut feed = provider.open_live(request).await.expect("feed");
    let _ = next_event(&mut feed).await;
    let _ = next_event(&mut feed).await;
    feed.request_shutdown();
    let deadline = checked_deadline(manual.now(), Duration::from_secs(1)).expect("deadline");
    timeout(Duration::from_secs(2), feed.join(deadline))
        .await
        .expect("join timeout")
        .expect("join");
    await_server(server).await;
}
