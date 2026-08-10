#![cfg(feature = "test-transport")]

use std::{sync::Arc, time::Duration};

use fccli::{
    clock::{Clock, ManualClock},
    error::ProviderError,
    model::{
        ConnectionStatus, GapGeneration, Instrument, Market, MarketEvent, MonoInstant, ProviderId,
        Timeframe,
    },
    provider::binance::{
        BinanceProvider, BinanceTestConfig, CONTROL_CAPACITY, EMERGENCY_CONTROL_CAPACITY,
        FIRST_KLINE_HANDSHAKE_TIMEOUT, KEYED_CANDLE_CAPACITY, LiveSupervisorConfig,
        MARKET_EVENT_CHANNEL_CAPACITY, MAX_CONNECTION_AGE, RECONCILE_ACK_TIMEOUT,
    },
    provider::{
        LiveRequest, MarketDataProvider, ProducerCompletion, ProviderRegistry, ReconcileAck,
        accepted_watermark_channel, reconcile_ack_channel,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

const OPEN_TIME: i64 = 1_700_000_040_000;

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
    json!({
        "e": "kline",
        "E": open_time + 60_001,
        "s": "BTCUSDT",
        "k": {
            "t": open_time,
            "T": open_time + 59_999,
            "s": "BTCUSDT",
            "i": "1m",
            "o": "37000.00",
            "c": close,
            "h": "37050.00",
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
    let (watermark_tx, watermark_rx) = accepted_watermark_channel(startup_watermark);
    let (ack_tx, ack_rx) = reconcile_ack_channel();
    let cancellation = fccli::provider::CancellationToken::new();
    (
        LiveRequest {
            instrument: instrument(),
            timeframe: Timeframe::Minute1,
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

#[test]
fn production_constants_capacity_boundaries_and_registry_are_exact() {
    let defaults = LiveSupervisorConfig::default();
    assert_eq!(defaults.keyed_candle_capacity, KEYED_CANDLE_CAPACITY);
    assert_eq!(defaults.control_capacity, CONTROL_CAPACITY);
    assert_eq!(
        defaults.market_event_capacity,
        MARKET_EVENT_CHANNEL_CAPACITY
    );
    assert_eq!(defaults.first_kline_timeout, FIRST_KLINE_HANDSHAKE_TIMEOUT);
    assert_eq!(defaults.reconcile_ack_timeout, RECONCILE_ACK_TIMEOUT);
    assert_eq!(defaults.max_connection_age, MAX_CONNECTION_AGE);
    assert_eq!(EMERGENCY_CONTROL_CAPACITY, 2);
    assert!(defaults.validate().is_ok());

    for field in 0..3 {
        for value in [0, 65_537] {
            let mut invalid = defaults.clone();
            match field {
                0 => invalid.keyed_candle_capacity = value,
                1 => invalid.control_capacity = value,
                2 => invalid.market_event_capacity = value,
                _ => unreachable!(),
            }
            assert!(matches!(
                invalid.validate(),
                Err(ProviderError::Configuration(_))
            ));
        }
        for value in [1, 65_536] {
            let mut valid = defaults.clone();
            match field {
                0 => valid.keyed_candle_capacity = value,
                1 => valid.control_capacity = value,
                2 => valid.market_event_capacity = value,
                _ => unreachable!(),
            }
            assert!(valid.validate().is_ok());
        }
    }

    for field in 0..2 {
        for value in [
            Duration::ZERO,
            Duration::from_secs(60) + Duration::from_nanos(1),
        ] {
            let mut invalid = defaults.clone();
            if field == 0 {
                invalid.first_kline_timeout = value;
            } else {
                invalid.reconcile_ack_timeout = value;
            }
            assert!(matches!(
                invalid.validate(),
                Err(ProviderError::Configuration(_))
            ));
        }
    }

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let rest = MockServer::start().await;
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let binance = Arc::new(BinanceProvider::new_test(rest.uri(), clock).expect("provider"));
        let registry = ProviderRegistry::new(Arc::clone(&binance));
        assert!(Arc::ptr_eq(&registry.binance(), &binance));
        assert_eq!(
            registry
                .get(ProviderId::new("binance").expect("id"))
                .expect("registered")
                .id()
                .as_str(),
            "binance"
        );
        assert!(matches!(
            registry.get(ProviderId::new("other").expect("id")),
            Err(ProviderError::Configuration(_))
        ));
    });
}

#[tokio::test]
async fn empty_startup_waits_for_first_ws_then_ack_gates_connected_and_live_candles() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (pong_tx, pong_rx) = oneshot::channel();
    let (close_tx, close_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Ping(vec![1, 2, 3].into()))
            .await
            .expect("ping");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        let pong = timeout(Duration::from_secs(1), websocket.next())
            .await
            .expect("pong timeout")
            .expect("socket")
            .expect("pong");
        pong_tx
            .send(matches!(pong, Message::Pong(payload) if payload.as_ref() == [1, 2, 3]))
            .ok();
        close_rx.await.expect("release closed kline");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, true, "37030.00").into()))
            .await
            .expect("closed kline");
        let _ = websocket.next().await;
    });

    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
    let (request, _watermark_tx, ack_tx) = request(None);
    let mut feed = provider.open_live(request).await.expect("live feed");

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
    let (generation, revision, target, candles) = match next_event(&mut feed).await {
        MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time,
            candles,
        } => (generation, revision, target_open_time, candles),
        other => panic!("expected reconcile batch, got {other:?}"),
    };
    assert_eq!(generation, GapGeneration(1));
    assert_eq!(target, OPEN_TIME);
    assert_eq!(
        candles.first().map(|candle| candle.open_time()),
        Some(OPEN_TIME)
    );
    ack_tx
        .publish(ReconcileAck {
            generation,
            revision,
            through: target,
        })
        .expect("ack");
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    close_tx.send(()).expect("release closed kline");

    match next_event(&mut feed).await {
        MarketEvent::Candle { generation, candle } => {
            assert_eq!(generation, GapGeneration(1));
            assert_eq!(candle.open_time(), OPEN_TIME);
            assert!(candle.is_closed());
        }
        other => panic!("expected authoritative candle, got {other:?}"),
    }
    assert!(pong_rx.await.expect("pong observation"));

    feed.request_shutdown();
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Stopped,
    );
    assert!(matches!(
        feed.producer_completion.changed().await,
        Ok(ProducerCompletion::Finished(Ok(())))
    ));
    server.await.expect("WS server");
}

#[tokio::test]
async fn acknowledgement_timeout_emits_exact_error_then_backoff_before_second_generation() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                .await
                .expect("kline");
            let _ = websocket.next().await;
        }
    });

    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let mut live = LiveSupervisorConfig::default();
    live.reconcile_ack_timeout = Duration::from_secs(2);
    let provider = provider(&rest.uri(), &ws_uri, manual.clone(), live);
    let (request, _watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
    let mut feed = provider.open_live(request).await.expect("live feed");

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
    let (revision, target) = match next_event(&mut feed).await {
        MarketEvent::ReconcileBatch {
            revision,
            target_open_time,
            ..
        } => (revision, target_open_time),
        other => panic!("expected batch, got {other:?}"),
    };
    tokio::task::yield_now().await;
    manual
        .advance_by(Duration::from_secs(2))
        .expect("ack deadline");
    match next_event(&mut feed).await {
        MarketEvent::RecoverableError {
            generation,
            error:
                ProviderError::ReconcileAckTimeout {
                    generation: error_generation,
                    revision: error_revision,
                    target_open_time,
                },
            rate_gate_deadline: None,
        } => {
            assert_eq!(generation, Some(GapGeneration(1)));
            assert_eq!(error_generation, GapGeneration(1));
            assert_eq!(error_revision, revision);
            assert_eq!(target_open_time, target);
        }
        other => panic!("expected ack timeout, got {other:?}"),
    }
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    tokio::task::yield_now().await;
    manual
        .advance_by(Duration::from_secs(1))
        .expect("first backoff");
    assert_status(
        next_event(&mut feed).await,
        Some(2),
        ConnectionStatus::Connecting,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn server_shutdown_reconnects_and_second_generation_uses_current_watermark() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.expect("first accept");
        let mut first = accept_async(first).await.expect("first upgrade");
        first
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        first
            .send(Message::Text(
                json!({"e":"serverShutdown","s":"BTCUSDT"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("shutdown");
        let (second, _) = listener.accept().await.expect("second accept");
        let mut second = accept_async(second).await.expect("second upgrade");
        second
            .send(Message::Text(
                ws_kline(OPEN_TIME + 60_000, false, "37040.00").into(),
            ))
            .await
            .expect("second kline");
        let _ = second.next().await;
    });

    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(
        &rest.uri(),
        &ws_uri,
        manual.clone(),
        LiveSupervisorConfig::default(),
    );
    let (request, watermark_tx, ack_tx) = request(Some(OPEN_TIME));
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
    let (generation, revision, target) = match next_event(&mut feed).await {
        MarketEvent::ReconcileBatch {
            generation,
            revision,
            target_open_time,
            ..
        } => (generation, revision, target_open_time),
        other => panic!("expected first batch, got {other:?}"),
    };
    ack_tx
        .publish(ReconcileAck {
            generation,
            revision,
            through: target,
        })
        .expect("first ack");
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    watermark_tx
        .publish(Some(target))
        .expect("accepted watermark");
    match next_event(&mut feed).await {
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            ..
        } => {}
        other => panic!("expected reconnect error, got {other:?}"),
    }
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    tokio::task::yield_now().await;
    manual.advance_by(Duration::from_secs(1)).expect("backoff");
    assert_status(
        next_event(&mut feed).await,
        Some(2),
        ConnectionStatus::Connecting,
    );
    assert_status(
        next_event(&mut feed).await,
        Some(2),
        ConnectionStatus::GapSync,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn gap_sync_non_special_client_status_is_terminal_in_band_and_stops_producer() {
    let rest = rest_server(
        ResponseTemplate::new(403).set_body_json(json!({"code": -1000, "msg": "denied"})),
    )
    .await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("kline");
        let _ = websocket.next().await;
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
    let (request, _watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
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
    match next_event(&mut feed).await {
        MarketEvent::TerminalError(ProviderError::ClientStatus { status: 403, .. }) => {}
        other => panic!("expected terminal 403, got {other:?}"),
    }
    assert!(matches!(
        feed.producer_completion.changed().await,
        Ok(ProducerCompletion::Finished(Err(
            ProviderError::ClientStatus { status: 403, .. }
        )))
    ));
    assert!(
        timeout(Duration::from_millis(50), feed.events.next())
            .await
            .expect("stream closes")
            .is_none()
    );
    server.abort();
}

#[tokio::test]
async fn invalid_418_ban_expiry_emits_out_of_generation_error_then_stopped() {
    let rest = rest_server(ResponseTemplate::new(418).set_body_json(json!({
        "code": -1003,
        "msg": "banned"
    })))
    .await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("kline");
        let _ = websocket.next().await;
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
    let (request, _watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
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
    assert_eq!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: None,
            error: ProviderError::InvalidBanExpiry,
            rate_gate_deadline: None,
        }
    );
    assert_status(next_event(&mut feed).await, None, ConnectionStatus::Stopped);
    assert!(matches!(
        feed.producer_completion.changed().await,
        Ok(ProducerCompletion::Finished(Err(
            ProviderError::InvalidBanExpiry
        )))
    ));
    server.abort();
}
