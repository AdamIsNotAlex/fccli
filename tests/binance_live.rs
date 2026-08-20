#![cfg(feature = "test-transport")]

use std::{sync::Arc, time::Duration};

use fccli::{
    clock::{Clock, ManualClock},
    error::{
        ErrorContext, ErrorOperation, ModelError, PayloadError, ProviderError, SanitizedCause,
        SanitizedMessage, TimeoutKind,
    },
    model::{
        ConnectionStatus, GapGeneration, HistoryRequest, Instrument, InstrumentSpec, Market,
        MarketEvent, MonoInstant, ProviderId, RateGateState, ReplayRevision, Timeframe,
    },
    provider::binance::{
        BinanceProvider, BinanceTestConfig, CONTROL_CAPACITY, EMERGENCY_CONTROL_CAPACITY,
        FIRST_KLINE_HANDSHAKE_TIMEOUT, KEYED_CANDLE_CAPACITY, LiveCompletionDisposition,
        LiveErrorDisposition, LiveInBandEventDisposition, LiveInputClassification,
        LiveSupervisorConfig, MARKET_EVENT_CHANNEL_CAPACITY, MAX_CONNECTION_AGE,
        RECONCILE_ACK_TIMEOUT, classify_live_error_for_test, classify_live_input_for_test,
    },
    provider::test_transport::{BinanceDecoded, DecodedFrame},
    provider::{
        CancellationToken, LiveRequest, MarketDataProvider, ProducerCompletion, ProviderRegistry,
        ReconcileAck, ReconcileAckPublishError, accepted_watermark_channel, reconcile_ack_channel,
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

async fn next_after_optional_startup_statuses(feed: &mut fccli::provider::LiveFeed) -> MarketEvent {
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

    for value in [Duration::from_millis(1), Duration::from_secs(60)] {
        let mut valid = defaults.clone();
        valid.first_kline_timeout = value;
        valid.reconcile_ack_timeout = value;
        assert!(valid.validate().is_ok());
    }
    for value in [Duration::from_nanos(1), Duration::from_micros(999)] {
        let mut invalid = defaults.clone();
        invalid.first_kline_timeout = value;
        assert!(matches!(
            invalid.validate(),
            Err(ProviderError::Configuration(_))
        ));
        invalid = defaults.clone();
        invalid.reconcile_ack_timeout = value;
        assert!(matches!(
            invalid.validate(),
            Err(ProviderError::Configuration(_))
        ));
    }
    let mut zero_age = defaults.clone();
    zero_age.max_connection_age = Duration::ZERO;
    assert!(matches!(
        zero_age.validate(),
        Err(ProviderError::Configuration(_))
    ));

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
async fn registry_trait_object_uses_shared_history_live_and_rate_gate_state() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        let _ = websocket.next().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::ZERO));
    let concrete = provider(
        &rest.uri(),
        &ws_uri,
        Arc::<ManualClock>::clone(&manual),
        LiveSupervisorConfig::default(),
    );
    let registry = ProviderRegistry::new(Arc::clone(&concrete));
    let selected = registry
        .get(ProviderId::new("binance").expect("provider id"))
        .expect("registered provider");
    assert_eq!(selected.id().as_str(), "binance");
    assert_eq!(selected.rate_gate().current(), Ok(RateGateState::Open));

    let specification = InstrumentSpec::new(
        ProviderId::new("binance").expect("provider id"),
        "btc",
        Some("usdt"),
    )
    .expect("instrument specification");
    let canonical = selected
        .canonicalize(&specification)
        .expect("trait canonicalization");
    assert_eq!(canonical.provider_symbol(), "BTCUSDT");
    let foreign = InstrumentSpec::new(
        ProviderId::new("okx").expect("provider id"),
        "btc",
        None::<String>,
    )
    .expect("foreign instrument specification");
    let error = selected
        .canonicalize(&foreign)
        .expect_err("Binance provider rejects foreign known-provider specs");
    assert!(
        error
            .to_string()
            .contains("instrument is not valid for Binance"),
        "{error}"
    );

    let history = selected
        .history(
            &canonical,
            Timeframe::Minute1,
            HistoryRequest::latest(1).expect("latest request"),
            CancellationToken::new(),
        )
        .await
        .expect("trait history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].open_time(), OPEN_TIME);

    let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
    let mut feed = selected.open_live(request).await.expect("trait live feed");
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
    feed.request_shutdown();
    assert_status(next_event(&mut feed).await, None, ConnectionStatus::Stopped);
    assert_eq!(
        feed.producer_completion
            .changed()
            .await
            .expect("completion observation"),
        ProducerCompletion::Finished(Ok(()))
    );
    feed.join(MonoInstant::from_nanos(1))
        .await
        .expect("consuming join returns same completion");
    server.abort();
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
    assert_status(next_event(&mut feed).await, None, ConnectionStatus::Stopped);
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
    let live = LiveSupervisorConfig {
        reconcile_ack_timeout: Duration::from_secs(2),
        ..LiveSupervisorConfig::default()
    };
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
async fn unacknowledged_generations_use_full_capped_backoff_sequence() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        for _ in 0..8 {
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
    let live = LiveSupervisorConfig {
        reconcile_ack_timeout: Duration::from_millis(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&rest.uri(), &ws_uri, manual.clone(), live);
    let (request, _watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
    let mut feed = provider.open_live(request).await.expect("feed");

    for (index, delay) in [1_u64, 2, 4, 8, 16, 30, 30].into_iter().enumerate() {
        let generation = u64::try_from(index + 1).expect("generation");
        assert_status(
            next_event(&mut feed).await,
            Some(generation),
            ConnectionStatus::Connecting,
        );
        assert_status(
            next_event(&mut feed).await,
            Some(generation),
            ConnectionStatus::GapSync,
        );
        let _ = next_batch(&mut feed).await;
        tokio::task::yield_now().await;
        manual
            .advance_by(Duration::from_millis(1))
            .expect("ack timeout");
        assert!(matches!(
            next_event(&mut feed).await,
            MarketEvent::RecoverableError {
                generation: Some(actual),
                error: ProviderError::ReconcileAckTimeout { .. },
                rate_gate_deadline: None,
            } if actual == GapGeneration(generation)
        ));
        assert_status(
            next_event(&mut feed).await,
            Some(generation),
            ConnectionStatus::Backoff,
        );
        tokio::task::yield_now().await;
        manual
            .advance_by(Duration::from_secs(delay))
            .expect("backoff deadline");
    }
    assert_status(
        next_event(&mut feed).await,
        Some(8),
        ConnectionStatus::Connecting,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn server_shutdown_reconnects_and_second_generation_uses_advanced_watermark() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (release_shutdown_tx, release_shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.expect("first accept");
        let mut first = accept_async(first).await.expect("first upgrade");
        first
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        release_shutdown_rx
            .await
            .expect("release server shutdown after generation 1 ack");
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
                ws_kline(OPEN_TIME + 120_000, false, "37040.00").into(),
            ))
            .await
            .expect("second kline distinct from startup and accepted watermark");
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

    let current_watermark = target + 60_000;
    watermark_tx
        .publish(Some(current_watermark))
        .expect("newer accepted watermark");
    release_shutdown_tx
        .send(())
        .expect("release deterministic server shutdown");

    match next_event(&mut feed).await {
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::Protocol { .. },
            ..
        } => {}
        other => panic!("expected shutdown reconnect error after Connected, got {other:?}"),
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
    let requests = timeout(Duration::from_secs(1), async {
        loop {
            let requests = rest.received_requests().await.expect("REST requests");
            if requests.len() >= 2 {
                break requests;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second-generation REST request");
    let second_start = requests[1]
        .url
        .query_pairs()
        .find_map(|(key, value)| (key == "startTime").then(|| value.into_owned()));
    assert_eq!(
        second_start,
        Some(current_watermark.to_string()),
        "second generation must start REST at the advanced accepted watermark"
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn server_shutdown_during_rest_emits_no_batch_and_recovers_exactly() {
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(60))
                .set_body_json(json!([rest_row(OPEN_TIME)])),
        )
        .mount(&rest)
        .await;
    let (listener, ws_uri) = websocket_listener().await;
    let (rest_started_tx, rest_started_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.expect("first accept");
        let mut first = accept_async(first).await.expect("first upgrade");
        first
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        rest_started_rx
            .await
            .expect("release shutdown after REST starts");
        first
            .send(Message::Text(
                json!({"e":"serverShutdown","s":"BTCUSDT"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("server shutdown during REST");

        let (second, _) = listener.accept().await.expect("second accept");
        let mut second = accept_async(second).await.expect("second upgrade");
        second
            .send(Message::Text(
                ws_kline(OPEN_TIME + 60_000, false, "37040.00").into(),
            ))
            .await
            .expect("second-generation first kline");
        futures_util::future::pending::<()>().await;
    });

    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(
        &rest.uri(),
        &ws_uri,
        manual.clone(),
        LiveSupervisorConfig::default(),
    );
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
    timeout(Duration::from_secs(1), async {
        loop {
            if !rest
                .received_requests()
                .await
                .expect("REST requests")
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first-generation REST start");
    rest_started_tx
        .send(())
        .expect("release server shutdown during REST");

    match next_event(&mut feed).await {
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error:
                ProviderError::Protocol {
                    detail: "WebSocket peer requested reconnect",
                    ..
                },
            rate_gate_deadline: None,
        } => {}
        other => panic!("expected shutdown recovery without a reconcile batch, got {other:?}"),
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
async fn sustained_websocket_traffic_does_not_starve_ready_rest_reconciliation() {
    let (rest_uri, rest_started_rx, rest_release_tx, rest_server) =
        held_rest_listener(json!([rest_row(OPEN_TIME)])).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (traffic_started_tx, traffic_started_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        traffic_started_tx.send(()).ok();
        let mut revision = 0_u64;
        loop {
            revision += 1;
            websocket
                .send(Message::Text(
                    ws_kline(
                        OPEN_TIME,
                        false,
                        &format!("{}.{:02}", 37_025 + revision % 10, revision % 100),
                    )
                    .into(),
                ))
                .await
                .expect("sustained candle");
            websocket
                .send(Message::Ping(vec![1, 2, 3].into()))
                .await
                .expect("sustained ping");
            tokio::task::yield_now().await;
        }
    });

    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest_uri, &ws_uri, manual, LiveSupervisorConfig::default());
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
    rest_started_rx.await.expect("REST request started");
    traffic_started_rx.await.expect("WS traffic started");
    rest_release_tx.send(()).expect("REST becomes ready");

    let (generation, _revision, target, candles) =
        timeout(Duration::from_secs(1), next_batch(&mut feed))
            .await
            .expect("ready REST must not be starved by continuously ready WS traffic");
    assert_eq!(generation, GapGeneration(1));
    assert_eq!(target, OPEN_TIME);
    assert_eq!(candles.len(), 1);
    assert_eq!(candles[0].open_time(), OPEN_TIME);

    feed.request_shutdown();
    server.abort();
    rest_server.abort();
}

#[tokio::test]
async fn ready_rest_page_drains_watermark_and_ws_candle_before_reconcile_batch() {
    let (rest_uri, rest_started_rx, rest_release_tx, rest_server) =
        held_rest_listener_with_followup(
            json!([rest_row(OPEN_TIME)]),
            Some(json!([
                rest_row(OPEN_TIME + 60_000),
                rest_row(OPEN_TIME + 120_000)
            ])),
        )
        .await;
    let (listener, ws_uri) = websocket_listener().await;
    let (mutation_ready_tx, mutation_ready_rx) = oneshot::channel();
    let socket_open_time = OPEN_TIME + 120_000;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        websocket
            .send(Message::Text(
                ws_kline(socket_open_time, false, "37027.50").into(),
            ))
            .await
            .expect("reconciliation candle");
        mutation_ready_tx.send(()).ok();
        futures_util::future::pending::<()>().await;
    });

    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest_uri, &ws_uri, manual, LiveSupervisorConfig::default());
    let (request, watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
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
    rest_started_rx.await.expect("REST request started");
    mutation_ready_rx.await.expect("WS mutation queued");
    watermark_tx
        .publish(Some(OPEN_TIME + 60_000))
        .expect("newer accepted watermark");
    rest_release_tx.send(()).expect("REST page becomes ready");

    let (generation, revision, target, candles) =
        timeout(Duration::from_secs(1), next_batch(&mut feed))
            .await
            .expect("ready REST page must complete after higher-priority inputs drain");
    assert_eq!(generation, GapGeneration(1));
    assert_eq!(revision, ReplayRevision(2));
    assert_eq!(target, socket_open_time);
    assert!(
        candles
            .iter()
            .any(|candle| candle.open_time() == socket_open_time),
        "the simultaneously ready socket mutation must be coalesced before the batch"
    );

    feed.request_shutdown();
    server.abort();
    rest_server.abort();
}

#[tokio::test]
async fn ready_rest_page_and_watermark_do_not_overtake_ready_ws_close() {
    let (rest_uri, rest_started_rx, rest_release_tx, rest_server) =
        held_rest_listener(json!([rest_row(OPEN_TIME)])).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (close_release_tx, close_release_rx) = oneshot::channel();
    let (close_ready_tx, close_ready_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        close_release_rx
            .await
            .expect("release peer close after REST starts");
        websocket
            .send(Message::Close(None))
            .await
            .expect("peer close");
        close_ready_tx.send(()).ok();
        futures_util::future::pending::<()>().await;
    });

    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest_uri, &ws_uri, manual, LiveSupervisorConfig::default());
    let (request, watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
    let mut feed = provider.open_live(request).await.expect("feed");
    timeout(Duration::from_secs(2), rest_started_rx)
        .await
        .expect("REST request start timeout")
        .expect("REST request started");
    close_release_tx
        .send(())
        .expect("release peer close after REST starts");
    timeout(Duration::from_secs(2), close_ready_rx)
        .await
        .expect("WS close send timeout")
        .expect("WS close queued");
    watermark_tx
        .publish(Some(OPEN_TIME + 60_000))
        .expect("newer accepted watermark");
    rest_release_tx.send(()).expect("REST page becomes ready");

    let event = timeout(
        Duration::from_secs(1),
        next_after_optional_startup_statuses(&mut feed),
    )
    .await
    .expect("ready socket terminal must not be starved by REST/watermark readiness");
    assert!(matches!(
        event,
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::Protocol { .. },
            rate_gate_deadline: None,
        }
    ));
    while let Ok(Some(event)) = timeout(Duration::from_millis(50), feed.events.next()).await {
        match event.expect("event-stream item") {
            MarketEvent::ReconcileBatch { .. } => {
                panic!("ready peer close must terminate reconciliation before batch emission")
            }
            MarketEvent::Status {
                status: ConnectionStatus::Backoff,
                ..
            } => break,
            _ => {}
        }
    }

    feed.request_shutdown();
    server.abort();
    rest_server.abort();
    timeout(Duration::from_secs(1), server)
        .await
        .expect("WS server abort join timeout")
        .expect_err("WS server task must be cancelled");
    timeout(Duration::from_secs(1), rest_server)
        .await
        .expect("REST server abort join timeout")
        .expect_err("REST server task must be cancelled");
}

#[tokio::test]
async fn shutdown_and_cancellation_dominate_a_simultaneously_ready_rest_page() {
    #[derive(Clone, Copy)]
    enum DominantSignal {
        ServerShutdown,
        Cancellation,
    }

    for signal in [DominantSignal::ServerShutdown, DominantSignal::Cancellation] {
        let (rest_uri, rest_started_rx, rest_release_tx, rest_server) =
            held_rest_listener(json!([rest_row(OPEN_TIME)])).await;
        let (listener, ws_uri) = websocket_listener().await;
        let (signal_release_tx, signal_release_rx) = oneshot::channel();
        let (signal_ready_tx, signal_ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                .await
                .expect("first kline");
            signal_release_rx
                .await
                .expect("release dominant signal after REST starts");
            if matches!(signal, DominantSignal::ServerShutdown) {
                websocket
                    .send(Message::Text(
                        json!({"e":"serverShutdown","s":"BTCUSDT"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .expect("server shutdown");
            }
            signal_ready_tx.send(()).ok();
            futures_util::future::pending::<()>().await;
        });

        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let provider = provider(&rest_uri, &ws_uri, manual, LiveSupervisorConfig::default());
        let (request, _watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
        let cancellation = request.cancellation.clone();
        let mut feed = provider.open_live(request).await.expect("feed");
        timeout(Duration::from_secs(2), rest_started_rx)
            .await
            .expect("REST request start timeout")
            .expect("REST request started");
        signal_release_tx
            .send(())
            .expect("release dominant signal after REST starts");
        timeout(Duration::from_secs(2), signal_ready_rx)
            .await
            .expect("dominant signal readiness timeout")
            .expect("dominant signal ready");
        if matches!(signal, DominantSignal::Cancellation) {
            cancellation.cancel();
        }
        rest_release_tx.send(()).expect("REST becomes ready");

        let event = next_after_optional_startup_statuses(&mut feed).await;
        match signal {
            DominantSignal::ServerShutdown => assert!(matches!(
                event,
                MarketEvent::RecoverableError {
                    generation: Some(GapGeneration(1)),
                    error: ProviderError::Protocol { .. },
                    rate_gate_deadline: None,
                }
            )),
            DominantSignal::Cancellation => assert_status(event, None, ConnectionStatus::Stopped),
        }
        loop {
            match timeout(Duration::from_millis(50), feed.events.next()).await {
                Ok(Some(Ok(MarketEvent::ReconcileBatch { .. }))) => {
                    panic!("dominant shutdown/cancellation must suppress ready REST batch")
                }
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(error))) => panic!("unexpected event-stream error: {error:?}"),
                Ok(None) | Err(_) => break,
            }
        }

        feed.request_shutdown();
        server.abort();
        rest_server.abort();
        timeout(Duration::from_secs(1), server)
            .await
            .expect("WS server abort join timeout")
            .expect_err("WS server task must be cancelled");
        timeout(Duration::from_secs(1), rest_server)
            .await
            .expect("REST server abort join timeout")
            .expect_err("REST server task must be cancelled");
    }
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
async fn gap_sync_generic_400_is_terminal_once_without_backoff_or_retry() {
    let rest = rest_server(ResponseTemplate::new(400).set_body_json(json!({
        "code": -1100,
        "msg": "malformed request secret=must-not-leak"
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
    let clock = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(
        &rest.uri(),
        &ws_uri,
        Arc::<ManualClock>::clone(&clock),
        LiveSupervisorConfig::default(),
    );
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
    let terminal = next_event(&mut feed).await;
    match &terminal {
        MarketEvent::TerminalError(ProviderError::ClientStatus {
            status: 400,
            message,
            ..
        }) => {
            let message = message.as_ref().expect("sanitized client message");
            assert_eq!(message.as_str(), "provider message redacted");
        }
        other => panic!("expected terminal generic 400, got {other:?}"),
    }
    assert!(!format!("{terminal:?}").contains("must-not-leak"));

    let completion = feed
        .producer_completion
        .changed()
        .await
        .expect("producer completion");
    assert!(matches!(
        completion,
        ProducerCompletion::Finished(Err(ProviderError::ClientStatus { status: 400, .. }))
    ));
    clock
        .advance_by(Duration::from_secs(60))
        .expect("advance clock");
    assert!(
        timeout(Duration::from_millis(50), feed.events.next())
            .await
            .expect("terminal stream closes without retry")
            .is_none()
    );
    server.abort();
}
#[tokio::test]
async fn confirmed_empty_and_short_pages_emit_exact_no_progress_before_one_backoff() {
    for (body, first_open, expected_last) in [
        (json!([]), OPEN_TIME, None),
        (
            json!([rest_row(OPEN_TIME)]),
            OPEN_TIME + 60_000,
            Some(OPEN_TIME),
        ),
    ] {
        let rest = rest_server(ResponseTemplate::new(200).set_body_json(body)).await;
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(
                    ws_kline(first_open, false, "37025.50").into(),
                ))
                .await
                .expect("first kline");
            let _ = websocket.next().await;
        });
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::ZERO));
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
                generation: Some(GapGeneration(1)),
                error: ProviderError::GapSyncNoProgress {
                    target_open_time: first_open,
                    last_open_time: expected_last,
                },
                rate_gate_deadline: None,
            }
        );
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Backoff,
        );
        feed.request_shutdown();
        server.abort();
    }
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

#[tokio::test]
async fn empty_history_uses_first_ws_candle_as_the_acknowledgeable_start() {
    let rest = rest_server(ResponseTemplate::new(200).set_body_json(json!([]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first kline");
        let _ = websocket.next().await;
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
    let (request, _watermark_tx, ack_tx) = request(None);
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
    let (generation, revision, target, candles) = next_batch(&mut feed).await;
    assert_eq!(target, OPEN_TIME);
    assert_eq!(candles.len(), 1);
    assert_eq!(candles[0].open_time(), OPEN_TIME);
    assert!(!candles[0].is_closed());
    acknowledge(&ack_tx, generation, revision, target);
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn target_growth_during_rest_requests_suffix_and_ws_finality_wins_same_key() {
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("startTime", OPEN_TIME.to_string()))
        .and(query_param("endTime", OPEN_TIME.to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
        .expect(1)
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(query_param("startTime", (OPEN_TIME + 1).to_string()))
        .and(query_param("endTime", (OPEN_TIME + 60_000).to_string()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME + 60_000)])),
        )
        .expect(1)
        .mount(&rest)
        .await;
    let (send_closed, receive_closed) = oneshot::channel();
    let (send_growth, receive_growth) = oneshot::channel();
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first open");
        receive_closed.await.expect("release same-key mutation");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, true, "37111.00").into()))
            .await
            .expect("same-key closed");
        receive_growth.await.expect("release target growth");
        websocket
            .send(Message::Text(
                ws_kline(OPEN_TIME + 60_000, false, "37125.00").into(),
            ))
            .await
            .expect("target growth");
        let _ = websocket.next().await;
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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

    let (generation, first_revision, first_target, first_batch) = next_batch(&mut feed).await;
    assert_eq!(first_revision.0, 1);
    assert_eq!(first_target, OPEN_TIME);
    assert_eq!(first_batch.len(), 1);
    assert!(!first_batch[0].is_closed());

    send_closed.send(()).expect("release same-key mutation");
    let (same_generation, second_revision, second_target, second_batch) =
        next_batch(&mut feed).await;
    assert_eq!(same_generation, generation);
    assert_eq!(second_revision.0, first_revision.0 + 1);
    assert_eq!(second_target, first_target);
    assert_eq!(second_batch.len(), 1);
    assert!(second_batch[0].is_closed());
    assert_eq!(second_batch[0].close(), 37_111.0);

    assert_eq!(
        ack_tx.publish(ReconcileAck {
            generation,
            revision: first_revision,
            through: first_target,
        }),
        Err(ReconcileAckPublishError::Stale),
        "an acknowledgement for the superseded batch cannot connect the generation"
    );

    send_growth.send(()).expect("release target growth");
    let (same_generation, third_revision, third_target, third_batch) = next_batch(&mut feed).await;
    assert_eq!(same_generation, generation);
    assert_eq!(third_revision.0, second_revision.0 + 1);
    assert_eq!(third_target, OPEN_TIME + 60_000);
    assert_eq!(
        third_batch
            .iter()
            .map(fccli::model::Candle::open_time)
            .collect::<Vec<_>>(),
        vec![OPEN_TIME, OPEN_TIME + 60_000]
    );
    assert!(third_batch[0].is_closed());
    assert_eq!(third_batch[0].close(), 37_111.0);
    acknowledge(&ack_tx, generation, third_revision, third_target);
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn cancellation_dominates_a_stalled_websocket_upgrade() {
    let rest = rest_server(ResponseTemplate::new(200).set_body_json(json!([]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        accepted_tx.send(()).ok();
        let mut request = [0_u8; 1024];
        let _ = stream.read(&mut request).await;
        futures_util::future::pending::<()>().await;
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
    let (request, _watermark_tx, _ack_tx) = request(None);
    let cancellation = request.cancellation.clone();
    let mut feed = provider.open_live(request).await.expect("feed");
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connecting,
    );
    accepted_rx.await.expect("TCP accepted");
    cancellation.cancel();
    let completion = timeout(Duration::from_secs(1), feed.producer_completion.changed())
        .await
        .expect("producer stops without handshake timeout")
        .expect("completion channel");
    assert_eq!(completion, ProducerCompletion::Finished(Ok(())));
    if let Ok(Some(Ok(event))) = timeout(Duration::from_millis(50), feed.events.next()).await {
        assert_status(event, None, ConnectionStatus::Stopped);
    }
    server.abort();
}

#[tokio::test]
async fn ignored_frames_cannot_starve_first_kline_deadline() {
    let rest = rest_server(ResponseTemplate::new(200).set_body_json(json!([]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        loop {
            if websocket
                .send(Message::Text(json!({"e":"bookTicker"}).to_string().into()))
                .await
                .is_err()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let live = LiveSupervisorConfig {
        first_kline_timeout: Duration::from_secs(1),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&rest.uri(), &ws_uri, manual.clone(), live);
    let (request, _watermark_tx, _ack_tx) = request(None);
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
    tokio::task::yield_now().await;
    manual.advance_by(Duration::from_secs(1)).expect("deadline");
    match next_event(&mut feed).await {
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::Timeout { .. },
            rate_gate_deadline: None,
        } => {}
        other => panic!("expected first-kline timeout, got {other:?}"),
    }
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn ignored_frames_cannot_starve_ready_ack() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("kline");
        loop {
            if websocket
                .send(Message::Text(json!({"e":"bookTicker"}).to_string().into()))
                .await
                .is_err()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn closed_ack_channel_is_terminal_once_without_reconnect() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
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
    let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
    drop(ack_tx);
    let mut feed = provider.open_live(request).await.expect("feed");
    let first = next_event(&mut feed).await;
    let terminal = match first {
        MarketEvent::Status {
            generation: Some(GapGeneration(1)),
            status: ConnectionStatus::Connecting,
        } => next_event(&mut feed).await,
        other => other,
    };
    assert!(matches!(
        terminal,
        MarketEvent::TerminalError(ProviderError::ChannelClosed { .. })
    ));
    assert!(matches!(
        feed.producer_completion.changed().await,
        Ok(ProducerCompletion::Finished(Err(
            ProviderError::ChannelClosed { .. }
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
async fn connection_max_age_is_not_starved_by_continuously_ready_candle_frames() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (flood_tx, flood_rx) = oneshot::channel();
    let (frames_ready_tx, frames_ready_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("initial kline");
        flood_rx.await.expect("flood release");
        for sequence in 0..256_u32 {
            websocket
                .send(Message::Text(
                    ws_kline(OPEN_TIME, false, &format!("37025.{:02}", sequence % 100)).into(),
                ))
                .await
                .expect("continuously ready same-key candle");
        }
        frames_ready_tx.send(()).ok();
        let _ = websocket.next().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let live = LiveSupervisorConfig {
        max_connection_age: Duration::from_secs(5),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&rest.uri(), &ws_uri, manual.clone(), live);
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
    flood_tx.send(()).expect("start continuously ready frames");
    frames_ready_rx.await.expect("all frames queued at peer");
    manual
        .advance_by(Duration::from_secs(5))
        .expect("exact max-age boundary");
    let recovery = timeout(Duration::from_secs(1), async {
        loop {
            match next_event(&mut feed).await {
                MarketEvent::Candle { .. } => {}
                event => break event,
            }
        }
    })
    .await
    .expect("continuously ready candles must not starve max-age recovery");
    match recovery {
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::Protocol { detail, .. },
            rate_gate_deadline: None,
        } => assert!(detail.contains("connection age")),
        other => panic!("expected max-age recovery, got {other:?}"),
    }
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn first_ws_before_equal_and_after_confirmed_watermark_sets_exact_bounds() {
    for (first_open, expected_target, expected_rows) in [
        (OPEN_TIME - 60_000, OPEN_TIME, vec![rest_row(OPEN_TIME)]),
        (OPEN_TIME, OPEN_TIME, vec![rest_row(OPEN_TIME)]),
        (
            OPEN_TIME + 60_000,
            OPEN_TIME + 60_000,
            vec![rest_row(OPEN_TIME), rest_row(OPEN_TIME + 60_000)],
        ),
    ] {
        let rest = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param("startTime", OPEN_TIME.to_string()))
            .and(query_param("endTime", expected_target.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!(expected_rows)))
            .expect(1)
            .mount(&rest)
            .await;
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(
                    ws_kline(first_open, false, "37025.50").into(),
                ))
                .await
                .expect("first kline");
            let _ = websocket.next().await;
        });
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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
        let (generation, revision, target, candles) = next_batch(&mut feed).await;
        assert_eq!(target, expected_target);
        assert!(
            candles
                .windows(2)
                .all(|pair| pair[0].open_time() < pair[1].open_time())
        );
        assert!(candles.iter().all(|candle| candle.open_time() >= OPEN_TIME));
        assert_eq!(
            candles.last().map(fccli::model::Candle::open_time),
            Some(expected_target)
        );
        acknowledge(&ack_tx, generation, revision, target);
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Connected,
        );
        feed.request_shutdown();
        server.abort();
    }
}

#[tokio::test]
async fn full_thousand_row_page_continues_at_last_open_plus_one_millisecond() {
    let target = OPEN_TIME + 1_000 * 60_000;
    let first_page: Vec<_> = (0..1_000)
        .map(|index| rest_row(OPEN_TIME + index * 60_000))
        .collect();
    let last_first_page = OPEN_TIME + 999 * 60_000;
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("startTime", OPEN_TIME.to_string()))
        .and(query_param("endTime", target.to_string()))
        .and(query_param("limit", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(first_page)))
        .expect(1)
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(query_param("startTime", (last_first_page + 1).to_string()))
        .and(query_param("endTime", target.to_string()))
        .and(query_param("limit", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([rest_row(target)])))
        .expect(1)
        .mount(&rest)
        .await;

    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(target, true, "37123.45").into()))
            .await
            .expect("target kline");
        let _ = websocket.next().await;
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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
    let (generation, revision, batch_target, candles) = next_batch(&mut feed).await;
    assert_eq!(batch_target, target);
    assert_eq!(candles.len(), 1_001);
    assert_eq!(
        candles.first().map(fccli::model::Candle::open_time),
        Some(OPEN_TIME)
    );
    assert_eq!(
        candles.last().map(fccli::model::Candle::open_time),
        Some(target)
    );
    assert!(
        candles
            .windows(2)
            .all(|pair| pair[0].open_time() < pair[1].open_time())
    );
    assert!(candles.last().expect("target candle").is_closed());
    acknowledge(&ack_tx, generation, revision, batch_target);
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn events_ready_at_first_kline_and_ack_deadlines_win_exactly() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (send_first_tx, send_first_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        send_first_rx.await.expect("release first kline");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first candle at deadline");
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual.clone();
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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

    send_first_tx.send(()).expect("release first kline");
    tokio::task::yield_now().await;
    manual
        .advance_by(FIRST_KLINE_HANDSHAKE_TIMEOUT)
        .expect("first-kline deadline");
    let (generation, revision, target, _) = next_batch(&mut feed).await;
    assert_eq!(generation, GapGeneration(1));

    acknowledge(&ack_tx, generation, revision, target);
    tokio::task::yield_now().await;
    manual
        .advance_by(RECONCILE_ACK_TIMEOUT)
        .expect("ack deadline");
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn cancellation_is_dominant_in_first_kline_ack_connected_and_backoff_states() {
    #[derive(Clone, Copy, Debug)]
    enum State {
        FirstKline,
        Ack,
        Connected,
        Backoff,
    }

    for state in [
        State::FirstKline,
        State::Ack,
        State::Connected,
        State::Backoff,
    ] {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            if !matches!(state, State::FirstKline) {
                websocket
                    .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                    .await
                    .expect("first candle");
            }
            if matches!(state, State::Backoff) {
                tokio::time::sleep(Duration::from_millis(25)).await;
                websocket.close(None).await.expect("peer close");
            } else {
                futures_util::future::pending::<()>().await;
            }
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual;
        let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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

        if !matches!(state, State::FirstKline) {
            let (generation, revision, target, _) = next_batch(&mut feed).await;
            if matches!(state, State::Connected | State::Backoff) {
                acknowledge(&ack_tx, generation, revision, target);
                assert_status(
                    next_event(&mut feed).await,
                    Some(1),
                    ConnectionStatus::Connected,
                );
            }
            if matches!(state, State::Backoff) {
                assert!(matches!(
                    next_event(&mut feed).await,
                    MarketEvent::RecoverableError {
                        generation: Some(GapGeneration(1)),
                        ..
                    }
                ));
                assert_status(
                    next_event(&mut feed).await,
                    Some(1),
                    ConnectionStatus::Backoff,
                );
            }
        }

        cancellation.cancel();
        assert_status(next_event(&mut feed).await, None, ConnectionStatus::Stopped);
        assert!(matches!(
            timeout(Duration::from_secs(1), feed.producer_completion.changed())
                .await
                .expect("completion timeout"),
            Ok(ProducerCompletion::Finished(Ok(())))
        ));
        assert!(
            timeout(Duration::from_millis(25), feed.events.next())
                .await
                .is_ok_and(|event| event.is_none())
        );
        server.abort();
    }
}

#[tokio::test]
async fn cancellation_dominates_a_held_rest_page() {
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
        .mount(&rest)
        .await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first candle");
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual;
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
    let (request, _watermark_tx, _ack_tx) = request(Some(OPEN_TIME));
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
    tokio::time::sleep(Duration::from_millis(25)).await;
    cancellation.cancel();
    assert_status(next_event(&mut feed).await, None, ConnectionStatus::Stopped);
    assert!(matches!(
        timeout(Duration::from_secs(1), feed.producer_completion.changed())
            .await
            .expect("completion timeout"),
        Ok(ProducerCompletion::Finished(Ok(())))
    ));
    server.abort();
}

#[tokio::test]
async fn peer_close_recovers_from_first_kline_ack_and_connected_without_status_substitution() {
    #[derive(Clone, Copy, Debug)]
    enum Phase {
        FirstKline,
        Ack,
        Connected,
    }

    for phase in [Phase::FirstKline, Phase::Ack, Phase::Connected] {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let (close_tx, close_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            if !matches!(phase, Phase::FirstKline) {
                websocket
                    .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                    .await
                    .expect("first candle");
                close_rx.await.expect("release peer close");
            }
            websocket.close(None).await.expect("peer close");
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual;
        let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
        let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
        let mut feed = provider.open_live(request).await.expect("feed");
        let mut close_tx = Some(close_tx);
        if !matches!(phase, Phase::FirstKline) {
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
        }
        if !matches!(phase, Phase::FirstKline) {
            let (generation, revision, target, _) = next_batch(&mut feed).await;
            match phase {
                Phase::Ack => close_tx
                    .take()
                    .expect("close sender")
                    .send(())
                    .expect("release peer close"),
                Phase::Connected => {
                    acknowledge(&ack_tx, generation, revision, target);
                    assert_status(
                        next_event(&mut feed).await,
                        Some(1),
                        ConnectionStatus::Connected,
                    );
                    close_tx
                        .take()
                        .expect("close sender")
                        .send(())
                        .expect("release peer close");
                }
                Phase::FirstKline => unreachable!(),
            }
        }
        let fault = if matches!(phase, Phase::FirstKline) {
            next_after_optional_startup_statuses(&mut feed).await
        } else {
            next_event(&mut feed).await
        };
        assert!(matches!(
            fault,
            MarketEvent::RecoverableError {
                generation: Some(GapGeneration(1)),
                error: ProviderError::Protocol { .. } | ProviderError::Transport { .. },
                rate_gate_deadline: None,
            }
        ));
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Backoff,
        );
        feed.request_shutdown();
        server.abort();
    }
}

#[tokio::test]
async fn repeated_full_duplicate_page_stops_once_without_same_generation_spin() {
    let repeated = vec![rest_row(OPEN_TIME); 1_000];
    let rest = rest_server(ResponseTemplate::new(200).set_body_json(repeated)).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(
                ws_kline(OPEN_TIME + 60_000, false, "37025.50").into(),
            ))
            .await
            .expect("target candle");
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual;
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
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::GapSyncNoProgress {
                target_open_time,
                last_open_time: Some(last_open_time),
            },
            rate_gate_deadline: None,
        } if target_open_time == OPEN_TIME + 60_000 && last_open_time == OPEN_TIME
    ));
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    let requests = rest.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 2, "duplicate page must not spin");
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn accepted_watermark_closure_is_terminal_in_band_before_stream_closure() {
    for connected in [false, true] {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            if connected {
                websocket
                    .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                    .await
                    .expect("first candle");
            }
            futures_util::future::pending::<()>().await;
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual;
        let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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
        if connected {
            let (generation, revision, target, _) = next_batch(&mut feed).await;
            acknowledge(&ack_tx, generation, revision, target);
            assert_status(
                next_event(&mut feed).await,
                Some(1),
                ConnectionStatus::Connected,
            );
        }
        drop(watermark_tx);
        let terminal = next_event(&mut feed).await;
        assert!(matches!(
            terminal,
            MarketEvent::TerminalError(ProviderError::ChannelClosed { .. })
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), feed.producer_completion.changed())
                .await
                .expect("completion timeout"),
            Ok(ProducerCompletion::Finished(Err(
                ProviderError::ChannelClosed { .. }
            )))
        ));
        assert!(feed.events.next().await.is_none());
        server.abort();
    }
}

#[tokio::test]
async fn stale_and_insufficient_acknowledgements_never_unlock_connected() {
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("startTime", OPEN_TIME.to_string()))
        .and(query_param("endTime", OPEN_TIME.to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
        .expect(1)
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(query_param("startTime", (OPEN_TIME + 1).to_string()))
        .and(query_param("endTime", (OPEN_TIME + 60_000).to_string()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME + 60_000)])),
        )
        .expect(1)
        .mount(&rest)
        .await;
    let (listener, ws_uri) = websocket_listener().await;
    let (release_second_tx, release_second_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first candle");
        release_second_rx.await.expect("release revision candle");
        websocket
            .send(Message::Text(
                ws_kline(OPEN_TIME + 60_000, false, "37030.00").into(),
            ))
            .await
            .expect("revision candle");
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual;
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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
    let (generation, first_revision, first_target, first_batch) = next_batch(&mut feed).await;
    assert_eq!(first_target, OPEN_TIME);
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0].open_time(), OPEN_TIME);
    release_second_tx.send(()).expect("release revision candle");
    let (latest_generation, latest_revision, latest_target, latest_batch) =
        next_batch(&mut feed).await;
    assert_eq!(generation, latest_generation);
    assert!(latest_revision > first_revision);
    assert_eq!(latest_target, OPEN_TIME + 60_000);
    assert_eq!(
        latest_batch
            .iter()
            .map(fccli::model::Candle::open_time)
            .collect::<Vec<_>>(),
        vec![OPEN_TIME, OPEN_TIME + 60_000]
    );

    assert_eq!(
        ack_tx.publish(ReconcileAck {
            generation,
            revision: first_revision,
            through: first_target,
        }),
        Err(ReconcileAckPublishError::Stale)
    );
    assert_eq!(
        ack_tx.publish(ReconcileAck {
            generation,
            revision: latest_revision,
            through: latest_target - 1,
        }),
        Err(ReconcileAckPublishError::ThroughBeforeTarget)
    );
    assert!(
        timeout(Duration::from_millis(25), feed.events.next())
            .await
            .is_err()
    );
    acknowledge(&ack_tx, generation, latest_revision, latest_target);
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn malformed_decoded_payload_recovers_in_first_kline_and_connected_phases() {
    for connected in [false, true] {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            if connected {
                websocket
                    .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                    .await
                    .expect("first candle");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            websocket
                .send(Message::Text("{".into()))
                .await
                .expect("malformed decoded payload");
            futures_util::future::pending::<()>().await;
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual;
        let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
        let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
        let mut feed = provider.open_live(request).await.expect("feed");
        if connected {
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
        }
        if connected {
            let (generation, revision, target, _) = next_batch(&mut feed).await;
            acknowledge(&ack_tx, generation, revision, target);
            assert_status(
                next_event(&mut feed).await,
                Some(1),
                ConnectionStatus::Connected,
            );
        }
        let fault = if connected {
            next_event(&mut feed).await
        } else {
            next_after_optional_startup_statuses(&mut feed).await
        };
        assert!(matches!(
            fault,
            MarketEvent::RecoverableError {
                generation: Some(GapGeneration(1)),
                error: ProviderError::Payload { .. },
                rate_gate_deadline: None,
            }
        ));
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Backoff,
        );
        feed.request_shutdown();
        server.abort();
    }
}

#[tokio::test]
async fn decoded_invalid_symbol_is_terminal_in_first_kline_and_connected_phases() {
    for connected in [false, true] {
        let rest =
            rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)])))
                .await;
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            if connected {
                websocket
                    .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
                    .await
                    .expect("first candle");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            websocket
                .send(Message::Text(
                    r#"{"code":-1121,"msg":"Invalid symbol"}"#.into(),
                ))
                .await
                .expect("decoded invalid symbol");
            futures_util::future::pending::<()>().await;
        });
        let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
        let clock: Arc<dyn Clock> = manual.clone();
        let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
        let (request, _watermark_tx, ack_tx) = request(Some(OPEN_TIME));
        let mut feed = provider.open_live(request).await.expect("feed");
        if connected {
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
        }
        if connected {
            let (generation, revision, target, _) = next_batch(&mut feed).await;
            acknowledge(&ack_tx, generation, revision, target);
            assert_status(
                next_event(&mut feed).await,
                Some(1),
                ConnectionStatus::Connected,
            );
        }
        let terminal = if connected {
            next_event(&mut feed).await
        } else {
            next_after_optional_startup_statuses(&mut feed).await
        };
        assert!(matches!(
            terminal,
            MarketEvent::TerminalError(ProviderError::InvalidSymbol { code: -1121, .. })
        ));
        assert!(matches!(
            feed.producer_completion.changed().await,
            Ok(ProducerCompletion::Finished(Err(
                ProviderError::InvalidSymbol { code: -1121, .. }
            )))
        ));
        manual
            .advance_by(Duration::from_secs(60))
            .expect("advance beyond every reconnect deadline");
        assert!(
            timeout(Duration::from_millis(50), feed.events.next())
                .await
                .expect("terminal stream closes without backoff or retry")
                .is_none()
        );
        server.abort();
    }
}

#[tokio::test]
async fn preconnected_max_age_and_backoff_deadlines_fire_at_exact_equality() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let _websocket = accept_async(stream).await.expect("upgrade");
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual.clone();
    let live = LiveSupervisorConfig {
        max_connection_age: Duration::from_secs(5),
        ..LiveSupervisorConfig::default()
    };
    let provider = provider(&rest.uri(), &ws_uri, clock, live);
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
    manual
        .advance_by(Duration::from_secs(5))
        .expect("max-age equality");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::Protocol { .. },
            rate_gate_deadline: None,
        }
    ));
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    assert!(
        timeout(Duration::from_millis(25), feed.events.next())
            .await
            .is_err()
    );
    manual
        .advance_by(Duration::from_secs(1))
        .expect("backoff equality");
    assert_status(
        next_event(&mut feed).await,
        Some(2),
        ConnectionStatus::Connecting,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn rate_gate_deadline_dominates_backoff_and_wakes_at_exact_equality() {
    let rest = rest_server(
        ResponseTemplate::new(429)
            .insert_header("Retry-After", "5")
            .set_body_json(json!({"code": -1003, "msg": "limited"})),
    )
    .await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first candle");
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual.clone();
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
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::RateLimited { status: 429, .. },
            rate_gate_deadline: Some(deadline),
        } if deadline == MonoInstant::from_nanos(5_000_000_000)
    ));
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    manual
        .advance_by(Duration::from_secs(4))
        .expect("before gate deadline");
    assert!(
        timeout(Duration::from_millis(25), feed.events.next())
            .await
            .is_err()
    );
    manual
        .advance_by(Duration::from_secs(1))
        .expect("gate equality");
    assert_status(
        next_event(&mut feed).await,
        Some(2),
        ConnectionStatus::Connecting,
    );
    feed.request_shutdown();
    server.abort();
}

#[test]
fn supervisor_classifies_all_remaining_decoded_outcomes_and_errors() {
    use LiveCompletionDisposition::{FinishedErr, Running};
    use LiveErrorDisposition::{Recoverable, Terminal};
    use LiveInBandEventDisposition::{RecoverableInBand, TerminalInBand};

    let market = instrument();
    let context = || {
        ErrorContext::operation(ErrorOperation::WebSocket).with_market(&market, Timeframe::Minute1)
    };
    let cases = vec![
        (
            "provider recoverable",
            ProviderError::ServerStatus {
                context: context(),
                status: 503,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            "provider terminal",
            ProviderError::InvalidSymbol {
                context: context(),
                code: -1121,
                message: SanitizedMessage::InvalidSymbol,
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            "protocol",
            ProviderError::Protocol {
                context: context(),
                detail: "invalid WebSocket framing",
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            "payload",
            ProviderError::Payload {
                context: context(),
                source: PayloadError::MalformedProtocol,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            "transport",
            ProviderError::Transport {
                context: context(),
                cause: SanitizedCause::Io,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            "timeout",
            ProviderError::Timeout {
                context: context(),
                kind: TimeoutKind::StalledWrite,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
        (
            "client",
            ProviderError::ClientStatus {
                context: context(),
                status: 403,
                code: None,
                message: None,
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            "configuration",
            ProviderError::Configuration("invalid live configuration"),
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            "websocket configuration",
            ProviderError::WebSocketConfiguration {
                context: context(),
                detail: "invalid WebSocket configuration",
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            "invariant",
            ProviderError::Invariant("live invariant"),
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            "channel",
            ProviderError::ChannelClosed {
                context: ErrorContext::operation(ErrorOperation::Channel)
                    .with_market(&market, Timeframe::Minute1),
            },
            Terminal,
            TerminalInBand,
            FinishedErr,
            false,
        ),
        (
            "domain",
            ProviderError::Domain {
                context: context(),
                source: ModelError::InvalidRange,
            },
            Recoverable,
            RecoverableInBand,
            Running,
            true,
        ),
    ];

    for (name, error, disposition, event, completion, retries) in cases {
        let actual = classify_live_error_for_test(&error);
        assert_eq!(actual.disposition, disposition, "{name} disposition");
        assert_eq!(actual.event, event, "{name} in-band event");
        assert_eq!(actual.completion, completion, "{name} completion");
        assert_eq!(actual.retries, retries, "{name} retry policy");
    }

    for (name, input) in [
        ("close", Ok(DecodedFrame::Close(None))),
        ("serverShutdown", Ok(DecodedFrame::ReconnectRequested)),
    ] {
        let LiveInputClassification::Error { error, policy } =
            classify_live_input_for_test(input, &market, Timeframe::Minute1)
        else {
            panic!("{name} must request reconnect");
        };
        assert!(
            matches!(
                error,
                ProviderError::Protocol {
                    detail: "WebSocket peer requested reconnect",
                    ..
                }
            ),
            "{name}"
        );
        assert_eq!(policy.disposition, Recoverable, "{name} disposition");
        assert_eq!(policy.event, RecoverableInBand, "{name} event");
        assert_eq!(policy.completion, Running, "{name} completion");
        assert!(policy.retries, "{name} retry");
    }
}
#[tokio::test]
async fn month1_multi_page_target_revision_race_uses_plus_one_cursor_and_latest_ack() {
    fn month_open(index: usize) -> i64 {
        let absolute_month = 2000_i32 * 12 + 7 + i32::try_from(index).expect("month offset");
        let year = absolute_month.div_euclid(12);
        let month =
            time::Month::try_from(u8::try_from(absolute_month.rem_euclid(12) + 1).expect("month"))
                .expect("valid month");
        time::Date::from_calendar_date(year, month, 1)
            .expect("valid date")
            .midnight()
            .assume_utc()
            .unix_timestamp()
            * 1_000
    }

    let opens: Vec<_> = (0..=1_001).map(month_open).collect();
    let start = opens[0];
    let first_target = opens[1_000];
    let latest_target = opens[1_001];
    assert_ne!(
        time::OffsetDateTime::from_unix_timestamp(first_target / 1_000)
            .expect("first target")
            .year(),
        time::OffsetDateTime::from_unix_timestamp(latest_target / 1_000)
            .expect("latest target")
            .year()
    );

    let first_page: Vec<_> = opens[..1_000].iter().copied().map(rest_row).collect();
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("startTime", start.to_string()))
        .and(query_param("endTime", first_target.to_string()))
        .and(query_param("limit", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(first_page)))
        .expect(1)
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(query_param("startTime", (opens[999] + 1).to_string()))
        .and(query_param("endTime", first_target.to_string()))
        .and(query_param("limit", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([rest_row(first_target)])))
        .expect(1)
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(query_param("startTime", (first_target + 1).to_string()))
        .and(query_param("endTime", latest_target.to_string()))
        .and(query_param("limit", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([rest_row(latest_target)])))
        .expect(1)
        .mount(&rest)
        .await;

    let (listener, ws_uri) = websocket_listener().await;
    let (advance_tx, advance_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(
                ws_kline_for(first_target, false, "37025.50", "1M").into(),
            ))
            .await
            .expect("first monthly target");
        advance_rx.await.expect("advance target");
        websocket
            .send(Message::Text(
                ws_kline_for(latest_target, true, "37030.00", "1M").into(),
            ))
            .await
            .expect("latest monthly target");
        futures_util::future::pending::<()>().await;
    });
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
    let (request, _watermark_tx, ack_tx) = request_for(Some(start), Timeframe::Month1);
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
    let (generation, revision, target, candles) = next_batch(&mut feed).await;
    assert_eq!(target, first_target);
    assert_eq!(candles.len(), 1_001);
    advance_tx.send(()).expect("advance websocket target");
    let (latest_generation, latest_revision, target, candles) = next_batch(&mut feed).await;
    assert_eq!(latest_generation, generation);
    assert!(latest_revision > revision);
    assert_eq!(target, latest_target);
    assert_eq!(
        candles.last().map(fccli::model::Candle::open_time),
        Some(latest_target)
    );
    assert_eq!(
        ack_tx.publish(ReconcileAck {
            generation,
            revision,
            through: first_target,
        }),
        Err(ReconcileAckPublishError::Stale)
    );
    acknowledge(&ack_tx, generation, latest_revision, latest_target);
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn missing_middle_withholds_ack_until_timeout_purges_generation_and_retries_cleanly() {
    let middle = OPEN_TIME + 60_000;
    let target = OPEN_TIME + 120_000;
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("startTime", OPEN_TIME.to_string()))
        .and(query_param("endTime", target.to_string()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([rest_row(OPEN_TIME), rest_row(target)])),
        )
        .expect(1)
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(query_param("startTime", middle.to_string()))
        .and(query_param("endTime", target.to_string()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!([rest_row(middle), rest_row(target)])),
        )
        .expect(1)
        .mount(&rest)
        .await;
    let (listener, ws_uri) = websocket_listener().await;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept generation");
            tokio::spawn(async move {
                let mut websocket = accept_async(stream).await.expect("upgrade");
                websocket
                    .send(Message::Text(ws_kline(target, false, "37025.50").into()))
                    .await
                    .expect("target candle");
                futures_util::future::pending::<()>().await;
            });
        }
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual.clone();
    let provider = provider(&rest.uri(), &ws_uri, clock, LiveSupervisorConfig::default());
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
    let (generation, revision, batch_target, candles) = next_batch(&mut feed).await;
    assert_eq!(generation, GapGeneration(1));
    assert_eq!(batch_target, target);
    assert_eq!(
        candles
            .iter()
            .map(fccli::model::Candle::open_time)
            .collect::<Vec<_>>(),
        vec![OPEN_TIME, target]
    );
    assert_eq!(
        ack_tx.publish(ReconcileAck {
            generation,
            revision,
            through: middle
        }),
        Err(ReconcileAckPublishError::ThroughBeforeTarget)
    );
    manual
        .advance_by(RECONCILE_ACK_TIMEOUT - Duration::from_nanos(1))
        .expect("before timeout");
    assert!(
        timeout(Duration::from_millis(25), feed.events.next())
            .await
            .is_err()
    );
    manual
        .advance_by(Duration::from_nanos(1))
        .expect("timeout equality");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::ReconcileAckTimeout { generation: GapGeneration(1), target_open_time, .. },
            rate_gate_deadline: None,
        } if target_open_time == target
    ));
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    assert!(
        timeout(Duration::from_millis(25), feed.events.next())
            .await
            .is_err()
    );
    watermark_tx
        .publish(Some(middle))
        .expect("advance accepted watermark for retry");
    manual
        .advance_by(Duration::from_secs(1))
        .expect("first backoff");
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
    let (generation, revision, batch_target, candles) = next_batch(&mut feed).await;
    assert_eq!(generation, GapGeneration(2));
    assert_eq!(batch_target, target);
    assert_eq!(
        candles
            .iter()
            .map(fccli::model::Candle::open_time)
            .collect::<Vec<_>>(),
        vec![middle, target]
    );
    acknowledge(&ack_tx, generation, revision, batch_target);
    assert_status(
        next_event(&mut feed).await,
        Some(2),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}

#[tokio::test]
async fn stalled_write_recovers_reconnects_and_preserves_pong_continuity() {
    let rest =
        rest_server(ResponseTemplate::new(200).set_body_json(json!([rest_row(OPEN_TIME)]))).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (first_pong_tx, first_pong_rx) = oneshot::channel();
    let (second_pong_tx, second_pong_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first_stream, _) = listener.accept().await.expect("first generation accept");
        let mut first = accept_async(first_stream).await.expect("first upgrade");
        first
            .send(Message::Ping(b"first".to_vec().into()))
            .await
            .expect("first ping");
        while let Some(message) = first.next().await {
            match message.expect("first generation frame") {
                Message::Pong(payload) if payload.as_ref() == b"first" => {
                    first_pong_tx.send(()).ok();
                    break;
                }
                _ => {}
            }
        }
        let (second_stream, _) = listener.accept().await.expect("second generation accept");
        let mut second = accept_async(second_stream).await.expect("second upgrade");
        second
            .send(Message::Ping(b"second".to_vec().into()))
            .await
            .expect("second ping");
        let mut second_pong_tx = Some(second_pong_tx);
        second
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("second first candle");
        while let Some(message) = second.next().await {
            match message.expect("second generation frame") {
                Message::Pong(payload) if payload.as_ref() == b"second" => {
                    if let Some(sender) = second_pong_tx.take() {
                        sender.send(()).ok();
                    }
                }
                _ => {}
            }
        }
    });

    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let clock: Arc<dyn Clock> = manual.clone();
    let mut live = LiveSupervisorConfig {
        stalled_write_probe_frames: 256,
        ..LiveSupervisorConfig::default()
    };
    live.ws_config.stalled_write_timeout = Duration::from_millis(20);
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
    timeout(Duration::from_secs(2), first_pong_rx)
        .await
        .expect("first Pong deadline")
        .expect("first Pong observed");
    assert!(matches!(
        timeout(Duration::from_secs(2), next_event(&mut feed))
            .await
            .expect("stalled-write recovery deadline"),
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::Timeout {
                kind: TimeoutKind::StalledWrite,
                ..
            },
            rate_gate_deadline: None,
        }
    ));
    assert_status(
        next_event(&mut feed).await,
        Some(1),
        ConnectionStatus::Backoff,
    );
    manual
        .advance_by(Duration::from_secs(1))
        .expect("first reconnect backoff");
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
    timeout(Duration::from_secs(2), second_pong_rx)
        .await
        .expect("second Pong deadline")
        .expect("second Pong observed");
    let (generation, revision, target, _) = next_batch(&mut feed).await;
    assert_eq!(generation, GapGeneration(2));
    acknowledge(&ack_tx, generation, revision, target);
    assert_status(
        next_event(&mut feed).await,
        Some(2),
        ConnectionStatus::Connected,
    );
    feed.request_shutdown();
    server.abort();
}
