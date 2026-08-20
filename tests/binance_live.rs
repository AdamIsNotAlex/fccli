#![cfg(feature = "test-transport")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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
    provider::binance::{BinanceProvider, BinanceTestConfig, MAX_CONNECTION_AGE, decode_ws_frame},
    provider::{
        CONTROL_CAPACITY, CancellationToken, EMERGENCY_CONTROL_CAPACITY,
        FIRST_KLINE_HANDSHAKE_TIMEOUT, KEYED_CANDLE_CAPACITY, LiveRequest, LiveSupervisorConfig,
        MARKET_EVENT_CHANNEL_CAPACITY, MarketDataProvider, ProducerCompletion, ProviderRegistry,
        RECONCILE_ACK_TIMEOUT, ReconcileAck, ReconcileAckPublishError, accepted_watermark_channel,
        reconcile_ack_channel,
        test_transport::{DecodedFrame, WsConfig},
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
fn rest_month_row(open_time: i64, successor_open_time: i64) -> serde_json::Value {
    let mut row = rest_row(open_time);
    row[6] = json!(successor_open_time - 1);
    row
}

fn ws_kline(open_time: i64, closed: bool, close: &str) -> String {
    ws_kline_for(open_time, closed, close, "1m")
}

fn ws_kline_for(open_time: i64, closed: bool, close: &str, interval: &str) -> String {
    let close_time = if interval == "1M" {
        let date = time::OffsetDateTime::from_unix_timestamp(open_time / 1_000)
            .expect("monthly open timestamp")
            .date();
        let (year, month) = if date.month() == time::Month::December {
            (date.year() + 1, time::Month::January)
        } else {
            (date.year(), date.month().next())
        };
        time::Date::from_calendar_date(year, month, 1)
            .expect("next monthly open")
            .midnight()
            .assume_utc()
            .unix_timestamp()
            * 1_000
            - 1
    } else {
        open_time + 59_999
    };
    json!({
        "e": "kline",
        "E": close_time + 2,
        "s": "BTCUSDT",
        "k": {
            "t": open_time,
            "T": close_time,
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
    provider_with_max_age(rest_uri, websocket_uri, clock, live, MAX_CONNECTION_AGE)
}

fn provider_with_max_age(
    rest_uri: &str,
    websocket_uri: &str,
    clock: Arc<dyn Clock>,
    live: LiveSupervisorConfig,
    max_connection_age: Duration,
) -> Arc<BinanceProvider> {
    let mut config = BinanceTestConfig::loopback(rest_uri).with_websocket_base(websocket_uri);
    config.live = live;
    config.max_connection_age = max_connection_age;
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
#[test]
fn binance_live_rejects_off_grid_bad_close_and_far_future_candles_before_state_use() {
    let config = WsConfig::default();
    let malformed = |open_time: i64, close_time: i64| {
        Message::Text(
            json!({
                "e": "kline",
                "s": "BTCUSDT",
                "k": {
                    "t": open_time,
                    "T": close_time,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "o": "37000.00",
                    "c": "37025.50",
                    "h": "37200.00",
                    "l": "36975.25",
                    "v": "12.5",
                    "x": false
                }
            })
            .to_string()
            .into(),
        )
    };
    let far_future = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_millis(),
    )
    .expect("millisecond clock")
        + 24 * 60 * 60 * 1_000;
    for frame in [
        malformed(OPEN_TIME + 1, OPEN_TIME + 60_000),
        malformed(OPEN_TIME, OPEN_TIME + 60_000),
        malformed(far_future, far_future + 59_999),
    ] {
        assert!(matches!(
            decode_ws_frame(frame, &instrument(), Timeframe::Minute1, &config),
            DecodedFrame::ProviderError(ProviderError::Payload {
                source: PayloadError::MalformedProtocol,
                ..
            })
        ));
    }

    assert!(matches!(
        decode_ws_frame(
            Message::Text(ws_kline(OPEN_TIME + 120_000, false, "37025.50").into()),
            &instrument(),
            Timeframe::Minute1,
            &config,
        ),
        DecodedFrame::Provider(_)
    ));
}

#[test]
fn binance_live_accepts_calendar_month_lengths_and_rejects_invalid_month_boundaries() {
    let config = WsConfig::default();
    let month_open = |year, month| {
        time::Date::from_calendar_date(year, month, 1)
            .expect("valid month")
            .midnight()
            .assume_utc()
            .unix_timestamp()
            * 1_000
    };

    for open_time in [
        month_open(2023, time::Month::February),
        month_open(2024, time::Month::February),
        month_open(2024, time::Month::December),
    ] {
        assert!(matches!(
            decode_ws_frame(
                Message::Text(ws_kline_for(open_time, false, "37025.50", "1M").into()),
                &instrument(),
                Timeframe::Month1,
                &config,
            ),
            DecodedFrame::Provider(_)
        ));
    }

    let january_open = month_open(2024, time::Month::January);
    for (open_time, close_time) in [
        (
            january_open + 86_400_000,
            month_open(2024, time::Month::February) - 1,
        ),
        (january_open, month_open(2024, time::Month::February)),
    ] {
        let mut frame: serde_json::Value =
            serde_json::from_str(&ws_kline_for(open_time, false, "37025.50", "1M"))
                .expect("monthly frame");
        frame["k"]["T"] = json!(close_time);
        assert!(matches!(
            decode_ws_frame(
                Message::Text(frame.to_string().into()),
                &instrument(),
                Timeframe::Month1,
                &config,
            ),
            DecodedFrame::ProviderError(ProviderError::Payload {
                source: PayloadError::MalformedProtocol,
                ..
            })
        ));
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
    let mut zero_age =
        BinanceTestConfig::loopback("http://127.0.0.1:1").with_websocket_base("ws://127.0.0.1:1");
    zero_age.max_connection_age = Duration::ZERO;
    assert!(matches!(
        BinanceProvider::new_test_live(zero_age, Arc::new(ManualClock::new(MonoInstant::ZERO))),
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
        let capabilities = registry
            .get(ProviderId::new("binance").expect("id"))
            .expect("registered")
            .capabilities();
        assert_eq!(capabilities.markets, &[Market::Spot, Market::Perpetual]);
        assert_eq!(capabilities.timeframes, &Timeframe::ALL);
        assert_eq!(capabilities.history_page_limit, 1000);
        assert!(matches!(
            registry.get(ProviderId::new("other").expect("id")),
            Err(ProviderError::Configuration(_))
        ));
    });
}

#[tokio::test]
async fn zero_advertised_history_limit_is_rejected_before_live_io() {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let mut config =
        BinanceTestConfig::loopback("http://127.0.0.1:1").with_websocket_base("ws://127.0.0.1:1");
    config.advertised_history_page_limit = 0;
    let provider = BinanceProvider::new_test_live(config, clock).expect("provider");
    let (request, _watermark, _ack) = request(None);
    let error = match provider.open_live(request).await {
        Ok(_) => panic!("zero advertised history limit must fail"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        ProviderError::Configuration("provider history page limit must be non-zero")
    );
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
    let live = LiveSupervisorConfig::default();
    let provider = provider_with_max_age(
        &rest.uri(),
        &ws_uri,
        manual.clone(),
        live,
        Duration::from_secs(5),
    );
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
async fn reconciliation_request_64_succeeds_and_65_reconnects_before_network_io() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("REST listener");
    let rest_uri = format!("http://{}", listener.local_addr().expect("REST address"));
    let request_count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&request_count);
    let rest_server = tokio::spawn(async move {
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
            let index = observed.fetch_add(1, Ordering::SeqCst);
            assert!(index < 64, "request 65 reached network I/O");
            let body = serde_json::to_vec(&json!([rest_row(
                OPEN_TIME + i64::try_from(index).expect("page index") * 60_000
            )]))
            .expect("REST JSON");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("REST headers");
            stream.write_all(&body).await.expect("REST body");
            stream.shutdown().await.expect("REST shutdown");
        }
    });
    let (listener, ws_uri) = websocket_listener().await;
    let ws_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(
                ws_kline(OPEN_TIME + 64 * 60_000, false, "37025.50").into(),
            ))
            .await
            .expect("target candle");
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let mut config = BinanceTestConfig::loopback(&rest_uri).with_websocket_base(&ws_uri);
    config.advertised_history_page_limit = 1;
    config.max_gap_reconciliation_pages = 64;
    let provider = Arc::new(BinanceProvider::new_test_live(config, manual).expect("provider"));
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
            error: ProviderError::Protocol {
                detail: "Binance gap reconciliation exceeded the per-generation page limit",
                ..
            },
            rate_gate_deadline: None,
        }
    ));
    assert_eq!(request_count.load(Ordering::SeqCst), 64);
    feed.request_shutdown();
    ws_server.abort();
    rest_server.abort();
}

#[tokio::test]
async fn pending_history_rejects_distinct_successor_flood_before_additional_rest_work() {
    let (rest_uri, rest_started_rx, _rest_release_tx, rest_server) =
        held_rest_listener(json!([rest_row(OPEN_TIME)])).await;
    let (listener, ws_uri) = websocket_listener().await;
    let (flood_tx, flood_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut websocket = accept_async(stream).await.expect("upgrade");
        websocket
            .send(Message::Text(ws_kline(OPEN_TIME, false, "37025.50").into()))
            .await
            .expect("first candle");
        flood_rx.await.expect("release successor flood");
        for successor in 1..=4 {
            websocket
                .send(Message::Text(
                    ws_kline(OPEN_TIME + successor * 60_000, false, "37025.50").into(),
                ))
                .await
                .expect("successor candle");
        }
        futures_util::future::pending::<()>().await;
    });
    let manual = Arc::new(ManualClock::new(MonoInstant::from_nanos(0)));
    let mut config = BinanceTestConfig::loopback(&rest_uri).with_websocket_base(&ws_uri);
    config.max_gap_reconciliation_candles = 3;
    config.max_gap_reconciliation_pages = 64;
    let provider = Arc::new(BinanceProvider::new_test_live(config, manual).expect("provider"));
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
    rest_started_rx.await.expect("history request started");
    flood_tx.send(()).expect("release flood");
    assert!(matches!(
        next_event(&mut feed).await,
        MarketEvent::RecoverableError {
            generation: Some(GapGeneration(1)),
            error: ProviderError::Protocol {
                detail: "Binance gap reconciliation exceeded the distinct buffered-candle limit",
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
    feed.request_shutdown();
    server.abort();
    rest_server.abort();
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
async fn invalid_gap_rest_rows_reject_the_generation_before_reconcile_batch_emission() {
    let target = OPEN_TIME + 60_000;
    let mut inconsistent_close = rest_row(OPEN_TIME);
    inconsistent_close.as_array_mut().expect("REST row")[6] = json!(OPEN_TIME + 60_000);
    for (case, rows) in [
        ("off-grid", json!([rest_row(OPEN_TIME + 1)])),
        ("inconsistent close", json!([inconsistent_close])),
        ("out of window", json!([rest_row(OPEN_TIME - 60_000)])),
        (
            "duplicate",
            json!([rest_row(OPEN_TIME), rest_row(OPEN_TIME)]),
        ),
        ("regressive", json!([rest_row(target), rest_row(OPEN_TIME)])),
    ] {
        let rest = rest_server(ResponseTemplate::new(200).set_body_json(rows)).await;
        let (listener, ws_uri) = websocket_listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut websocket = accept_async(stream).await.expect("upgrade");
            websocket
                .send(Message::Text(ws_kline(target, false, "37025.50").into()))
                .await
                .expect("target candle");
            futures_util::future::pending::<()>().await;
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
        assert!(
            matches!(
                next_event(&mut feed).await,
                MarketEvent::RecoverableError {
                    generation: Some(GapGeneration(1)),
                    error: ProviderError::Payload {
                        source: PayloadError::MalformedProtocol,
                        ..
                    },
                    rate_gate_deadline: None,
                }
            ),
            "{case}"
        );
        assert_status(
            next_event(&mut feed).await,
            Some(1),
            ConnectionStatus::Backoff,
        );
        assert_eq!(
            rest.received_requests()
                .await
                .expect("received requests")
                .len(),
            1,
            "{case} must reject before another same-generation history request"
        );
        feed.request_shutdown();
        server.abort();
    }
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
    let live = LiveSupervisorConfig::default();
    let provider = provider_with_max_age(&rest.uri(), &ws_uri, clock, live, Duration::from_secs(5));
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
