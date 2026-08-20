#![cfg(feature = "test-transport")]

use std::{sync::Arc, time::Duration};

use fccli::{
    clock::{Clock, ManualClock},
    error::{PayloadError, ProviderError},
    model::{
        ConnectionStatus, GapGeneration, Instrument, Market, MarketEvent, MonoInstant, ProviderId,
        Timeframe,
    },
    provider::binance::{BinanceProvider, BinanceTestConfig, MAX_CONNECTION_AGE, decode_ws_frame},
    provider::{
        LiveRequest, LiveSupervisorConfig, MarketDataProvider, ProducerCompletion, ReconcileAck,
        accepted_watermark_channel, reconcile_ack_channel,
        test_transport::{DecodedFrame, WsConfig},
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
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
