#![cfg(feature = "test-transport")]

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use fccli::{
    clock::{Clock, ClockFuture, ManualClock},
    error::{ModelError, PayloadError, ProviderError, SanitizedMessage, TimeoutKind},
    model::{
        FinalityAuthority, HistoryRequest, Instrument, Market, MonoInstant, ProcessBlocker,
        ProviderId, RateGateState, Timeframe,
    },
    provider::binance::{BinanceProvider, BinanceTestConfig, REST_BODY_LIMIT},
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

const VALID: &str = include_str!("fixtures/binance_klines.json");
const EMPTY: &str = "[]";

struct ConcurrentHttpServer {
    uri: String,
    limit_2_observed: Option<tokio::sync::oneshot::Receiver<()>>,
    release_limit_2: tokio::sync::watch::Sender<bool>,
    limit_3_observed: tokio::sync::mpsc::UnboundedReceiver<()>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    join: tokio::task::JoinHandle<()>,
}

impl ConcurrentHttpServer {
    async fn start(limit_2_status: u16, limit_2_retry_after: Option<&str>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind concurrent HTTP server");
        let uri = format!(
            "http://{}",
            listener.local_addr().expect("concurrent server address")
        );
        let (limit_2_tx, limit_2_observed) = tokio::sync::oneshot::channel();
        let limit_2_tx = Arc::new(Mutex::new(Some(limit_2_tx)));
        let (release_limit_2, release_rx) = tokio::sync::watch::channel(false);
        let (limit_3_tx, limit_3_observed) = tokio::sync::mpsc::unbounded_channel();
        let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let retry_after = limit_2_retry_after.map(str::to_owned);
        let join = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.expect("accept concurrent HTTP connection");
                        let limit_2_tx = Arc::clone(&limit_2_tx);
                        let mut release_rx = release_rx.clone();
                        let limit_3_tx = limit_3_tx.clone();
                        let retry_after = retry_after.clone();
                        connections.spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};

                            let mut request = Vec::new();
                            let mut chunk = [0_u8; 1024];
                            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                                let read = stream.read(&mut chunk).await.expect("read HTTP request");
                                assert_ne!(read, 0, "connection closed before HTTP headers");
                                request.extend_from_slice(&chunk[..read]);
                            }
                            let request = String::from_utf8(request).expect("ASCII HTTP request");
                            let target = request
                                .lines()
                                .next()
                                .and_then(|line| line.split_whitespace().nth(1))
                                .expect("HTTP request target");
                            let limit = target
                                .split_once('?')
                                .map(|(_, query)| query)
                                .expect("request query")
                                .split('&')
                                .find_map(|field| field.strip_prefix("limit="))
                                .expect("request limit");
                            let (status, reason, extra_header, body) = match limit {
                                "1" => (
                                    429,
                                    "Too Many Requests",
                                    "Retry-After: 60\r\n".to_owned(),
                                    "",
                                ),
                                "2" => {
                                    limit_2_tx
                                        .lock()
                                        .expect("limit-2 signal mutex poisoned")
                                        .take()
                                        .expect("limit-2 requested more than once")
                                        .send(())
                                        .expect("test stopped before observing limit-2");
                                    while !*release_rx.borrow() {
                                        release_rx.changed().await.expect("limit-2 release sender");
                                    }
                                    let header = retry_after
                                        .as_deref()
                                        .map(|value| format!("Retry-After: {value}\r\n"))
                                        .unwrap_or_default();
                                    let reason = if limit_2_status == 418 { "I'm a teapot" } else { "Too Many Requests" };
                                    (limit_2_status, reason, header, "")
                                }
                                "3" => {
                                    limit_3_tx.send(()).expect("limit-3 observer");
                                    (200, "OK", "Content-Type: application/json\r\n".to_owned(), EMPTY)
                                }
                                other => panic!("unexpected request limit {other}"),
                            };
                            let response = format!(
                                "HTTP/1.1 {status} {reason}\r\n{extra_header}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            stream.write_all(response.as_bytes()).await.expect("write HTTP response");
                            stream.shutdown().await.expect("shutdown HTTP response");
                        });
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
            while let Some(result) = connections.join_next().await {
                result.expect("concurrent HTTP connection task");
            }
        });
        Self {
            uri,
            limit_2_observed: Some(limit_2_observed),
            release_limit_2,
            limit_3_observed,
            shutdown,
            join,
        }
    }

    async fn wait_for_limit_2(&mut self) {
        self.limit_2_observed
            .take()
            .expect("limit-2 observation awaited more than once")
            .await
            .expect("server stopped before limit-2 request");
    }

    fn release_limit_2(&self) {
        let _ = self.release_limit_2.send_replace(true);
    }

    fn limit_3_was_not_observed(&mut self) -> bool {
        matches!(
            self.limit_3_observed.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        )
    }

    async fn wait_for_limit_3(&mut self) {
        self.limit_3_observed
            .recv()
            .await
            .expect("server stopped before limit-3 request");
    }

    async fn shutdown(self) {
        let _ = self.shutdown.send(());
        self.join.await.expect("concurrent HTTP server task");
    }
}

struct SleepObservedClock {
    inner: Arc<ManualClock>,
    observed: tokio::sync::mpsc::UnboundedSender<MonoInstant>,
}

impl Clock for SleepObservedClock {
    fn now(&self) -> MonoInstant {
        self.inner.now()
    }

    fn sleep_until<'a>(&'a self, deadline: MonoInstant) -> ClockFuture<'a> {
        let _ = self.observed.send(deadline);
        self.inner.sleep_until(deadline)
    }
}

fn sleep_observed_clock() -> (
    Arc<ManualClock>,
    Arc<SleepObservedClock>,
    tokio::sync::mpsc::UnboundedReceiver<MonoInstant>,
) {
    let inner = clock();
    let (observed, observed_rx) = tokio::sync::mpsc::unbounded_channel();
    (
        Arc::clone(&inner),
        Arc::new(SleepObservedClock { inner, observed }),
        observed_rx,
    )
}
fn single_kline_fixture() -> serde_json::Value {
    let mut payload =
        serde_json::from_str::<serde_json::Value>(VALID).expect("valid kline fixture JSON");
    payload
        .as_array_mut()
        .expect("kline fixture root array")
        .truncate(1);
    payload
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

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(MonoInstant::ZERO))
}

fn provider(server: &MockServer, clock: Arc<ManualClock>) -> BinanceProvider {
    BinanceProvider::new_test(server.uri(), clock).expect("loopback provider")
}

async fn history(
    provider: &BinanceProvider,
    request: HistoryRequest,
) -> Result<Vec<fccli::model::Candle>, ProviderError> {
    provider
        .history(
            &instrument(),
            Timeframe::Minute1,
            request,
            CancellationToken::new(),
        )
        .await
}

fn stalled_body_server(status: u16) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled server");
    let address = listener.local_addr().expect("stalled server address");
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        write!(
            stream,
            "HTTP/1.1 {status} Response\r\nContent-Length: 64\r\nConnection: close\r\n\r\n"
        )
        .expect("write response headers");
        stream.flush().expect("flush headers");
        thread::sleep(Duration::from_millis(100));
    });
    (format!("http://{address}"), join)
}

fn assert_cancelled(error: ProviderError) {
    assert!(matches!(
        &error,
        ProviderError::Transport {
            cause: fccli::error::SanitizedCause::Cancelled,
            ..
        }
    ));
    assert!(!error.is_recoverable_for_history());
}

fn chunked_over_cap_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind chunked server");
    let address = listener.local_addr().expect("chunked server address");
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        let body = " ".repeat(REST_BODY_LIMIT + 1);
        let chunk_size = format!("{:X}", body.len());
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{chunk_size}\r\n{body}\r\n0\r\n\r\n"
        )
        .expect("write chunked response");
    });
    (format!("http://{address}"), join)
}
fn declared_length_server(body: String) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind declared-length server");
    let address = listener
        .local_addr()
        .expect("declared-length server address");
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .expect("write declared-length headers");
        stream.flush().expect("flush declared-length headers");
        let _ = stream.write_all(body.as_bytes());
    });
    (format!("http://{address}"), join)
}

fn accepting_close_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind closing server");
    let address = listener.local_addr().expect("closing server address");
    let join = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept request");
        stream
            .shutdown(Shutdown::Both)
            .expect("reset accepted connection");
    });
    (format!("http://{address}"), join)
}

fn observable_server(
    response: Option<&'static str>,
    observed: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind observable server");
    listener
        .set_nonblocking(true)
        .expect("make observable server nonblocking");
    let address = listener.local_addr().expect("observable server address");
    let join = thread::spawn(move || {
        let safety_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if stop.load(Ordering::SeqCst) || Instant::now() >= safety_deadline {
                return;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    observed.store(true, Ordering::SeqCst);
                    let mut request = [0_u8; 4096];
                    let _ = stream.read(&mut request);
                    if let Some(body) = response {
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .expect("write observable response");
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept observable request: {error}"),
            }
        }
    });
    (format!("http://{address}"), join)
}

#[test]
fn hostile_proxy_subprocess_helper() {
    let Some(base_url) = std::env::var_os("FCCLI_PROXY_HELPER_TARGET") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().expect("helper runtime");
    runtime.block_on(async {
        let provider =
            BinanceProvider::new_test(base_url.into_string().expect("UTF-8 target"), clock())
                .expect("loopback provider");
        history(&provider, HistoryRequest::latest(1).expect("latest"))
            .await
            .expect("provider bypasses hostile proxy");
    });
}

fn payload_source(error: ProviderError) -> PayloadError {
    match error {
        ProviderError::Payload { source, .. } => source,
        other => panic!("expected payload error, got {other:?}"),
    }
}

#[tokio::test]
async fn latest_query_uses_exact_endpoint_utc_defaults_user_agent_and_no_extra_parameters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "1m"))
        .and(query_param("limit", "500"))
        .and(header(
            "user-agent",
            concat!("fccli/", env!("CARGO_PKG_VERSION")),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_raw(VALID, "application/json"))
        .expect(1)
        .mount(&server)
        .await;

    let candles = history(
        &provider(&server, clock()),
        HistoryRequest::latest(500).expect("latest"),
    )
    .await
    .expect("valid klines");
    assert_eq!(candles.len(), 2);
    assert_eq!(candles[0].open_time(), 1_704_067_200_000);
    assert_eq!(candles[1].open_time(), 1_704_067_260_000);
    assert_eq!(
        candles[0].authority(),
        FinalityAuthority::RestProvisionalOpen
    );
    assert!(!candles[0].is_closed());
    assert_eq!(
        (candles[0].open(), candles[0].high(), candles[0].low()),
        (42_000.10, 42_125.50, 41_950.25)
    );
    assert_eq!(
        (candles[0].close(), candles[0].base_volume()),
        (42_075.75, 123.456)
    );

    let requests = server.received_requests().await.expect("requests");
    let query: Vec<_> = requests[0].url.query_pairs().collect();
    assert_eq!(
        query.len(),
        3,
        "latest must omit startTime, endTime, and timeZone"
    );
}

#[tokio::test]
async fn older_and_gap_queries_use_checked_exact_cursors_limits_and_no_timezone() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "1m"))
        .and(query_param("limit", "1000"))
        .and(query_param("endTime", "1704067199999"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(EMPTY, "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "1M"))
        .and(query_param("limit", "17"))
        .and(query_param("startTime", "1704067200001"))
        .and(query_param("endTime", "1706745600000"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(EMPTY, "application/json"))
        .expect(1)
        .mount(&server)
        .await;

    history(
        &provider(&server, clock()),
        HistoryRequest::older(1_704_067_200_000, 1000).expect("older"),
    )
    .await
    .expect("older response");
    provider(&server, clock())
        .history(
            &instrument(),
            Timeframe::Month1,
            HistoryRequest::gap(1_704_067_200_001, 1_706_745_600_000, 17).expect("gap"),
            CancellationToken::new(),
        )
        .await
        .expect("gap response");

    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 2);
    for request in requests {
        let mut query: Vec<_> = request
            .url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        query.sort();
        if query.iter().any(|(key, _)| key == "startTime") {
            assert_eq!(
                query,
                vec![
                    ("endTime".to_owned(), "1706745600000".to_owned()),
                    ("interval".to_owned(), "1M".to_owned()),
                    ("limit".to_owned(), "17".to_owned()),
                    ("startTime".to_owned(), "1704067200001".to_owned()),
                    ("symbol".to_owned(), "BTCUSDT".to_owned()),
                ]
            );
        } else {
            assert_eq!(
                query,
                vec![
                    ("endTime".to_owned(), "1704067199999".to_owned()),
                    ("interval".to_owned(), "1m".to_owned()),
                    ("limit".to_owned(), "1000".to_owned()),
                    ("symbol".to_owned(), "BTCUSDT".to_owned()),
                ]
            );
        }
    }
}

#[tokio::test]
async fn redirects_are_not_followed_and_test_constructor_rejects_every_public_or_unsafe_base() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", "https://data-api.binance.vision/api/v3/klines"),
        )
        .expect(1)
        .mount(&server)
        .await;
    assert!(matches!(
        history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest")
        )
        .await,
        Err(ProviderError::ClientStatus { status: 302, .. })
    ));

    for base in [
        "https://127.0.0.1:1234",
        "http://example.com",
        "http://8.8.8.8",
        "http://localhost:1234",
        "http://user:secret@127.0.0.1:1234",
        "http://127.0.0.1:1234?next=https://example.com",
    ] {
        assert!(
            matches!(
                BinanceProvider::new_test(base, clock()),
                Err(ProviderError::Configuration(_))
            ),
            "accepted unsafe test base {base}"
        );
    }
}

#[tokio::test]
async fn structural_payload_failures_are_typed() {
    let cases = [
        ("{", PayloadError::MalformedJson),
        ("{}", PayloadError::ExpectedArray),
        (
            "[null]",
            PayloadError::WrongArity {
                expected: 12,
                actual: 0,
            },
        ),
        (
            "[[1,2]]",
            PayloadError::WrongArity {
                expected: 12,
                actual: 2,
            },
        ),
        (
            "[[null,\"1\",\"1\",\"1\",\"1\",\"1\",2,0,0,0,0,0]]",
            PayloadError::InvalidField { field: "open_time" },
        ),
        (
            "[[1,1,\"1\",\"1\",\"1\",\"1\",2,0,0,0,0,0]]",
            PayloadError::InvalidField { field: "open" },
        ),
    ];
    for (body, expected) in cases {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/klines"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let error = history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest"),
        )
        .await
        .expect_err("invalid payload");
        assert_eq!(payload_source(error), expected);
    }
}

#[tokio::test]
async fn invalid_numbers_nonfinite_values_and_candle_domain_errors_are_distinct() {
    let cases = [("nope", "open"), ("NaN", "open"), ("inf", "open")];
    for (invalid_open, expected_field) in cases {
        let server = MockServer::start().await;
        let mut payload = single_kline_fixture();
        payload[0][1] = serde_json::Value::String(invalid_open.to_owned());
        let body = serde_json::to_string(&payload).expect("serializable kline fixture JSON");
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let error = history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest"),
        )
        .await
        .expect_err("invalid number");
        match error {
            ProviderError::Payload {
                source: PayloadError::InvalidField { field },
                ..
            } => assert_eq!(field, expected_field),
            ProviderError::Domain {
                source: ModelError::NonFinite { field },
                ..
            } => assert_eq!(field, "open"),
            other => panic!("unexpected numeric error {other:?}"),
        }
    }

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            "[[1,\"2\",\"1\",\"0\",\"1\",\"1\",2,\"0\",0,\"0\",\"0\",\"0\"]]",
            "application/json",
        ))
        .mount(&server)
        .await;
    assert!(matches!(
        history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest")
        )
        .await,
        Err(ProviderError::Domain {
            source: ModelError::InvalidBodyBounds,
            ..
        })
    ));
}

#[tokio::test]
async fn body_cap_is_enforced_before_decoding() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b' '; 65]))
        .mount(&server)
        .await;
    let config = BinanceTestConfig {
        base_url: server.uri(),
        request_timeout: Duration::from_secs(1),
        body_limit: 64,
        rate_limit_fallback: Duration::from_secs(30),
    };
    let provider =
        BinanceProvider::new_test_with_config_and_clock(config, clock()).expect("provider");
    assert_eq!(
        payload_source(
            history(&provider, HistoryRequest::latest(1).expect("latest"))
                .await
                .expect_err("over budget")
        ),
        PayloadError::OverBudget { limit_bytes: 64 }
    );
}

#[tokio::test]
async fn invalid_symbol_and_generic_client_statuses_are_sanitized_and_nonretryable() {
    for (status, body, invalid_symbol) in [
        (400, r#"{"code":-1121,"msg":"Invalid symbol."}"#, true),
        (400, r#"{"code":-1100,"msg":"api_key_ABC123"}"#, false),
        (403, r#"{"code":-2015,"msg":"Basic dXNlcjpwYXNz"}"#, false),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let error = history(
            &provider(&server, clock()),
            HistoryRequest::latest(1).expect("latest"),
        )
        .await
        .expect_err("client error");
        if invalid_symbol {
            assert!(matches!(
                &error,
                ProviderError::InvalidSymbol {
                    code: -1121,
                    message: SanitizedMessage::InvalidSymbol,
                    ..
                }
            ));
        } else {
            assert!(
                matches!(&error, ProviderError::ClientStatus { status: actual, .. } if *actual == status)
            );
            assert!(!error.is_recoverable_for_history());
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains("ABC123") && !rendered.contains("dXNlcjpwYXNz"));
        }
    }
}

#[tokio::test]
async fn server_status_timeout_and_transport_are_typed_and_recoverable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).set_body_string("untrusted body"))
        .mount(&server)
        .await;
    let error = history(
        &provider(&server, clock()),
        HistoryRequest::latest(1).expect("latest"),
    )
    .await
    .expect_err("server status");
    assert!(matches!(
        &error,
        ProviderError::ServerStatus { status: 503, .. }
    ));
    assert!(error.is_recoverable_for_history());

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(50))
                .set_body_raw(EMPTY, "application/json"),
        )
        .mount(&server)
        .await;
    let provider = BinanceProvider::new_test_with_config_and_clock(
        BinanceTestConfig {
            base_url: server.uri(),
            request_timeout: Duration::from_millis(5),
            body_limit: 1024,
            rate_limit_fallback: Duration::from_secs(30),
        },
        clock(),
    )
    .expect("provider");
    let error = history(&provider, HistoryRequest::latest(1).expect("latest"))
        .await
        .expect_err("timeout");
    assert!(matches!(
        &error,
        ProviderError::Timeout {
            kind: TimeoutKind::Request,
            ..
        }
    ));
    assert!(error.is_recoverable_for_history());

    let (base_url, join) = accepting_close_server();
    let dead = BinanceProvider::new_test(base_url, clock()).expect("loopback provider");
    let error = history(&dead, HistoryRequest::latest(1).expect("latest"))
        .await
        .expect_err("transport");
    join.join().expect("closing server");
    assert!(matches!(&error, ProviderError::Transport { .. }));
    assert!(error.is_recoverable_for_history());
}

#[tokio::test]
async fn retry_after_matrix_extends_shared_gate_and_429_falls_back() {
    for (status, header_value, expected_ms) in [
        (429, Some("0"), 0),
        (429, Some("7"), 7_000),
        (429, None, 30_000),
        (429, Some("invalid"), 30_000),
        (429, Some("+1"), 30_000),
        (429, Some("-1"), 30_000),
        (429, Some("18446744073709551615"), 30_000),
        (418, Some("0"), 0),
        (418, Some("11"), 11_000),
    ] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(status);
        if let Some(value) = header_value {
            response = response.insert_header("retry-after", value);
        }
        Mock::given(method("GET"))
            .respond_with(response)
            .mount(&server)
            .await;
        let provider = provider(&server, clock());
        let error = history(&provider, HistoryRequest::latest(1).expect("latest"))
            .await
            .expect_err("rate limit");
        assert!(
            matches!(error, ProviderError::RateLimited { status: actual, .. } if actual == status)
        );
        assert_eq!(
            provider.rate_gate().current(),
            Ok(RateGateState::TimedUntil(
                MonoInstant::from_millis(expected_ms).expect("deadline")
            ))
        );
    }
}

#[tokio::test]
async fn missing_invalid_negative_or_overflowing_418_absorbs_the_shared_process_gate() {
    for value in [
        None,
        Some("invalid"),
        Some("-1"),
        Some("18446744073709551615"),
    ] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(418);
        if let Some(value) = value {
            response = response.insert_header("retry-after", value);
        }
        Mock::given(method("GET"))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
        let provider = provider(&server, clock());
        assert!(matches!(
            history(&provider, HistoryRequest::latest(1).expect("latest")).await,
            Err(ProviderError::InvalidBanExpiry)
        ));
        assert_eq!(
            provider.rate_gate().current(),
            Ok(RateGateState::ProcessBlocked(
                ProcessBlocker::InvalidBanExpiry
            ))
        );
        assert!(matches!(
            history(&provider, HistoryRequest::latest(1).expect("latest")).await,
            Err(ProviderError::InvalidBanExpiry)
        ));
    }
}

#[tokio::test]
async fn concurrent_rate_limits_share_the_maximum_deadline_and_never_shorten_it() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("limit", "1"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "3"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "9"))
        .expect(1)
        .mount(&server)
        .await;
    let provider = provider(&server, clock());
    let (first, second) = tokio::join!(
        history(&provider, HistoryRequest::latest(1).expect("latest")),
        history(&provider, HistoryRequest::latest(2).expect("latest")),
    );
    assert!(matches!(first, Err(ProviderError::RateLimited { .. })));
    assert!(matches!(second, Err(ProviderError::RateLimited { .. })));
    assert_eq!(
        provider.rate_gate().current(),
        Ok(RateGateState::TimedUntil(
            MonoInstant::from_millis(9_000).expect("deadline")
        ))
    );
}

#[tokio::test]
async fn ignored_numeric_fields_are_finite_and_nonnegative() {
    for (index, value, field) in [
        (
            7,
            serde_json::Value::String("NaN".to_owned()),
            "quote_volume",
        ),
        (8, serde_json::json!(-1), "trade_count"),
        (9, serde_json::json!("-0.1"), "taker_buy_base_volume"),
        (
            10,
            serde_json::Value::String("inf".to_owned()),
            "taker_buy_quote_volume",
        ),
    ] {
        let server = MockServer::start().await;
        let mut payload = single_kline_fixture();
        payload[0][index] = value;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(payload))
            .mount(&server)
            .await;
        assert_eq!(
            payload_source(
                history(
                    &provider(&server, clock()),
                    HistoryRequest::latest(1).expect("latest"),
                )
                .await
                .expect_err("invalid ignored field")
            ),
            PayloadError::InvalidField { field }
        );
    }
}

#[tokio::test]
async fn oversized_client_error_body_preserves_known_status() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403).set_body_bytes(vec![b'x'; 65]))
        .mount(&server)
        .await;
    let provider = BinanceProvider::new_test_with_config_and_clock(
        BinanceTestConfig {
            base_url: server.uri(),
            request_timeout: Duration::from_secs(1),
            body_limit: 64,
            rate_limit_fallback: Duration::from_secs(30),
        },
        clock(),
    )
    .expect("provider");
    assert!(matches!(
        history(&provider, HistoryRequest::latest(1).expect("latest")).await,
        Err(ProviderError::ClientStatus { status: 403, .. })
    ));
}

#[tokio::test]
async fn cancellation_before_gate_or_send_is_nonrecoverable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "60"))
        .expect(1)
        .mount(&server)
        .await;
    let provider = provider(&server, clock());
    let _ = history(&provider, HistoryRequest::latest(1).expect("latest")).await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = provider
        .history(
            &instrument(),
            Timeframe::Minute1,
            HistoryRequest::latest(2).expect("latest"),
            cancellation,
        )
        .await
        .expect_err("cancelled gate wait");
    assert!(matches!(
        &error,
        ProviderError::Transport {
            cause: fccli::error::SanitizedCause::Cancelled,
            ..
        }
    ));
    assert!(!error.is_recoverable_for_history());
}

#[tokio::test]
async fn cancellation_during_send_and_body_is_nonrecoverable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(1))
                .set_body_raw(EMPTY, "application/json"),
        )
        .mount(&server)
        .await;
    let provider = provider(&server, clock());
    let cancellation = CancellationToken::new();
    let send_instrument = instrument();
    let future = provider.history(
        &send_instrument,
        Timeframe::Minute1,
        HistoryRequest::latest(1).expect("latest"),
        cancellation.clone(),
    );
    let cancel = async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(future, cancel);
    assert_cancelled(result.expect_err("send cancellation"));

    let (base_url, join) = stalled_body_server(200);
    let provider = BinanceProvider::new_test(base_url, clock()).expect("provider");
    let cancellation = CancellationToken::new();
    let body_instrument = instrument();
    let future = provider.history(
        &body_instrument,
        Timeframe::Minute1,
        HistoryRequest::latest(1).expect("latest"),
        cancellation.clone(),
    );
    let cancel = async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(future, cancel);
    assert_cancelled(result.expect_err("body cancellation"));
    join.join().expect("stalled server");
}

#[tokio::test]
async fn stalled_client_error_body_preserves_known_status() {
    let (base_url, join) = stalled_body_server(400);
    let provider = BinanceProvider::new_test_with_config_and_clock(
        BinanceTestConfig {
            base_url,
            request_timeout: Duration::from_millis(10),
            body_limit: 64,
            rate_limit_fallback: Duration::from_secs(30),
        },
        clock(),
    )
    .expect("provider");
    assert!(matches!(
        history(&provider, HistoryRequest::latest(1).expect("latest")).await,
        Err(ProviderError::ClientStatus { status: 400, .. })
    ));
    join.join().expect("stalled server");
}

#[tokio::test]
async fn declared_content_length_accepts_exact_limit_and_rejects_one_over() {
    let exact_body = format!("[]{}", " ".repeat(REST_BODY_LIMIT - 2));
    let (base_url, join) = declared_length_server(exact_body);
    let provider = BinanceProvider::new_test_with_config_and_clock(
        BinanceTestConfig {
            base_url,
            request_timeout: Duration::from_secs(30),
            body_limit: REST_BODY_LIMIT,
            rate_limit_fallback: Duration::from_secs(30),
        },
        clock(),
    )
    .expect("provider");
    assert!(
        history(&provider, HistoryRequest::latest(1).expect("latest"))
            .await
            .expect("body exactly at production limit")
            .is_empty()
    );
    join.join().expect("exact-length server");

    let over_body = " ".repeat(REST_BODY_LIMIT + 1);
    let (base_url, join) = declared_length_server(over_body);
    let provider = BinanceProvider::new_test_with_config_and_clock(
        BinanceTestConfig {
            base_url,
            request_timeout: Duration::from_secs(30),
            body_limit: REST_BODY_LIMIT,
            rate_limit_fallback: Duration::from_secs(30),
        },
        clock(),
    )
    .expect("provider");
    assert_eq!(
        payload_source(
            history(&provider, HistoryRequest::latest(1).expect("latest"))
                .await
                .expect_err("declared body over production cap")
        ),
        PayloadError::OverBudget {
            limit_bytes: REST_BODY_LIMIT,
        }
    );
    join.join().expect("over-length server");
}

#[tokio::test]
async fn chunked_response_without_length_is_capped() {
    let (base_url, join) = chunked_over_cap_server();
    let provider = BinanceProvider::new_test_with_config_and_clock(
        BinanceTestConfig {
            base_url,
            request_timeout: Duration::from_secs(1),
            body_limit: REST_BODY_LIMIT,
            rate_limit_fallback: Duration::from_secs(30),
        },
        clock(),
    )
    .expect("provider");
    assert_eq!(
        payload_source(
            history(&provider, HistoryRequest::latest(1).expect("latest"))
                .await
                .expect_err("chunked body over cap")
        ),
        PayloadError::OverBudget {
            limit_bytes: REST_BODY_LIMIT,
        }
    );
    join.join().expect("chunked server");
}

#[test]
fn hostile_proxy_environment_does_not_capture_loopback_request() {
    let target_observed = Arc::new(AtomicBool::new(false));
    let proxy_observed = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let (target_url, target_join) =
        observable_server(Some(EMPTY), Arc::clone(&target_observed), Arc::clone(&stop));
    let (proxy_url, proxy_join) =
        observable_server(None, Arc::clone(&proxy_observed), Arc::clone(&stop));

    let mut command = Command::new(std::env::current_exe().expect("current test executable"));
    command
        .args(["--exact", "hostile_proxy_subprocess_helper", "--nocapture"])
        .env("FCCLI_PROXY_HELPER_TARGET", target_url);
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
        "REQUEST_METHOD",
    ] {
        command.env_remove(key);
    }
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        command.env(key, &proxy_url);
    }

    let output = command.output().expect("run isolated proxy helper");
    stop.store(true, Ordering::SeqCst);
    target_join.join().expect("target server");
    proxy_join.join().expect("proxy server");
    assert!(
        output.status.success(),
        "proxy helper failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(target_observed.load(Ordering::SeqCst));
    assert!(!proxy_observed.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_gate_waiter_wakes_when_concurrent_418_blocks_process() {
    let mut server = ConcurrentHttpServer::start(418, None).await;
    let (clock, observed_clock, mut sleep_observed) = sleep_observed_clock();
    let provider = BinanceProvider::new_test(server.uri.clone(), observed_clock)
        .expect("loopback provider with observed clock");
    let concurrent = {
        let provider = provider.clone();
        tokio::spawn(
            async move { history(&provider, HistoryRequest::latest(2).expect("latest")).await },
        )
    };
    server.wait_for_limit_2().await;
    assert!(matches!(
        history(&provider, HistoryRequest::latest(1).expect("latest")).await,
        Err(ProviderError::RateLimited { .. })
    ));
    assert_eq!(
        provider.rate_gate().current(),
        Ok(RateGateState::TimedUntil(
            MonoInstant::from_millis(60_000).expect("deadline")
        ))
    );
    let waiter = {
        let provider = provider.clone();
        tokio::spawn(
            async move { history(&provider, HistoryRequest::latest(3).expect("latest")).await },
        )
    };
    assert_eq!(
        sleep_observed
            .recv()
            .await
            .expect("waiter did not enter gate sleep"),
        MonoInstant::from_millis(60_000).expect("deadline")
    );
    server.release_limit_2();
    assert!(matches!(
        concurrent.await.expect("concurrent request"),
        Err(ProviderError::InvalidBanExpiry)
    ));
    assert_eq!(
        provider.rate_gate().current(),
        Ok(RateGateState::ProcessBlocked(
            ProcessBlocker::InvalidBanExpiry
        ))
    );
    assert!(matches!(
        waiter.await.expect("waiter task"),
        Err(ProviderError::InvalidBanExpiry)
    ));
    assert_eq!(clock.now(), MonoInstant::ZERO);
    assert!(server.limit_3_was_not_observed());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_gate_waiter_observes_deadline_extension_before_release() {
    let mut server = ConcurrentHttpServer::start(429, Some("90")).await;
    let (clock, observed_clock, mut sleep_observed) = sleep_observed_clock();
    let provider = BinanceProvider::new_test(server.uri.clone(), observed_clock)
        .expect("loopback provider with observed clock");
    let extension = {
        let provider = provider.clone();
        tokio::spawn(
            async move { history(&provider, HistoryRequest::latest(2).expect("latest")).await },
        )
    };
    server.wait_for_limit_2().await;
    assert!(matches!(
        history(&provider, HistoryRequest::latest(1).expect("latest")).await,
        Err(ProviderError::RateLimited { .. })
    ));
    assert_eq!(
        provider.rate_gate().current(),
        Ok(RateGateState::TimedUntil(
            MonoInstant::from_millis(60_000).expect("deadline")
        ))
    );
    let waiter = {
        let provider = provider.clone();
        tokio::spawn(
            async move { history(&provider, HistoryRequest::latest(3).expect("latest")).await },
        )
    };
    let deadline_60 = MonoInstant::from_millis(60_000).expect("deadline");
    assert_eq!(
        sleep_observed
            .recv()
            .await
            .expect("waiter did not enter gate sleep"),
        deadline_60
    );
    server.release_limit_2();
    assert!(matches!(
        extension.await.expect("extension task"),
        Err(ProviderError::RateLimited { .. })
    ));
    let deadline_90 = MonoInstant::from_millis(90_000).expect("deadline");
    assert_eq!(
        provider.rate_gate().current(),
        Ok(RateGateState::TimedUntil(deadline_90))
    );
    loop {
        let observed = sleep_observed
            .recv()
            .await
            .expect("waiter did not observe extended sleep");
        assert!(
            observed == deadline_60 || observed == deadline_90,
            "unexpected gate sleep deadline: {observed:?}"
        );
        if observed == deadline_90 {
            break;
        }
    }
    clock
        .advance_to(deadline_60)
        .expect("advance to old deadline");
    tokio::task::yield_now().await;
    assert!(server.limit_3_was_not_observed());
    assert!(!waiter.is_finished());
    clock
        .advance_to(deadline_90)
        .expect("advance to extended deadline");
    waiter
        .await
        .expect("waiter task")
        .expect("history after gate");
    server.wait_for_limit_3().await;
    server.shutdown().await;
}
