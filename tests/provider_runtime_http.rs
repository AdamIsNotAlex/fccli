#![cfg(feature = "test-transport")]

use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use fccli::{
    clock::{Clock, ManualClock},
    error::{
        ErrorContext, ErrorOperation, PayloadError, ProviderError, SanitizedCause, TimeoutKind,
    },
    model::{MonoInstant, ProcessBlocker, RateGateState},
    provider::test_transport::{
        HttpRuntime, RateLimitDecision, StatusDisposition, classify_status,
    },
};
use reqwest::StatusCode;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(MonoInstant::ZERO))
}

fn runtime_with(clock: Arc<dyn Clock>, body_limit: usize) -> HttpRuntime {
    HttpRuntime::new(clock, Duration::from_secs(1), body_limit).expect("runtime")
}

fn runtime() -> HttpRuntime {
    runtime_with(clock(), 1024)
}

fn context() -> ErrorContext {
    ErrorContext::operation(ErrorOperation::History)
}

fn chunked_server(body: Vec<u8>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind chunked server");
    let address = listener.local_addr().expect("chunked server address");
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n",
            body.len()
        )
        .expect("write chunk header");
        stream.write_all(&body).expect("write chunk body");
        stream.write_all(b"\r\n0\r\n\r\n").expect("finish chunks");
    });
    (format!("http://{address}"), join)
}
fn truncated_body_server(
    declared_length: usize,
    body: Vec<u8>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind truncated body server");
    let address = listener
        .local_addr()
        .expect("truncated body server address");
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {declared_length}\r\nConnection: close\r\n\r\n"
        )
        .expect("write response headers");
        stream
            .write_all(&body)
            .expect("write partial response body");
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
    let Some(target) = std::env::var_os("FCCLI_PROXY_HELPER_TARGET") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("helper runtime")
        .block_on(async {
            let runtime = runtime();
            let response = runtime
                .send(
                    runtime
                        .client()
                        .get(target.into_string().expect("UTF-8 target")),
                    &CancellationToken::new(),
                    &context(),
                )
                .await
                .expect("client bypasses hostile proxy");
            runtime
                .read_capped(response, &CancellationToken::new(), &context())
                .await
                .expect("read response");
        });
}

#[test]
fn safe_client_configuration_rejects_zero_limits() {
    let clock: Arc<dyn Clock> = clock();
    assert!(matches!(
        HttpRuntime::new(Arc::clone(&clock), Duration::ZERO, 1),
        Err(ProviderError::Configuration(_))
    ));
    assert!(matches!(
        HttpRuntime::new(clock, Duration::from_secs(1), 0),
        Err(ProviderError::Configuration(_))
    ));
}

#[tokio::test]
async fn safe_client_sets_user_agent_and_does_not_follow_redirects() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/start"))
        .and(header(
            "user-agent",
            concat!("fccli/", env!("CARGO_PKG_VERSION")),
        ))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/target"))
        .mount(&server)
        .await;
    let runtime = runtime();
    let response = runtime
        .send(
            runtime.client().get(format!("{}/start", server.uri())),
            &CancellationToken::new(),
            &context(),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FOUND);
}

#[test]
fn hostile_proxy_environment_does_not_capture_requests() {
    let target_observed = Arc::new(AtomicBool::new(false));
    let proxy_observed = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let (target_url, target_join) =
        observable_server(Some("ok"), Arc::clone(&target_observed), Arc::clone(&stop));
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
    assert!(output.status.success(), "proxy helper failed: {output:?}");
    assert!(target_observed.load(Ordering::SeqCst));
    assert!(!proxy_observed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn request_cancellation_and_timeout_use_shared_transport_mapping() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(1)))
        .mount(&server)
        .await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let runtime = runtime();
    let cancelled = runtime
        .send(
            runtime.client().get(server.uri()),
            &cancellation,
            &context(),
        )
        .await
        .expect_err("cancelled");
    assert!(matches!(
        &cancelled,
        ProviderError::Transport {
            cause: SanitizedCause::Cancelled,
            ..
        }
    ));
    assert!(!cancelled.is_recoverable_for_history());

    let short = HttpRuntime::new(clock(), Duration::from_millis(5), 1024).expect("runtime");
    let timeout = short
        .send(
            short.client().get(server.uri()),
            &CancellationToken::new(),
            &context(),
        )
        .await
        .expect_err("timeout");
    assert!(matches!(
        &timeout,
        ProviderError::Timeout {
            kind: TimeoutKind::Request,
            ..
        }
    ));
    assert!(timeout.is_recoverable_for_history());
}
#[tokio::test]
async fn request_and_body_io_failures_are_recoverable_connection_errors() {
    let runtime = runtime();
    let request_error = runtime
        .send(
            runtime.client().get("http://127.0.0.1:0"),
            &CancellationToken::new(),
            &context(),
        )
        .await
        .expect_err("connection must fail before a request is sent");
    assert!(matches!(
        &request_error,
        ProviderError::Transport {
            cause: SanitizedCause::Connection,
            ..
        }
    ));
    assert!(request_error.is_recoverable_for_history());

    let (url, join) = truncated_body_server(16, vec![b'x'; 1]);
    let response = runtime
        .send(
            runtime.client().get(url),
            &CancellationToken::new(),
            &context(),
        )
        .await
        .expect("response headers");
    let body_error = runtime
        .read_capped(response, &CancellationToken::new(), &context())
        .await
        .expect_err("truncated body must fail");
    assert!(matches!(
        &body_error,
        ProviderError::Transport {
            cause: SanitizedCause::Connection,
            ..
        }
    ));
    assert!(body_error.is_recoverable_for_history());
    join.join().expect("truncated body server");
}

#[tokio::test]
async fn body_read_cancellation_uses_shared_transport_mapping() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled body server");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("stalled body address")
    );
    let (body_started, body_started_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await.expect("read request");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n100\r\nx",
            )
            .await
            .expect("write partial body");
        body_started.send(()).expect("body reader stopped early");
        let _ = release_rx.await;
    });

    let runtime = runtime();
    let cancellation = CancellationToken::new();
    let response = runtime
        .send(runtime.client().get(url), &cancellation, &context())
        .await
        .expect("response headers");
    body_started_rx.await.expect("partial body written");
    cancellation.cancel();
    assert!(matches!(
        runtime
            .read_capped(response, &cancellation, &context())
            .await,
        Err(ProviderError::Transport {
            cause: SanitizedCause::Cancelled,
            ..
        })
    ));
    let _ = release.send(());
    server.await.expect("stalled body server");
}

#[tokio::test]
async fn capped_body_accepts_exact_limit_and_rejects_one_byte_over() {
    let runtime = runtime_with(clock(), 16);

    for (size, accepted) in [(16, true), (17, false)] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; size]))
            .mount(&server)
            .await;
        let response = runtime
            .send(
                runtime.client().get(server.uri()),
                &CancellationToken::new(),
                &context(),
            )
            .await
            .expect("declared response");
        assert_eq!(response.content_length(), Some(size as u64));
        let result = runtime
            .read_capped(response, &CancellationToken::new(), &context())
            .await;
        if accepted {
            assert_eq!(
                result.expect("exact declared limit must be accepted").len(),
                16
            );
        } else {
            assert!(matches!(
                result,
                Err(ProviderError::Payload {
                    source: PayloadError::OverBudget { limit_bytes: 16 },
                    ..
                })
            ));
        }
    }

    for (size, accepted) in [(16, true), (17, false)] {
        let (url, join) = chunked_server(vec![b'x'; size]);
        let response = runtime
            .send(
                runtime.client().get(url),
                &CancellationToken::new(),
                &context(),
            )
            .await
            .expect("streamed response");
        assert_eq!(response.content_length(), None);
        let result = runtime
            .read_capped(response, &CancellationToken::new(), &context())
            .await;
        if accepted {
            assert_eq!(
                result.expect("exact streamed limit must be accepted").len(),
                16
            );
        } else {
            assert!(matches!(
                result,
                Err(ProviderError::Payload {
                    source: PayloadError::OverBudget { limit_bytes: 16 },
                    ..
                })
            ));
        }
        join.join().expect("chunked server");
    }
}

#[tokio::test]
async fn common_response_handling_uses_provider_callback_only_for_client_payloads() {
    for (status, expected) in [
        (StatusCode::FOUND, "redirect"),
        (StatusCode::BAD_REQUEST, "client"),
        (StatusCode::BAD_GATEWAY, "server"),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status.as_u16()).set_body_string("provider body"))
            .mount(&server)
            .await;
        let runtime = runtime();
        let response = runtime
            .send(
                runtime.client().get(server.uri()),
                &CancellationToken::new(),
                &context(),
            )
            .await
            .expect("response");
        let error = runtime
            .read_response(
                response,
                &CancellationToken::new(),
                context(),
                |status, body, context| {
                    assert_eq!(status, StatusCode::BAD_REQUEST);
                    assert_eq!(body, b"provider body");
                    ProviderError::ClientStatus {
                        context,
                        status: status.as_u16(),
                        code: Some(7),
                        message: None,
                    }
                },
            )
            .await
            .expect_err(expected);
        assert_eq!(error.is_recoverable_for_history(), expected == "server");
        match expected {
            "redirect" => assert!(matches!(
                error,
                ProviderError::ClientStatus {
                    status: 302,
                    code: None,
                    ..
                }
            )),
            "client" => assert!(matches!(
                error,
                ProviderError::ClientStatus {
                    status: 400,
                    code: Some(7),
                    ..
                }
            )),
            "server" => assert!(matches!(
                error,
                ProviderError::ServerStatus { status: 502, .. }
            )),
            _ => unreachable!(),
        }
    }
}

#[test]
fn common_status_classes_are_provider_neutral() {
    assert_eq!(classify_status(StatusCode::OK), StatusDisposition::Success);
    assert_eq!(
        classify_status(StatusCode::FOUND),
        StatusDisposition::Redirection
    );
    assert_eq!(
        classify_status(StatusCode::BAD_REQUEST),
        StatusDisposition::ClientError
    );
    assert_eq!(
        classify_status(StatusCode::BAD_GATEWAY),
        StatusDisposition::ServerError
    );
}

#[tokio::test]
async fn oversized_client_error_body_preserves_status_through_shared_skeleton() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403).set_body_bytes(vec![b'x'; 17]))
        .mount(&server)
        .await;
    let runtime = runtime_with(clock(), 16);
    let response = runtime
        .send(
            runtime.client().get(server.uri()),
            &CancellationToken::new(),
            &context(),
        )
        .await
        .expect("response");
    let error = runtime
        .read_response(
            response,
            &CancellationToken::new(),
            context(),
            |status, body, context| {
                assert!(body.is_empty());
                ProviderError::ClientStatus {
                    context,
                    status: status.as_u16(),
                    code: None,
                    message: None,
                }
            },
        )
        .await
        .expect_err("known client status");
    assert!(matches!(
        error,
        ProviderError::ClientStatus { status: 403, .. }
    ));
}

#[tokio::test]
async fn gate_wait_honors_deadline_updates_cancellation_and_absorbing_process_blocks() {
    let clock = clock();
    let runtime = runtime_with(clock.clone(), 1024);
    let first = MonoInstant::from_millis(10).expect("deadline");
    let later = MonoInstant::from_millis(20).expect("deadline");
    let earlier = MonoInstant::from_millis(5).expect("deadline");

    let _ = runtime.apply_rate_limit(
        RateLimitDecision::TimedUntil(first),
        context(),
        StatusCode::TOO_MANY_REQUESTS,
    );
    assert_eq!(
        runtime.gate_snapshot().current(),
        Ok(RateGateState::TimedUntil(first))
    );

    let waiter = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .await_gate(&CancellationToken::new(), &context(), |_| {
                    ProviderError::InvalidBanExpiry
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    let _ = runtime.apply_rate_limit(
        RateLimitDecision::TimedUntil(later),
        context(),
        StatusCode::TOO_MANY_REQUESTS,
    );
    let _ = runtime.apply_rate_limit(
        RateLimitDecision::TimedUntil(earlier),
        context(),
        StatusCode::TOO_MANY_REQUESTS,
    );
    assert_eq!(
        runtime.gate_snapshot().current(),
        Ok(RateGateState::TimedUntil(later))
    );
    clock.advance_to(first).expect("advance to old deadline");
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    clock
        .advance_to(later)
        .expect("advance to maximum deadline");
    waiter.await.expect("waiter task").expect("gate opens");

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let future = MonoInstant::from_millis(30).expect("future deadline");
    let _ = runtime.apply_rate_limit(
        RateLimitDecision::TimedUntil(future),
        context(),
        StatusCode::TOO_MANY_REQUESTS,
    );
    assert!(matches!(
        runtime
            .await_gate(&cancellation, &context(), |_| {
                ProviderError::InvalidBanExpiry
            })
            .await,
        Err(ProviderError::Transport {
            cause: SanitizedCause::Cancelled,
            ..
        })
    ));

    let blocked_waiter = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .await_gate(&CancellationToken::new(), &context(), |_| {
                    ProviderError::InvalidBanExpiry
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(
        runtime.apply_rate_limit(
            RateLimitDecision::ProcessBlocked(ProcessBlocker::InvalidBanExpiry),
            context(),
            StatusCode::IM_A_TEAPOT,
        ),
        Err(ProviderError::InvalidBanExpiry)
    );
    assert!(matches!(
        blocked_waiter.await.expect("blocked waiter task"),
        Err(ProviderError::InvalidBanExpiry)
    ));
    let _ = runtime.apply_rate_limit(
        RateLimitDecision::TimedUntil(MonoInstant::from_millis(40).expect("deadline")),
        context(),
        StatusCode::TOO_MANY_REQUESTS,
    );
    assert_eq!(
        runtime.gate_snapshot().current(),
        Ok(RateGateState::ProcessBlocked(
            ProcessBlocker::InvalidBanExpiry
        ))
    );
    assert!(matches!(
        runtime
            .await_gate(&CancellationToken::new(), &context(), |_| {
                ProviderError::InvalidBanExpiry
            })
            .await,
        Err(ProviderError::InvalidBanExpiry)
    ));
}
