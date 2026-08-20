use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{Sink, Stream};
use reqwest::Url;
use tokio::{net::TcpStream, sync::Notify};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message,
        error::CapacityError,
        protocol::{WebSocketConfig, frame::coding::CloseCode},
    },
};

use crate::{
    error::{
        ErrorContext, ErrorOperation, PayloadError, ProviderError, SanitizedCause, TimeoutKind,
    },
    model::{Candle, Instrument, Timeframe},
};

const WS_BYTE_LIMIT_MAX: usize = 16 * 1024 * 1024;
pub const WS_READ_BUFFER_SIZE: usize = 128 * 1024;
pub const WS_MESSAGE_SIZE: usize = 1024 * 1024;
pub const WS_FRAME_SIZE: usize = 256 * 1024;
pub const WS_WRITE_BUFFER_SIZE: usize = 64 * 1024;
pub const WS_MAX_WRITE_BUFFER_SIZE: usize = 1024 * 1024;
pub const WS_STALLED_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const WS_MESSAGE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WsConfig {
    pub read_buffer_size: usize,
    pub max_message_size: usize,
    pub max_frame_size: usize,
    pub write_buffer_size: usize,
    pub max_write_buffer_size: usize,
    pub stalled_write_timeout: Duration,
    pub message_inactivity_timeout: Duration,
}

impl WsConfig {
    #[must_use]
    pub const fn production() -> Self {
        Self {
            read_buffer_size: WS_READ_BUFFER_SIZE,
            max_message_size: WS_MESSAGE_SIZE,
            max_frame_size: WS_FRAME_SIZE,
            write_buffer_size: WS_WRITE_BUFFER_SIZE,
            max_write_buffer_size: WS_MAX_WRITE_BUFFER_SIZE,
            stalled_write_timeout: WS_STALLED_WRITE_TIMEOUT,
            message_inactivity_timeout: WS_MESSAGE_INACTIVITY_TIMEOUT,
        }
    }

    pub fn validate(self) -> Result<Self, ProviderError> {
        let byte_sizes = [
            self.read_buffer_size,
            self.max_message_size,
            self.max_frame_size,
            self.write_buffer_size,
            self.max_write_buffer_size,
        ];
        if byte_sizes
            .into_iter()
            .any(|size| !(1..=WS_BYTE_LIMIT_MAX).contains(&size))
        {
            return Err(ProviderError::Configuration(
                "WebSocket byte limits must be within 1..=16 MiB",
            ));
        }
        if self.max_frame_size > self.max_message_size {
            return Err(ProviderError::Configuration(
                "WebSocket frame limit must not exceed message limit",
            ));
        }
        if self.write_buffer_size >= self.max_write_buffer_size {
            return Err(ProviderError::Configuration(
                "WebSocket write buffer must be smaller than max write buffer",
            ));
        }
        let required_control_headroom = self
            .write_buffer_size
            .checked_add(self.max_frame_size.min(125) + 6)
            .ok_or(ProviderError::Configuration(
                "WebSocket control-frame headroom overflowed",
            ))?;
        if self.max_write_buffer_size < required_control_headroom {
            return Err(ProviderError::Configuration(
                "WebSocket max write buffer lacks automatic control-frame headroom",
            ));
        }
        if !(Duration::from_millis(1)..=Duration::from_secs(60))
            .contains(&self.stalled_write_timeout)
        {
            return Err(ProviderError::Configuration(
                "WebSocket stalled-write timeout must be within 1 ms..=60 s",
            ));
        }
        if !(Duration::from_millis(1)..=Duration::from_secs(120))
            .contains(&self.message_inactivity_timeout)
        {
            return Err(ProviderError::Configuration(
                "WebSocket message-inactivity timeout must be within 1 ms..=120 s",
            ));
        }
        Ok(self)
    }

    pub(crate) fn tungstenite(self) -> Result<WebSocketConfig, ProviderError> {
        let validated = self.validate()?;
        Ok(WebSocketConfig::default()
            .read_buffer_size(validated.read_buffer_size)
            .write_buffer_size(validated.write_buffer_size)
            .max_write_buffer_size(validated.max_write_buffer_size)
            .max_message_size(Some(validated.max_message_size))
            .max_frame_size(Some(validated.max_frame_size)))
    }
}

impl Default for WsConfig {
    fn default() -> Self {
        Self::production()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DecodedFrame {
    Candle(Candle),
    SubscribeAccepted,
    ApplicationPong,
    Ignored,
    ProviderError(ProviderError),
    ReconnectRequested,
    Close(Option<CloseCode>),
}

pub trait WsCodec: Send {
    fn decode(
        &mut self,
        message: Message,
        instrument: &Instrument,
        timeframe: Timeframe,
        config: &WsConfig,
        output: &mut VecDeque<DecodedFrame>,
    );
}

pub(crate) fn validate_websocket_base(
    base_url: &str,
    loopback_only: bool,
) -> Result<Url, ProviderError> {
    let url = Url::parse(base_url)
        .map_err(|_| ProviderError::Configuration("invalid WebSocket base URL"))?;
    let valid_scheme = if loopback_only {
        url.scheme() == "ws"
    } else {
        url.scheme() == "wss"
    };
    if !valid_scheme || url.query().is_some() || url.fragment().is_some() {
        return Err(ProviderError::Configuration("invalid WebSocket base URL"));
    }
    if loopback_only {
        let host = url.host_str().ok_or(ProviderError::Configuration(
            "WebSocket test URL requires a host",
        ))?;
        let ip_literal = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        let ip = ip_literal.parse::<std::net::IpAddr>().map_err(|_| {
            ProviderError::Configuration("WebSocket test URL must use a literal loopback host")
        })?;
        if !ip.is_loopback()
            || url.port().is_none_or(|port| port == 0)
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(ProviderError::Configuration(
                "WebSocket test URL must be plain WS on a literal loopback host with an explicit nonzero port",
            ));
        }
    }
    Ok(url)
}

pub(crate) fn map_websocket_error(error: WebSocketError, context: &ErrorContext) -> ProviderError {
    match error {
        WebSocketError::Capacity(CapacityError::MessageTooLong { max_size, .. }) => {
            ProviderError::Payload {
                context: context.clone(),
                source: PayloadError::OverBudget {
                    limit_bytes: max_size,
                },
            }
        }
        WebSocketError::Protocol(_) => ProviderError::Protocol {
            context: context.clone(),
            detail: "invalid WebSocket framing",
        },
        WebSocketError::Utf8(_) => ProviderError::Protocol {
            context: context.clone(),
            detail: "invalid WebSocket UTF-8",
        },
        WebSocketError::AttackAttempt => ProviderError::Protocol {
            context: context.clone(),
            detail: "WebSocket attack attempt rejected",
        },
        WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed => {
            ProviderError::Transport {
                context: context.clone(),
                cause: SanitizedCause::Closed,
            }
        }
        WebSocketError::Tls(_) => ProviderError::Transport {
            context: context.clone(),
            cause: SanitizedCause::Tls,
        },
        WebSocketError::Io(_) => ProviderError::Transport {
            context: context.clone(),
            cause: SanitizedCause::Io,
        },
        WebSocketError::Url(_) | WebSocketError::Http(_) | WebSocketError::HttpFormat(_) => {
            ProviderError::Transport {
                context: context.clone(),
                cause: SanitizedCause::Connection,
            }
        }
        WebSocketError::Capacity(_) | WebSocketError::WriteBufferFull(_) => {
            ProviderError::Protocol {
                context: context.clone(),
                detail: "WebSocket capacity invariant failed",
            }
        }
    }
}

#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct HeartbeatTestHook {
    pub started: Arc<Notify>,
    pub due: Arc<Notify>,
}
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct ReadinessDecodedAckTestHook {
    pub observed: Arc<Notify>,
    pub release: Arc<Notify>,
}
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct ReadinessDrainBudgetTestHook {
    pub observed: Arc<Notify>,
    pub release: Arc<Notify>,
}
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct SubscribeFlushTestHook {
    pub blocked: Arc<Notify>,
    pub release: Arc<Notify>,
}
#[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
#[derive(Clone, Debug)]
pub struct CloseFlushTestHook {
    pub blocked: Arc<Notify>,
    pub release: Arc<Notify>,
}
// Stop draining after this many continuously-ready frames; actionable retained outcomes still
// arbitrate before cancellation and deadline branches receive their fairness poll.
const READINESS_DRAIN_POLL_BUDGET: usize = 256;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadinessMode {
    Ordinary,
    PreSubscription,
}

impl ReadinessMode {
    const fn drains_readiness_frames(self) -> bool {
        matches!(self, Self::PreSubscription)
    }

    const fn exposes_close_before_reply_flush(self) -> bool {
        matches!(self, Self::PreSubscription)
    }
}

pub struct RawWebSocket<C> {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    config: WsConfig,
    context: ErrorContext,
    instrument: Instrument,
    timeframe: Timeframe,
    codec: C,
    decoded: VecDeque<DecodedFrame>,
    outbound: VecDeque<Message>,
    flush_pending: bool,
    write_stall_deadline: Option<tokio::time::Instant>,
    last_data_message: tokio::time::Instant,
    terminal_io: bool,
    application_heartbeat_interval: Duration,
    application_heartbeat_message: Option<Message>,
    pending_terminal_error: Option<ProviderError>,
    stalled_write_error: Option<ProviderError>,
    next_application_ping: Option<tokio::time::Instant>,
    peer_close_received: bool,
    readiness_drain_yielded: bool,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub(crate) heartbeat_test_hook: Option<HeartbeatTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub(crate) readiness_inactivity_test_hook: Option<Arc<Notify>>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub(crate) readiness_decoded_ack_test_hook: Option<ReadinessDecodedAckTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub(crate) readiness_drain_budget_test_hook: Option<ReadinessDrainBudgetTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub(crate) force_stalled_write_after_readiness_frame: bool,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub(crate) subscribe_flush_test_hook: Option<SubscribeFlushTestHook>,
    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub(crate) close_flush_test_hook: Option<CloseFlushTestHook>,
}

impl<C: WsCodec> RawWebSocket<C> {
    #[must_use]
    pub const fn config(&self) -> &WsConfig {
        &self.config
    }

    pub fn start_application_heartbeat(&mut self, interval: Duration) {
        self.application_heartbeat_interval = interval;
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if let Some(hook) = &self.heartbeat_test_hook {
            hook.started.notify_one();
            return;
        }
        self.next_application_ping = tokio::time::Instant::now().checked_add(interval);
    }

    #[must_use]
    pub const fn application_heartbeat_started(&self) -> bool {
        self.next_application_ping.is_some()
    }

    fn application_heartbeat_enabled(&self) -> bool {
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if self.heartbeat_test_hook.is_some() {
            return true;
        }
        self.next_application_ping.is_some()
    }

    #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
    pub fn force_application_heartbeat_due_for_test(&mut self) {
        assert!(
            self.application_heartbeat_started(),
            "heartbeat must be enabled first"
        );
        self.next_application_ping = Some(tokio::time::Instant::now());
    }
    pub(crate) async fn read_readiness(&mut self) -> ReadinessInput {
        let inactivity_deadline = self.last_data_message + self.config.message_inactivity_timeout;
        futures_util::future::poll_fn(|cx| {
            self.readiness_drain_yielded = false;
            let io = self.poll_io(
                cx,
                inactivity_deadline,
                false,
                ReadinessMode::PreSubscription,
            );
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            if self.readiness_drain_yielded
                && let Some(hook) = self.readiness_drain_budget_test_hook.take()
            {
                hook.observed.notify_one();
                let waker = cx.waker().clone();
                tokio::spawn(async move {
                    hook.release.notified().await;
                    waker.wake();
                });
                return Poll::Pending;
            }
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            if let Some(hook) = &self.readiness_decoded_ack_test_hook
                && self
                    .decoded
                    .iter()
                    .any(|frame| matches!(frame, DecodedFrame::SubscribeAccepted))
            {
                hook.observed.notify_one();
                let release = Arc::clone(&hook.release);
                let waker = cx.waker().clone();
                self.readiness_decoded_ack_test_hook = None;
                tokio::spawn(async move {
                    release.notified().await;
                    waker.wake();
                });
                return Poll::Pending;
            }
            #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
            if self.force_stalled_write_after_readiness_frame && !self.decoded.is_empty() {
                self.force_stalled_write_after_readiness_frame = false;
                self.stalled_write_error = Some(ProviderError::Timeout {
                    context: self.context.clone(),
                    kind: TimeoutKind::StalledWrite,
                });
            }
            if !self.readiness_drain_yielded
                || has_actionable_readiness(
                    &self.decoded,
                    self.pending_terminal_error.as_ref(),
                    self.stalled_write_error.as_ref(),
                )
            {
                if let Some(input) = take_prioritized_readiness(
                    &mut self.decoded,
                    self.pending_terminal_error.take(),
                    self.stalled_write_error.clone(),
                ) {
                    return Poll::Ready(input);
                }
            }
            if self.readiness_drain_yielded {
                return Poll::Pending;
            }
            match io {
                Poll::Ready(Err(error)) => Poll::Ready(ReadinessInput::Error(error)),
                Poll::Ready(Ok(())) | Poll::Pending => Poll::Pending,
            }
        })
        .await
    }

    pub(crate) fn inactivity_deadline(&self) -> tokio::time::Instant {
        self.last_data_message + self.config.message_inactivity_timeout
    }

    pub async fn read(&mut self) -> Result<DecodedFrame, ProviderError> {
        loop {
            if self.stalled_write_error.is_some()
                && let Some(outcome) = self.decoded.front()
                && self.readiness_mode_allows_delivery(outcome, ReadinessMode::Ordinary)
            {
                return Ok(self.decoded.pop_front().expect("front outcome exists"));
            } else if !self.flush_pending
                && self.outbound.is_empty()
                && let Some(outcome) = self.decoded.pop_front()
            {
                return Ok(outcome);
            }
            if let Some(error) = self.pending_terminal_error.take() {
                return Err(error);
            }
            self.pump(false).await?;
        }
    }

    pub async fn send(&mut self, message: Message) -> Result<(), ProviderError> {
        self.reject_terminal_write()?;
        self.outbound.push_back(message);
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if let Some(hook) = self.subscribe_flush_test_hook.take() {
            hook.blocked.notify_one();
            hook.release.notified().await;
        }
        self.ensure_write_stall_deadline();
        while !self.outbound.is_empty() || self.flush_pending {
            self.pump(true).await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ProviderError> {
        self.reject_terminal_write()?;
        if self.flush_pending || !self.outbound.is_empty() {
            self.ensure_write_stall_deadline();
        }
        while !self.outbound.is_empty() || self.flush_pending {
            self.pump(true).await?;
        }
        Ok(())
    }

    fn reject_terminal_write(&self) -> Result<(), ProviderError> {
        if let Some(error) = &self.stalled_write_error {
            return Err(error.clone());
        }
        if !self.terminal_io {
            return Ok(());
        }
        Err(self
            .pending_terminal_error
            .clone()
            .unwrap_or_else(|| ProviderError::Transport {
                context: self.context.clone(),
                cause: SanitizedCause::Closed,
            }))
    }

    fn enter_stalled_write_drain(&mut self, error: ProviderError) {
        self.outbound.clear();
        self.flush_pending = false;
        self.write_stall_deadline = None;
        if self.stalled_write_error.is_none() {
            self.stalled_write_error = Some(error);
        }
    }

    fn ensure_write_stall_deadline(&mut self) {
        if self.write_stall_deadline.is_none() {
            self.write_stall_deadline = Some(
                tokio::time::Instant::now()
                    .checked_add(self.config.stalled_write_timeout)
                    .unwrap_or(tokio::time::Instant::now()),
            );
        }
    }

    fn finish_terminal_io(&mut self) {
        self.terminal_io = true;
        self.outbound.clear();
        self.flush_pending = false;
        self.write_stall_deadline = None;
    }

    fn finish_or_defer_terminal_error(
        &mut self,
        error: ProviderError,
        report_to_writer: bool,
    ) -> Result<(), ProviderError> {
        let error = self.stalled_write_error.clone().unwrap_or(error);
        self.peer_close_received = false;
        self.finish_terminal_io();
        if !self.decoded.is_empty() {
            if self.pending_terminal_error.is_none() {
                self.pending_terminal_error = Some(error.clone());
            }
            if report_to_writer {
                return Err(error);
            }
            Ok(())
        } else {
            Err(error)
        }
    }

    fn fail_write_and_drain(&mut self, error: ProviderError) -> Result<(), ProviderError> {
        self.enter_stalled_write_drain(error.clone());
        Err(error)
    }

    async fn pump(&mut self, writing: bool) -> Result<(), ProviderError> {
        if self.terminal_io {
            if writing {
                return self.reject_terminal_write();
            }
            if !self.decoded.is_empty() {
                return Ok(());
            }
            if let Some(error) = self.pending_terminal_error.take() {
                return Err(error);
            }
            return Err(ProviderError::Transport {
                context: self.context.clone(),
                cause: SanitizedCause::Closed,
            });
        }
        if self.stalled_write_error.is_none()
            && (writing || self.flush_pending || !self.outbound.is_empty())
        {
            self.ensure_write_stall_deadline();
        }
        let inactivity_deadline = self.last_data_message + self.config.message_inactivity_timeout;
        let heartbeat_deadline = self.next_application_ping;
        let heartbeat_enabled = self.application_heartbeat_enabled();
        let write_stall_deadline = self.write_stall_deadline;
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        let heartbeat_test_due = self
            .heartbeat_test_hook
            .as_ref()
            .map(|hook| Arc::clone(&hook.due));
        if write_stall_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            let error = ProviderError::Timeout {
                context: self.context.clone(),
                kind: TimeoutKind::StalledWrite,
            };
            return self.fail_write_and_drain(error);
        }
        let stall_sleep_deadline = write_stall_deadline.unwrap_or(inactivity_deadline);
        let inactivity_context = self.context.clone();
        let stalled_write_context = self.context.clone();
        tokio::select! {
            biased;
            result = futures_util::future::poll_fn(|cx| self.poll_io(cx, inactivity_deadline, writing, ReadinessMode::Ordinary)) => result,
            () = tokio::time::sleep_until(inactivity_deadline) => {
                let error = ProviderError::Timeout {
                    context: inactivity_context,
                    kind: TimeoutKind::WebSocketInactivity,
                };
                self.finish_or_defer_terminal_error(error, writing)
            },
            () = tokio::time::sleep_until(stall_sleep_deadline), if write_stall_deadline.is_some() => {
                let error = ProviderError::Timeout {
                    context: stalled_write_context,
                    kind: TimeoutKind::StalledWrite,
                };
                self.fail_write_and_drain(error)
            },
            () = async move {
                #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
                if let Some(due) = heartbeat_test_due {
                    due.notified().await;
                    return;
                }
                tokio::time::sleep_until(heartbeat_deadline.unwrap_or(inactivity_deadline)).await;
            }, if heartbeat_enabled => {
                self.outbound.push_back(
                    self.application_heartbeat_message
                        .clone()
                        .expect("enabled application heartbeat has a provider message"),
                );
                self.next_application_ping = tokio::time::Instant::now().checked_add(self.application_heartbeat_interval);
                self.ensure_write_stall_deadline();
                Ok(())
            },
        }
    }

    fn poll_io(
        &mut self,
        cx: &mut Context<'_>,
        inactivity_deadline: tokio::time::Instant,
        writing: bool,
        readiness_mode: ReadinessMode,
    ) -> Poll<Result<(), ProviderError>> {
        if self
            .write_stall_deadline
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            let error = ProviderError::Timeout {
                context: self.context.clone(),
                kind: TimeoutKind::StalledWrite,
            };
            return Poll::Ready(self.fail_write_and_drain(error));
        }
        let mut made_progress = false;
        let mut readiness_frames = 0;
        loop {
            if readiness_mode.drains_readiness_frames()
                && readiness_frames == READINESS_DRAIN_POLL_BUDGET
            {
                self.readiness_drain_yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if !readiness_mode.drains_readiness_frames() && self.decoded.len() > 62 {
                break;
            }
            match Stream::poll_next(Pin::new(&mut self.stream), cx) {
                Poll::Ready(Some(Ok(message))) => {
                    let is_data = matches!(message, Message::Text(_) | Message::Binary(_));
                    if is_data {
                        self.last_data_message = tokio::time::Instant::now();
                    } else if tokio::time::Instant::now() >= inactivity_deadline {
                        let error = ProviderError::Timeout {
                            context: self.context.clone(),
                            kind: TimeoutKind::WebSocketInactivity,
                        };
                        return Poll::Ready(self.finish_or_defer_terminal_error(error, writing));
                    }
                    self.flush_pending = true;
                    if self.stalled_write_error.is_none() {
                        self.ensure_write_stall_deadline();
                    }
                    self.codec.decode(
                        message,
                        &self.instrument,
                        self.timeframe,
                        &self.config,
                        &mut self.decoded,
                    );
                    if self
                        .decoded
                        .iter()
                        .any(|decoded| matches!(decoded, DecodedFrame::Close(_)))
                    {
                        self.outbound.clear();
                        self.peer_close_received = true;
                    }
                    made_progress = true;
                    if readiness_mode.drains_readiness_frames() {
                        coalesce_readiness_outcomes(&mut self.decoded);
                        readiness_frames += 1;
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    let error = map_websocket_error(error, &self.context);
                    return Poll::Ready(self.finish_or_defer_terminal_error(error, writing));
                }
                Poll::Ready(None) => {
                    let error = ProviderError::Transport {
                        context: self.context.clone(),
                        cause: SanitizedCause::Closed,
                    };
                    return Poll::Ready(self.finish_or_defer_terminal_error(error, writing));
                }
                Poll::Pending => break,
            }
        }
        if self.decoded.is_empty() && tokio::time::Instant::now() >= inactivity_deadline {
            let error = ProviderError::Timeout {
                context: self.context.clone(),
                kind: TimeoutKind::WebSocketInactivity,
            };
            return Poll::Ready(self.finish_or_defer_terminal_error(error, writing));
        }
        if self.stalled_write_error.is_none()
            && !self.peer_close_received
            && let Some(message) = self.outbound.pop_front()
        {
            let mut stream = Pin::new(&mut self.stream);
            match Sink::<Message>::poll_ready(stream.as_mut(), cx) {
                Poll::Ready(Ok(())) => match Sink::<Message>::start_send(stream, message) {
                    Ok(()) => {
                        self.flush_pending = true;
                        if self.stalled_write_error.is_none() {
                            self.ensure_write_stall_deadline();
                        }
                        made_progress = true;
                    }
                    Err(error) => {
                        let error = map_websocket_error(error, &self.context);
                        return Poll::Ready(self.finish_or_defer_terminal_error(error, writing));
                    }
                },
                Poll::Ready(Err(error)) => {
                    let error = map_websocket_error(error, &self.context);
                    return Poll::Ready(self.finish_or_defer_terminal_error(error, writing));
                }
                Poll::Pending => self.outbound.push_front(message),
            }
        }

        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        if self.peer_close_received
            && let Some(hook) = self.close_flush_test_hook.take()
        {
            hook.blocked.notify_one();
            let waker = cx.waker().clone();
            tokio::spawn(async move {
                hook.release.notified().await;
                waker.wake();
            });
            return Poll::Pending;
        }

        match Sink::<Message>::poll_flush(Pin::new(&mut self.stream), cx) {
            Poll::Ready(Ok(())) => {
                self.flush_pending = false;
                if self.peer_close_received {
                    self.peer_close_received = false;
                    let error = ProviderError::Transport {
                        context: self.context.clone(),
                        cause: SanitizedCause::Closed,
                    };
                    return Poll::Ready(self.finish_or_defer_terminal_error(error, writing));
                }
                if self.outbound.is_empty() {
                    self.write_stall_deadline = None;
                }
                if made_progress || !self.decoded.is_empty() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Err(error)) => {
                let error = map_websocket_error(error, &self.context);
                Poll::Ready(self.finish_or_defer_terminal_error(error, writing))
            }
            Poll::Pending if made_progress => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
    fn readiness_mode_allows_delivery(
        &self,
        outcome: &DecodedFrame,
        readiness_mode: ReadinessMode,
    ) -> bool {
        readiness_mode.exposes_close_before_reply_flush()
            || !matches!(outcome, DecodedFrame::Close(_))
            || (!self.peer_close_received && !self.flush_pending)
    }
}

pub(crate) async fn connect_websocket_url<C: WsCodec>(
    url: &Url,
    instrument: &Instrument,
    timeframe: Timeframe,
    config: WsConfig,
    codec: C,
    application_heartbeat_message: Option<Message>,
) -> Result<RawWebSocket<C>, ProviderError> {
    let context =
        ErrorContext::operation(ErrorOperation::WebSocket).with_market(instrument, timeframe);
    let config = config
        .validate()
        .map_err(|error| contextualize_websocket_configuration(error, instrument, timeframe))?;
    let tungstenite = config
        .tungstenite()
        .map_err(|error| contextualize_websocket_configuration(error, instrument, timeframe))?;
    let stream = connect_async_with_config(url.as_str(), Some(tungstenite), false)
        .await
        .map(|(socket, _)| socket)
        .map_err(|error| map_websocket_error(error, &context))?;
    Ok(RawWebSocket {
        stream,
        config,
        context,
        instrument: instrument.clone(),
        timeframe,
        codec,
        decoded: VecDeque::new(),
        outbound: VecDeque::new(),
        flush_pending: false,
        write_stall_deadline: None,
        last_data_message: tokio::time::Instant::now(),
        terminal_io: false,
        application_heartbeat_interval: Duration::ZERO,
        application_heartbeat_message,
        readiness_drain_yielded: false,
        pending_terminal_error: None,
        stalled_write_error: None,
        next_application_ping: None,
        peer_close_received: false,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        heartbeat_test_hook: None,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        subscribe_flush_test_hook: None,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        close_flush_test_hook: None,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        readiness_inactivity_test_hook: None,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        readiness_decoded_ack_test_hook: None,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        readiness_drain_budget_test_hook: None,
        #[cfg(all(feature = "test-transport", not(feature = "production-transport")))]
        force_stalled_write_after_readiness_frame: false,
    })
}
fn coalesce_readiness_outcomes(decoded: &mut VecDeque<DecodedFrame>) {
    let mut best: Option<DecodedFrame> = None;
    while let Some(frame) = decoded.pop_front() {
        if best
            .as_ref()
            .is_none_or(|retained| readiness_priority(&frame) < readiness_priority(retained))
        {
            best = Some(frame);
        }
    }
    if let Some(frame) = best {
        decoded.push_back(frame);
    }
}

fn has_actionable_readiness(
    decoded: &VecDeque<DecodedFrame>,
    terminal_error: Option<&ProviderError>,
    stalled_write_error: Option<&ProviderError>,
) -> bool {
    terminal_error.is_some()
        || stalled_write_error.is_some()
        || decoded
            .iter()
            .any(|frame| readiness_priority(frame) < readiness_priority(&DecodedFrame::Ignored))
}

fn readiness_priority(frame: &DecodedFrame) -> u8 {
    match frame {
        DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested => 0,
        DecodedFrame::ProviderError(_) | DecodedFrame::Candle(_) => 1,
        DecodedFrame::SubscribeAccepted => 2,
        DecodedFrame::Ignored | DecodedFrame::ApplicationPong => 3,
    }
}

pub async fn read_raw_websocket<C: WsCodec>(
    socket: &mut RawWebSocket<C>,
) -> Result<DecodedFrame, ProviderError> {
    socket.read().await
}

pub async fn send_raw_websocket<C: WsCodec>(
    socket: &mut RawWebSocket<C>,
    message: Message,
) -> Result<(), ProviderError> {
    socket.send(message).await
}

pub async fn flush_raw_websocket<C: WsCodec>(
    socket: &mut RawWebSocket<C>,
) -> Result<(), ProviderError> {
    socket.flush().await
}

pub(crate) fn contextualize_websocket_configuration(
    error: ProviderError,
    instrument: &Instrument,
    timeframe: Timeframe,
) -> ProviderError {
    match error {
        ProviderError::Configuration(detail) => ProviderError::WebSocketConfiguration {
            context: ErrorContext::operation(ErrorOperation::WebSocket)
                .with_market(instrument, timeframe),
            detail,
        },
        other => other,
    }
}

fn take_prioritized_readiness(
    decoded: &mut VecDeque<DecodedFrame>,
    terminal_error: Option<ProviderError>,
    stalled_write_error: Option<ProviderError>,
) -> Option<ReadinessInput> {
    if let Some(index) = decoded.iter().position(|frame| {
        matches!(
            frame,
            DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested
        )
    }) {
        return Some(ReadinessInput::Frame(
            decoded
                .remove(index)
                .expect("readiness terminal frame index"),
        ));
    }
    if let Some(error) = stalled_write_error.or(terminal_error) {
        return Some(ReadinessInput::Error(error));
    }
    let index = decoded
        .iter()
        .enumerate()
        .min_by_key(|(_, frame)| match frame {
            DecodedFrame::Close(_) | DecodedFrame::ReconnectRequested => 0,
            DecodedFrame::ProviderError(_) | DecodedFrame::Candle(_) => 1,
            DecodedFrame::SubscribeAccepted => 2,
            DecodedFrame::Ignored | DecodedFrame::ApplicationPong => 3,
        })
        .map(|(index, _)| index)?;
    Some(ReadinessInput::Frame(
        decoded.remove(index).expect("readiness frame index"),
    ))
}

pub(crate) enum ReadinessInput {
    Frame(DecodedFrame),
    Error(ProviderError),
}
