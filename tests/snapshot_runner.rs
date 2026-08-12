use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};

use fccli::{
    chart::{ChartLayoutResult, RenderPolicy},
    error::{AppError, ProviderError, RenderError},
    model::{
        Candle, HistoryRequest, HistoryRequestKind, Instrument, InstrumentSpec, Market, ProviderId,
        RateGateState, Timeframe,
    },
    provider::{
        LiveFeed, LiveRequest, MarketDataProvider, ProviderFuture, RateGateSender,
        RateGateSnapshot, rate_gate_channel,
    },
    snapshot::{NON_TTY_SNAPSHOT_SIZE, SnapshotOutputTarget, run_snapshot},
};
use ratatui::{layout::Size, text::Line};
use tokio_util::sync::CancellationToken;

struct FakeProvider {
    requests: Arc<Mutex<Vec<HistoryRequest>>>,
    candles: Vec<Candle>,
    history_error: Option<ProviderError>,
    wait_for_cancellation: bool,
    _gate_sender: RateGateSender,
    gate: RateGateSnapshot,
}

impl FakeProvider {
    fn new(candles: Vec<Candle>) -> Self {
        let (gate_sender, gate) = rate_gate_channel(RateGateState::Open);
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            candles,
            history_error: None,
            wait_for_cancellation: false,
            _gate_sender: gate_sender,
            gate,
        }
    }
}

impl MarketDataProvider for FakeProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("fake").expect("valid provider")
    }

    fn canonicalize(&self, spec: &InstrumentSpec) -> Result<Instrument, ProviderError> {
        Instrument::new(
            spec.provider().clone(),
            Market::Spot,
            spec.base(),
            spec.quote().unwrap_or("USDT"),
            format!("{}{}", spec.base(), spec.quote().unwrap_or("USDT")),
        )
        .map_err(|_| ProviderError::Invariant("fake canonicalization failed"))
    }

    fn history<'a>(
        &'a self,
        _instrument: &'a Instrument,
        _timeframe: Timeframe,
        request: HistoryRequest,
        cancellation: CancellationToken,
    ) -> ProviderFuture<'a, Vec<Candle>> {
        self.requests.lock().expect("request lock").push(request);
        let wait_for_cancellation = self.wait_for_cancellation;
        let result = self
            .history_error
            .clone()
            .map_or_else(|| Ok(self.candles.clone()), Err);
        Box::pin(async move {
            if wait_for_cancellation {
                cancellation.cancelled().await;
                return Err(ProviderError::Transport {
                    context: fccli::error::ErrorContext::operation(
                        fccli::error::ErrorOperation::History,
                    ),
                    cause: fccli::error::SanitizedCause::Cancelled,
                });
            }
            result
        })
    }

    fn open_live<'a>(&'a self, _request: LiveRequest) -> ProviderFuture<'a, LiveFeed> {
        Box::pin(async { Err(ProviderError::Invariant("snapshot must not open live feed")) })
    }

    fn rate_gate(&self) -> RateGateSnapshot {
        self.gate.clone()
    }
}

fn spec() -> InstrumentSpec {
    InstrumentSpec::new(
        ProviderId::new("fake").expect("provider"),
        "BTC",
        Some("USDT"),
    )
    .expect("instrument spec")
}

fn candles(count: usize) -> Vec<Candle> {
    (0..count)
        .map(|index| {
            let open_time = 1_700_000_000_000 + i64::try_from(index).expect("index") * 60_000;
            Candle::from_ws(
                open_time,
                open_time + 59_999,
                100.0,
                102.0,
                99.0,
                101.0,
                10.0,
                true,
            )
            .expect("candle")
        })
        .collect()
}

#[tokio::test]
async fn non_tty_requests_latest_500_and_writes_one_plain_fixed_frame() {
    let provider = FakeProvider::new(candles(700));
    let mut output = Vec::new();

    let result = run_snapshot(
        &provider,
        &spec(),
        Timeframe::Hour1,
        SnapshotOutputTarget::NonTty,
        RenderPolicy::StyleFree,
        CancellationToken::new(),
        &mut output,
    )
    .await
    .expect("snapshot succeeds");

    let requests = provider.requests.lock().expect("request lock");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].kind(), HistoryRequestKind::Latest);
    assert_eq!(requests[0].limit(), 500);
    assert_eq!(result.effective_size, NON_TTY_SNAPSHOT_SIZE);
    let ChartLayoutResult::Ready { layout } = result.layout else {
        panic!("120x36 must be ready");
    };
    assert_eq!(layout.main_plot.width, 105);

    let text = String::from_utf8(output).expect("UTF-8 frame");
    assert!(!text.contains('\x1b'));
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 36);
    assert!(lines.iter().all(|line| line.chars().count() == 120));
    assert!(lines[0].contains("SNAPSHOT"));
    assert!(lines[0].contains("BTC/USDT"));
}

#[tokio::test]
async fn tty_reserves_shell_row_at_exact_layout_boundary() {
    let mut frame_candles = candles(80);
    frame_candles[0] = Candle::from_ws(
        frame_candles[0].open_time(),
        frame_candles[0].close_time(),
        333.0,
        334.0,
        332.0,
        333.0,
        10.0,
        true,
    )
    .expect("recognizable oldest candle");
    let newest_index = frame_candles.len() - 1;
    frame_candles[newest_index] = Candle::from_ws(
        frame_candles[newest_index].open_time(),
        frame_candles[newest_index].close_time(),
        777.0,
        778.0,
        776.0,
        777.0,
        10.0,
        true,
    )
    .expect("recognizable newest candle");
    let mut provider = FakeProvider::new(frame_candles);
    provider.history_error = Some(ProviderError::Invariant("must not outrank resize"));
    let mut too_short = Vec::new();
    let pending_error = run_snapshot(
        &provider,
        &spec(),
        Timeframe::Minute1,
        SnapshotOutputTarget::Tty {
            physical_size: Size::new(60, 18),
        },
        RenderPolicy::StyleFree,
        CancellationToken::new(),
        &mut too_short,
    )
    .await
    .expect_err("undersized frame fails after rendering resize guidance");
    assert_eq!(
        pending_error,
        AppError::Render(RenderError::InsufficientSpace)
    );
    provider.history_error = None;
    let pending_text = String::from_utf8(too_short).expect("UTF-8 frame");
    assert_eq!(pending_text.lines().count(), 17);
    assert!(pending_text.starts_with("Resize terminal to at least 60x18 (current 60x17)"));
    assert!(provider.requests.lock().expect("request lock").is_empty());
    assert!(!pending_text.contains("Waiting for market data"));

    let mut adequate = Vec::new();
    let ready = run_snapshot(
        &provider,
        &spec(),
        Timeframe::Minute1,
        SnapshotOutputTarget::Tty {
            physical_size: Size::new(60, 19),
        },
        RenderPolicy::StyleFree,
        CancellationToken::new(),
        &mut adequate,
    )
    .await
    .expect("ready frame serializes");
    assert_eq!(ready.effective_size, Size::new(60, 18));
    assert!(matches!(ready.layout, ChartLayoutResult::Ready { .. }));
    let adequate_text = String::from_utf8(adequate).expect("UTF-8");
    assert!(!adequate_text.ends_with('\r'));
    assert!(!adequate_text.ends_with('\n'));
    assert!(
        !adequate_text.contains('\x1b'),
        "TTY snapshot must not emit mode sequences"
    );
    let rows: Vec<_> = adequate_text.split("\r\n").collect();
    assert_eq!(rows.len(), 18);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            Line::raw(*row).width(),
            60,
            "row {index} must be exactly 60 display cells"
        );
        assert!(
            !row.contains('\r') && !row.contains('\n'),
            "row {index} contains a stray line ending"
        );
    }
    assert_eq!(adequate_text.matches("\r\n").count(), 17);
    assert_eq!(Line::raw(rows[17]).width(), 60);
    assert!(
        !rows[17].trim().is_empty(),
        "final full-width row must contain rendered content"
    );
    assert!(
        adequate_text.contains("O:777"),
        "newest tail candle must be rendered"
    );
    assert!(
        !adequate_text.contains("333"),
        "oldest candle outside the latest tail must be excluded"
    );
}

#[tokio::test]
async fn explicit_tty_color_is_deterministic_and_resets_without_mode_sequences() {
    let mut frame_candles = candles(20);
    frame_candles[0] = Candle::from_ws(
        frame_candles[0].open_time(),
        frame_candles[0].close_time(),
        101.0,
        102.0,
        99.0,
        100.0,
        10.0,
        true,
    )
    .expect("bear candle");
    let provider = FakeProvider::new(frame_candles);
    let mut output = Vec::new();
    run_snapshot(
        &provider,
        &spec(),
        Timeframe::Minute1,
        SnapshotOutputTarget::Tty {
            physical_size: Size::new(80, 25),
        },
        RenderPolicy::Color,
        CancellationToken::new(),
        &mut output,
    )
    .await
    .expect("color snapshot");

    let text = String::from_utf8(output).expect("UTF-8 frame");
    assert!(text.contains("\x1b[32m"));
    assert!(text.contains("\x1b[38;2;52;208;88m"));
    assert!(text.contains("\x1b[38;2;234;74;90m"));
    assert!(text.contains("\x1b[0m"));
    for forbidden in ["\x1b[?1049", "\x1b[?25", "\x1b[?1000", "\x1b[2J", "\x1b[H"] {
        assert!(
            !text.contains(forbidden),
            "terminal mode sequence {forbidden:?}"
        );
    }
}

#[tokio::test]
async fn non_tty_rejects_color_and_provider_errors_remain_typed() {
    let provider = FakeProvider::new(Vec::new());
    let error = run_snapshot(
        &provider,
        &spec(),
        Timeframe::Minute1,
        SnapshotOutputTarget::NonTty,
        RenderPolicy::Color,
        CancellationToken::new(),
        &mut Vec::new(),
    )
    .await
    .expect_err("non-TTY color is invalid");
    assert_eq!(
        error,
        AppError::Render(RenderError::Invariant(
            "non-TTY snapshot output must be style-free"
        ))
    );

    let mut provider = FakeProvider::new(Vec::new());
    provider.history_error = Some(ProviderError::Invariant("typed history failure"));
    let error = run_snapshot(
        &provider,
        &spec(),
        Timeframe::Minute1,
        SnapshotOutputTarget::NonTty,
        RenderPolicy::StyleFree,
        CancellationToken::new(),
        &mut Vec::new(),
    )
    .await
    .expect_err("provider failure propagates");
    assert_eq!(
        error,
        AppError::Provider(ProviderError::Invariant("typed history failure"))
    );
}

#[tokio::test]
async fn caller_owned_cancellation_stops_history_without_output() {
    let mut provider = FakeProvider::new(Vec::new());
    provider.wait_for_cancellation = true;
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let mut output = Vec::new();
    let instrument_spec = spec();
    let _error = {
        let future = run_snapshot(
            &provider,
            &instrument_spec,
            Timeframe::Minute1,
            SnapshotOutputTarget::NonTty,
            RenderPolicy::StyleFree,
            cancellation,
            &mut output,
        );
        tokio::pin!(future);
        tokio::select! {
            result = &mut future => panic!("history completed before cancellation: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        cancel.cancel();
        let error = (&mut future).await.expect_err("cancelled history fails");
        assert!(matches!(
            error,
            AppError::Provider(ProviderError::Transport {
                cause: fccli::error::SanitizedCause::Cancelled,
                ..
            })
        ));
        error
    };
    assert!(output.is_empty());
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("secret-output-path"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn output_failure_is_typed_and_sanitized() {
    let provider = FakeProvider::new(candles(1));
    let error = run_snapshot(
        &provider,
        &spec(),
        Timeframe::Minute1,
        SnapshotOutputTarget::NonTty,
        RenderPolicy::StyleFree,
        CancellationToken::new(),
        &mut FailingWriter,
    )
    .await
    .expect_err("writer failure");

    assert!(matches!(error, AppError::Render(RenderError::Output(_))));
    assert!(!error.to_string().contains("secret-output-path"));
}
