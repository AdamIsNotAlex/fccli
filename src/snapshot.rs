//! Library-only injected snapshot runner.

use std::io::Write;

use ratatui::{
    buffer::Buffer,
    layout::{Rect, Size},
    style::{Color, Style},
};

use crate::{
    chart::{
        ChartLayoutResult, ChartViewState, ChartWidget, CurrentPriceFreshness, DisplayStatus,
        InteractiveChartState, LayoutMode, RESIZE_MESSAGE, RenderMode, RenderPolicy,
        RendererSnapshot, calculate_chart_layout,
    },
    error::{AppError, RenderError, SanitizedCause},
    model::{CandleSeries, HistoryRequest, InstrumentSpec, Timeframe},
    provider::{CancellationToken, MarketDataProvider},
};

pub const SNAPSHOT_HISTORY_LIMIT: u16 = 500;

fn history_request_limit(
    provider: &dyn MarketDataProvider,
    market: crate::model::Market,
    timeframe: Timeframe,
) -> Result<u16, crate::error::ProviderError> {
    let capabilities = provider.capabilities();
    if !capabilities.markets.contains(&market) {
        return Err(crate::error::ProviderError::Configuration(
            "provider does not support market",
        ));
    }
    if !capabilities.timeframes.contains(&timeframe) {
        return Err(crate::error::ProviderError::Configuration(
            "provider does not support timeframe",
        ));
    }
    if capabilities.history_page_limit == 0 {
        return Err(crate::error::ProviderError::Configuration(
            "provider history page limit must be non-zero",
        ));
    }
    Ok(SNAPSHOT_HISTORY_LIMIT.min(capabilities.history_page_limit))
}
pub const NON_TTY_SNAPSHOT_SIZE: Size = Size::new(120, 36);

/// Output capability metadata supplied by the caller after its own capability detection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotOutputTarget {
    /// An inline terminal frame. One physical row is reserved for the shell prompt.
    Tty { physical_size: Size },
    /// A deterministic plain-text frame independent of the surrounding process environment.
    NonTty,
}

impl SnapshotOutputTarget {
    #[must_use]
    pub fn effective_size(self) -> Size {
        match self {
            Self::Tty { physical_size } => {
                Size::new(physical_size.width, physical_size.height.saturating_sub(1))
            }
            Self::NonTty => NON_TTY_SNAPSHOT_SIZE,
        }
    }
}

/// Metadata for the single frame written by [`run_snapshot`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRunResult {
    pub effective_size: Size,
    pub layout: ChartLayoutResult,
}

/// Fetches and serializes one snapshot frame using only injected dependencies.
///
/// The caller owns TTY and `NO_COLOR` detection and supplies the resulting target and policy.
pub async fn run_snapshot(
    provider: &dyn MarketDataProvider,
    instrument_spec: &InstrumentSpec,
    timeframe: Timeframe,
    output_target: SnapshotOutputTarget,
    render_policy: RenderPolicy,
    cancellation: CancellationToken,
    output: &mut dyn Write,
) -> Result<SnapshotRunResult, AppError> {
    if output_target == SnapshotOutputTarget::NonTty && render_policy != RenderPolicy::StyleFree {
        return Err(RenderError::Invariant("non-TTY snapshot output must be style-free").into());
    }
    let history_limit = history_request_limit(provider, instrument_spec.market(), timeframe)?;

    let effective_size = output_target.effective_size();
    let frame = Rect::new(0, 0, effective_size.width, effective_size.height);
    let layout = calculate_chart_layout(frame, LayoutMode::Snapshot);
    if let ChartLayoutResult::LayoutPending { required, actual } = &layout {
        let mut buffer = Buffer::empty(frame);
        let message = RESIZE_MESSAGE
            .replace("{required_width}", &required.width.to_string())
            .replace("{required_height}", &required.height.to_string())
            .replace("{actual_width}", &actual.width.to_string())
            .replace("{actual_height}", &actual.height.to_string());
        buffer.set_string(frame.x, frame.y, message, Style::default());
        serialize_frame(&buffer, output_target, render_policy, output)?;
        return Err(RenderError::InsufficientSpace.into());
    }
    let instrument = provider.canonicalize(instrument_spec)?;
    let request = HistoryRequest::latest(history_limit)?;
    let candles = provider
        .history(&instrument, timeframe, request, cancellation)
        .await?;

    let mut series = CandleSeries::new(timeframe);
    series
        .replace(candles)
        .map_err(|_| AppError::Invariant("snapshot series was initialized more than once"))?;

    let ChartLayoutResult::Ready {
        layout: ready_layout,
    } = &layout
    else {
        unreachable!("undersized layouts returned before provider work");
    };
    let chart_state = InteractiveChartState::Ready(ChartViewState::snapshot(
        &series,
        usize::from(ready_layout.main_plot.width),
    ));
    let rate_gate = provider
        .rate_gate()
        .current()
        .map_err(|_| crate::error::ProviderError::Invariant("provider rate gate closed"))?;
    let snapshot = RendererSnapshot {
        mode: RenderMode::Snapshot,
        display_status: DisplayStatus::Snapshot,
        status_detail: None,
        rate_gate,
        instrument,
        timeframe,
        candles: series.into_arc(),
        current_price_freshness: CurrentPriceFreshness::Fresh,

        chart_state,
        footer: crate::chart::FooterPresentation::Help,
    };

    let mut buffer = Buffer::empty(frame);
    ChartWidget::new(&snapshot, &layout, render_policy).render_to(frame, &mut buffer);
    serialize_frame(&buffer, output_target, render_policy, output)?;

    Ok(SnapshotRunResult {
        effective_size,
        layout,
    })
}

pub(crate) fn serialize_frame(
    buffer: &Buffer,
    target: SnapshotOutputTarget,
    policy: RenderPolicy,
    output: &mut dyn Write,
) -> Result<(), AppError> {
    for y in buffer.area.y..buffer.area.bottom() {
        let mut active_style = Style::default();
        for x in buffer.area.x..buffer.area.right() {
            let cell = &buffer[(x, y)];
            if matches!(target, SnapshotOutputTarget::Tty { .. }) && policy == RenderPolicy::Color {
                let style = cell.style();
                if style != active_style {
                    write_ansi_style(output, style)?;
                    active_style = style;
                }
            }
            output
                .write_all(cell.symbol().as_bytes())
                .map_err(output_error)?;
        }
        if active_style != Style::default() {
            output.write_all(b"\x1b[0m").map_err(output_error)?;
        }
        match target {
            SnapshotOutputTarget::NonTty => output.write_all(b"\n").map_err(output_error)?,
            SnapshotOutputTarget::Tty { .. } if y + 1 < buffer.area.bottom() => {
                output.write_all(b"\r\n").map_err(output_error)?;
            }
            SnapshotOutputTarget::Tty { .. } => {}
        }
    }
    output.flush().map_err(output_error)?;
    Ok(())
}

fn write_ansi_style(output: &mut dyn Write, style: Style) -> Result<(), AppError> {
    output.write_all(b"\x1b[0m").map_err(output_error)?;
    if let Some(foreground) = style.fg {
        write_ansi_color(output, foreground, false)?;
    }
    if let Some(background) = style.bg {
        write_ansi_color(output, background, true)?;
    }
    Ok(())
}

fn write_ansi_color(
    output: &mut dyn Write,
    color: Color,
    background: bool,
) -> Result<(), AppError> {
    let base = if background { 40 } else { 30 };
    let bright_base = if background { 100 } else { 90 };
    match color {
        Color::Reset => write!(output, "\x1b[{}m", if background { 49 } else { 39 }),
        Color::Black => write!(output, "\x1b[{base}m"),
        Color::Red => write!(output, "\x1b[{}m", base + 1),
        Color::Green => write!(output, "\x1b[{}m", base + 2),
        Color::Yellow => write!(output, "\x1b[{}m", base + 3),
        Color::Blue => write!(output, "\x1b[{}m", base + 4),
        Color::Magenta => write!(output, "\x1b[{}m", base + 5),
        Color::Cyan => write!(output, "\x1b[{}m", base + 6),
        Color::Gray => write!(output, "\x1b[{}m", base + 7),
        Color::DarkGray => write!(output, "\x1b[{bright_base}m"),
        Color::LightRed => write!(output, "\x1b[{}m", bright_base + 1),
        Color::LightGreen => write!(output, "\x1b[{}m", bright_base + 2),
        Color::LightYellow => write!(output, "\x1b[{}m", bright_base + 3),
        Color::LightBlue => write!(output, "\x1b[{}m", bright_base + 4),
        Color::LightMagenta => write!(output, "\x1b[{}m", bright_base + 5),
        Color::LightCyan => write!(output, "\x1b[{}m", bright_base + 6),
        Color::White => write!(output, "\x1b[{}m", bright_base + 7),
        Color::Indexed(index) => write!(
            output,
            "\x1b[{};5;{index}m",
            if background { 48 } else { 38 }
        ),
        Color::Rgb(red, green, blue) => write!(
            output,
            "\x1b[{};2;{red};{green};{blue}m",
            if background { 48 } else { 38 }
        ),
    }
    .map_err(output_error)
}

fn output_error(_error: std::io::Error) -> AppError {
    RenderError::Output(SanitizedCause::Io).into()
}
