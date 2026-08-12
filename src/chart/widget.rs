//! Pure Ratatui `Buffer` renderer for the shared chart view.

use std::{ffi::OsStr, fmt, fmt::Write as _, sync::Arc};

use ratatui::{
    buffer::Buffer,
    layout::{Position, Rect, Size},
    style::{Color, Style},
    widgets::Widget,
};

use crate::{
    error::ProviderError,
    model::{Candle, Instrument, ProcessBlocker, RateGateState, Timeframe},
};

use super::{
    CandleSlotGeometry, ChartLayout, ChartLayoutResult, ChartViewState, InteractiveChartState,
    PriceRange, format_base_volume, format_utc_timestamp, price_ticks, select_utc_labels_indexed,
    utc_label_format,
};

pub const RESIZE_MESSAGE: &str = "Resize terminal to at least {required_width}x{required_height} (current {actual_width}x{actual_height})";
pub const EMPTY_MESSAGE: &str = "Waiting for market data";
pub const FOOTER_MESSAGE: &str =
    ": market/timeframe  A/D pan  W/S price  h/H time  v/V price  End live  r reset  q quit";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FooterPresentation {
    Help,
    Editing { text: String, cursor: usize },
    Preparing { target: String },
    Error { message: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderMode {
    Snapshot,
    Interactive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderPolicy {
    Color,
    StyleFree,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CurrentPriceFreshness {
    Fresh,
    Stale,
}

/// Reports whether `NO_COLOR` was present in a single captured environment lookup.
///
/// Presence alone is authoritative, including empty, Unicode, and non-Unicode values.
#[must_use]
pub const fn no_color_present(value: Option<&OsStr>) -> bool {
    value.is_some()
}

/// Selects the sole rendering policy from the captured output capability and `NO_COLOR`
/// presence. The environment value is intentionally irrelevant: an empty or non-Unicode value
/// still means that `NO_COLOR` is present.
#[must_use]
pub const fn detect_render_policy(stdout_is_tty: bool, no_color_present: bool) -> RenderPolicy {
    if stdout_is_tty && !no_color_present {
        RenderPolicy::Color
    } else {
        RenderPolicy::StyleFree
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DisplayStatus {
    Snapshot,
    Connected,
    Connecting,
    Backoff,
    GapSync,
    Stopped,
    Backfilling,
    TerminalError,
    Disconnected,
}

impl DisplayStatus {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Snapshot => "SNAPSHOT",
            Self::Connected => "LIVE",
            Self::Connecting => "CONNECTING",
            Self::Backoff => "RECONNECTING",
            Self::GapSync => "SYNCING",
            Self::Stopped => "STOPPED",
            Self::Backfilling => "BACKFILLING",
            Self::TerminalError => "ERROR",
            Self::Disconnected => "DISCONNECTED",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RendererSnapshot {
    pub mode: RenderMode,
    pub display_status: DisplayStatus,
    pub status_detail: Option<ProviderError>,
    pub rate_gate: RateGateState,
    pub instrument: Instrument,
    pub timeframe: Timeframe,
    pub candles: Arc<[Candle]>,
    pub current_price_freshness: CurrentPriceFreshness,

    pub chart_state: InteractiveChartState,
    pub footer: FooterPresentation,
}

pub struct ChartWidget<'a> {
    snapshot: &'a RendererSnapshot,
    layout: &'a ChartLayoutResult,
    policy: RenderPolicy,
}

impl<'a> ChartWidget<'a> {
    #[must_use]
    pub const fn new(
        snapshot: &'a RendererSnapshot,
        layout: &'a ChartLayoutResult,
        policy: RenderPolicy,
    ) -> Self {
        Self {
            snapshot,
            layout,
            policy,
        }
    }

    pub fn render_to(&self, area: Rect, buffer: &mut Buffer) {
        match self.layout {
            ChartLayoutResult::LayoutPending { required, actual } => {
                render_resize(area, buffer, *required, *actual)
            }
            ChartLayoutResult::Ready { layout } => {
                if area.intersection(layout.frame) == layout.frame
                    && buffer.area.intersection(layout.frame) == layout.frame
                {
                    render_ready(self.snapshot, *layout, self.policy, buffer);
                }
            }
        }
    }
}

impl Widget for ChartWidget<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        self.render_to(area, buffer);
    }
}

fn render_resize(area: Rect, buffer: &mut Buffer, required: Size, actual: Size) {
    let message = RESIZE_MESSAGE
        .replace("{required_width}", &required.width.to_string())
        .replace("{required_height}", &required.height.to_string())
        .replace("{actual_width}", &actual.width.to_string())
        .replace("{actual_height}", &actual.height.to_string());
    clear_rect(area, buffer);
    write_clipped(buffer, area, area.x, area.y, &message, Style::default());
}

fn render_ready(
    snapshot: &RendererSnapshot,
    layout: ChartLayout,
    policy: RenderPolicy,
    buffer: &mut Buffer,
) {
    clear_rect(layout.frame, buffer);
    render_header(snapshot, layout.header, policy, buffer);

    let ChartViewState::Data(viewport) = chart_view(snapshot) else {
        write_clipped(
            buffer,
            layout.main_plot,
            layout.main_plot.x,
            layout.main_plot.y,
            EMPTY_MESSAGE,
            Style::default(),
        );
        render_footer(snapshot, layout.footer, buffer);
        return;
    };

    let visible = visible_range(
        snapshot.candles.len(),
        viewport.visible_count(),
        viewport.right_index(),
    );
    if visible.is_empty() || visible.len() > usize::from(layout.main_plot.width) {
        render_footer(snapshot, layout.footer, buffer);
        return;
    }
    let candles = &snapshot.candles[visible.clone()];
    let Some(geometry) =
        CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, candles.len())
    else {
        render_footer(snapshot, layout.footer, buffer);
        return;
    };
    let y_range = viewport.y_range();
    render_grid_and_price_axis(layout, y_range, policy, buffer);
    render_utc_axis(layout, candles, geometry, buffer);
    render_candles(layout, candles, geometry, y_range, policy, buffer);
    render_volume(layout, candles, geometry, policy, buffer);
    render_footer(snapshot, layout.footer, buffer);
    render_current_price(snapshot, layout, y_range, policy, buffer);

    if snapshot.mode == RenderMode::Interactive
        && viewport.active_drag().is_none()
        && let Some(hover) = viewport.coordinate_hover()
        && let Some(index) = candles
            .iter()
            .position(|candle| candle.open_time() == hover.open_time)
    {
        render_crosshair(
            layout,
            &candles[index],
            geometry,
            index,
            hover.price,
            y_range,
            utc_label_format(
                candles.first().map_or(0, Candle::open_time),
                candles.last().map_or(0, Candle::open_time),
            ),
            policy,
            buffer,
        );
    }
}

fn chart_view(snapshot: &RendererSnapshot) -> &ChartViewState {
    match &snapshot.chart_state {
        InteractiveChartState::Ready(view) => view,
        InteractiveChartState::LayoutPending => {
            static EMPTY: ChartViewState = ChartViewState::Empty;
            &EMPTY
        }
    }
}

fn visible_range(
    series_len: usize,
    visible_count: usize,
    right_index: usize,
) -> std::ops::Range<usize> {
    if series_len == 0 || visible_count == 0 || right_index >= series_len {
        return 0..0;
    }
    let end = right_index.saturating_add(1);
    end.saturating_sub(visible_count)..end
}

fn render_header(
    snapshot: &RendererSnapshot,
    header: Rect,
    policy: RenderPolicy,
    buffer: &mut Buffer,
) {
    if header.height == 0 {
        return;
    }
    let effective = effective_status(snapshot);
    let width = usize::from(header.width);
    let status_token_width = effective.token.len().min(width);
    let identity_gap = usize::from(status_token_width != 0 && status_token_width < width);
    let identity_budget = width
        .saturating_sub(status_token_width)
        .saturating_sub(identity_gap);
    let market = match snapshot.instrument.market() {
        crate::model::Market::Spot => "Spot",
    };
    let identity = header_identity(
        snapshot.instrument.provider().as_str(),
        market,
        snapshot.instrument.display_pair(),
        snapshot.timeframe.as_str(),
        identity_budget,
    );
    let gap = usize::from(!identity.is_empty() && status_token_width != 0);
    let status_width = width.saturating_sub(identity.len()).saturating_sub(gap);
    let status = effective.render(status_width);
    write_header_sides(
        buffer,
        header,
        header.y,
        &identity,
        &status,
        effective.style(policy),
    );

    if header.height < 2 {
        return;
    }
    let visible = visible_candles(snapshot);
    let hovered = hovered_candle(snapshot, visible);
    let detail_candle = hovered
        .or_else(|| visible.last())
        .or_else(|| snapshot.candles.last());
    let time = hovered.and_then(|candle| {
        let first = visible.first()?;
        let last = visible.last()?;
        format_utc_timestamp(
            candle.open_time(),
            utc_label_format(first.open_time(), last.open_time()),
        )
    });
    let second = detail_candle.map_or_else(
        || EMPTY_MESSAGE.to_owned(),
        |candle| format_ohlcv(candle, time.as_deref(), usize::from(header.width)),
    );
    write_clipped(
        buffer,
        header,
        header.x,
        header.y.saturating_add(1),
        &second,
        Style::default(),
    );
}

#[derive(Clone, Copy, Debug)]
struct EffectiveStatus<'a> {
    token: &'static str,
    rate_deadline_ms: Option<u64>,
    detail: Option<&'a ProviderError>,
    color: Color,
}

impl EffectiveStatus<'_> {
    fn style(self, policy: RenderPolicy) -> Style {
        if policy == RenderPolicy::Color {
            Style::default().fg(self.color)
        } else {
            Style::default()
        }
    }

    fn render(self, width: usize) -> String {
        let mut output = AsciiCellSink::new(width);
        let _ = fmt::Write::write_str(&mut output, self.token);
        if let Some(deadline_ms) = self.rate_deadline_ms {
            let _ = write!(&mut output, " UNTIL {deadline_ms}ms");
        }
        if let Some(detail) = self.detail {
            let _ = write!(&mut output, ": {detail}");
        }
        output.finish()
    }
}

struct AsciiCellSink {
    text: String,
    remaining: usize,
}

impl AsciiCellSink {
    fn new(capacity: usize) -> Self {
        Self {
            text: String::with_capacity(capacity),
            remaining: capacity,
        }
    }

    fn finish(self) -> String {
        self.text
    }
}

impl fmt::Write for AsciiCellSink {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        for character in value.chars() {
            if self.remaining == 0 {
                return Err(fmt::Error);
            }
            self.text
                .push(if character.is_ascii_graphic() || character == ' ' {
                    character
                } else {
                    '?'
                });
            self.remaining -= 1;
        }
        Ok(())
    }
}

fn effective_status(snapshot: &RendererSnapshot) -> EffectiveStatus<'_> {
    if snapshot.mode == RenderMode::Snapshot {
        return EffectiveStatus {
            token: "SNAPSHOT",
            rate_deadline_ms: None,
            detail: None,
            color: Color::Green,
        };
    }
    let terminal = matches!(
        snapshot.display_status,
        DisplayStatus::Disconnected | DisplayStatus::TerminalError
    );
    let (token, rate_deadline_ms, color) = if terminal {
        (
            snapshot.display_status.label(),
            None,
            if snapshot.display_status == DisplayStatus::TerminalError {
                Color::Red
            } else {
                Color::Yellow
            },
        )
    } else {
        match snapshot.rate_gate {
            RateGateState::Open => (
                snapshot.display_status.label(),
                None,
                match snapshot.display_status {
                    DisplayStatus::Connected | DisplayStatus::Snapshot => Color::Green,
                    DisplayStatus::Connecting
                    | DisplayStatus::GapSync
                    | DisplayStatus::Backfilling => Color::Cyan,
                    DisplayStatus::Backoff | DisplayStatus::Disconnected => Color::Yellow,
                    DisplayStatus::Stopped => Color::DarkGray,
                    DisplayStatus::TerminalError => Color::Red,
                },
            ),
            RateGateState::TimedUntil(deadline) => {
                ("RATE LIMITED", Some(deadline.as_millis()), Color::Yellow)
            }
            RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry) => {
                ("RATE BLOCKED", None, Color::Red)
            }
        }
    };
    EffectiveStatus {
        token,
        rate_deadline_ms,
        detail: snapshot.status_detail.as_ref(),
        color,
    }
}

fn visible_candles(snapshot: &RendererSnapshot) -> &[Candle] {
    let ChartViewState::Data(viewport) = chart_view(snapshot) else {
        return &[];
    };
    let range = visible_range(
        snapshot.candles.len(),
        viewport.visible_count(),
        viewport.right_index(),
    );
    &snapshot.candles[range]
}

fn hovered_candle<'a>(snapshot: &RendererSnapshot, visible: &'a [Candle]) -> Option<&'a Candle> {
    if snapshot.mode != RenderMode::Interactive {
        return None;
    }
    let InteractiveChartState::Ready(ChartViewState::Data(viewport)) = &snapshot.chart_state else {
        return None;
    };
    let hover = viewport.coordinate_hover()?;
    visible
        .iter()
        .find(|candle| candle.open_time() == hover.open_time)
}

fn render_grid_and_price_axis(
    layout: ChartLayout,
    range: PriceRange,
    policy: RenderPolicy,
    buffer: &mut Buffer,
) {
    let ticks = price_ticks(
        range.low,
        range.high,
        layout.main_plot.height,
        layout.price_axis.width,
    );
    let grid_style = if policy == RenderPolicy::Color {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };
    for tick in ticks {
        let y = price_row(tick.value, range, layout.main_plot);
        fill_row(buffer, layout.main_plot, y, "─", grid_style);
        write_padded_row(buffer, layout.price_axis, y, &tick.label, Style::default());
    }
}

fn render_utc_axis(
    layout: ChartLayout,
    candles: &[Candle],
    geometry: CandleSlotGeometry,
    buffer: &mut Buffer,
) {
    for label in select_utc_labels_indexed(
        candles.len(),
        |index| candles.get(index).map(Candle::open_time),
        |index| geometry.center(index),
        layout.utc_axis.x,
        layout.utc_axis.width,
    ) {
        write_clipped(
            buffer,
            layout.utc_axis,
            label.x,
            layout.utc_axis.y,
            &label.text,
            Style::default(),
        );
    }
}

fn render_candles(
    layout: ChartLayout,
    candles: &[Candle],
    geometry: CandleSlotGeometry,
    range: PriceRange,
    policy: RenderPolicy,
    buffer: &mut Buffer,
) {
    let max_half = layout.main_plot.height.saturating_mul(2).saturating_sub(1);
    for (index, candle) in candles.iter().enumerate() {
        let Some(slot) = geometry.slot(index) else {
            continue;
        };
        if candle.low() > range.high || candle.high() < range.low {
            continue;
        }
        let high = price_half(candle.high(), range, layout.main_plot);
        let low = price_half(candle.low(), range, layout.main_plot);
        let direction = direction(candle);
        let body_intersects = candle.open().min(candle.close()) <= range.high
            && candle.open().max(candle.close()) >= range.low;
        let body = body_intersects.then(|| {
            let open = price_half(candle.open(), range, layout.main_plot);
            let close = price_half(candle.close(), range, layout.main_plot);
            (open.min(close), open.max(close))
        });
        let style = price_candle_style(direction, policy);

        for relative_row in high / 2..=low / 2 {
            let upper = relative_row.saturating_mul(2).min(max_half);
            let lower = upper.saturating_add(1).min(max_half);
            let upper_weight = half_weight(upper, high, low, body, direction);
            let lower_weight = half_weight(lower, high, low, body, direction);
            let y = layout.main_plot.y.saturating_add(relative_row);
            let body_overlaps =
                upper_weight == StrokeWeight::Heavy || lower_weight == StrokeWeight::Heavy;
            if direction != Direction::Doji
                && let Some(body_edge) = body_edge_glyph(upper_weight, lower_weight)
            {
                for x in slot.painted_range() {
                    let x = x as u16;
                    if x != slot.center() {
                        set_cell(buffer, x, y, body_edge, style);
                    }
                }
            }
            let symbol = if body_overlaps && direction != Direction::Doji {
                Some(if policy == RenderPolicy::Color {
                    "█"
                } else {
                    body_symbol(direction)
                })
            } else {
                half_cell_glyph(upper_weight, lower_weight)
            };
            if let Some(symbol) = symbol {
                set_cell(buffer, slot.center(), y, symbol, style);
            }
        }
        if direction == Direction::Doji && body_intersects {
            let row = layout
                .main_plot
                .y
                .saturating_add(price_half(candle.open(), range, layout.main_plot) / 2);
            for x in slot.painted_range() {
                set_cell(buffer, x as u16, row, "━", style);
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Direction {
    Bull,
    Bear,
    Doji,
}

fn direction(candle: &Candle) -> Direction {
    if candle.close() > candle.open() {
        Direction::Bull
    } else if candle.close() < candle.open() {
        Direction::Bear
    } else {
        Direction::Doji
    }
}

fn body_symbol(direction: Direction) -> &'static str {
    match direction {
        Direction::Bull => "█",
        Direction::Bear => "▓",
        Direction::Doji => "━",
    }
}

fn price_candle_style(direction: Direction, policy: RenderPolicy) -> Style {
    if policy == RenderPolicy::StyleFree {
        return Style::default();
    }
    match direction {
        Direction::Bull => Style::default().fg(Color::Rgb(52, 208, 88)),
        Direction::Bear => Style::default().fg(Color::Rgb(234, 74, 90)),
        Direction::Doji => Style::default(),
    }
}

fn volume_symbol(direction: Direction, policy: RenderPolicy) -> &'static str {
    if policy == RenderPolicy::Color {
        "█"
    } else {
        body_symbol(direction)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StrokeWeight {
    None,
    Light,
    Heavy,
}

fn half_weight(
    half: u16,
    high: u16,
    low: u16,
    body: Option<(u16, u16)>,
    direction: Direction,
) -> StrokeWeight {
    if half < high || half > low {
        StrokeWeight::None
    } else if direction != Direction::Doji
        && body.is_some_and(|(body_top, body_bottom)| half >= body_top && half <= body_bottom)
    {
        StrokeWeight::Heavy
    } else {
        StrokeWeight::Light
    }
}

fn half_cell_glyph(upper: StrokeWeight, lower: StrokeWeight) -> Option<&'static str> {
    match (upper, lower) {
        (StrokeWeight::None, StrokeWeight::None) => None,
        (StrokeWeight::Light, StrokeWeight::Light) => Some("│"),
        (StrokeWeight::Heavy, StrokeWeight::Heavy) => Some("┃"),
        (StrokeWeight::None, StrokeWeight::Light) => Some("╷"),
        (StrokeWeight::Light, StrokeWeight::None) => Some("╵"),
        (StrokeWeight::None, StrokeWeight::Heavy) => Some("╻"),
        (StrokeWeight::Heavy, StrokeWeight::None) => Some("╹"),
        (StrokeWeight::Light, StrokeWeight::Heavy) => Some("╽"),
        (StrokeWeight::Heavy, StrokeWeight::Light) => Some("╿"),
    }
}

fn body_edge_glyph(upper: StrokeWeight, lower: StrokeWeight) -> Option<&'static str> {
    match (upper == StrokeWeight::Heavy, lower == StrokeWeight::Heavy) {
        (false, false) => None,
        (true, false) => Some("▀"),
        (false, true) => Some("▄"),
        (true, true) => Some("█"),
    }
}

fn render_volume(
    layout: ChartLayout,
    candles: &[Candle],
    geometry: CandleSlotGeometry,
    policy: RenderPolicy,
    buffer: &mut Buffer,
) {
    let maximum = candles
        .iter()
        .map(Candle::base_volume)
        .fold(0.0_f64, f64::max);
    if maximum <= 0.0 || layout.volume.height == 0 {
        return;
    }
    for (index, candle) in candles.iter().enumerate() {
        if candle.base_volume() <= 0.0 {
            continue;
        }
        let Some(slot) = geometry.slot(index) else {
            continue;
        };
        let scaled = candle.base_volume() / maximum * f64::from(layout.volume.height);
        let height = (scaled.ceil() as u16).clamp(1, layout.volume.height);
        let start = layout.volume.bottom().saturating_sub(height);
        let direction = direction(candle);
        let symbol = volume_symbol(direction, policy);
        let style = price_candle_style(direction, policy);
        for y in start..layout.volume.bottom() {
            for x in slot.painted_range() {
                set_cell(buffer, x as u16, y, symbol, style);
            }
        }
    }
}

fn render_footer(snapshot: &RendererSnapshot, footer: Option<Rect>, buffer: &mut Buffer) {
    if snapshot.mode != RenderMode::Interactive {
        return;
    }
    if let Some(footer) = footer {
        let text = match &snapshot.footer {
            FooterPresentation::Help => FOOTER_MESSAGE.to_owned(),
            FooterPresentation::Editing { text, cursor } => {
                let cursor = (*cursor).min(text.len());
                let (before, after) = text.split_at(cursor);
                format!(":{before}│{after}")
            }
            FooterPresentation::Preparing { target } => format!("Preparing {target}…"),
            FooterPresentation::Error { message } => format!("Error: {message}"),
        };
        write_clipped(buffer, footer, footer.x, footer.y, &text, Style::default());
    }
}

fn render_current_price(
    snapshot: &RendererSnapshot,
    layout: ChartLayout,
    range: PriceRange,
    policy: RenderPolicy,
    buffer: &mut Buffer,
) {
    let Some(price) = snapshot.candles.last().map(Candle::close) else {
        return;
    };
    if !price.is_finite() || price < range.low || price > range.high {
        return;
    }

    let (symbol, color) = match snapshot.current_price_freshness {
        CurrentPriceFreshness::Fresh => ("┄", Color::Cyan),

        CurrentPriceFreshness::Stale => ("╌", Color::DarkGray),
    };
    let style = if policy == RenderPolicy::Color {
        Style::default().fg(color)
    } else {
        Style::default()
    };

    let y = price_row(price, range, layout.main_plot);
    fill_overlay_row(buffer, layout.main_plot, y, symbol, style);
    fill_row(buffer, layout.gutter, y, symbol, style);
    let label = compact_price(price);
    write_padded_row(buffer, layout.price_axis, y, &label, style);
}

#[allow(clippy::too_many_arguments)]
fn render_crosshair(
    layout: ChartLayout,
    candle: &Candle,
    geometry: CandleSlotGeometry,
    index: usize,
    price: f64,
    range: PriceRange,
    utc_format: super::UtcLabelFormat,
    policy: RenderPolicy,
    buffer: &mut Buffer,
) {
    let Some(x) = geometry.center(index) else {
        return;
    };
    let y = price_row(price, range, layout.main_plot);
    let style = if policy == RenderPolicy::Color {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };
    for row in layout.main_plot.y..layout.main_plot.bottom() {
        set_overlay_cell(buffer, x, row, "┆", style);
    }
    fill_overlay_row(buffer, layout.main_plot, y, "┄", style);
    fill_row(buffer, layout.gutter, y, "┄", style);
    set_overlay_cell(buffer, x, y, "┼", style);

    let overlay_style = if policy == RenderPolicy::Color {
        Style::default().fg(Color::Black).bg(Color::Yellow)
    } else {
        Style::default()
    };
    if let Some(label) = format_utc_timestamp(candle.open_time(), utc_format)
        && let Ok(width) = u16::try_from(label.chars().count())
        && width <= layout.utc_axis.width
    {
        let desired = x.saturating_sub(width / 2);
        let max_start = layout.utc_axis.right().saturating_sub(width);
        let start = desired.clamp(layout.utc_axis.x, max_start);
        fill_row(
            buffer,
            layout.utc_axis,
            layout.utc_axis.y,
            " ",
            Style::default(),
        );
        write_clipped(
            buffer,
            layout.utc_axis,
            start,
            layout.utc_axis.y,
            &label,
            overlay_style,
        );
    }
    let label = compact_price(price);
    write_padded_row(buffer, layout.price_axis, y, &label, overlay_style);
}

fn price_half(value: f64, range: PriceRange, rect: Rect) -> u16 {
    let half_rows = rect.height.saturating_mul(2);
    if half_rows <= 1 || !value.is_finite() || range.span() <= 0.0 {
        return 0;
    }
    let fraction = ((range.high - value) / range.span()).clamp(0.0, 1.0);
    (fraction * f64::from(half_rows - 1)).round() as u16
}

fn price_row(value: f64, range: PriceRange, rect: Rect) -> u16 {
    if rect.height <= 1 || !value.is_finite() || range.span() <= 0.0 {
        return rect.y;
    }
    let fraction = ((range.high - value) / range.span()).clamp(0.0, 1.0);
    rect.y
        .saturating_add((fraction * f64::from(rect.height - 1)).round() as u16)
}

fn header_identity(
    provider: &str,
    market: &str,
    pair: &str,
    timeframe: &str,
    width: usize,
) -> String {
    let mut identity = String::with_capacity(width);
    let mut used = 0;
    for component in [provider, market, pair, timeframe] {
        if used == width {
            break;
        }
        if used != 0 {
            if used + 1 == width {
                break;
            }
            identity.push(' ');
            used += 1;
        }
        for character in component.chars().take(width - used) {
            identity.push(character);
            used += 1;
        }
    }
    identity
}

fn write_header_sides(
    buffer: &mut Buffer,
    rect: Rect,
    y: u16,
    left: &str,
    right: &str,
    right_style: Style,
) {
    let width = usize::from(rect.width);
    if width == 0 {
        return;
    }

    let left_width = left.len().min(width);
    let gap = usize::from(left_width != 0 && !right.is_empty() && left_width < width);
    let right_width = right
        .len()
        .min(width.saturating_sub(left_width).saturating_sub(gap));
    let right_x = rect
        .right()
        .saturating_sub(u16::try_from(right_width).unwrap_or(rect.width));
    write_clipped(
        buffer,
        rect,
        rect.x,
        y,
        &left[..left_width],
        Style::default(),
    );
    write_clipped(buffer, rect, right_x, y, &right[..right_width], right_style);
}

fn format_ohlcv(candle: &Candle, time: Option<&str>, width: usize) -> String {
    let prefix = time.map_or_else(String::new, |time| format!("{time}Z "));
    let fixed = prefix.len().saturating_add(14);
    let value_width = width
        .saturating_sub(fixed)
        .checked_div(5)
        .unwrap_or(0)
        .max(1);
    let clip = |value: String| value.chars().take(value_width).collect::<String>();
    format!(
        "{prefix}O:{} H:{} L:{} C:{} V:{}",
        clip(compact_price(candle.open())),
        clip(compact_price(candle.high())),
        clip(compact_price(candle.low())),
        clip(compact_price(candle.close())),
        clip(format_base_volume(candle.base_volume(), value_width)),
    )
}

fn compact_price(value: f64) -> String {
    let magnitude = value.abs();
    let mut text = if magnitude != 0.0 && !(0.0001..1_000_000_000.0).contains(&magnitude) {
        format!("{value:.6e}")
    } else {
        let decimals = if magnitude >= 1_000_000.0 {
            2
        } else if magnitude >= 1.0 {
            6
        } else {
            8
        };
        format!("{value:.decimals$}")
    };
    if !text.contains('e') && text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text.len() <= 14 {
        text
    } else {
        format!("{value:.6e}").chars().take(14).collect()
    }
}

fn clear_rect(rect: Rect, buffer: &mut Buffer) {
    for y in rect.y..rect.bottom() {
        for x in rect.x..rect.right() {
            set_cell(buffer, x, y, " ", Style::default());
        }
    }
}

fn fill_overlay_row(buffer: &mut Buffer, rect: Rect, y: u16, symbol: &str, style: Style) {
    if y < rect.y || y >= rect.bottom() {
        return;
    }
    for x in rect.x..rect.right() {
        set_overlay_cell(buffer, x, y, symbol, style);
    }
}

fn set_overlay_cell(buffer: &mut Buffer, x: u16, y: u16, symbol: &str, style: Style) {
    if buffer
        .cell(Position::new(x, y))
        .is_some_and(|cell| is_price_candle_glyph(cell.symbol()))
    {
        return;
    }
    set_cell(buffer, x, y, symbol, style);
}

fn is_price_candle_glyph(symbol: &str) -> bool {
    matches!(
        symbol,
        "│" | "┃" | "╷" | "╵" | "╻" | "╹" | "╽" | "╿" | "█" | "▓" | "▀" | "▄" | "━"
    )
}

fn fill_row(buffer: &mut Buffer, rect: Rect, y: u16, symbol: &str, style: Style) {
    if y < rect.y || y >= rect.bottom() {
        return;
    }
    for x in rect.x..rect.right() {
        set_cell(buffer, x, y, symbol, style);
    }
}

fn write_padded_row(buffer: &mut Buffer, rect: Rect, y: u16, text: &str, style: Style) {
    if y < rect.y || y >= rect.bottom() {
        return;
    }
    let width = usize::from(rect.width);
    let clipped: String = text.chars().take(width).collect();
    let padded = format!("{clipped:<width$}");
    write_clipped(buffer, rect, rect.x, y, &padded, style);
}

fn write_clipped(buffer: &mut Buffer, rect: Rect, x: u16, y: u16, text: &str, style: Style) {
    if y < rect.y || y >= rect.bottom() || x >= rect.right() {
        return;
    }
    let mut column = x.max(rect.x);
    for character in text.chars() {
        if column >= rect.right() {
            break;
        }
        let mut encoded = [0_u8; 4];
        set_cell(
            buffer,
            column,
            y,
            character.encode_utf8(&mut encoded),
            style,
        );
        column = column.saturating_add(1);
    }
}

fn set_cell(buffer: &mut Buffer, x: u16, y: u16, symbol: &str, style: Style) {
    if let Some(cell) = buffer.cell_mut(Position::new(x, y)) {
        cell.reset();
        cell.set_symbol(symbol).set_style(style);
    }
}
