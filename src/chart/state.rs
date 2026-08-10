use std::ops::Range;

use crate::model::{CHART_PRICE_MAX, Candle, MutationSummary};

const X_ZOOM_IN: f64 = 0.8;
const X_ZOOM_OUT: f64 = 1.25;
const Y_ZOOM_IN: f64 = 0.8;
const Y_ZOOM_OUT: f64 = 1.25;
const X_PAN_FRACTION: f64 = 0.05;
const Y_PAN_FRACTION: f64 = 0.10;
const AUTO_Y_PADDING: f64 = 0.05;
const EPSILON_SCALE: f64 = 1.0 / ((1_u64 << 40) as f64);
const MAX_MANUAL_SPAN: f64 = 1.10 * (2.0 * CHART_PRICE_MAX);

#[derive(Clone, Debug, PartialEq)]
pub enum InteractiveChartState {
    LayoutPending,
    Ready(ChartViewState),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PriceRange {
    pub low: f64,
    pub high: f64,
}

impl PriceRange {
    #[must_use]
    pub fn span(self) -> f64 {
        self.high - self.low
    }

    #[must_use]
    pub fn center(self) -> f64 {
        self.low + self.span() * 0.5
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CoordinateHover {
    pub open_time: i64,
    pub price: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DragKind {
    Plot,
    PriceAxis,
    TimeAxis,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ActiveDrag {
    pub kind: DragKind,
    pub anchor_open_time: Option<i64>,
    pub anchor_price: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ChartViewState {
    Empty,
    Data(ChartViewport),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChartViewport {
    pub visible_count: usize,
    pub right_index: usize,
    pub right_open_time: i64,
    pub follow_live: bool,
    pub auto_y: bool,
    pub y_range: PriceRange,
    pub coordinate_hover: Option<CoordinateHover>,
    pub active_drag: Option<ActiveDrag>,
}

impl ChartViewState {
    #[must_use]
    pub fn snapshot(candles: &[Candle], plot_width: usize) -> Self {
        Self::initialize(candles, plot_width, false)
    }

    #[must_use]
    pub fn interactive(candles: &[Candle], plot_width: usize) -> Self {
        Self::initialize(candles, plot_width, true)
    }

    fn initialize(candles: &[Candle], plot_width: usize, interactive: bool) -> Self {
        assert!(plot_width > 0, "plot_width must be positive");
        if candles.is_empty() {
            return Self::Empty;
        }
        let len = candles.len();
        let visible_count = if interactive {
            if len < 10 {
                len.min(plot_width)
            } else {
                let half_width = plot_width.div_ceil(2);
                len.min(plot_width).min(10_usize.max(half_width))
            }
        } else {
            len.min(plot_width)
        };
        let right_index = len - 1;
        let y_range = auto_y_range(candles, right_index + 1 - visible_count..right_index + 1);
        Self::Data(ChartViewport {
            visible_count,
            right_index,
            right_open_time: candles[right_index].open_time(),
            follow_live: true,
            auto_y: true,
            y_range,
            coordinate_hover: None,
            active_drag: None,
        })
    }

    #[must_use]
    pub fn viewport(&self) -> Option<&ChartViewport> {
        match self {
            Self::Empty => None,
            Self::Data(view) => Some(view),
        }
    }

    pub fn viewport_mut(&mut self) -> Option<&mut ChartViewport> {
        match self {
            Self::Empty => None,
            Self::Data(view) => Some(view),
        }
    }

    #[must_use]
    pub fn visible_range(&self) -> Range<usize> {
        let Some(view) = self.viewport() else {
            return 0..0;
        };
        let end = view.right_index.saturating_add(1);
        end.saturating_sub(view.visible_count)..end
    }

    #[must_use]
    pub fn min_visible_count(series_len: usize, plot_width: usize) -> usize {
        series_len.min(plot_width).min(10)
    }

    pub fn pan_x_older(&mut self, candles: &[Candle]) {
        self.pan_x(candles, false);
    }

    pub fn pan_x_newer(&mut self, candles: &[Candle]) {
        self.pan_x(candles, true);
    }

    fn pan_x(&mut self, candles: &[Candle], newer: bool) {
        let Some(view) = self.viewport_mut() else {
            return;
        };
        let step = ((view.visible_count as f64) * X_PAN_FRACTION).ceil() as usize;
        let step = step.max(1);
        view.right_index = if newer {
            view.right_index.saturating_add(step).min(candles.len() - 1)
        } else {
            view.right_index
                .saturating_sub(step)
                .max(view.visible_count - 1)
        };
        view.follow_live = view.right_index + 1 == candles.len();
        refresh_open_time_and_auto_y(view, candles);
    }

    pub fn zoom_x_in(&mut self, candles: &[Candle], plot_width: usize) {
        self.zoom_x_by_factor(candles, plot_width, X_ZOOM_IN);
    }

    pub fn zoom_x_out(&mut self, candles: &[Candle], plot_width: usize) {
        self.zoom_x_by_factor(candles, plot_width, X_ZOOM_OUT);
    }

    pub fn zoom_x_by_factor(&mut self, candles: &[Candle], plot_width: usize, factor: f64) {
        assert!(plot_width > 0, "plot_width must be positive");
        let Some(view) = self.viewport_mut() else {
            return;
        };
        let minimum = Self::min_visible_count(candles.len(), plot_width);
        let maximum = candles.len().min(plot_width);
        let next = round_ties_away(view.visible_count as f64 * factor).clamp(minimum, maximum);
        if next == view.visible_count {
            return;
        }
        let old_left = view.right_index + 1 - view.visible_count;
        let center = old_left as f64 + (view.visible_count.saturating_sub(1) as f64 * 0.5);
        view.visible_count = next;
        let half = view.visible_count.saturating_sub(1) as f64 * 0.5;
        view.right_index = round_ties_away(center + half).min(candles.len() - 1);
        view.right_index = view.right_index.max(view.visible_count - 1);
        view.follow_live = view.right_index + 1 == candles.len();
        refresh_open_time_and_auto_y(view, candles);
    }

    pub fn pan_y_up(&mut self) {
        self.pan_y(1.0);
    }
    pub fn pan_y_down(&mut self) {
        self.pan_y(-1.0);
    }

    fn pan_y(&mut self, direction: f64) {
        let Some(view) = self.viewport_mut() else {
            return;
        };
        let delta = view.y_range.span() * Y_PAN_FRACTION * direction;
        view.y_range = normalized_manual_range(view.y_range.low + delta, view.y_range.high + delta);
        view.auto_y = false;
    }

    pub fn zoom_y_in(&mut self) {
        self.zoom_y_by_factor(Y_ZOOM_IN);
    }
    pub fn zoom_y_out(&mut self) {
        self.zoom_y_by_factor(Y_ZOOM_OUT);
    }

    pub fn zoom_y_by_factor(&mut self, factor: f64) {
        let Some(view) = self.viewport_mut() else {
            return;
        };
        let center = view.y_range.center();
        let half = view.y_range.span() * factor * 0.5;
        view.y_range = normalized_manual_range(center - half, center + half);
        view.auto_y = false;
    }

    pub fn end(&mut self, candles: &[Candle]) {
        let Some(view) = self.viewport_mut() else {
            return;
        };
        view.right_index = candles.len() - 1;
        view.follow_live = true;
        refresh_open_time_and_auto_y(view, candles);
    }

    pub fn reset(&mut self, candles: &[Candle], plot_width: usize) {
        *self = Self::interactive(candles, plot_width);
    }

    pub fn resize(&mut self, candles: &[Candle], plot_width: usize) {
        assert!(plot_width > 0, "plot_width must be positive");
        let Some(view) = self.viewport_mut() else {
            return;
        };
        let old_left = view.right_index + 1 - view.visible_count;
        let center_open_time = candles[old_left + view.visible_count / 2].open_time();
        view.visible_count = view.visible_count.min(candles.len()).min(plot_width).max(1);
        let center_index = candles
            .binary_search_by_key(&center_open_time, Candle::open_time)
            .unwrap_or_else(|i| i.min(candles.len() - 1));
        view.right_index = center_index
            .saturating_add(view.visible_count / 2)
            .min(candles.len() - 1)
            .max(view.visible_count - 1);
        if view.follow_live {
            view.right_index = candles.len() - 1;
        }
        refresh_open_time_and_auto_y(view, candles);
    }

    pub fn apply_mutation(
        &mut self,
        candles: &[Candle],
        summary: &MutationSummary,
        plot_width: usize,
    ) {
        assert!(plot_width > 0, "plot_width must be positive");
        if candles.is_empty() {
            *self = Self::Empty;
            return;
        }
        let Some(view) = self.viewport_mut() else {
            *self = Self::interactive(candles, plot_width);
            return;
        };
        let old_right = view.right_index;
        let mapped = summary.old_to_new.map(old_right).or_else(|| {
            candles
                .binary_search_by_key(&view.right_open_time, Candle::open_time)
                .ok()
        });
        view.right_index = if view.follow_live {
            candles.len() - 1
        } else {
            mapped.unwrap_or_else(|| old_right.min(candles.len() - 1))
        };
        view.visible_count = view.visible_count.min(candles.len()).min(plot_width).max(1);
        view.right_index = view
            .right_index
            .max(view.visible_count - 1)
            .min(candles.len() - 1);
        view.coordinate_hover = None;
        view.active_drag = None;
        refresh_open_time_and_auto_y(view, candles);
    }
}

fn refresh_open_time_and_auto_y(view: &mut ChartViewport, candles: &[Candle]) {
    view.right_open_time = candles[view.right_index].open_time();
    if view.auto_y {
        let end = view.right_index + 1;
        view.y_range = auto_y_range(candles, end - view.visible_count..end);
    }
}

#[must_use]
pub fn auto_y_range(candles: &[Candle], range: Range<usize>) -> PriceRange {
    if range.is_empty() || range.end > candles.len() {
        return PriceRange {
            low: -EPSILON_SCALE * 0.5,
            high: EPSILON_SCALE * 0.5,
        };
    }
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    for candle in &candles[range] {
        low = low.min(candle.low());
        high = high.max(candle.high());
    }
    padded_range(low, high)
}

fn padded_range(low: f64, high: f64) -> PriceRange {
    let epsilon = price_epsilon(low, high);
    let raw = high - low;
    if raw.is_finite() && raw > epsilon {
        normalized_auto_range(
            low - AUTO_Y_PADDING * raw,
            high + AUTO_Y_PADDING * raw,
            epsilon,
        )
    } else {
        let center = low * 0.5 + high * 0.5;
        normalized_auto_range(center - epsilon * 0.5, center + epsilon * 0.5, epsilon)
    }
}

fn price_epsilon(low: f64, high: f64) -> f64 {
    low.abs()
        .max(high.abs())
        .max(1.0)
        .mul_add(EPSILON_SCALE, 0.0)
        .max(f64::MIN_POSITIVE)
}

fn normalized_auto_range(low: f64, high: f64, epsilon: f64) -> PriceRange {
    if low.is_finite() && high.is_finite() && high > low {
        return PriceRange { low, high };
    }
    let center = (low.clamp(-CHART_PRICE_MAX, CHART_PRICE_MAX) * 0.5)
        + (high.clamp(-CHART_PRICE_MAX, CHART_PRICE_MAX) * 0.5);
    PriceRange {
        low: center - epsilon * 0.5,
        high: center + epsilon * 0.5,
    }
}

fn normalized_manual_range(low: f64, high: f64) -> PriceRange {
    let center = low * 0.5 + high * 0.5;
    let epsilon = price_epsilon(low, high);
    let span = (high - low).clamp(epsilon, MAX_MANUAL_SPAN);
    let half = span * 0.5;
    let max_center = CHART_PRICE_MAX + half;
    let center = center.clamp(-max_center, max_center);
    PriceRange {
        low: center - half,
        high: center + half,
    }
}

fn round_ties_away(value: f64) -> usize {
    value.round().max(0.0) as usize
}

/// Computes a repeated zoom multiplier without ever overflowing.
#[must_use]
pub fn bounded_zoom_factor(base: f64, steps: usize, limit: f64) -> f64 {
    if !base.is_finite() || base <= 0.0 || !limit.is_finite() || limit < 1.0 {
        return 1.0;
    }
    let mut factor = 1.0;
    for _ in 0..steps {
        let next = factor * base;
        if !next.is_finite() || next > limit {
            break;
        }
        factor = next;
    }
    factor
}
