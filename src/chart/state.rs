use std::ops::Range;

use crate::model::{CHART_PRICE_MAX, CandleSeries, MutationSummary};

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
    visible_count: usize,
    right_index: usize,
    right_open_time: i64,
    follow_live: bool,
    auto_y: bool,
    y_range: PriceRange,
    coordinate_hover: Option<CoordinateHover>,
    active_drag: Option<ActiveDrag>,
}

impl ChartViewport {
    #[must_use]
    pub const fn visible_count(&self) -> usize {
        self.visible_count
    }

    #[must_use]
    pub const fn right_index(&self) -> usize {
        self.right_index
    }

    #[must_use]
    pub const fn right_open_time(&self) -> i64 {
        self.right_open_time
    }

    #[must_use]
    pub const fn follows_live(&self) -> bool {
        self.follow_live
    }

    #[must_use]
    pub const fn auto_y(&self) -> bool {
        self.auto_y
    }

    #[must_use]
    pub const fn y_range(&self) -> PriceRange {
        self.y_range
    }

    #[must_use]
    pub const fn coordinate_hover(&self) -> Option<CoordinateHover> {
        self.coordinate_hover
    }

    #[must_use]
    pub const fn active_drag(&self) -> Option<ActiveDrag> {
        self.active_drag
    }
}

impl ChartViewState {
    #[must_use]
    pub fn snapshot(candles: &CandleSeries, plot_width: usize) -> Self {
        Self::initialize(candles, plot_width, false)
    }

    #[must_use]
    pub fn interactive(candles: &CandleSeries, plot_width: usize) -> Self {
        Self::initialize(candles, plot_width, true)
    }

    fn initialize(candles: &CandleSeries, plot_width: usize, interactive: bool) -> Self {
        assert!(plot_width > 0, "plot_width must be positive");
        if candles.is_empty() {
            return Self::Empty;
        }
        let len = candles.len();
        let visible_count = if interactive {
            if len < 10 {
                len.min(plot_width)
            } else {
                let half_width = plot_width / 2;
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
            right_open_time: candles
                .get(right_index)
                .expect("new viewport right edge is in the series")
                .open_time(),
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

    pub fn set_coordinate_hover(&mut self, candles: &CandleSeries, hover: Option<CoordinateHover>) {
        let visible = self.visible_range();
        let checked = hover.filter(|hover| {
            hover.price.is_finite()
                && candles
                    .index_of_open_time(hover.open_time)
                    .is_some_and(|index| visible.contains(&index))
        });
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        view.coordinate_hover = checked;
    }

    pub fn set_active_drag(&mut self, candles: &CandleSeries, drag: Option<ActiveDrag>) {
        let visible = self.visible_range();
        let checked = drag.filter(|drag| {
            drag.anchor_price.is_none_or(f64::is_finite)
                && drag.anchor_open_time.is_none_or(|open_time| {
                    candles
                        .index_of_open_time(open_time)
                        .is_some_and(|index| visible.contains(&index))
                })
        });
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        view.active_drag = checked;
    }

    fn viewport_mut_internal(&mut self) -> Option<&mut ChartViewport> {
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

    pub fn pan_x_older(&mut self, candles: &CandleSeries) {
        self.pan_x(candles, false);
    }

    pub fn pan_x_newer(&mut self, candles: &CandleSeries) {
        self.pan_x(candles, true);
    }

    fn pan_x(&mut self, candles: &CandleSeries, newer: bool) {
        let Some(view) = self.viewport_mut_internal() else {
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
    /// Pans by an exact candle count; positive values move toward newer data.
    pub(super) fn pan_x_by_candles(&mut self, candles: &CandleSeries, delta: isize) {
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let minimum_right = view.visible_count - 1;
        view.right_index = if delta >= 0 {
            view.right_index
                .saturating_add(delta.unsigned_abs())
                .min(candles.len() - 1)
        } else {
            view.right_index
                .saturating_sub(delta.unsigned_abs())
                .max(minimum_right)
        };
        view.follow_live = view.right_index + 1 == candles.len();
        refresh_open_time_and_auto_y(view, candles);
    }

    /// Moves to the next wider integer candle-slot cadence.
    pub fn zoom_x_in(&mut self, candles: &CandleSeries, plot_width: usize) {
        assert!(plot_width > 0, "plot_width must be positive");
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let minimum = Self::min_visible_count(candles.len(), plot_width);
        if view.visible_count <= minimum {
            return;
        }
        let maximum = candles.len().min(plot_width);
        let next_slot_width = plot_width / view.visible_count + 1;
        let next = (plot_width / next_slot_width).clamp(minimum, maximum);
        set_centered_visible_count(view, candles, next);
    }

    /// Moves to the next narrower integer candle-slot cadence.
    pub fn zoom_x_out(&mut self, candles: &CandleSeries, plot_width: usize) {
        assert!(plot_width > 0, "plot_width must be positive");
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let maximum = candles.len().min(plot_width);
        if view.visible_count >= maximum {
            return;
        }
        let minimum = Self::min_visible_count(candles.len(), plot_width);
        let next_slot_width = plot_width / (view.visible_count + 1);
        let next = (plot_width / next_slot_width).clamp(minimum, maximum);
        set_centered_visible_count(view, candles, next);
    }

    /// Snaps a requested zoom factor to the nearest equal-width slot cadence.
    pub fn zoom_x_by_factor(&mut self, candles: &CandleSeries, plot_width: usize, factor: f64) {
        assert!(plot_width > 0, "plot_width must be positive");
        if !factor.is_finite() || factor <= 0.0 {
            return;
        }
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let minimum = Self::min_visible_count(candles.len(), plot_width);
        let maximum = candles.len().min(plot_width);
        let target = round_ties_away(view.visible_count as f64 * factor).clamp(minimum, maximum);
        let next =
            closest_uniform_visible_count(plot_width, target, view.visible_count, minimum, maximum);
        set_centered_visible_count(view, candles, next);
    }

    /// Zooms X around a fixed candle rather than the viewport center.
    pub(super) fn zoom_x_at(
        &mut self,
        candles: &CandleSeries,
        plot_width: usize,
        factor: f64,
        anchor_open_time: i64,
    ) {
        assert!(plot_width > 0, "plot_width must be positive");
        if !factor.is_finite() || factor <= 0.0 {
            return;
        }
        let Some(anchor_index) = candles.index_of_open_time(anchor_open_time) else {
            return;
        };
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let old_left = view.right_index + 1 - view.visible_count;
        let anchor_offset = anchor_index
            .saturating_sub(old_left)
            .min(view.visible_count - 1);
        let minimum = Self::min_visible_count(candles.len(), plot_width);
        let maximum = candles.len().min(plot_width);
        let target = round_ties_away(view.visible_count as f64 * factor).clamp(minimum, maximum);
        let next =
            closest_uniform_visible_count(plot_width, target, view.visible_count, minimum, maximum);
        if next == view.visible_count {
            return;
        }
        let scaled_offset =
            round_ties_away(anchor_offset as f64 * next as f64 / view.visible_count as f64)
                .min(next - 1);
        let left = anchor_index.saturating_sub(scaled_offset);
        view.visible_count = next;
        view.right_index = left
            .saturating_add(next - 1)
            .min(candles.len() - 1)
            .max(next - 1);
        view.follow_live = view.right_index + 1 == candles.len();
        refresh_open_time_and_auto_y(view, candles);
    }
    /// Places a fixed candle in the requested visible slot after interaction
    /// geometry has changed. The slot is clamped only when a series boundary
    /// makes the exact placement impossible.
    pub(super) fn align_x_anchor(
        &mut self,
        candles: &CandleSeries,
        anchor_open_time: i64,
        visible_slot: usize,
    ) {
        let Some(anchor_index) = candles.index_of_open_time(anchor_open_time) else {
            return;
        };
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let visible_slot = visible_slot.min(view.visible_count - 1);
        let maximum_left = candles.len() - view.visible_count;
        let left = anchor_index.saturating_sub(visible_slot).min(maximum_left);
        view.right_index = left + view.visible_count - 1;
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
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let span = view.y_range.span();
        let delta = span * Y_PAN_FRACTION * direction;
        if !delta.is_finite() {
            return;
        }
        if let Some(candidate) =
            normalized_manual_range(view.y_range.low + delta, view.y_range.high + delta)
        {
            view.y_range = candidate;
            view.auto_y = false;
        }
    }
    /// Pans the manual price range by a fraction of its current span.
    pub(super) fn pan_y_by_fraction(&mut self, fraction: f64) {
        if !fraction.is_finite() {
            return;
        }
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let delta = view.y_range.span() * fraction;
        if let Some(candidate) =
            normalized_manual_range(view.y_range.low + delta, view.y_range.high + delta)
        {
            view.y_range = candidate;
            view.auto_y = false;
        }
    }

    pub fn zoom_y_in(&mut self) {
        self.zoom_y_by_factor(Y_ZOOM_IN);
    }
    pub fn zoom_y_out(&mut self) {
        self.zoom_y_by_factor(Y_ZOOM_OUT);
    }

    pub fn zoom_y_by_factor(&mut self, factor: f64) {
        if !factor.is_finite() || factor <= 0.0 {
            return;
        }
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let previous = view.y_range;
        if let Some(candidate) = zoomed_manual_range(previous, factor) {
            view.y_range = candidate;
            view.auto_y = false;
        }
    }
    /// Zooms the manual price range around a fixed price.
    pub(super) fn zoom_y_at(&mut self, factor: f64, anchor_price: f64) {
        if !factor.is_finite() || factor <= 0.0 || !anchor_price.is_finite() {
            return;
        }
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let span = view.y_range.span();
        if !span.is_finite() || span <= 0.0 {
            return;
        }
        let ratio = ((anchor_price - view.y_range.low) / span).clamp(0.0, 1.0);
        let Some(zoomed) = zoomed_manual_range(view.y_range, factor) else {
            return;
        };
        let next_span = zoomed.span();
        let low = anchor_price - next_span * ratio;
        if let Some(candidate) = normalized_manual_range(low, low + next_span) {
            view.y_range = candidate;
            view.auto_y = false;
        }
    }

    pub fn end(&mut self, candles: &CandleSeries) {
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        view.right_index = candles.len() - 1;
        view.follow_live = true;
        refresh_open_time_and_auto_y(view, candles);
    }

    pub fn reset(&mut self, candles: &CandleSeries, plot_width: usize) {
        *self = Self::interactive(candles, plot_width);
    }

    pub fn resize(&mut self, candles: &CandleSeries, plot_width: usize) {
        assert!(plot_width > 0, "plot_width must be positive");
        let Some(view) = self.viewport_mut_internal() else {
            return;
        };
        let was_following = view.follow_live;
        let center_open_time = candles
            .get(logical_center_index(view))
            .expect("viewport center is in the series")
            .open_time();
        view.visible_count = view.visible_count.min(candles.len()).min(plot_width).max(1);
        if was_following {
            view.right_index = candles.len() - 1;
            view.follow_live = true;
        } else {
            let center_index = candles
                .index_of_open_time(center_open_time)
                .unwrap_or_else(|| insertion_index(candles, center_open_time));
            view.right_index =
                right_index_for_center(center_index, view.visible_count, candles.len());
            view.follow_live = false;
        }
        refresh_open_time_and_auto_y(view, candles);
    }

    pub fn apply_mutation(
        &mut self,
        candles: &CandleSeries,
        summary: &MutationSummary,
        plot_width: usize,
    ) {
        assert!(plot_width > 0, "plot_width must be positive");
        if candles.is_empty() {
            *self = Self::Empty;
            return;
        }
        let Some(view) = self.viewport_mut_internal() else {
            *self = Self::interactive(candles, plot_width);
            return;
        };
        let was_following = view.follow_live;
        let old_anchor_index = logical_center_index(view);
        view.visible_count = view.visible_count.min(candles.len()).min(plot_width).max(1);
        if was_following {
            view.right_index = candles.len() - 1;
            view.follow_live = true;
        } else {
            let anchor_index = resolve_mutation_anchor(candles, summary, old_anchor_index);
            view.right_index =
                right_index_for_center(anchor_index, view.visible_count, candles.len());
            view.follow_live = false;
        }
        view.coordinate_hover = None;
        view.active_drag = None;
        refresh_open_time_and_auto_y(view, candles);
    }
}

fn refresh_open_time_and_auto_y(view: &mut ChartViewport, candles: &CandleSeries) {
    view.right_open_time = candles
        .get(view.right_index)
        .expect("viewport right edge is in the series")
        .open_time();
    if view.auto_y {
        let end = view.right_index + 1;
        view.y_range = auto_y_range(candles, end - view.visible_count..end);
    }
}

#[must_use]
pub fn auto_y_range(candles: &CandleSeries, range: Range<usize>) -> PriceRange {
    let Some(mut visible) = candles.range(range) else {
        return PriceRange {
            low: -EPSILON_SCALE * 0.5,
            high: EPSILON_SCALE * 0.5,
        };
    };
    let Some(first) = visible.next() else {
        return PriceRange {
            low: -EPSILON_SCALE * 0.5,
            high: EPSILON_SCALE * 0.5,
        };
    };
    let mut low = first.low();
    let mut high = first.high();
    for candle in visible {
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

fn normalized_manual_range(low: f64, high: f64) -> Option<PriceRange> {
    if !low.is_finite() || !high.is_finite() {
        return None;
    }
    let center = low * 0.5 + high * 0.5;
    let epsilon = price_epsilon(low, high);
    let raw_span = high - low;
    let span = if raw_span.is_finite() {
        raw_span.clamp(epsilon, MAX_MANUAL_SPAN)
    } else {
        MAX_MANUAL_SPAN
    };
    let half = span * 0.5;
    let max_center = CHART_PRICE_MAX + half;
    let center = center.clamp(-max_center, max_center);
    let range = PriceRange {
        low: center - half,
        high: center + half,
    };
    (range.low.is_finite() && range.high.is_finite() && range.high > range.low).then_some(range)
}

fn zoomed_manual_range(range: PriceRange, factor: f64) -> Option<PriceRange> {
    let span = range.span();
    if !span.is_finite() || span <= 0.0 || !range.center().is_finite() {
        return None;
    }
    let epsilon = price_epsilon(range.low, range.high);
    let target_span = if factor >= MAX_MANUAL_SPAN / span {
        MAX_MANUAL_SPAN
    } else if factor <= epsilon / span {
        epsilon
    } else {
        span * factor
    };
    let half = target_span * 0.5;
    normalized_manual_range(range.center() - half, range.center() + half)
}

/// Chooses the `floor(plot_width / slot_width)` level nearest a requested
/// candle count. Direction breaks exact ties so a drag never reverses itself.
fn closest_uniform_visible_count(
    plot_width: usize,
    target: usize,
    current: usize,
    minimum: usize,
    maximum: usize,
) -> usize {
    debug_assert!(0 < minimum && minimum <= target && target <= maximum);
    debug_assert!(maximum <= plot_width);
    if target == current {
        return current;
    }

    let floor_pitch = plot_width / target;
    let ceil_pitch = plot_width.div_ceil(target);
    let at_or_above = (plot_width / floor_pitch).clamp(minimum, maximum);
    let at_or_below = (plot_width / ceil_pitch).clamp(minimum, maximum);
    let above_distance = at_or_above.abs_diff(target);
    let below_distance = at_or_below.abs_diff(target);
    let nearest = if above_distance < below_distance {
        at_or_above
    } else if below_distance < above_distance {
        at_or_below
    } else if target < current {
        at_or_below.min(at_or_above)
    } else {
        at_or_below.max(at_or_above)
    };

    if target < current {
        nearest.min(current)
    } else {
        nearest.max(current)
    }
}

fn set_centered_visible_count(
    view: &mut ChartViewport,
    candles: &CandleSeries,
    visible_count: usize,
) {
    if visible_count == view.visible_count {
        return;
    }
    let was_following = view.follow_live;
    let anchor_index = logical_center_index(view);
    view.visible_count = visible_count;
    if was_following {
        view.right_index = candles.len() - 1;
        view.follow_live = true;
    } else {
        view.right_index = right_index_for_center(anchor_index, visible_count, candles.len());
        view.follow_live = view.right_index + 1 == candles.len();
    }
    refresh_open_time_and_auto_y(view, candles);
}

fn logical_center_index(view: &ChartViewport) -> usize {
    let left = view.right_index + 1 - view.visible_count;
    left + view.visible_count / 2
}

fn right_index_for_center(center_index: usize, visible_count: usize, series_len: usize) -> usize {
    let cells_right = visible_count - 1 - visible_count / 2;
    center_index
        .saturating_add(cells_right)
        .min(series_len - 1)
        .max(visible_count - 1)
}

fn insertion_index(candles: &CandleSeries, open_time: i64) -> usize {
    candles
        .iter()
        .position(|candle| candle.open_time() >= open_time)
        .unwrap_or_else(|| candles.len() - 1)
}

fn resolve_mutation_anchor(
    candles: &CandleSeries,
    summary: &MutationSummary,
    old_anchor_index: usize,
) -> usize {
    if let Some(mapped) = summary.old_to_new.map(old_anchor_index)
        && mapped < candles.len()
    {
        return mapped;
    }
    summary
        .resolved
        .iter()
        .find_map(|resolved| {
            (resolved.final_index < candles.len()
                && candles
                    .get(resolved.final_index)
                    .is_some_and(|candle| candle.open_time() == resolved.open_time))
            .then_some(resolved.final_index)
        })
        .unwrap_or_else(|| old_anchor_index.min(candles.len() - 1))
}

fn round_ties_away(value: f64) -> usize {
    value.round().max(0.0) as usize
}

/// Computes a repeated zoom multiplier without ever overflowing.
#[must_use]
pub fn bounded_zoom_factor(base: f64, steps: usize, limit: f64) -> f64 {
    if !base.is_finite() || base <= 0.0 || !limit.is_finite() || limit < 1.0 || steps == 0 {
        return 1.0;
    }
    if base == 1.0 {
        return 1.0;
    }
    if base < 1.0 {
        let lower_bound = limit.recip();
        let maximum_steps = (lower_bound.ln() / base.ln()).floor().max(0.0) as usize;
        let mut applied_steps = steps.min(maximum_steps);
        let mut factor = base.powf(applied_steps as f64);

        // Correct logarithm/power rounding at the boundary without walking the
        // requested steps or evaluating the first crossing (possibly underflowing) power.
        while factor < lower_bound {
            factor /= base;
            applied_steps = applied_steps.saturating_sub(1);
        }
        while applied_steps < steps && factor >= lower_bound / base {
            factor *= base;
            applied_steps += 1;
        }
        return factor;
    }

    let power_within_limit = |mut exponent: usize| {
        let mut result = 1.0;
        let mut power = base;
        let mut power_exceeds_limit = false;

        while exponent != 0 {
            if exponent & 1 == 1 {
                if power_exceeds_limit || result > limit / power {
                    return None;
                }
                result *= power;
                if !result.is_finite() || result > limit {
                    return None;
                }
            }

            exponent >>= 1;
            if exponent != 0 {
                if power_exceeds_limit || power > limit / power {
                    power_exceeds_limit = true;
                } else {
                    power *= power;
                }
            }
        }

        Some(result)
    };

    let mut lower = 0;
    let mut upper = steps;
    while lower < upper {
        let middle = lower + (upper - lower).div_ceil(2);
        if power_within_limit(middle).is_some() {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }

    power_within_limit(lower).unwrap_or(1.0)
}
