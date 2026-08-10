//! Pure retained chart layout and hit-region ownership.

use ratatui::layout::{Position, Rect, Size};

pub const MIN_CHART_WIDTH: u16 = 60;
pub const MIN_CHART_HEIGHT: u16 = 18;
pub const PRICE_LABEL_BUDGET: u16 = 14;
pub const PRICE_AXIS_GUTTER: u16 = 1;

const HEADER_HEIGHT: u16 = 2;
const UTC_AXIS_HEIGHT: u16 = 1;
const INTERACTIVE_FOOTER_HEIGHT: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutMode {
    Snapshot,
    Interactive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChartLayout {
    pub frame: Rect,
    pub header: Rect,
    pub main_plot: Rect,
    pub volume: Rect,
    pub gutter: Rect,
    pub price_axis: Rect,
    pub utc_axis: Rect,
    pub footer: Option<Rect>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChartLayoutResult {
    LayoutPending { required: Size, actual: Size },
    Ready { layout: ChartLayout },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChartHitRegion {
    MainPlot,
    PriceAxis,
    UtcAxis,
}

impl ChartLayout {
    /// Returns the sole interactive owner of a coordinate.
    ///
    /// All retained rectangles use universal half-open containment. Header,
    /// volume, gutter, footer, and coordinates outside the frame are inert.
    #[must_use]
    pub fn hit_region(&self, position: Position) -> Option<ChartHitRegion> {
        if !contains(self.frame, position) {
            return None;
        }
        if contains(self.main_plot, position) {
            return Some(ChartHitRegion::MainPlot);
        }
        if contains(self.price_axis, position) {
            return Some(ChartHitRegion::PriceAxis);
        }
        if contains(self.utc_axis, position) {
            return Some(ChartHitRegion::UtcAxis);
        }
        None
    }
}

/// Computes the retained chart rectangles without mutating application state.
#[must_use]
pub fn calculate_chart_layout(frame: Rect, mode: LayoutMode) -> ChartLayoutResult {
    let actual = Size::new(frame.width, frame.height);
    let required = Size::new(MIN_CHART_WIDTH, MIN_CHART_HEIGHT);
    if frame.width < MIN_CHART_WIDTH || frame.height < MIN_CHART_HEIGHT {
        return ChartLayoutResult::LayoutPending { required, actual };
    }

    let footer_height = match mode {
        LayoutMode::Snapshot => 0,
        LayoutMode::Interactive => INTERACTIVE_FOOTER_HEIGHT,
    };
    let gutter_width = if frame.width >= 2 {
        PRICE_AXIS_GUTTER
    } else {
        0
    };
    let price_width =
        PRICE_LABEL_BUDGET.min(frame.width.saturating_sub(gutter_width.saturating_add(1)));
    let plot_width = frame
        .width
        .saturating_sub(gutter_width)
        .saturating_sub(price_width);

    let reserved_height = HEADER_HEIGHT
        .saturating_add(UTC_AXIS_HEIGHT)
        .saturating_add(footer_height);
    let chart_height = frame.height.saturating_sub(reserved_height);
    debug_assert!(chart_height >= 4);
    let volume_height = chart_height
        .saturating_add(2)
        .checked_div(5)
        .unwrap_or(0)
        .clamp(3, chart_height.saturating_sub(1));
    let main_plot_height = chart_height.saturating_sub(volume_height);

    let chart_y = frame.y.saturating_add(HEADER_HEIGHT);
    let volume_y = chart_y.saturating_add(main_plot_height);
    let utc_y = chart_y.saturating_add(chart_height);
    let axis_x = frame.x.saturating_add(plot_width);
    let price_x = axis_x.saturating_add(gutter_width);

    ChartLayoutResult::Ready {
        layout: ChartLayout {
            frame,
            header: Rect::new(frame.x, frame.y, frame.width, HEADER_HEIGHT),
            main_plot: Rect::new(frame.x, chart_y, plot_width, main_plot_height),
            volume: Rect::new(frame.x, volume_y, plot_width, volume_height),
            gutter: Rect::new(axis_x, chart_y, gutter_width, chart_height),
            price_axis: Rect::new(price_x, chart_y, price_width, chart_height),
            utc_axis: Rect::new(frame.x, utc_y, plot_width, UTC_AXIS_HEIGHT),
            footer: match mode {
                LayoutMode::Snapshot => None,
                LayoutMode::Interactive => Some(Rect::new(
                    frame.x,
                    utc_y.saturating_add(UTC_AXIS_HEIGHT),
                    frame.width,
                    INTERACTIVE_FOOTER_HEIGHT,
                )),
            },
        },
    }
}

#[must_use]
pub fn contains(rect: Rect, position: Position) -> bool {
    let right = u32::from(rect.x) + u32::from(rect.width);
    let bottom = u32::from(rect.y) + u32::from(rect.height);
    let x = u32::from(position.x);
    let y = u32::from(position.y);
    x >= u32::from(rect.x) && x < right && y >= u32::from(rect.y) && y < bottom
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(frame: Rect, mode: LayoutMode) -> ChartLayout {
        match calculate_chart_layout(frame, mode) {
            ChartLayoutResult::Ready { layout } => layout,
            pending => panic!("expected ready layout, got {pending:?}"),
        }
    }

    #[test]
    fn minimum_guard_precedes_all_reservations() {
        assert_eq!(
            calculate_chart_layout(Rect::new(9, 13, 59, 18), LayoutMode::Snapshot),
            ChartLayoutResult::LayoutPending {
                required: Size::new(60, 18),
                actual: Size::new(59, 18),
            }
        );
        assert_eq!(
            calculate_chart_layout(Rect::new(9, 13, 60, 17), LayoutMode::Interactive),
            ChartLayoutResult::LayoutPending {
                required: Size::new(60, 18),
                actual: Size::new(60, 17),
            }
        );
    }

    #[test]
    fn exact_60_by_18_snapshot_layout_at_nonzero_origin() {
        let frame = Rect::new(7, 11, 60, 18);
        let layout = ready(frame, LayoutMode::Snapshot);
        assert_eq!(
            layout,
            ChartLayout {
                frame,
                header: Rect::new(7, 11, 60, 2),
                main_plot: Rect::new(7, 13, 45, 12),
                volume: Rect::new(7, 25, 45, 3),
                gutter: Rect::new(52, 13, 1, 15),
                price_axis: Rect::new(53, 13, 14, 15),
                utc_axis: Rect::new(7, 28, 45, 1),
                footer: None,
            }
        );
        assert_eq!(layout.main_plot.width, 45);
    }

    #[test]
    fn exact_60_by_18_interactive_layout() {
        let frame = Rect::new(3, 5, 60, 18);
        let layout = ready(frame, LayoutMode::Interactive);
        assert_eq!(layout.header, Rect::new(3, 5, 60, 2));
        assert_eq!(layout.main_plot, Rect::new(3, 7, 45, 11));
        assert_eq!(layout.volume, Rect::new(3, 18, 45, 3));
        assert_eq!(layout.gutter, Rect::new(48, 7, 1, 14));
        assert_eq!(layout.price_axis, Rect::new(49, 7, 14, 14));
        assert_eq!(layout.utc_axis, Rect::new(3, 21, 45, 1));
        assert_eq!(layout.footer, Some(Rect::new(3, 22, 60, 1)));
    }

    #[test]
    fn exact_80_by_24_snapshot_layout_and_half_open_hits() {
        let frame = Rect::new(4, 6, 80, 24);
        let layout = ready(frame, LayoutMode::Snapshot);
        assert_eq!(layout.main_plot, Rect::new(4, 8, 65, 17));
        assert_eq!(layout.volume, Rect::new(4, 25, 65, 4));
        assert_eq!(layout.gutter, Rect::new(69, 8, 1, 21));
        assert_eq!(layout.price_axis, Rect::new(70, 8, 14, 21));
        assert_eq!(layout.utc_axis, Rect::new(4, 29, 65, 1));
        assert_eq!(layout.footer, None);

        assert_eq!(
            layout.hit_region(Position::new(4, 8)),
            Some(ChartHitRegion::MainPlot)
        );
        assert_eq!(
            layout.hit_region(Position::new(68, 24)),
            Some(ChartHitRegion::MainPlot)
        );
        assert_eq!(layout.hit_region(Position::new(69, 24)), None);
        assert_eq!(
            layout.hit_region(Position::new(70, 8)),
            Some(ChartHitRegion::PriceAxis)
        );
        assert_eq!(
            layout.hit_region(Position::new(83, 28)),
            Some(ChartHitRegion::PriceAxis)
        );
        assert_eq!(layout.hit_region(Position::new(84, 28)), None);
        assert_eq!(
            layout.hit_region(Position::new(4, 29)),
            Some(ChartHitRegion::UtcAxis)
        );
        assert_eq!(
            layout.hit_region(Position::new(68, 29)),
            Some(ChartHitRegion::UtcAxis)
        );
        assert_eq!(layout.hit_region(Position::new(69, 29)), None);
    }

    #[test]
    fn exact_120_by_36_interactive_layout_and_inert_regions() {
        let frame = Rect::new(10, 20, 120, 36);
        let layout = ready(frame, LayoutMode::Interactive);
        assert_eq!(layout.main_plot, Rect::new(10, 22, 105, 26));
        assert_eq!(layout.volume, Rect::new(10, 48, 105, 6));
        assert_eq!(layout.gutter, Rect::new(115, 22, 1, 32));
        assert_eq!(layout.price_axis, Rect::new(116, 22, 14, 32));
        assert_eq!(layout.utc_axis, Rect::new(10, 54, 105, 1));
        assert_eq!(layout.footer, Some(Rect::new(10, 55, 120, 1)));

        for position in [
            Position::new(10, 20),
            Position::new(10, 48),
            Position::new(115, 22),
            Position::new(10, 55),
            Position::new(130, 20),
            Position::new(10, 56),
        ] {
            assert_eq!(layout.hit_region(position), None, "position {position:?}");
        }
    }
}
