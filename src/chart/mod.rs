//! Chart boundary.
pub mod format;
pub mod interaction;
pub mod layout;
pub mod slot_geometry;
pub mod state;
pub mod widget;
pub use format::{
    PriceTick, UtcLabel, UtcLabelFormat, format_base_volume, format_utc_timestamp, price_ticks,
    select_utc_labels, select_utc_labels_indexed, utc_label_format,
};
pub use interaction::{InteractionAction, InteractionController};
pub use layout::{
    ChartHitRegion, ChartLayout, ChartLayoutResult, LayoutMode, MIN_CHART_HEIGHT, MIN_CHART_WIDTH,
    PRICE_AXIS_GUTTER, PRICE_LABEL_BUDGET, calculate_chart_layout, contains,
};
pub use slot_geometry::{CandleSlot, CandleSlotGeometry};
pub use state::{
    ActiveDrag, ChartViewState, ChartViewport, CoordinateHover, DragKind, InteractiveChartState,
    PriceRange, auto_y_range, bounded_zoom_factor,
};
pub use widget::{
    ChartWidget, DisplayStatus, EMPTY_MESSAGE, FOOTER_MESSAGE, RESIZE_MESSAGE, RenderMode,
    RenderPolicy, RendererSnapshot, detect_render_policy, no_color_present,
};
