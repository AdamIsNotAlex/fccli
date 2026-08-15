//! Pure keyboard and mouse interaction mapping.

use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::Position;

use crate::model::CandleSeries;

use super::{
    ActiveDrag, CandleSlotGeometry, ChartHitRegion, ChartLayout, ChartViewState, CoordinateHover,
    DragKind, PriceRange, bounded_zoom_factor,
};

const KEYBOARD_Y_ZOOM_IN: f64 = 0.8;
const KEYBOARD_Y_ZOOM_OUT: f64 = 1.25;
const MOUSE_ZOOM_STEP: f64 = 1.05;

/// Result of mapping one terminal input event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractionAction {
    /// The event was ignored and did not change interaction state.
    Ignored,
    /// The chart or retained pointer state changed and should be redrawn.
    Redraw,
    /// Request the common graceful-shutdown path.
    Quit,
}

#[derive(Clone, Debug)]
struct DragStart {
    kind: DragKind,
    position: Position,
    view: ChartViewState,
    anchor_open_time: Option<i64>,
    anchor_price: Option<f64>,
}

/// Retained, provider-neutral keyboard and pointer mapper.
#[derive(Clone, Debug, Default)]
pub struct InteractionController {
    pointer: Option<Position>,
    drag: Option<DragStart>,
}

impl InteractionController {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pointer: None,
            drag: None,
        }
    }

    #[must_use]
    pub const fn pointer(&self) -> Option<Position> {
        self.pointer
    }

    #[must_use]
    pub fn is_dragging(&self) -> bool {
        self.drag.is_some()
    }

    /// Maps a key press/repeat. Releases are ignored to avoid platform duplicates.
    pub fn key(
        &mut self,
        event: KeyEvent,
        state: &mut ChartViewState,
        candles: &CandleSeries,
        layout: &ChartLayout,
    ) -> InteractionAction {
        if event.kind == KeyEventKind::Release {
            return InteractionAction::Ignored;
        }
        let modifiers = event.modifiers;
        let code = match event.code {
            KeyCode::Char(character)
                if modifiers == KeyModifiers::SHIFT && character.is_ascii_lowercase() =>
            {
                KeyCode::Char(character.to_ascii_uppercase())
            }
            code => code,
        };
        let ordinary_char = modifiers == KeyModifiers::NONE || modifiers == KeyModifiers::SHIFT;
        let unmodified = modifiers == KeyModifiers::NONE;
        let plain_quit = unmodified && matches!(code, KeyCode::Char('q') | KeyCode::Esc);
        let control_quit = matches!(code, KeyCode::Char('c' | 'C' | 'd' | 'D'))
            && (modifiers == KeyModifiers::CONTROL
                || modifiers == (KeyModifiers::CONTROL | KeyModifiers::SHIFT));
        if plain_quit || control_quit {
            return InteractionAction::Quit;
        }

        let plot_width = usize::from(layout.main_plot.width);
        let changed = match code {
            KeyCode::Char('a' | 'A') if ordinary_char => {
                state.pan_x_older(candles);
                true
            }
            KeyCode::Char('d' | 'D') if ordinary_char => {
                state.pan_x_newer(candles);
                true
            }
            KeyCode::Char('w' | 'W') if ordinary_char => {
                state.pan_y_up();
                true
            }
            KeyCode::Char('s' | 'S') if ordinary_char => {
                state.pan_y_down();
                true
            }
            KeyCode::Left if unmodified => {
                state.pan_x_older(candles);
                true
            }
            KeyCode::Right if unmodified => {
                state.pan_x_newer(candles);
                true
            }
            KeyCode::Up if unmodified => {
                state.pan_y_up();
                true
            }
            KeyCode::Down if unmodified => {
                state.pan_y_down();
                true
            }
            KeyCode::Char('h') if ordinary_char => {
                state.zoom_x_in(candles, plot_width);
                true
            }
            KeyCode::Char('H') if ordinary_char => {
                state.zoom_x_out(candles, plot_width);
                true
            }
            KeyCode::Char('v') if ordinary_char => {
                state.zoom_y_by_factor(KEYBOARD_Y_ZOOM_IN);
                true
            }
            KeyCode::Char('V') if ordinary_char => {
                state.zoom_y_by_factor(KEYBOARD_Y_ZOOM_OUT);
                true
            }
            KeyCode::End if unmodified => {
                state.end(candles);
                true
            }
            KeyCode::Char('r') if ordinary_char => {
                state.reset(candles, plot_width);
                true
            }
            _ => false,
        };
        if changed {
            self.reproject(state, candles, layout);
            InteractionAction::Redraw
        } else {
            InteractionAction::Ignored
        }
    }

    /// Maps one mouse event while retaining pointer and drag ownership.
    pub fn mouse(
        &mut self,
        event: MouseEvent,
        state: &mut ChartViewState,
        candles: &CandleSeries,
        layout: &ChartLayout,
    ) -> InteractionAction {
        let raw = Position::new(event.column, event.row);
        self.pointer = Some(raw);
        match event.kind {
            MouseEventKind::Moved => {
                if self.drag.is_some() {
                    return InteractionAction::Ignored;
                }
                self.reproject(state, candles, layout);
                InteractionAction::Redraw
            }
            MouseEventKind::Down(MouseButton::Left) => self.begin_drag(raw, state, candles, layout),
            MouseEventKind::Drag(MouseButton::Left) => {
                self.update_drag(raw, state, candles, layout)
            }
            MouseEventKind::Up(MouseButton::Left) => {
                if self.drag.take().is_none() {
                    return InteractionAction::Ignored;
                }
                state.set_active_drag(candles, None);
                self.reproject(state, candles, layout);
                InteractionAction::Redraw
            }
            _ => InteractionAction::Ignored,
        }
    }

    /// Cancels interaction state made stale by a series mutation or layout
    /// change, then reprojects the retained pointer through the current view.
    pub fn sync_after_view_change(
        &mut self,
        state: &mut ChartViewState,
        candles: &CandleSeries,
        layout: &ChartLayout,
    ) {
        self.drag = None;
        state.set_active_drag(candles, None);
        self.reproject(state, candles, layout);
    }

    /// Reprojects retained coordinates after a view-only input change.
    fn reproject(&self, state: &mut ChartViewState, candles: &CandleSeries, layout: &ChartLayout) {
        if self.drag.is_some() {
            state.set_coordinate_hover(candles, None);
            return;
        }
        let hover = self
            .pointer
            .and_then(|position| project_hover(state, candles, layout, position));
        state.set_coordinate_hover(candles, hover);
    }

    fn begin_drag(
        &mut self,
        position: Position,
        state: &mut ChartViewState,
        candles: &CandleSeries,
        layout: &ChartLayout,
    ) -> InteractionAction {
        let Some(kind) = layout.hit_region(position).map(|region| match region {
            ChartHitRegion::MainPlot => DragKind::Plot,
            ChartHitRegion::PriceAxis => DragKind::PriceAxis,
            ChartHitRegion::UtcAxis => DragKind::TimeAxis,
        }) else {
            return InteractionAction::Ignored;
        };
        let anchor_open_time = nearest_open_time(state, candles, layout, position.x);
        let anchor_price = price_at_row(state, layout, position.y);
        self.drag = Some(DragStart {
            kind,
            position,
            view: state.clone(),
            anchor_open_time,
            anchor_price,
        });
        state.set_coordinate_hover(candles, None);
        state.set_active_drag(
            candles,
            Some(ActiveDrag {
                kind,
                anchor_open_time,
                anchor_price,
            }),
        );
        InteractionAction::Redraw
    }

    fn update_drag(
        &mut self,
        raw: Position,
        state: &mut ChartViewState,
        candles: &CandleSeries,
        layout: &ChartLayout,
    ) -> InteractionAction {
        let Some(start) = self.drag.as_ref() else {
            return InteractionAction::Ignored;
        };
        let current = clamp_to_frame(raw, layout);
        let dx = i32::from(current.x) - i32::from(start.position.x);
        let dy = i32::from(current.y) - i32::from(start.position.y);
        *state = start.view.clone();
        match start.kind {
            DragKind::Plot => {
                let slots = state.viewport().map_or(0, |view| view.visible_count());
                let candle_delta = if slots == 0 {
                    0
                } else {
                    let geometry =
                        CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, slots);
                    let initial = geometry.and_then(|g| g.index_at_x(start.position.x));
                    let now = geometry.and_then(|g| {
                        let plot_end = layout
                            .main_plot
                            .x
                            .saturating_add(layout.main_plot.width.saturating_sub(1));
                        g.index_at_x(current.x.clamp(layout.main_plot.x, plot_end))
                    });
                    match (initial, now) {
                        (Some(a), Some(b)) => {
                            isize::try_from(b).unwrap_or(isize::MAX)
                                - isize::try_from(a).unwrap_or(isize::MAX)
                        }
                        _ => 0,
                    }
                };
                state.pan_x_by_candles(candles, -candle_delta);
                if layout.main_plot.height > 1 {
                    state.pan_y_by_fraction(f64::from(dy) / f64::from(layout.main_plot.height - 1));
                }
            }
            DragKind::TimeAxis => {
                let steps = dx.unsigned_abs() as usize;
                let factor = if dx > 0 {
                    bounded_zoom_factor(MOUSE_ZOOM_STEP.recip(), steps, f64::MAX)
                } else {
                    bounded_zoom_factor(MOUSE_ZOOM_STEP, steps, f64::MAX)
                };
                if let Some(anchor) = start.anchor_open_time {
                    state.zoom_x_at(candles, usize::from(layout.main_plot.width), factor, anchor);
                    if let Some(visible_count) = state.viewport().map(|view| view.visible_count())
                        && let Some(anchor_slot) = CandleSlotGeometry::new(
                            layout.main_plot.x,
                            layout.main_plot.width,
                            visible_count,
                        )
                        .and_then(|geometry| geometry.index_at_x(start.position.x))
                    {
                        state.align_x_anchor(candles, anchor, anchor_slot);
                    }
                }
            }
            DragKind::PriceAxis => {
                let steps = dy.unsigned_abs() as usize;
                let factor = if dy < 0 {
                    bounded_zoom_factor(MOUSE_ZOOM_STEP.recip(), steps, f64::MAX)
                } else {
                    bounded_zoom_factor(MOUSE_ZOOM_STEP, steps, f64::MAX)
                };
                if let Some(anchor) = start.anchor_price {
                    state.zoom_y_at(factor, anchor);
                }
            }
        }
        state.set_coordinate_hover(candles, None);
        state.set_active_drag(
            candles,
            Some(ActiveDrag {
                kind: start.kind,
                anchor_open_time: start.anchor_open_time,
                anchor_price: start.anchor_price,
            }),
        );
        InteractionAction::Redraw
    }
}

fn project_hover(
    state: &ChartViewState,
    candles: &CandleSeries,
    layout: &ChartLayout,
    position: Position,
) -> Option<CoordinateHover> {
    if layout.hit_region(position) != Some(ChartHitRegion::MainPlot) {
        return None;
    }
    Some(CoordinateHover {
        open_time: nearest_open_time(state, candles, layout, position.x)?,
        price: price_at_row(state, layout, position.y)?,
    })
}

fn nearest_open_time(
    state: &ChartViewState,
    candles: &CandleSeries,
    layout: &ChartLayout,
    x: u16,
) -> Option<i64> {
    let range = state.visible_range();
    let geometry =
        CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, range.len())?;
    let visible_index = geometry.index_at_x(x)?;
    candles
        .get(range.start + visible_index)
        .map(|candle| candle.open_time())
}

fn price_at_row(state: &ChartViewState, layout: &ChartLayout, y: u16) -> Option<f64> {
    let range: PriceRange = state.viewport()?.y_range();
    if layout.main_plot.height <= 1 {
        return Some(range.center());
    }
    let bottom = layout
        .main_plot
        .y
        .saturating_add(layout.main_plot.height.saturating_sub(1));
    let row = f64::from(y.clamp(layout.main_plot.y, bottom) - layout.main_plot.y);
    let fraction = row / f64::from(layout.main_plot.height - 1);
    Some(range.high - range.span() * fraction)
}

fn clamp_to_frame(position: Position, layout: &ChartLayout) -> Position {
    let max_x = layout
        .frame
        .x
        .saturating_add(layout.frame.width.saturating_sub(1));
    let max_y = layout
        .frame
        .y
        .saturating_add(layout.frame.height.saturating_sub(1));
    Position::new(
        position.x.clamp(layout.frame.x, max_x),
        position.y.clamp(layout.frame.y, max_y),
    )
}
