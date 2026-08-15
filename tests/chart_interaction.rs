use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use fccli::chart::{
    CandleSlotGeometry, ChartLayout, ChartLayoutResult, ChartViewState, InteractionAction,
    InteractionController, LayoutMode, calculate_chart_layout,
};
use fccli::model::{Candle, CandleSeries, Timeframe};
use ratatui::layout::{Position, Rect};

const MINUTE: i64 = 60_000;
const BASE: i64 = 1_700_000_040_000;

fn series(len: usize) -> CandleSeries {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let candles = (0..len)
        .map(|index| {
            let open = BASE + i64::try_from(index).unwrap() * MINUTE;
            Candle::from_rest(
                open,
                open + MINUTE - 1,
                100.0 + index as f64,
                102.0 + index as f64,
                99.0 + index as f64,
                101.0 + index as f64,
                1.0,
            )
            .unwrap()
        })
        .collect();
    series.replace(candles).unwrap();
    series
}

fn layout() -> ChartLayout {
    match calculate_chart_layout(Rect::new(7, 11, 80, 24), LayoutMode::Interactive) {
        ChartLayoutResult::Ready { layout } => layout,
        other => panic!("expected layout, got {other:?}"),
    }
}

fn key(code: KeyCode, kind: KeyEventKind, modifiers: KeyModifiers) -> KeyEvent {
    KeyEvent {
        code,
        modifiers,
        kind,
        state: KeyEventState::NONE,
    }
}

fn mouse(kind: MouseEventKind, x: u16, y: u16) -> MouseEvent {
    MouseEvent {
        kind,
        column: x,
        row: y,
        modifiers: KeyModifiers::NONE,
    }
}
fn assert_close(actual: f64, expected: f64) {
    let tolerance = expected.abs().max(1.0) * 1.0e-12;
    assert!(
        (actual - expected).abs() <= tolerance,
        "actual={actual}, expected={expected}"
    );
}

fn candle(open: i64) -> Candle {
    Candle::from_rest(open, open + MINUTE - 1, 90.0, 92.0, 89.0, 91.0, 1.0).unwrap()
}

#[test]
fn keys_map_press_and_repeat_once_release_is_ignored_and_quit_is_deferred() {
    let candles = series(100);
    let layout = layout();
    let mut state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
    let mut controller = InteractionController::new();

    let before = state.clone();
    assert_eq!(
        controller.key(
            key(KeyCode::Left, KeyEventKind::Release, KeyModifiers::NONE),
            &mut state,
            &candles,
            &layout
        ),
        InteractionAction::Ignored
    );
    assert_eq!(state, before);
    assert_eq!(
        controller.key(
            key(KeyCode::Left, KeyEventKind::Press, KeyModifiers::NONE),
            &mut state,
            &candles,
            &layout
        ),
        InteractionAction::Redraw
    );
    let after_press = state.clone();
    assert_eq!(
        controller.key(
            key(KeyCode::Right, KeyEventKind::Repeat, KeyModifiers::NONE),
            &mut state,
            &candles,
            &layout
        ),
        InteractionAction::Redraw
    );
    assert_ne!(state, after_press);

    for code in [
        KeyCode::Char('a'),
        KeyCode::Char('A'),
        KeyCode::Left,
        KeyCode::Char('d'),
        KeyCode::Char('D'),
        KeyCode::Right,
        KeyCode::Char('w'),
        KeyCode::Char('W'),
        KeyCode::Up,
        KeyCode::Char('s'),
        KeyCode::Char('S'),
        KeyCode::Down,
        KeyCode::Char('h'),
        KeyCode::Char('H'),
        KeyCode::Char('v'),
        KeyCode::Char('V'),
        KeyCode::End,
        KeyCode::Char('r'),
    ] {
        assert_eq!(
            controller.key(
                key(code, KeyEventKind::Press, KeyModifiers::NONE),
                &mut state,
                &candles,
                &layout
            ),
            InteractionAction::Redraw,
            "{code:?}"
        );
    }

    for (code, modifiers) in [
        (KeyCode::Char('q'), KeyModifiers::NONE),
        (KeyCode::Esc, KeyModifiers::NONE),
        (KeyCode::Char('c'), KeyModifiers::CONTROL),
        (KeyCode::Char('d'), KeyModifiers::CONTROL),
    ] {
        assert_eq!(
            controller.key(
                key(code, KeyEventKind::Press, modifiers),
                &mut state,
                &candles,
                &layout
            ),
            InteractionAction::Quit
        );
    }
}

#[test]
fn key_modifiers_are_exact_and_do_not_leak_into_plain_shortcuts() {
    let candles = series(100);
    let layout = layout();

    for (code, modifiers) in [
        (KeyCode::Char('r'), KeyModifiers::CONTROL),
        (KeyCode::Char('v'), KeyModifiers::CONTROL),
        (KeyCode::Char('q'), KeyModifiers::CONTROL),
        (KeyCode::Char('h'), KeyModifiers::ALT),
        (KeyCode::Char('w'), KeyModifiers::ALT),
        (KeyCode::Left, KeyModifiers::CONTROL),
        (KeyCode::End, KeyModifiers::ALT),
        (KeyCode::Esc, KeyModifiers::SUPER),
    ] {
        let mut state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
        let before = state.clone();
        assert_eq!(
            InteractionController::new().key(
                key(code, KeyEventKind::Press, modifiers),
                &mut state,
                &candles,
                &layout,
            ),
            InteractionAction::Ignored,
            "{code:?} {modifiers:?}"
        );
        assert_eq!(state, before, "{code:?} {modifiers:?}");
    }

    for code in ['A', 'D', 'W', 'S', 'H', 'V'] {
        let mut state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
        assert_eq!(
            InteractionController::new().key(
                key(
                    KeyCode::Char(code),
                    KeyEventKind::Press,
                    KeyModifiers::SHIFT,
                ),
                &mut state,
                &candles,
                &layout,
            ),
            InteractionAction::Redraw,
            "shift-{code}"
        );
    }

    for (code, modifiers) in [
        (KeyCode::Char('c'), KeyModifiers::CONTROL),
        (KeyCode::Char('C'), KeyModifiers::CONTROL),
        (KeyCode::Char('d'), KeyModifiers::CONTROL),
        (KeyCode::Char('D'), KeyModifiers::CONTROL),
        (
            KeyCode::Char('c'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ),
        (
            KeyCode::Char('C'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ),
        (
            KeyCode::Char('d'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ),
        (
            KeyCode::Char('D'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ),
    ] {
        let mut state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
        assert_eq!(
            InteractionController::new().key(
                key(code, KeyEventKind::Press, modifiers),
                &mut state,
                &candles,
                &layout,
            ),
            InteractionAction::Quit,
            "{code:?} {modifiers:?}"
        );
    }

    for modifiers in [
        KeyModifiers::CONTROL | KeyModifiers::ALT,
        KeyModifiers::CONTROL | KeyModifiers::SUPER,
        KeyModifiers::CONTROL | KeyModifiers::SHIFT | KeyModifiers::ALT,
    ] {
        for code in [KeyCode::Char('C'), KeyCode::Char('D')] {
            let mut state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
            assert_eq!(
                InteractionController::new().key(
                    key(code, KeyEventKind::Press, modifiers),
                    &mut state,
                    &candles,
                    &layout,
                ),
                InteractionAction::Ignored,
                "{code:?} with extras {modifiers:?}"
            );
        }
    }
}

#[test]
fn lowercase_and_uppercase_wasd_apply_identical_state_changes() {
    let candles = series(100);
    let layout = layout();
    let plot_width = usize::from(layout.main_plot.width);
    let mut baseline = ChartViewState::snapshot(&candles, plot_width);
    baseline.pan_x_older(&candles);
    baseline.pan_x_older(&candles);

    for (lower, upper) in [('a', 'A'), ('d', 'D'), ('w', 'W'), ('s', 'S')] {
        let mut expected = baseline.clone();
        match lower {
            'a' => expected.pan_x_older(&candles),
            'd' => expected.pan_x_newer(&candles),
            'w' => expected.pan_y_up(),
            's' => expected.pan_y_down(),
            _ => unreachable!(),
        }
        assert_ne!(expected, baseline, "{lower} must change state");

        let mut lowercase_state = baseline.clone();
        let lowercase_action = InteractionController::new().key(
            key(
                KeyCode::Char(lower),
                KeyEventKind::Press,
                KeyModifiers::NONE,
            ),
            &mut lowercase_state,
            &candles,
            &layout,
        );
        let mut uppercase_state = baseline.clone();
        let uppercase_action = InteractionController::new().key(
            key(
                KeyCode::Char(upper),
                KeyEventKind::Press,
                KeyModifiers::NONE,
            ),
            &mut uppercase_state,
            &candles,
            &layout,
        );

        assert_eq!(lowercase_action, InteractionAction::Redraw, "{lower}");
        assert_eq!(uppercase_action, InteractionAction::Redraw, "{upper}");
        assert_eq!(lowercase_state, expected, "{lower}");
        assert_eq!(uppercase_state, expected, "{upper}");
    }
}

#[test]
fn shifted_lowercase_ascii_normalizes_before_press_and_repeat_dispatch() {
    let candles = series(100);
    let layout = layout();
    let plot_width = usize::from(layout.main_plot.width);
    let mut baseline = ChartViewState::snapshot(&candles, plot_width);
    baseline.pan_x_older(&candles);
    baseline.pan_x_older(&candles);

    for kind in [KeyEventKind::Press, KeyEventKind::Repeat] {
        for (lower, upper) in [
            ('a', 'A'),
            ('d', 'D'),
            ('w', 'W'),
            ('s', 'S'),
            ('h', 'H'),
            ('v', 'V'),
        ] {
            let mut shifted_state = baseline.clone();
            let shifted_action = InteractionController::new().key(
                key(KeyCode::Char(lower), kind, KeyModifiers::SHIFT),
                &mut shifted_state,
                &candles,
                &layout,
            );
            let mut uppercase_state = baseline.clone();
            let uppercase_action = InteractionController::new().key(
                key(KeyCode::Char(upper), kind, KeyModifiers::NONE),
                &mut uppercase_state,
                &candles,
                &layout,
            );

            assert_eq!(
                shifted_action,
                InteractionAction::Redraw,
                "{kind:?} shift-{lower}"
            );
            assert_eq!(shifted_action, uppercase_action, "{kind:?} shift-{lower}");
            assert_eq!(shifted_state, uppercase_state, "{kind:?} shift-{lower}");
        }

        for lower in ['q', 'r'] {
            let mut state = baseline.clone();
            assert_eq!(
                InteractionController::new().key(
                    key(KeyCode::Char(lower), kind, KeyModifiers::SHIFT),
                    &mut state,
                    &candles,
                    &layout,
                ),
                InteractionAction::Ignored,
                "{kind:?} shift-{lower}"
            );
            assert_eq!(state, baseline, "{kind:?} shift-{lower}");
        }
    }
}

#[test]
fn hover_uses_shared_slots_half_open_regions_and_clears_outside() {
    let candles = series(30);
    let layout = layout();
    let mut state = ChartViewState::interactive(&candles, usize::from(layout.main_plot.width));
    let mut controller = InteractionController::new();
    let position = Position::new(layout.main_plot.x, layout.main_plot.y);
    assert_eq!(
        controller.mouse(
            mouse(MouseEventKind::Moved, position.x, position.y),
            &mut state,
            &candles,
            &layout
        ),
        InteractionAction::Redraw
    );
    let hover = state.viewport().unwrap().coordinate_hover().unwrap();
    assert_eq!(
        hover.open_time,
        candles
            .get(state.visible_range().start)
            .unwrap()
            .open_time()
    );
    assert_eq!(hover.price, state.viewport().unwrap().y_range().high);

    let outside_x = layout.main_plot.x + layout.main_plot.width;
    controller.mouse(
        mouse(MouseEventKind::Moved, outside_x, position.y),
        &mut state,
        &candles,
        &layout,
    );
    assert_eq!(state.viewport().unwrap().coordinate_hover(), None);
    assert_eq!(
        controller.pointer(),
        Some(Position::new(outside_x, position.y))
    );
}

#[test]
fn plot_drag_has_fixed_owner_hides_hover_and_content_follows_downward() {
    let candles = series(100);
    let layout = layout();
    let mut state = ChartViewState::interactive(&candles, usize::from(layout.main_plot.width));
    state.pan_x_older(&candles);
    let mut controller = InteractionController::new();
    let x = layout.main_plot.x + layout.main_plot.width / 2;
    let y = layout.main_plot.y + layout.main_plot.height / 2;
    controller.mouse(
        mouse(MouseEventKind::Moved, x, y),
        &mut state,
        &candles,
        &layout,
    );
    let initial_right = state.viewport().unwrap().right_index();
    let initial_center = state.viewport().unwrap().y_range().center();
    assert_eq!(
        controller.mouse(
            mouse(MouseEventKind::Down(MouseButton::Left), x, y),
            &mut state,
            &candles,
            &layout
        ),
        InteractionAction::Redraw
    );
    assert!(controller.is_dragging());
    assert_eq!(state.viewport().unwrap().coordinate_hover(), None);

    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), x + 8, y + 3),
        &mut state,
        &candles,
        &layout,
    );
    assert!(
        state.viewport().unwrap().right_index() < initial_right,
        "dragging content right reveals older candles"
    );
    assert!(
        state.viewport().unwrap().y_range().center() > initial_center,
        "dragging down raises viewed price center"
    );
    assert!(state.viewport().unwrap().active_drag().is_some());

    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), x + 8, y + 3),
        &mut state,
        &candles,
        &layout,
    );
    assert!(!controller.is_dragging());
    assert!(state.viewport().unwrap().active_drag().is_none());
    assert!(state.viewport().unwrap().coordinate_hover().is_some());
}

#[test]
fn axis_drags_keep_initial_anchors_clamp_frame_and_enforce_x_floor() {
    let candles = series(100);
    let layout = layout();
    let mut state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
    state.pan_x_older(&candles);
    let mut controller = InteractionController::new();

    let tx = layout.utc_axis.x + layout.utc_axis.width / 2;
    let ty = layout.utc_axis.y;
    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), tx, ty),
        &mut state,
        &candles,
        &layout,
    );
    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), u16::MAX, ty),
        &mut state,
        &candles,
        &layout,
    );
    assert_eq!(state.viewport().unwrap().visible_count(), 10);
    let floor = state.clone();
    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), u16::MAX, ty),
        &mut state,
        &candles,
        &layout,
    );
    assert_eq!(state, floor);
    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), u16::MAX, ty),
        &mut state,
        &candles,
        &layout,
    );

    let px = layout.price_axis.x;
    let py = layout.price_axis.y + layout.price_axis.height / 2;
    let old_span = state.viewport().unwrap().y_range().span();
    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), px, py),
        &mut state,
        &candles,
        &layout,
    );
    let anchor = state
        .viewport()
        .unwrap()
        .active_drag()
        .unwrap()
        .anchor_price
        .unwrap();
    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), px, 0),
        &mut state,
        &candles,
        &layout,
    );
    let range = state.viewport().unwrap().y_range();
    assert!(range.span() < old_span);
    assert!(range.low <= anchor && anchor <= range.high);
}

#[test]
fn axis_drags_keep_exact_y_powers_and_snap_x_to_uniform_cadence() {
    let candles = series(100);
    let layout = layout();
    let mut controller = InteractionController::new();
    let px = layout.price_axis.x;
    let py = layout.main_plot.y + layout.main_plot.height / 2;

    let mut upward = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
    let original_span = upward.viewport().unwrap().y_range().span();
    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), px, py),
        &mut upward,
        &candles,
        &layout,
    );
    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), px, py - 3),
        &mut upward,
        &candles,
        &layout,
    );
    assert_close(
        upward.viewport().unwrap().y_range().span(),
        original_span / 1.05_f64.powi(3),
    );
    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), px, py - 3),
        &mut upward,
        &candles,
        &layout,
    );

    let mut downward = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), px, py),
        &mut downward,
        &candles,
        &layout,
    );
    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), px, py + 3),
        &mut downward,
        &candles,
        &layout,
    );
    assert_close(
        downward.viewport().unwrap().y_range().span(),
        original_span * 1.05_f64.powi(3),
    );
    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), px, py + 3),
        &mut downward,
        &candles,
        &layout,
    );

    let tx = layout.utc_axis.x + 10;
    let ty = layout.utc_axis.y;
    let mut rightward = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), tx, ty),
        &mut rightward,
        &candles,
        &layout,
    );
    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), tx + 10, ty),
        &mut rightward,
        &candles,
        &layout,
    );
    assert_eq!(rightward.viewport().unwrap().visible_count(), 32);
    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), tx + 10, ty),
        &mut rightward,
        &candles,
        &layout,
    );

    let mut leftward = ChartViewState::interactive(&candles, usize::from(layout.main_plot.width));
    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), tx, ty),
        &mut leftward,
        &candles,
        &layout,
    );
    controller.mouse(
        mouse(MouseEventKind::Drag(MouseButton::Left), tx - 10, ty),
        &mut leftward,
        &candles,
        &layout,
    );
    assert_eq!(leftward.viewport().unwrap().visible_count(), 65);
    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), tx - 10, ty),
        &mut leftward,
        &candles,
        &layout,
    );
}

#[test]
fn mouse_x_zoom_enforces_each_minimum_floor_and_next_step_is_a_no_op() {
    for (series_len, plot_width, expected_floor) in [(7, 20, 7), (100, 7, 7), (100, 45, 10)] {
        let candles = series(series_len);
        let mut layout = layout();
        layout.main_plot.width = plot_width;
        layout.utc_axis.width = plot_width;
        let mut state = ChartViewState::snapshot(&candles, usize::from(plot_width));
        let mut controller = InteractionController::new();
        let x = layout.utc_axis.x;
        let y = layout.utc_axis.y;
        controller.mouse(
            mouse(MouseEventKind::Down(MouseButton::Left), x, y),
            &mut state,
            &candles,
            &layout,
        );
        controller.mouse(
            mouse(MouseEventKind::Drag(MouseButton::Left), u16::MAX, y),
            &mut state,
            &candles,
            &layout,
        );
        assert_eq!(
            state.viewport().unwrap().visible_count(),
            expected_floor,
            "series={series_len}, plot={plot_width}"
        );
        let at_floor = state.clone();
        controller.mouse(
            mouse(MouseEventKind::Drag(MouseButton::Left), u16::MAX, y),
            &mut state,
            &candles,
            &layout,
        );
        assert_eq!(
            state, at_floor,
            "next zoom must be blocked at series={series_len}, plot={plot_width}"
        );
    }
}

#[test]
fn nonzero_nondivisible_geometry_retains_initial_candle_under_cursor_after_zoom() {
    let candles = series(100);
    let mut layout = layout();
    layout.main_plot.width = 47;
    layout.utc_axis.width = 47;
    let mut state = ChartViewState::interactive(&candles, 47);
    state.pan_x_older(&candles);
    let initial_geometry = CandleSlotGeometry::new(
        layout.main_plot.x,
        layout.main_plot.width,
        state.visible_range().len(),
    )
    .unwrap();
    let initial_visible_index = initial_geometry
        .index_at_x(layout.main_plot.x + layout.main_plot.width / 2)
        .unwrap();
    let x = u16::try_from(initial_geometry.slot(initial_visible_index).unwrap().end() - 1).unwrap();
    let expected = candles
        .get(state.visible_range().start + initial_visible_index)
        .unwrap()
        .open_time();
    let mut controller = InteractionController::new();
    controller.mouse(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            x,
            layout.utc_axis.y,
        ),
        &mut state,
        &candles,
        &layout,
    );
    controller.mouse(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            x + 5,
            layout.frame.y,
        ),
        &mut state,
        &candles,
        &layout,
    );

    let zoomed_geometry = CandleSlotGeometry::new(
        layout.main_plot.x,
        layout.main_plot.width,
        state.visible_range().len(),
    )
    .unwrap();
    let zoomed_slot = zoomed_geometry.index_at_x(x).unwrap();
    assert_eq!(
        candles
            .get(state.visible_range().start + zoomed_slot)
            .unwrap()
            .open_time(),
        expected,
    );
}

#[test]
fn drag_ownership_survives_leaving_origin_region_and_release_reprojects_or_clears() {
    let candles = series(100);
    let layout = layout();
    let x = layout.utc_axis.x + layout.utc_axis.width / 2;
    let y = layout.utc_axis.y;
    let mut state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
    let mut controller = InteractionController::new();
    let initial_count = state.viewport().unwrap().visible_count();
    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), x, y),
        &mut state,
        &candles,
        &layout,
    );
    controller.mouse(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            x + 10,
            layout.header.y,
        ),
        &mut state,
        &candles,
        &layout,
    );
    assert!(controller.is_dragging());
    assert!(
        state.viewport().unwrap().visible_count() < initial_count,
        "time-axis ownership remains fixed in the header"
    );

    let inside = Position::new(layout.main_plot.x + 2, layout.main_plot.y + 2);
    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), inside.x, inside.y),
        &mut state,
        &candles,
        &layout,
    );
    assert_eq!(controller.pointer(), Some(inside));
    assert!(state.viewport().unwrap().coordinate_hover().is_some());

    controller.mouse(
        mouse(MouseEventKind::Down(MouseButton::Left), inside.x, inside.y),
        &mut state,
        &candles,
        &layout,
    );
    let outside = Position::new(layout.frame.x + layout.frame.width, layout.frame.y);
    controller.mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), outside.x, outside.y),
        &mut state,
        &candles,
        &layout,
    );
    assert_eq!(controller.pointer(), Some(outside));
    assert_eq!(state.viewport().unwrap().coordinate_hover(), None);
}

#[test]
fn real_series_mutation_during_drag_cancels_controller_and_reprojects_pointer() {
    let mut candles = series(30);
    let layout = layout();
    let mut state = ChartViewState::interactive(&candles, usize::from(layout.main_plot.width));
    let mut controller = InteractionController::new();
    let pointer = Position::new(layout.main_plot.x + 3, layout.main_plot.y + 2);
    controller.mouse(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            pointer.x,
            pointer.y,
        ),
        &mut state,
        &candles,
        &layout,
    );
    assert!(controller.is_dragging());
    assert!(state.viewport().unwrap().active_drag().is_some());

    let summary = candles.upsert(candle(BASE - MINUTE));
    assert_eq!(summary.inserted, 1);
    state.apply_mutation(&candles, &summary, usize::from(layout.main_plot.width));
    assert!(state.viewport().unwrap().active_drag().is_none());
    assert_eq!(state.viewport().unwrap().coordinate_hover(), None);

    controller.sync_after_view_change(&mut state, &candles, &layout);
    assert!(!controller.is_dragging());
    let hover = state.viewport().unwrap().coordinate_hover().unwrap();
    let geometry = CandleSlotGeometry::new(
        layout.main_plot.x,
        layout.main_plot.width,
        state.visible_range().len(),
    )
    .unwrap();
    let slot = geometry.index_at_x(pointer.x).unwrap();
    assert_eq!(
        hover.open_time,
        candles
            .get(state.visible_range().start + slot)
            .unwrap()
            .open_time()
    );
}
