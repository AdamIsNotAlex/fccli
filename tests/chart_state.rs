use fccli::chart::{
    ActiveDrag, ChartViewState, CoordinateHover, DragKind, PriceRange, auto_y_range,
    bounded_zoom_factor,
};
use fccli::model::{
    CHART_PRICE_MAX, Candle, IndexMapping, MutationKind, MutationSummary, ResolvedMutation,
};

const MINUTE: i64 = 60_000;
const BASE: i64 = 1_700_000_040_000;

fn candle_at(open_time: i64, low: f64, high: f64) -> Candle {
    Candle::from_rest(open_time, open_time + MINUTE - 1, low, high, low, high, 1.0)
        .expect("test candle is valid")
}

fn candle(index: usize, low: f64, high: f64) -> Candle {
    let open_time = BASE + i64::try_from(index).expect("test index fits") * MINUTE;
    candle_at(open_time, low, high)
}

fn candles(len: usize) -> Vec<Candle> {
    (0..len)
        .map(|index| candle(index, index as f64 + 1.0, index as f64 + 2.0))
        .collect()
}

fn data(state: &ChartViewState) -> &fccli::chart::ChartViewport {
    state.viewport().expect("chart contains data")
}

fn data_mut(state: &mut ChartViewState) -> &mut fccli::chart::ChartViewport {
    state.viewport_mut().expect("chart contains data")
}

fn assert_close(actual: f64, expected: f64) {
    let tolerance = expected.abs().max(1.0) * 1.0e-12;
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected:?}, got {actual:?}"
    );
}

fn summary(mapping: IndexMapping, inserted: usize) -> MutationSummary {
    MutationSummary {
        inserted,
        replaced: 0,
        unchanged: 0,
        old_to_new: mapping,
        resolved: Vec::new(),
        empty_input: false,
        duplicate_only: false,
        no_progress: false,
    }
}

#[test]
fn initializers_distinguish_empty_snapshot_and_interactive_counts() {
    assert_eq!(ChartViewState::snapshot(&[], 20), ChartViewState::Empty);
    assert_eq!(ChartViewState::interactive(&[], 20), ChartViewState::Empty);

    let series = candles(100);
    let snapshot = ChartViewState::snapshot(&series, 30);
    assert_eq!(data(&snapshot).visible_count, 30);
    assert_eq!(snapshot.visible_range(), 70..100);

    for (width, expected) in [(1, 1), (9, 9), (20, 10), (21, 11), (40, 20), (200, 100)] {
        let state = ChartViewState::interactive(&series, width);
        assert_eq!(data(&state).visible_count, expected, "width={width}");
        assert_eq!(data(&state).right_index, 99);
        assert!(data(&state).follow_live);
    }

    for len in 1..=9 {
        let short = candles(len);
        assert_eq!(
            data(&ChartViewState::interactive(&short, 50)).visible_count,
            len
        );
        assert_eq!(ChartViewState::min_visible_count(len, 50), len);
    }
}

#[test]
#[should_panic(expected = "plot_width must be positive")]
fn initializer_rejects_zero_plot_width() {
    let _ = ChartViewState::interactive(&candles(1), 0);
}

#[test]
fn x_pan_zoom_use_exact_factors_rounding_and_bounds() {
    let series = candles(100);
    let mut state = ChartViewState::snapshot(&series, 21);
    assert_eq!(data(&state).visible_count, 21);

    state.pan_x_older(&series);
    assert_eq!(data(&state).right_index, 97, "ceil(21 * 5%) is two");
    assert!(!data(&state).follow_live);

    state.zoom_x_in(&series, 21);
    assert_eq!(data(&state).visible_count, 17, "21 * 0.8 rounds to 17");
    state.zoom_x_out(&series, 21);
    assert_eq!(data(&state).visible_count, 21, "17 * 1.25 rounds to 21");

    while data(&state).visible_count > 10 {
        state.zoom_x_in(&series, 21);
    }
    assert_eq!(data(&state).visible_count, 10);
    let at_floor = state.clone();
    state.zoom_x_in(&series, 21);
    assert_eq!(
        state, at_floor,
        "next zoom-in at the exact floor is blocked"
    );

    state.end(&series);
    while data(&state).visible_count < 21 {
        state.zoom_x_out(&series, 21);
    }
    assert_eq!(data(&state).visible_count, 21);
    let at_ceiling = state.clone();
    state.zoom_x_out(&series, 21);
    assert_eq!(
        state, at_ceiling,
        "one candle per plot column is the ceiling"
    );
}

#[test]
fn nearest_rounding_uses_ties_away_from_zero() {
    let series = candles(100);
    let mut state = ChartViewState::snapshot(&series, 50);
    data_mut(&mut state).visible_count = 25;
    state.zoom_x_by_factor(&series, 50, 0.5);
    assert_eq!(data(&state).visible_count, 13, "12.5 rounds away from zero");

    data_mut(&mut state).visible_count = 15;
    state.zoom_x_by_factor(&series, 50, 0.5);
    assert_eq!(
        data(&state).visible_count,
        10,
        "the exact interactive floor still wins"
    );
}

#[test]
fn repeated_zoom_factor_stops_before_overflow_or_bound_crossing() {
    assert_eq!(bounded_zoom_factor(2.0, 100, 10.0), 8.0);
    assert_eq!(bounded_zoom_factor(1.25, 0, 100.0), 1.0);
    assert_eq!(bounded_zoom_factor(f64::INFINITY, 2, 100.0), 1.0);
    assert_eq!(bounded_zoom_factor(0.0, 2, 100.0), 1.0);

    let factor = bounded_zoom_factor(1.25, usize::MAX, f64::MAX);
    assert!(factor.is_finite());
    assert!(factor <= f64::MAX);
}

#[test]
fn y_pan_zoom_use_ten_percent_and_point_eight_one_point_two_five() {
    let series = vec![candle(0, 10.0, 20.0)];
    let mut state = ChartViewState::interactive(&series, 10);
    let initial = data(&state).y_range;
    assert_close(initial.low, 9.5);
    assert_close(initial.high, 20.5);

    state.pan_y_up();
    assert!(!data(&state).auto_y);
    assert_close(data(&state).y_range.low, 10.6);
    assert_close(data(&state).y_range.high, 21.6);

    state.zoom_y_in();
    assert_close(data(&state).y_range.span(), 8.8);
    state.zoom_y_out();
    assert_close(data(&state).y_range.span(), 11.0);

    state.pan_y_down();
    assert_close(data(&state).y_range.low, 9.5);
    assert_close(data(&state).y_range.high, 20.5);
}

#[test]
fn end_resumes_follow_without_resetting_manual_y_but_reset_restores_defaults() {
    let series = candles(50);
    let mut state = ChartViewState::interactive(&series, 20);
    let default_count = data(&state).visible_count;

    state.pan_x_older(&series);
    state.zoom_x_in(&series, 20);
    state.pan_y_up();
    let manual_range = data(&state).y_range;
    assert!(!data(&state).follow_live);
    assert!(!data(&state).auto_y);

    state.end(&series);
    assert!(data(&state).follow_live);
    assert_eq!(data(&state).right_index, series.len() - 1);
    assert!(!data(&state).auto_y);
    assert_eq!(data(&state).y_range, manual_range);

    state.reset(&series, 20);
    assert!(data(&state).follow_live);
    assert!(data(&state).auto_y);
    assert_eq!(data(&state).visible_count, default_count);
    assert_eq!(data(&state).right_index, series.len() - 1);
}

#[test]
fn live_append_advances_only_while_following() {
    let old = candles(30);
    let mut following = ChartViewState::interactive(&old, 20);
    let mut appended = candles(31);
    let append = summary(IndexMapping::Identity { len: 30 }, 1);
    following.apply_mutation(&appended, &append, 20);
    assert_eq!(data(&following).right_index, 30);
    assert_eq!(data(&following).right_open_time, appended[30].open_time());

    let mut paused = ChartViewState::interactive(&old, 20);
    paused.pan_x_older(&old);
    let paused_right = data(&paused).right_index;
    let paused_time = data(&paused).right_open_time;
    paused.apply_mutation(&appended, &append, 20);
    assert_eq!(data(&paused).right_index, paused_right);
    assert_eq!(data(&paused).right_open_time, paused_time);
    assert!(!data(&paused).follow_live);

    appended.push(candle(31, 32.0, 33.0));
    let second_append = summary(IndexMapping::Identity { len: 31 }, 1);
    paused.apply_mutation(&appended, &second_append, 20);
    assert_eq!(data(&paused).right_open_time, paused_time);
}

#[test]
fn identity_shift_and_explicit_mappings_preserve_the_same_logical_anchor() {
    let old = candles(30);
    let mut base = ChartViewState::interactive(&old, 12);
    base.pan_x_older(&old);
    let old_right = data(&base).right_index;
    let anchor_time = data(&base).right_open_time;
    let visible_count = data(&base).visible_count;

    let shifted: Vec<_> = std::iter::once(candle_at(BASE - MINUTE, 0.5, 0.75))
        .chain(old.iter().cloned())
        .collect();
    let cases = [
        IndexMapping::ShiftSuffix {
            len: 30,
            from: 0,
            delta: 1,
        },
        IndexMapping::Explicit((1..=30).collect()),
    ];
    for mapping in cases {
        let mut state = base.clone();
        state.apply_mutation(&shifted, &summary(mapping, 1), 12);
        assert_eq!(data(&state).right_index, old_right + 1);
        assert_eq!(data(&state).right_open_time, anchor_time);
        assert_eq!(data(&state).visible_count, visible_count);
    }

    let mut identity = base.clone();
    identity.apply_mutation(&old, &summary(IndexMapping::Identity { len: 30 }, 0), 12);
    assert_eq!(data(&identity).right_index, old_right);
    assert_eq!(data(&identity).right_open_time, anchor_time);
    assert_eq!(data(&identity).visible_count, visible_count);
}

#[test]
fn mutation_before_inside_after_view_preserves_anchor_and_cancels_transients() {
    let old = candles(40);
    let mut state = ChartViewState::interactive(&old, 16);
    state.pan_x_older(&old);
    let anchor_time = data(&state).right_open_time;
    data_mut(&mut state).coordinate_hover = Some(CoordinateHover {
        open_time: anchor_time,
        price: 5.0,
    });
    data_mut(&mut state).active_drag = Some(ActiveDrag {
        kind: DragKind::Plot,
        anchor_open_time: Some(anchor_time),
        anchor_price: Some(5.0),
    });

    let mut changed = old.clone();
    changed.insert(0, candle_at(BASE - MINUTE, 0.1, 0.2));
    let mapped: Vec<usize> = (1..=40).collect();
    let mut change = summary(IndexMapping::Explicit(mapped), 3);
    change.resolved = vec![
        ResolvedMutation {
            open_time: BASE - MINUTE,
            final_index: 0,
            kind: MutationKind::Inserted,
        },
        ResolvedMutation {
            open_time: BASE + 10 * MINUTE + 1,
            final_index: 12,
            kind: MutationKind::Inserted,
        },
        ResolvedMutation {
            open_time: BASE + 100 * MINUTE,
            final_index: 40,
            kind: MutationKind::Inserted,
        },
    ];
    state.apply_mutation(&changed, &change, 16);

    assert_eq!(data(&state).right_open_time, anchor_time);
    assert_eq!(data(&state).coordinate_hover, None);
    assert_eq!(data(&state).active_drag, None);
    assert!(data(&state).visible_count <= 16);
    assert!(state.visible_range().end <= changed.len());
}

#[test]
fn auto_y_is_finite_for_empty_single_flat_tiny_negative_and_huge_values() {
    let empty = ChartViewState::interactive(&[], 10);
    assert_eq!(empty.visible_range(), 0..0);

    let fixtures = [
        candle(0, 0.0, 0.0),
        candle(1, -5.0, -5.0),
        candle(2, f64::MIN_POSITIVE, f64::MIN_POSITIVE * 2.0),
        candle(3, CHART_PRICE_MAX * 0.99, CHART_PRICE_MAX),
        candle(4, -CHART_PRICE_MAX, -CHART_PRICE_MAX * 0.99),
    ];
    for item in fixtures {
        let range = auto_y_range(std::slice::from_ref(&item), 0..1);
        assert!(range.low.is_finite());
        assert!(range.high.is_finite());
        assert!(range.high > range.low);
        assert!(range.span().is_finite());
    }

    let flat = auto_y_range(&[candle(5, 2.0, 2.0)], 0..1);
    let epsilon = 2.0 / ((1_u64 << 40) as f64);
    assert_close(flat.span(), epsilon);

    let ordinary = auto_y_range(&[candle(6, -10.0, 10.0)], 0..1);
    assert_close(ordinary.low, -11.0);
    assert_close(ordinary.high, 11.0);
}

#[test]
fn manual_y_operations_disable_auto_and_remain_finite_at_numeric_bounds() {
    let series = vec![candle(0, CHART_PRICE_MAX * 0.99, CHART_PRICE_MAX)];
    let mut state = ChartViewState::interactive(&series, 1);
    for _ in 0..2_000 {
        state.zoom_y_out();
        state.pan_y_up();
    }
    let view = data(&state);
    assert!(!view.auto_y);
    assert!(view.y_range.low.is_finite());
    assert!(view.y_range.high.is_finite());
    assert!(view.y_range.high > view.y_range.low);
    assert!(view.y_range.span() <= 1.10 * (2.0 * CHART_PRICE_MAX));

    data_mut(&mut state).y_range = PriceRange {
        low: 1.0,
        high: 1.0,
    };
    data_mut(&mut state).auto_y = true;
    state.zoom_y_in();
    assert!(
        !data(&state).auto_y,
        "manual operation disables auto even when clamped"
    );
    assert!(data(&state).y_range.span() > 0.0);
}

#[test]
fn resize_preserves_center_or_latest_and_reclamps_to_plot_width() {
    let series = candles(100);
    let mut inspected = ChartViewState::snapshot(&series, 20);
    inspected.pan_x_older(&series);
    inspected.pan_x_older(&series);
    let old_range = inspected.visible_range();
    let old_center_time = series[old_range.start + old_range.len() / 2].open_time();

    inspected.resize(&series, 7);
    assert_eq!(data(&inspected).visible_count, 7);
    let new_range = inspected.visible_range();
    assert!(
        new_range.contains(
            &series
                .binary_search_by_key(&old_center_time, Candle::open_time)
                .expect("center remains")
        )
    );
    assert!(new_range.end <= series.len());

    inspected.resize(&series, 200);
    assert_eq!(
        data(&inspected).visible_count,
        7,
        "resize does not invent a zoom-out"
    );

    let mut following = ChartViewState::interactive(&series, 40);
    following.resize(&series, 3);
    assert_eq!(data(&following).visible_count, 3);
    assert_eq!(data(&following).right_index, 99);
    assert!(data(&following).follow_live);

    let shortened = candles(2);
    let shrink = summary(IndexMapping::Explicit(vec![0, 1]), 0);
    following.apply_mutation(&shortened, &shrink, 1);
    assert_eq!(data(&following).visible_count, 1);
    assert_eq!(data(&following).right_index, 1);
    assert_eq!(following.visible_range(), 1..2);
}
