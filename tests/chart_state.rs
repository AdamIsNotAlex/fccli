use fccli::chart::{
    ActiveDrag, ChartViewState, CoordinateHover, DragKind, PriceRange, auto_y_range,
    bounded_zoom_factor,
};
use fccli::model::{CHART_PRICE_MAX, Candle, CandleSeries, IndexMapping, Timeframe};

const MINUTE: i64 = 60_000;
const BASE: i64 = 1_700_000_040_000;

fn candle_at(open_time: i64, low: f64, high: f64) -> Candle {
    Candle::from_rest(open_time, open_time + MINUTE - 1, low, high, low, high, 1.0)
        .expect("test candle is valid")
}

fn candle(index: usize, low: f64, high: f64) -> Candle {
    candle_at(BASE + i64::try_from(index).unwrap() * MINUTE, low, high)
}

fn candle_vec(len: usize) -> Vec<Candle> {
    (0..len)
        .map(|index| candle(index, index as f64 + 1.0, index as f64 + 2.0))
        .collect()
}

fn series_from(items: Vec<Candle>) -> CandleSeries {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    series.replace(items).expect("new series is empty");
    series
}

fn candles(len: usize) -> CandleSeries {
    series_from(candle_vec(len))
}

fn data(state: &ChartViewState) -> &fccli::chart::ChartViewport {
    state.viewport().expect("chart contains data")
}

fn at(series: &CandleSeries, index: usize) -> &Candle {
    series.get(index).expect("index is in series")
}

fn center_index(state: &ChartViewState) -> usize {
    let range = state.visible_range();
    range.start + range.len() / 2
}

fn center_open_time(state: &ChartViewState, series: &CandleSeries) -> i64 {
    at(series, center_index(state)).open_time()
}

fn assert_close(actual: f64, expected: f64) {
    let tolerance = expected.abs().max(1.0) * 1.0e-12;
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected:?}, got {actual:?}"
    );
}

#[test]
fn initializers_distinguish_empty_snapshot_and_interactive_counts() {
    let empty = candles(0);
    assert_eq!(ChartViewState::snapshot(&empty, 20), ChartViewState::Empty);
    assert_eq!(
        ChartViewState::interactive(&empty, 20),
        ChartViewState::Empty
    );

    let series = candles(100);
    let snapshot = ChartViewState::snapshot(&series, 30);
    assert_eq!(data(&snapshot).visible_count(), 30);
    assert_eq!(snapshot.visible_range(), 70..100);

    for (width, expected) in [(1, 1), (9, 9), (20, 10), (21, 11), (40, 20), (200, 100)] {
        let state = ChartViewState::interactive(&series, width);
        assert_eq!(data(&state).visible_count(), expected, "width={width}");
        assert_eq!(data(&state).right_index(), 99);
        assert!(data(&state).follows_live());
    }

    for len in 1..=9 {
        let short = candles(len);
        assert_eq!(
            data(&ChartViewState::interactive(&short, 50)).visible_count(),
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
    state.pan_x_older(&series);
    assert_eq!(data(&state).right_index(), 97, "ceil(21 * 5%) is two");
    assert!(!data(&state).follows_live());

    state.zoom_x_in(&series, 21);
    assert_eq!(data(&state).visible_count(), 17);
    state.zoom_x_out(&series, 21);
    assert_eq!(data(&state).visible_count(), 21);

    while data(&state).visible_count() > 10 {
        state.zoom_x_in(&series, 21);
    }
    assert_eq!(data(&state).visible_count(), 10);
    let at_floor = state.clone();
    state.zoom_x_in(&series, 21);
    assert_eq!(state, at_floor);

    state.end(&series);
    while data(&state).visible_count() < 21 {
        state.zoom_x_out(&series, 21);
    }
    let at_ceiling = state.clone();
    state.zoom_x_out(&series, 21);
    assert_eq!(state, at_ceiling);
}

#[test]
fn x_zoom_floors_cover_each_limiter_for_keyboard_and_generic_factors() {
    for (series_len, plot_width, expected_floor) in [(7, 20, 7), (100, 6, 6), (100, 20, 10)] {
        let series = candles(series_len);

        let mut keyboard = ChartViewState::snapshot(&series, plot_width);
        while data(&keyboard).visible_count() > expected_floor {
            keyboard.zoom_x_in(&series, plot_width);
        }
        assert_eq!(
            data(&keyboard).visible_count(),
            expected_floor,
            "keyboard floor for series_len={series_len}, plot_width={plot_width}"
        );
        let keyboard_floor = keyboard.clone();
        keyboard.zoom_x_in(&series, plot_width);
        assert_eq!(
            keyboard, keyboard_floor,
            "keyboard next zoom is blocked for series_len={series_len}, plot_width={plot_width}"
        );

        let mut generic = ChartViewState::snapshot(&series, plot_width);
        generic.zoom_x_by_factor(&series, plot_width, 0.01);
        assert_eq!(
            data(&generic).visible_count(),
            expected_floor,
            "generic floor for series_len={series_len}, plot_width={plot_width}"
        );
        let generic_floor = generic.clone();
        generic.zoom_x_by_factor(&series, plot_width, 0.5);
        assert_eq!(
            generic, generic_floor,
            "generic next zoom is blocked for series_len={series_len}, plot_width={plot_width}"
        );
    }
}

#[test]
fn paused_zoom_out_reaching_latest_restores_live_append_following() {
    let mut series = candles(100);
    let mut state = ChartViewState::interactive(&series, 40);
    state.pan_x_older(&series);
    assert!(!data(&state).follows_live());
    assert_eq!(data(&state).right_index(), 98);

    state.zoom_x_out(&series, 40);
    assert_eq!(data(&state).right_index(), 99);
    assert!(data(&state).follows_live());

    let summary = series.append(candle(100, 101.0, 102.0));
    state.apply_mutation(&series, &summary, 40);
    assert_eq!(data(&state).right_index(), 100);
    assert_eq!(data(&state).right_open_time(), at(&series, 100).open_time());
    assert!(data(&state).follows_live());
}

#[test]
fn zoom_while_following_stays_latest_and_advances_on_live_append() {
    let mut series = candles(100);
    let mut state = ChartViewState::interactive(&series, 40);
    state.zoom_x_in(&series, 40);
    assert_eq!(data(&state).visible_count(), 16);
    assert_eq!(data(&state).right_index(), 99);
    assert!(data(&state).follows_live());

    let summary = series.append(candle(100, 101.0, 102.0));
    state.apply_mutation(&series, &summary, 40);
    assert_eq!(data(&state).right_index(), 100);
    assert_eq!(data(&state).right_open_time(), at(&series, 100).open_time());
    assert!(data(&state).follows_live());
}

#[test]
fn inverse_and_parity_changing_zoom_preserve_paused_center() {
    let series = candles(100);
    let mut state = ChartViewState::interactive(&series, 26);
    state.pan_x_older(&series);
    let anchor = center_open_time(&state, &series);
    assert_eq!(data(&state).visible_count(), 13);

    state.zoom_x_in(&series, 26);
    assert_eq!(data(&state).visible_count(), 10);
    assert_eq!(center_open_time(&state, &series), anchor);
    state.zoom_x_out(&series, 26);
    assert_eq!(data(&state).visible_count(), 13);
    assert_eq!(center_open_time(&state, &series), anchor);
    assert!(!data(&state).follows_live());
}

#[test]
fn nearest_rounding_uses_ties_away_from_zero_without_mutation_escape_hatch() {
    let series = candles(100);
    let mut state = ChartViewState::snapshot(&series, 25);
    state.zoom_x_by_factor(&series, 50, 0.5);
    assert_eq!(
        data(&state).visible_count(),
        13,
        "12.5 rounds away from zero"
    );

    let series = candles(15);
    let mut state = ChartViewState::snapshot(&series, 15);
    state.zoom_x_by_factor(&series, 50, 0.5);
    assert_eq!(data(&state).visible_count(), 10, "interactive floor wins");
}

#[test]
fn maximum_step_bounded_factor_is_positive_and_reaches_zoom_minima() {
    assert_eq!(bounded_zoom_factor(2.0, 100, 10.0), 8.0);
    assert_eq!(bounded_zoom_factor(1.25, 0, 100.0), 1.0);
    assert_eq!(bounded_zoom_factor(f64::INFINITY, 2, 100.0), 1.0);
    assert_eq!(bounded_zoom_factor(0.0, 2, 100.0), 1.0);
    assert_eq!(bounded_zoom_factor(1.0, usize::MAX, 100.0), 1.0);
    assert_eq!(bounded_zoom_factor(0.5, 4, 10.0), 0.125);
    assert_eq!(bounded_zoom_factor(0.5, usize::MAX, 10.0), 0.125);

    let minimum_factor = bounded_zoom_factor(0.8, usize::MAX, f64::MAX);
    assert!(minimum_factor.is_finite());
    assert!(minimum_factor > 0.0);

    let x_series = candles(100);
    let mut x_state = ChartViewState::snapshot(&x_series, 100);
    x_state.zoom_x_by_factor(&x_series, 100, minimum_factor);
    assert_eq!(data(&x_state).visible_count(), 10);

    let y_series = series_from(vec![candle(0, 0.0, 0.0)]);
    let mut y_state = ChartViewState::snapshot(&y_series, 1);
    y_state.zoom_y_by_factor(minimum_factor);
    assert_eq!(
        data(&y_state).y_range().span(),
        1.0 / ((1_u64 << 40) as f64)
    );
}

#[test]
fn y_pan_zoom_use_ten_percent_and_exact_factors() {
    let series = series_from(vec![candle(0, 10.0, 20.0)]);
    let mut state = ChartViewState::interactive(&series, 10);
    assert_close(data(&state).y_range().low, 9.5);
    assert_close(data(&state).y_range().high, 20.5);
    state.pan_y_up();
    assert!(!data(&state).auto_y());
    assert_close(data(&state).y_range().low, 10.6);
    state.zoom_y_in();
    assert_close(data(&state).y_range().span(), 8.8);
    state.zoom_y_out();
    assert_close(data(&state).y_range().span(), 11.0);
    state.pan_y_down();
    assert_close(data(&state).y_range().low, 9.5);
}

#[test]
fn invalid_y_factors_retain_a_finite_valid_range() {
    let series = series_from(vec![candle(0, 10.0, 20.0)]);
    for factor in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0] {
        let mut state = ChartViewState::interactive(&series, 10);
        let before = data(&state).y_range();
        state.zoom_y_by_factor(factor);
        assert_eq!(data(&state).y_range(), before, "factor={factor:?}");
        assert!(data(&state).y_range().low.is_finite());
        assert!(data(&state).y_range().high.is_finite());
    }

    let mut state = ChartViewState::interactive(&series, 10);
    state.zoom_y_by_factor(f64::MAX);
    let range = data(&state).y_range();
    assert!(range.low.is_finite() && range.high.is_finite());
    assert!(range.high > range.low);
    assert_eq!(range.span(), 1.10 * (2.0 * CHART_PRICE_MAX));
}

#[test]
fn end_and_reset_have_distinct_y_and_x_semantics() {
    let series = candles(50);
    let mut state = ChartViewState::interactive(&series, 20);
    let default_count = data(&state).visible_count();
    state.pan_x_older(&series);
    state.zoom_x_in(&series, 20);
    state.pan_y_up();
    let manual_range = data(&state).y_range();

    state.end(&series);
    assert!(data(&state).follows_live());
    assert_eq!(data(&state).right_index(), series.len() - 1);
    assert!(!data(&state).auto_y());
    assert_eq!(data(&state).y_range(), manual_range);

    state.reset(&series, 20);
    assert!(data(&state).follows_live());
    assert!(data(&state).auto_y());
    assert_eq!(data(&state).visible_count(), default_count);
}

#[test]
fn real_series_insertions_preserve_paused_center_and_clear_transients() {
    for inserted_index in [0, 31, 36, 40] {
        let mut series = candles(40);
        let mut state = ChartViewState::interactive(&series, 16);
        state.pan_x_older(&series);
        let anchor = center_open_time(&state, &series);
        state.set_coordinate_hover(
            &series,
            Some(CoordinateHover {
                open_time: anchor,
                price: 5.0,
            }),
        );
        state.set_active_drag(
            &series,
            Some(ActiveDrag {
                kind: DragKind::Plot,
                anchor_open_time: Some(anchor),
                anchor_price: Some(5.0),
            }),
        );

        let open_time = if inserted_index == 0 {
            BASE - MINUTE
        } else if inserted_index == 40 {
            BASE + 40 * MINUTE
        } else {
            BASE + i64::from(inserted_index) * MINUTE - 1
        };
        let summary = series.merge(vec![candle_at(open_time, 0.25, 0.5)]);
        state.apply_mutation(&series, &summary, 16);

        assert_eq!(
            center_open_time(&state, &series),
            anchor,
            "inserted_index={inserted_index}"
        );
        assert_eq!(data(&state).coordinate_hover(), None);
        assert_eq!(data(&state).active_drag(), None);
        assert!(!data(&state).follows_live());
        assert!(data(&state).visible_count() <= 16);
    }
}

#[test]
fn real_series_summaries_cover_canonical_mapping_shapes_without_center_drift() {
    let mut appended_series = candles(40);
    let mut appended_state = ChartViewState::interactive(&appended_series, 16);
    appended_state.pan_x_older(&appended_series);
    let appended_anchor = center_open_time(&appended_state, &appended_series);
    let appended = appended_series.append(candle(40, 41.0, 42.0));
    assert!(matches!(
        &appended.old_to_new,
        IndexMapping::Identity { len: 40 }
    ));
    appended_state.apply_mutation(&appended_series, &appended, 16);
    assert_eq!(
        center_open_time(&appended_state, &appended_series),
        appended_anchor
    );

    let mut prepended_series = candles(40);
    let mut prepended_state = ChartViewState::interactive(&prepended_series, 16);
    prepended_state.pan_x_older(&prepended_series);
    let prepended_anchor = center_open_time(&prepended_state, &prepended_series);
    let prepended = prepended_series.prepend(vec![candle_at(BASE - MINUTE, 0.25, 0.5)]);
    assert!(matches!(
        &prepended.old_to_new,
        IndexMapping::ShiftSuffix {
            len: 40,
            from: 0,
            delta: 1
        }
    ));
    prepended_state.apply_mutation(&prepended_series, &prepended, 16);
    assert_eq!(
        center_open_time(&prepended_state, &prepended_series),
        prepended_anchor
    );

    let mut merged_series = candles(40);
    let mut merged_state = ChartViewState::interactive(&merged_series, 16);
    merged_state.pan_x_older(&merged_series);
    let merged_anchor = center_open_time(&merged_state, &merged_series);
    let merged = merged_series.merge(vec![
        candle_at(BASE + 31 * MINUTE - 1, 0.25, 0.5),
        candle_at(BASE + 36 * MINUTE - 1, 0.25, 0.5),
    ]);
    assert!(matches!(&merged.old_to_new, IndexMapping::Explicit(_)));
    merged_state.apply_mutation(&merged_series, &merged, 16);
    assert_eq!(
        center_open_time(&merged_state, &merged_series),
        merged_anchor
    );
}

#[test]
fn mutation_reclamps_visible_count_to_smaller_plot_width() {
    let mut series = candles(100);
    let mut state = ChartViewState::snapshot(&series, 30);
    state.pan_x_older(&series);
    let anchor = center_open_time(&state, &series);

    let summary = series.append(candle(100, 101.0, 102.0));
    state.apply_mutation(&series, &summary, 7);

    assert_eq!(data(&state).visible_count(), 7);
    assert_eq!(center_open_time(&state, &series), anchor);
    assert!(!data(&state).follows_live());
}

#[test]
fn live_append_advances_only_while_following() {
    let mut following_series = candles(30);
    let mut following = ChartViewState::interactive(&following_series, 20);
    let append = following_series.append(candle(30, 31.0, 32.0));
    following.apply_mutation(&following_series, &append, 20);
    assert_eq!(data(&following).right_index(), 30);

    let mut paused_series = candles(30);
    let mut paused = ChartViewState::interactive(&paused_series, 20);
    paused.pan_x_older(&paused_series);
    let paused_center = center_open_time(&paused, &paused_series);
    let append = paused_series.append(candle(30, 31.0, 32.0));
    paused.apply_mutation(&paused_series, &append, 20);
    assert_eq!(center_open_time(&paused, &paused_series), paused_center);
    assert!(!data(&paused).follows_live());
}

#[test]
fn resize_preserves_exact_center_for_same_width_and_even_odd_shrinks() {
    for initial_width in [19, 20] {
        for resized_width in [initial_width, 12, 11] {
            let series = candles(100);
            let mut state = ChartViewState::snapshot(&series, initial_width);
            state.pan_x_older(&series);
            state.pan_x_older(&series);
            let anchor = center_open_time(&state, &series);
            state.resize(&series, resized_width);
            assert_eq!(
                center_open_time(&state, &series),
                anchor,
                "{initial_width}->{resized_width}"
            );
            assert_eq!(
                data(&state).visible_count(),
                initial_width.min(resized_width)
            );
            assert!(!data(&state).follows_live());
        }
    }

    let series = candles(100);
    let mut following = ChartViewState::interactive(&series, 40);
    following.resize(&series, 3);
    assert_eq!(data(&following).visible_count(), 3);
    assert_eq!(data(&following).right_index(), 99);
    assert!(data(&following).follows_live());
}

#[test]
fn auto_y_range_uses_canonical_epsilon_for_empty_and_zero_length_ranges() {
    let epsilon = 1.0 / ((1_u64 << 40) as f64);
    let empty = candles(0);
    assert_eq!(auto_y_range(&empty, 0..0).span(), epsilon);

    let series = series_from(vec![candle(0, 10.0, 20.0)]);
    assert_eq!(auto_y_range(&series, 0..0).span(), epsilon);
}

#[test]
fn auto_y_is_finite_for_empty_flat_subnormal_negative_and_extreme_values() {
    let empty = candles(0);
    assert_eq!(
        ChartViewState::interactive(&empty, 10).visible_range(),
        0..0
    );

    let fixtures = [
        candle(0, 0.0, 0.0),
        candle(1, -5.0, -5.0),
        candle(2, f64::from_bits(1), f64::from_bits(2)),
        candle(3, CHART_PRICE_MAX * 0.99, CHART_PRICE_MAX),
        candle(4, -CHART_PRICE_MAX, -CHART_PRICE_MAX * 0.99),
    ];
    for item in fixtures {
        let series = series_from(vec![item]);
        let range = auto_y_range(&series, 0..1);
        assert!(range.low.is_finite() && range.high.is_finite());
        assert!(range.high > range.low);
    }

    let flat = series_from(vec![candle(5, 2.0, 2.0)]);
    assert_close(
        auto_y_range(&flat, 0..1).span(),
        2.0 / ((1_u64 << 40) as f64),
    );
    let ordinary = series_from(vec![candle(6, -10.0, 10.0)]);
    assert_close(auto_y_range(&ordinary, 0..1).low, -11.0);
    assert_close(auto_y_range(&ordinary, 0..1).high, 11.0);
    let extremes = series_from(vec![candle(7, -CHART_PRICE_MAX, CHART_PRICE_MAX)]);
    let range = auto_y_range(&extremes, 0..1);
    assert!(range.low.is_finite() && range.high.is_finite() && range.high > range.low);
}

#[test]
fn manual_y_operations_clamp_to_exact_numeric_bounds() {
    let series = series_from(vec![
        Candle::from_rest(BASE, BASE + MINUTE - 1, 0.0, 0.5, -0.5, 0.0, 1.0)
            .expect("symmetric non-flat candle is valid"),
    ]);

    let mut upper = ChartViewState::interactive(&series, 1);
    upper.zoom_y_by_factor(f64::MAX);
    let upper_range = data(&upper).y_range();
    assert!(!data(&upper).auto_y());
    assert!(upper_range.low.is_finite() && upper_range.high.is_finite());
    assert_eq!(upper_range.span(), 1.10 * (2.0 * CHART_PRICE_MAX));

    let mut lower = ChartViewState::interactive(&series, 1);
    lower.zoom_y_by_factor(f64::MIN_POSITIVE);
    let lower_range = data(&lower).y_range();
    assert!(!data(&lower).auto_y());
    assert!(lower_range.low.is_finite() && lower_range.high.is_finite());
    assert_eq!(lower_range.span(), 1.0 / ((1_u64 << 40) as f64));
}

#[test]
fn public_mutation_methods_accept_valid_transients_then_reject_or_clear_invalid_state() {
    let series = candles(20);
    let mut state = ChartViewState::interactive(&series, 10);
    let valid_time = at(&series, 15).open_time();
    let valid_hover = CoordinateHover {
        open_time: valid_time,
        price: 15.5,
    };
    let valid_drag = ActiveDrag {
        kind: DragKind::Plot,
        anchor_open_time: Some(valid_time),
        anchor_price: Some(15.5),
    };

    state.set_coordinate_hover(&series, Some(valid_hover));
    assert_eq!(data(&state).coordinate_hover(), Some(valid_hover));
    state.set_active_drag(&series, Some(valid_drag));
    assert_eq!(data(&state).active_drag(), Some(valid_drag));

    state.set_coordinate_hover(
        &series,
        Some(CoordinateHover {
            open_time: valid_time,
            price: f64::NAN,
        }),
    );
    assert_eq!(data(&state).coordinate_hover(), None);
    state.set_coordinate_hover(&series, Some(valid_hover));
    state.set_coordinate_hover(
        &series,
        Some(CoordinateHover {
            open_time: BASE - 1,
            price: 1.0,
        }),
    );
    assert_eq!(data(&state).coordinate_hover(), None);

    state.set_active_drag(
        &series,
        Some(ActiveDrag {
            kind: DragKind::Plot,
            anchor_open_time: Some(valid_time),
            anchor_price: Some(f64::INFINITY),
        }),
    );
    assert_eq!(data(&state).active_drag(), None);
    state.set_active_drag(&series, Some(valid_drag));
    state.set_active_drag(
        &series,
        Some(ActiveDrag {
            kind: DragKind::Plot,
            anchor_open_time: Some(BASE - 1),
            anchor_price: Some(1.0),
        }),
    );
    assert_eq!(data(&state).active_drag(), None);

    let before = state.clone();
    state.zoom_y_by_factor(f64::NAN);
    assert_eq!(state, before);
    assert!(data(&state).visible_count() > 0);
    assert!(data(&state).right_index() < series.len());
    assert!(data(&state).y_range().low.is_finite());
    assert!(data(&state).y_range().high.is_finite());
}

#[test]
fn price_range_accessors_are_stable() {
    let range = PriceRange {
        low: -3.0,
        high: 5.0,
    };
    assert_eq!(range.span(), 8.0);
    assert_eq!(range.center(), 1.0);
}
