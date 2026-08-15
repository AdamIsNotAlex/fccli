use fccli::{
    chart::{
        ChartLayoutResult, ChartViewState, ChartWidget, CoordinateHover, CurrentPriceFreshness,
        DisplayStatus, InteractiveChartState, LayoutMode, RenderMode, RenderPolicy,
        RendererSnapshot, calculate_chart_layout,
    },
    error::{ErrorContext, ErrorOperation, ProviderError},
    model::{
        Candle, CandleSeries, Instrument, Market, MonoInstant, ProcessBlocker, ProviderId,
        RateGateState, Timeframe,
    },
};
use ratatui::{
    buffer::{Buffer, Cell},
    layout::{Rect, Size},
    style::{Color, Modifier, Style},
    widgets::Widget,
};

fn candle(open_time: i64, open: f64, high: f64, low: f64, close: f64, volume: f64) -> Candle {
    Candle::from_ws(
        open_time,
        open_time + 59_999,
        open,
        high,
        low,
        close,
        volume,
        false,
    )
    .expect("valid candle")
}

fn instrument() -> Instrument {
    Instrument::new(
        ProviderId::new("binance").expect("provider"),
        Market::Spot,
        "BTC",
        "USDT",
        "BTCUSDT",
    )
    .expect("instrument")
}

fn series() -> CandleSeries {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let _ = series.replace(vec![
        candle(1_700_000_000_000, 100.0, 112.0, 96.0, 108.0, 1_000.0),
        candle(1_700_000_060_000, 108.0, 111.0, 92.0, 97.0, 500.0),
        candle(1_700_000_120_000, 101.0, 109.0, 94.0, 101.0, 250.0),
    ]);
    series
}

fn snapshot(
    series: &CandleSeries,
    chart_state: ChartViewState,
    mode: RenderMode,
) -> RendererSnapshot {
    RendererSnapshot {
        mode,
        display_status: if mode == RenderMode::Snapshot {
            DisplayStatus::Snapshot
        } else {
            DisplayStatus::Connected
        },
        status_detail: None,
        rate_gate: RateGateState::Open,
        instrument: instrument(),
        timeframe: Timeframe::Minute1,
        candles: series.iter().cloned().collect::<Vec<_>>().into(),
        current_price_freshness: CurrentPriceFreshness::Fresh,
        chart_state: InteractiveChartState::Ready(chart_state),
        footer: fccli::chart::FooterPresentation::Help,
    }
}

fn symbol(buffer: &Buffer, x: u16, y: u16) -> &str {
    buffer[(x, y)].symbol()
}

fn style(buffer: &Buffer, x: u16, y: u16) -> Style {
    buffer[(x, y)].style()
}
fn assert_complete_style(cell: &Cell, fg: Color, bg: Color) {
    assert_eq!(cell.fg, fg);
    assert_eq!(cell.bg, bg);
    assert_eq!(cell.underline_color, Color::Reset);
    assert!(cell.modifier.is_empty());
}

fn symbols_in(buffer: &Buffer, area: Rect) -> Vec<String> {
    let mut symbols = Vec::with_capacity(usize::from(area.width) * usize::from(area.height));
    for y in area.y..area.bottom() {
        for x in area.x..area.right() {
            symbols.push(symbol(buffer, x, y).to_owned());
        }
    }
    symbols
}

fn is_price_candle_glyph(symbol: &str) -> bool {
    matches!(
        symbol,
        "█" | "▓" | "▀" | "▄" | "━" | "│" | "┃" | "╽" | "╿" | "╷" | "╵" | "╻" | "╹"
    )
}

#[test]
fn pending_renders_only_stable_resize_message_and_clears_styles() {
    let area = Rect::new(7, 5, 59, 17);
    let mut buffer = Buffer::filled(
        area,
        ratatui::buffer::Cell::new("X")
            .set_style(Style::default().fg(Color::Magenta))
            .clone(),
    );
    let snapshot = snapshot(&series(), ChartViewState::Empty, RenderMode::Interactive);
    let layout = ChartLayoutResult::LayoutPending {
        required: Size::new(60, 18),
        actual: Size::new(59, 17),
    };
    ChartWidget::new(&snapshot, &layout, RenderPolicy::StyleFree).render(area, &mut buffer);

    let line: String = (area.x..area.right())
        .map(|x| symbol(&buffer, x, area.y))
        .collect();
    assert!(line.starts_with("Resize terminal to at least 60x18 (current 59x17)"));
    assert_eq!(buffer[(area.x, area.y)].fg, Color::Reset);
    assert_eq!(buffer[(area.x, area.y)].bg, Color::Reset);
    assert_eq!(symbol(&buffer, area.x, area.y + 1), " ");
}

#[test]
fn snapshot_header_is_literal_snapshot_even_when_inputs_are_blocked() {
    let area = Rect::new(0, 0, 80, 24);
    let layout = calculate_chart_layout(area, LayoutMode::Snapshot);
    let series = series();
    let state = ChartViewState::snapshot(&series, 65);
    let mut snapshot = snapshot(&series, state, RenderMode::Snapshot);
    snapshot.display_status = DisplayStatus::TerminalError;
    snapshot.rate_gate =
        RateGateState::ProcessBlocked(fccli::model::ProcessBlocker::InvalidBanExpiry);
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(&snapshot, &layout, RenderPolicy::StyleFree).render(area, &mut buffer);
    let first: String = (0..80).map(|x| symbol(&buffer, x, 0)).collect();
    assert!(first.contains("SNAPSHOT"));
    assert!(!first.contains("ERROR"));
    assert!(!first.contains("BLOCKED"));
}

#[test]
fn maximum_length_identity_at_minimum_width_preserves_styled_status() {
    let area = Rect::new(0, 0, 60, 19);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("minimum interactive area must be adequate");
    };
    let series = series();
    let state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    let mut snapshot = snapshot(&series, state, RenderMode::Interactive);
    let base = "A".repeat(252);
    snapshot.instrument = Instrument::new(
        ProviderId::new("binance").expect("provider"),
        Market::Spot,
        &base,
        "USDT",
        format!("{base}USDT"),
    )
    .expect("maximum-length valid instrument");
    let mut buffer = Buffer::empty(area);

    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut buffer);

    let header: String = (layout.header.x..layout.header.right())
        .map(|x| symbol(&buffer, x, layout.header.y))
        .collect();
    assert!(header.starts_with("binance Spot A"), "{header:?}");
    assert!(header.ends_with(" LIVE"), "{header:?}");
    let status_start = layout.header.right() - "LIVE".len() as u16;
    for x in status_start..layout.header.right() {
        assert_complete_style(&buffer[(x, layout.header.y)], Color::Green, Color::Reset);
    }
}

#[test]
fn unbounded_provider_identity_is_capped_and_preserves_live_status() {
    let area = Rect::new(0, 0, 60, 19);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("minimum interactive area must be adequate");
    };
    let series = series();
    let state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    let mut snapshot = snapshot(&series, state, RenderMode::Interactive);
    snapshot.instrument = Instrument::new(
        ProviderId::new("P".repeat(1_000_000)).expect("long alphanumeric provider is valid"),
        Market::Spot,
        "BTC",
        "USDT",
        "BTCUSDT",
    )
    .expect("valid instrument");
    let mut buffer = Buffer::empty(area);

    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut buffer);

    let header: String = (layout.header.x..layout.header.right())
        .map(|x| symbol(&buffer, x, layout.header.y))
        .collect();
    assert_eq!(header.chars().count(), usize::from(layout.header.width));
    assert!(header.starts_with("PPPPPPPP"), "{header:?}");
    assert!(header.ends_with(" LIVE"), "{header:?}");
    assert!(
        !header.contains("Spot"),
        "identity exceeded its width budget: {header:?}"
    );
    let status_start = layout.header.right() - "LIVE".len() as u16;
    for x in status_start..layout.header.right() {
        assert_complete_style(&buffer[(x, layout.header.y)], Color::Green, Color::Reset);
    }
}

#[test]
fn minimum_width_preserves_every_canonical_status_token() {
    let area = Rect::new(0, 0, 60, 19);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("minimum interactive area must be adequate");
    };
    let series = series();
    let state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));

    let cases = [
        (DisplayStatus::Connected, RateGateState::Open, "LIVE"),
        (DisplayStatus::Connecting, RateGateState::Open, "CONNECTING"),
        (DisplayStatus::Backoff, RateGateState::Open, "RECONNECTING"),
        (DisplayStatus::GapSync, RateGateState::Open, "SYNCING"),
        (DisplayStatus::Stopped, RateGateState::Open, "STOPPED"),
        (
            DisplayStatus::Backfilling,
            RateGateState::Open,
            "BACKFILLING",
        ),
        (DisplayStatus::TerminalError, RateGateState::Open, "ERROR"),
        (
            DisplayStatus::Disconnected,
            RateGateState::Open,
            "DISCONNECTED",
        ),
        (
            DisplayStatus::Connected,
            RateGateState::TimedUntil(MonoInstant::from_millis(123).expect("instant")),
            "RATE LIMITED",
        ),
        (
            DisplayStatus::Connected,
            RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry),
            "RATE BLOCKED",
        ),
    ];

    for (display_status, rate_gate, token) in cases {
        let mut snapshot = snapshot(&series, state.clone(), RenderMode::Interactive);
        snapshot.display_status = display_status;
        snapshot.rate_gate = rate_gate;
        snapshot.status_detail = Some(ProviderError::InvalidBanExpiry);
        let mut buffer = Buffer::empty(area);
        ChartWidget::new(
            &snapshot,
            &ChartLayoutResult::Ready { layout },
            RenderPolicy::StyleFree,
        )
        .render(area, &mut buffer);
        let header: String = (layout.header.x..layout.header.right())
            .map(|x| symbol(&buffer, x, layout.header.y))
            .collect();
        assert!(header.contains(token), "missing {token:?} in {header:?}");
    }

    let snapshot = snapshot(&series, state, RenderMode::Snapshot);
    let snapshot_layout = calculate_chart_layout(area, LayoutMode::Snapshot);
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(&snapshot, &snapshot_layout, RenderPolicy::StyleFree)
        .render(area, &mut buffer);
    let header: String = (0..60).map(|x| symbol(&buffer, x, 0)).collect();
    assert!(header.contains("SNAPSHOT"), "{header:?}");
}

#[test]
fn million_character_status_provider_is_bounded_without_hiding_error_token() {
    let area = Rect::new(0, 0, 60, 19);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("minimum interactive area must be adequate");
    };
    let series = series();
    let state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    let mut snapshot = snapshot(&series, state, RenderMode::Interactive);
    snapshot.display_status = DisplayStatus::TerminalError;
    let provider = ProviderId::new("P".repeat(1_000_000)).expect("provider");
    snapshot.status_detail = Some(ProviderError::ChannelClosed {
        context: ErrorContext::operation(ErrorOperation::Channel).with_provider(&provider),
    });
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut buffer);

    let header: String = (0..60)
        .map(|x| symbol(&buffer, x, layout.header.y))
        .collect();
    assert_eq!(header.len(), 60);
    assert!(header.contains("ERROR"), "{header:?}");
}

#[test]
fn non_ascii_combining_and_control_status_detail_is_single_cell_normalized_and_clipped() {
    let area = Rect::new(0, 0, 60, 19);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("minimum interactive area must be adequate");
    };
    let series = series();
    let state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    let mut snapshot = snapshot(&series, state, RenderMode::Interactive);
    snapshot.display_status = DisplayStatus::TerminalError;
    snapshot.instrument = Instrument::new(
        ProviderId::new("b").expect("provider"),
        Market::Spot,
        "B",
        "U",
        "BU",
    )
    .expect("short instrument");
    snapshot.status_detail = Some(ProviderError::Invariant("界e\u{301}\n\u{1b}[31m界"));
    let mut buffer = Buffer::filled(area, Cell::new("X"));
    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::StyleFree,
    )
    .render(area, &mut buffer);

    let header: String = (0..60)
        .map(|x| symbol(&buffer, x, layout.header.y))
        .collect();
    assert_eq!(header.len(), 60);
    assert!(header.contains("ERROR"), "{header:?}");
    assert!(header.is_ascii(), "{header:?}");
    assert!(
        header.contains('?'),
        "normalized detail was not rendered: {header:?}"
    );
    assert!(!header.contains('界'));
    assert!(!header.contains('\u{301}'));
    assert!(!header.contains('\n'));
    assert!(!header.contains('\u{1b}'));
    assert!(
        !header.contains('X'),
        "status spilled outside cleared header: {header:?}"
    );
}

#[test]
fn style_free_grid_candles_volume_and_axes_use_default_style() {
    let area = Rect::new(3, 2, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate layout");
    };
    let series = series();
    let state = ChartViewState::snapshot(&series, usize::from(layout.main_plot.width));
    let snapshot = snapshot(&series, state, RenderMode::Snapshot);
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::StyleFree,
    )
    .render(area, &mut buffer);

    for y in area.y..area.bottom() {
        for x in area.x..area.right() {
            let cell = &buffer[(x, y)];
            assert_eq!(cell.fg, Color::Reset);
            assert_eq!(cell.bg, Color::Reset);
            assert!(cell.modifier.is_empty());
        }
    }
    let all = symbols_in(&buffer, area).concat();
    assert!(all.contains('─'));
    assert!(all.contains('█'));
    assert!(all.contains('▓'));
    assert!(all.contains('━'));
}

#[test]
fn half_cell_projection_exercises_complete_wick_and_body_edge_inventory() {
    let area = Rect::new(0, 0, 120, 36);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate layout");
    };
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let shapes = [
        (2.0, 10.0, 0.0, 8.0),
        (8.0, 10.0, 0.0, 2.0),
        (1.0, 10.0, 0.0, 9.5),
        (9.5, 9.5, 1.0, 1.0),
        (2.5, 9.5, 1.0, 7.5),
        (7.5, 9.0, 1.0, 2.5),
        (5.0, 10.0, 0.0, 5.0),
    ];
    let candles: Vec<_> = (0..84)
        .map(|index| {
            let (open, high, low, close) = shapes[index % shapes.len()];
            candle(
                1_700_100_000_000 + i64::try_from(index).expect("small index") * 60_000,
                open,
                high,
                low,
                close,
                1.0,
            )
        })
        .collect();
    let _ = series.replace(candles);
    let state = ChartViewState::snapshot(&series, usize::from(layout.main_plot.width));
    let snapshot = snapshot(&series, state, RenderMode::Snapshot);
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut buffer);
    let symbols: std::collections::BTreeSet<_> =
        symbols_in(&buffer, layout.main_plot).into_iter().collect();
    for required in ["│", "┃", "╷", "╵", "╻", "╹", "╽", "╿"] {
        assert!(
            symbols.contains(required),
            "missing projection glyph {required}"
        );
    }
}
#[test]
fn color_policy_uses_direction_grid_and_volume_styles() {
    let area = Rect::new(0, 0, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate layout");
    };
    let series = series();
    let state = ChartViewState::snapshot(&series, usize::from(layout.main_plot.width));
    let snapshot = snapshot(&series, state, RenderMode::Snapshot);
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut buffer);

    let geometry = fccli::chart::CandleSlotGeometry::new(
        layout.main_plot.x,
        layout.main_plot.width,
        series.len(),
    )
    .expect("geometry");
    for (index, expected) in [(0, Color::Rgb(52, 208, 88)), (1, Color::Rgb(234, 74, 90))] {
        let x = geometry.center(index).expect("candle center");
        assert!(
            (layout.main_plot.y..layout.main_plot.bottom()).any(|y| {
                is_price_candle_glyph(symbol(&buffer, x, y))
                    && style(&buffer, x, y).fg == Some(expected)
            }),
            "candle {index} did not use {expected:?}"
        );
    }
    let doji_x = geometry.center(2).expect("doji center");
    let current_price_y = (layout.main_plot.y..layout.main_plot.bottom())
        .find(|&y| {
            (layout.main_plot.x..layout.main_plot.right()).any(|x| symbol(&buffer, x, y) == "┄")
        })
        .expect("current price line present on non-candle columns");
    // The current-price overlay is candle-transparent: the doji's `━` glyph
    // is preserved at the doji column, while `┄` appears on empty columns.
    assert_eq!(symbol(&buffer, doji_x, current_price_y), "━");
    assert!(
        (layout.main_plot.x..layout.main_plot.right())
            .any(|x| symbol(&buffer, x, current_price_y) == "┄")
    );
    let overlay_x = (layout.main_plot.x..layout.main_plot.right())
        .find(|&x| symbol(&buffer, x, current_price_y) == "┄")
        .expect("non-candle overlay cell");
    assert_complete_style(
        &buffer[(overlay_x, current_price_y)],
        Color::Cyan,
        Color::Reset,
    );

    let mut saw_volume_green = false;
    let mut saw_volume_red = false;
    let mut saw_grid = false;
    for y in layout.volume.y..layout.volume.bottom() {
        for x in layout.main_plot.x..layout.main_plot.right() {
            let cell = &buffer[(x, y)];
            saw_volume_green |=
                cell.symbol() == "█" && cell.style().fg == Some(Color::Rgb(52, 208, 88));
            saw_volume_red |=
                cell.symbol() == "█" && cell.style().fg == Some(Color::Rgb(234, 74, 90));
        }
    }
    for y in layout.main_plot.y..layout.main_plot.bottom() {
        for x in layout.main_plot.x..layout.main_plot.right() {
            let cell = &buffer[(x, y)];
            saw_grid |= cell.symbol() == "─" && cell.style().fg == Some(Color::DarkGray);
        }
    }
    assert!(saw_volume_green && saw_volume_red && saw_grid);
}

#[test]
fn crosshair_is_last_continues_through_gutter_and_axes_are_final() {
    let area = Rect::new(5, 4, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate layout");
    };
    let series = series();
    let mut state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    state.set_coordinate_hover(
        &series,
        Some(CoordinateHover {
            open_time: 1_700_000_060_000,
            price: 100.0,
        }),
    );
    let snapshot = snapshot(&series, state, RenderMode::Interactive);
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut buffer);

    let geometry = fccli::chart::CandleSlotGeometry::new(
        layout.main_plot.x,
        layout.main_plot.width,
        series.len(),
    )
    .expect("geometry");
    let crosshair_x = geometry.center(1).expect("hovered candle center");
    let crosshair_y = layout.main_plot.y
        + ((112.0 - 100.0) / 20.0 * f64::from(layout.main_plot.height - 1)).round() as u16;

    assert!(is_price_candle_glyph(symbol(
        &buffer,
        crosshair_x,
        crosshair_y
    )));
    for x in layout.gutter.x..layout.gutter.right() {
        assert_eq!(symbol(&buffer, x, crosshair_y), "┄");
        assert_eq!(style(&buffer, x, crosshair_y).fg, Some(Color::Yellow));
    }
    let axis_text: String = (layout.price_axis.x..layout.price_axis.right())
        .map(|x| symbol(&buffer, x, crosshair_y))
        .collect();
    assert_eq!(
        axis_text.chars().count(),
        usize::from(layout.price_axis.width)
    );
    for x in layout.price_axis.x..layout.price_axis.right() {
        assert_eq!(style(&buffer, x, crosshair_y).fg, Some(Color::Black));
        assert_eq!(style(&buffer, x, crosshair_y).bg, Some(Color::Yellow));
    }
    assert!(
        (layout.main_plot.y..layout.main_plot.bottom()).all(|y| matches!(
            symbol(&buffer, crosshair_x, y),
            "┆"
        ) || is_price_candle_glyph(
            symbol(&buffer, crosshair_x, y)
        ))
    );
    assert!(
        (layout.main_plot.y..layout.main_plot.bottom())
            .any(|y| symbol(&buffer, crosshair_x, y) == "┆")
    );
    assert_ne!(symbol(&buffer, crosshair_x, layout.volume.y), "┆");
    assert_ne!(symbol(&buffer, crosshair_x, layout.header.y), "┆");
}
#[test]
fn current_price_uses_canonical_tail_and_crosshair_wins_same_row() {
    let area = Rect::new(5, 4, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate layout");
    };
    let series = series();
    let mut state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    state.set_coordinate_hover(
        &series,
        Some(CoordinateHover {
            open_time: 1_700_000_060_000,
            price: 101.0,
        }),
    );
    let snapshot = snapshot(&series, state, RenderMode::Interactive);
    let buffer = render_with_sentinel(&snapshot, layout, RenderPolicy::Color);
    let y = layout.main_plot.y
        + ((112.0 - 101.0) / 20.0 * f64::from(layout.main_plot.height - 1)).round() as u16;

    assert_eq!(row_text(&buffer, layout.price_axis, y).trim_end(), "101");
    for x in layout.price_axis.x..layout.price_axis.right() {
        assert_complete_style(&buffer[(x, y)], Color::Black, Color::Yellow);
    }
    assert!(
        (layout.main_plot.x..layout.main_plot.right())
            .any(|x| symbol(&buffer, x, y) == "┄" || symbol(&buffer, x, y) == "┼")
    );
}

#[test]
fn current_price_fresh_stale_and_offscreen_contracts_cover_both_policies() {
    let area = Rect::new(6, 4, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate layout");
    };
    let series = series();
    let state = ChartViewState::snapshot(&series, usize::from(layout.main_plot.width));

    for (freshness, policy, glyph, foreground) in [
        (
            CurrentPriceFreshness::Fresh,
            RenderPolicy::Color,
            "┄",
            Color::Cyan,
        ),
        (
            CurrentPriceFreshness::Stale,
            RenderPolicy::Color,
            "╌",
            Color::DarkGray,
        ),
        (
            CurrentPriceFreshness::Fresh,
            RenderPolicy::StyleFree,
            "┄",
            Color::Reset,
        ),
        (
            CurrentPriceFreshness::Stale,
            RenderPolicy::StyleFree,
            "╌",
            Color::Reset,
        ),
    ] {
        let mut input = snapshot(&series, state.clone(), RenderMode::Snapshot);
        input.current_price_freshness = freshness;
        let rendered = render_with_sentinel(&input, layout, policy);
        let range = state.viewport().expect("data").y_range();
        let y = layout.main_plot.y
            + ((range.high - 101.0) / range.span() * f64::from(layout.main_plot.height - 1)).round()
                as u16;
        // The overlay is candle-transparent: non-candle columns show the
        // price glyph, candle columns preserve the candle glyph.
        assert!(
            (layout.main_plot.x..layout.main_plot.right())
                .all(|x| is_price_candle_glyph(symbol(&rendered, x, y))
                    || symbol(&rendered, x, y) == glyph)
        );
        assert!(
            (layout.main_plot.x..layout.main_plot.right())
                .any(|x| symbol(&rendered, x, y) == glyph)
        );
        for x in layout.price_axis.x..layout.price_axis.right() {
            assert_eq!(rendered[(x, y)].fg, foreground);
        }
        assert_eq!(rendered[(layout.gutter.x, y)].fg, foreground);
    }

    let mut offscreen = snapshot(&series, state, RenderMode::Snapshot);
    offscreen.candles = vec![candle(1_700_000_180_000, 200.0, 210.0, 190.0, 200.0, 1.0)].into();
    let rendered = render_with_sentinel(&offscreen, layout, RenderPolicy::StyleFree);
    assert!(
        !symbols_in(&rendered, layout.main_plot)
            .iter()
            .any(|glyph| glyph == "┄" || glyph == "╌")
    );
    assert!(
        (layout.main_plot.y..layout.main_plot.bottom()).all(|y| row_text(
            &rendered,
            layout.price_axis,
            y
        )
        .trim_end()
            != "200")
    );
}

#[test]
fn hover_none_reverts_header_to_latest_and_removes_all_overlays() {
    let area = Rect::new(0, 0, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate layout");
    };
    let series = series();
    let state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    let snapshot = snapshot(&series, state, RenderMode::Interactive);
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(
        &snapshot,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut buffer);

    let all = symbols_in(&buffer, area).concat();
    assert!(!all.contains('┆'));
    assert!(!all.contains('┼'));
    let header: String = (layout.header.x..layout.header.right())
        .map(|x| symbol(&buffer, x, layout.header.y + 1))
        .collect();
    assert!(header.contains("C:101"));
}

#[test]
fn renderer_preserves_cells_outside_nonzero_retained_frame() {
    let buffer_area = Rect::new(0, 0, 100, 35);
    let frame = Rect::new(8, 6, 80, 24);
    let layout = calculate_chart_layout(frame, LayoutMode::Snapshot);
    let series = series();
    let state = ChartViewState::snapshot(&series, 65);
    let snapshot = snapshot(&series, state, RenderMode::Snapshot);
    let sentinel = ratatui::buffer::Cell::new("Z")
        .set_style(Style::default().fg(Color::Magenta))
        .clone();
    let mut buffer = Buffer::filled(buffer_area, sentinel);
    ChartWidget::new(&snapshot, &layout, RenderPolicy::Color).render(frame, &mut buffer);

    assert_eq!(symbol(&buffer, 0, 0), "Z");
    assert_eq!(style(&buffer, 0, 0).fg, Some(Color::Magenta));
    assert_eq!(symbol(&buffer, frame.right(), frame.y), "Z");
    assert_eq!(symbol(&buffer, frame.x, frame.bottom()), "Z");
}

#[test]
fn plot_width_ceiling_rejects_greater_than_width_but_renders_equal_boundary() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate layout");
    };
    let width = usize::from(layout.main_plot.width);
    let mut many = CandleSeries::new(Timeframe::Minute1);
    let _ = many.replace(
        (0..=width)
            .map(|index| {
                candle(
                    1_700_200_000_000 + i64::try_from(index).expect("small") * 60_000,
                    100.0,
                    102.0,
                    99.0,
                    101.0,
                    1.0,
                )
            })
            .collect(),
    );

    let too_wide = snapshot(
        &many,
        ChartViewState::snapshot(&many, width + 1),
        RenderMode::Snapshot,
    );
    let sentinel = Cell::new("X")
        .set_style(
            Style::default()
                .fg(Color::Magenta)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        )
        .clone();
    let mut rejected = Buffer::filled(area, sentinel.clone());
    ChartWidget::new(
        &too_wide,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut rejected);
    assert!(
        symbols_in(&rejected, layout.main_plot)
            .iter()
            .all(|cell| cell == " "),
        "visible_count > plot_width must render no aggregated candle or grid"
    );
    assert!(
        symbols_in(&rejected, layout.price_axis)
            .iter()
            .all(|cell| cell == " ")
    );

    let exactly_wide = snapshot(
        &many,
        ChartViewState::snapshot(&many, width),
        RenderMode::Snapshot,
    );
    let mut accepted = Buffer::filled(area, sentinel);
    ChartWidget::new(
        &exactly_wide,
        &ChartLayoutResult::Ready { layout },
        RenderPolicy::Color,
    )
    .render(area, &mut accepted);
    let rendered = symbols_in(&accepted, layout.main_plot);
    assert!(
        rendered
            .iter()
            .any(|cell| matches!(cell.as_str(), "█" | "│" | "╽" | "╿"))
    );
    assert!(rendered.iter().all(|cell| cell != "X"));
}

#[test]
fn layout_pending_chart_state_with_ready_layout_renders_empty_not_a_dummy_view() {
    let area = Rect::new(0, 0, 60, 18);
    let layout = calculate_chart_layout(area, LayoutMode::Interactive);
    let series = series();
    let mut snapshot = snapshot(&series, ChartViewState::Empty, RenderMode::Interactive);
    snapshot.chart_state = InteractiveChartState::LayoutPending;
    let mut buffer = Buffer::empty(area);
    ChartWidget::new(&snapshot, &layout, RenderPolicy::StyleFree).render(area, &mut buffer);
    let all = symbols_in(&buffer, area).concat();
    assert!(all.contains("Waiting for market data"));
    assert!(!all.contains('█'));
}

fn render_with_sentinel(
    snapshot: &RendererSnapshot,
    layout: fccli::chart::ChartLayout,
    policy: RenderPolicy,
) -> Buffer {
    let sentinel = Cell::new("X")
        .set_style(
            Style::default()
                .fg(Color::Magenta)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        )
        .clone();
    let mut buffer = Buffer::filled(
        Rect::new(0, 0, layout.frame.right() + 1, layout.frame.bottom() + 1),
        sentinel,
    );
    ChartWidget::new(snapshot, &ChartLayoutResult::Ready { layout }, policy)
        .render(layout.frame, &mut buffer);
    buffer
}

fn row_text(buffer: &Buffer, rect: Rect, y: u16) -> String {
    (rect.x..rect.right())
        .map(|x| symbol(buffer, x, y))
        .collect()
}

#[test]
fn projection_table_covers_every_half_cell_body_doji_clip_and_dynamic_body_widths() {
    let area = Rect::new(4, 3, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate layout");
    };
    let value_at_half = |half: u16| (10.5 - f64::from(half) * 11.0 / 23.0).clamp(0.0, 10.0);
    let specifications = [
        (4, 17),
        (17, 4),
        (5, 5),
        (1, 22),
        (2, 21),
        (3, 20),
        (7, 18),
        (18, 7),
    ];
    let mut candles = CandleSeries::new(Timeframe::Minute1);
    let _ = candles.replace(
        specifications
            .iter()
            .enumerate()
            .map(|(index, &(open_half, close_half))| {
                candle(
                    1_700_300_000_000 + i64::try_from(index).expect("small") * 60_000,
                    value_at_half(open_half),
                    10.0,
                    0.0,
                    value_at_half(close_half),
                    1.0,
                )
            })
            .collect(),
    );
    let renderer_snapshot = snapshot(
        &candles,
        ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width)),
        RenderMode::Snapshot,
    );
    let buffer = render_with_sentinel(&renderer_snapshot, layout, RenderPolicy::Color);
    let geometry = fccli::chart::CandleSlotGeometry::new(
        layout.main_plot.x,
        layout.main_plot.width,
        specifications.len(),
    )
    .expect("geometry");

    let exact_center_cells = [
        (0, 0, "╷"),
        (0, 1, "│"),
        (0, 2, "┃"),
        (0, 8, "┃"),
        (0, 9, "│"),
        (0, 10, "│"),
        (0, 11, "╵"),
        (1, 0, "╷"),
        (1, 1, "│"),
        (1, 2, "┃"),
        (1, 8, "┃"),
        (1, 9, "│"),
        (1, 10, "│"),
        (1, 11, "╵"),
        (2, 0, "╷"),
        (2, 2, "━"),
        (2, 11, "╵"),
        (3, 0, "╻"),
        (3, 1, "┃"),
        (3, 10, "┃"),
        (3, 11, "╹"),
        (4, 0, "╷"),
        (4, 1, "┃"),
        (4, 10, "┃"),
        (4, 11, "╵"),
        (5, 0, "╷"),
        (5, 1, "╽"),
        (5, 10, "╿"),
        (5, 11, "╵"),
        (6, 0, "╷"),
        (6, 3, "╽"),
        (6, 9, "╿"),
        (6, 11, "╵"),
        (7, 0, "╷"),
        (7, 3, "╽"),
        (7, 9, "╿"),
        (7, 11, "╵"),
    ];
    for &(index, row, expected) in &exact_center_cells {
        let x = geometry.center(index).expect("center");
        let y = layout.main_plot.y + row;
        let actual = symbol(&buffer, x, y);
        if actual != "┄" {
            assert_eq!(actual, expected, "slot {index}, row {row}");
        }
    }

    for index in [0_usize, 1, 3, 4, 5, 6, 7] {
        let slot = geometry.slot(index).expect("slot");
        let body_row = match index {
            0 | 1 | 4 | 5 => 5,
            3 => 6,
            6 | 7 => 6,
            _ => unreachable!(),
        };
        let center = slot.center();
        assert_eq!(
            symbol(&buffer, center, layout.main_plot.y + body_row),
            "┃",
            "center body slot {index}"
        );
        for x in slot.start()..slot.end() {
            let x = u16::try_from(x).expect("valid layout slot coordinate fits u16");
            if x == u16::try_from(slot.end() - 1).expect("gap column") {
                assert!(!is_price_candle_glyph(symbol(
                    &buffer,
                    x,
                    layout.main_plot.y + body_row
                )));
            } else if x != center {
                assert_eq!(
                    symbol(&buffer, x, layout.main_plot.y + body_row),
                    "█",
                    "body edge slot {index}"
                );
            }
        }
    }
    for (index, relative_row, expected) in [(5_usize, 1_u16, "▄"), (6, 3, "▄"), (7, 3, "▄")] {
        let slot = geometry.slot(index).expect("slot");
        for x in slot.painted_range() {
            let x = u16::try_from(x).expect("painted coordinate");
            if x != slot.center() {
                let actual = symbol(&buffer, x, layout.main_plot.y + relative_row);
                if actual != "┄" {
                    assert_eq!(actual, expected, "half-cell body edge slot {index}");
                }
            }
        }
    }
    let doji = geometry.slot(2).expect("doji slot");
    for x in doji.start()..doji.end() {
        let x = u16::try_from(x).expect("valid layout slot coordinate fits u16");
        if x < u16::try_from(doji.painted_range().end).expect("painted end") {
            assert_eq!(symbol(&buffer, x, layout.main_plot.y + 2), "━");
        } else {
            assert_ne!(symbol(&buffer, x, layout.main_plot.y + 2), "━");
        }
    }
    let mut clipped_state = ChartViewState::snapshot(&candles, usize::from(layout.main_plot.width));
    clipped_state.zoom_y_by_factor(0.5);
    let clipped_snapshot = snapshot(&candles, clipped_state, RenderMode::Snapshot);
    let clipped = render_with_sentinel(&clipped_snapshot, layout, RenderPolicy::Color);
    let full_body_center = geometry.center(3).expect("full body center");
    assert_eq!(symbol(&clipped, full_body_center, layout.main_plot.y), "┃");
    assert_eq!(
        symbol(&clipped, full_body_center, layout.main_plot.bottom() - 1),
        "┃"
    );
}

#[test]
fn interactive_status_precedence_is_a_complete_table() {
    let area = Rect::new(0, 0, 120, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let series = series();
    let state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
    let statuses = [
        (DisplayStatus::Snapshot, "SNAPSHOT"),
        (DisplayStatus::Connected, "LIVE"),
        (DisplayStatus::Connecting, "CONNECTING"),
        (DisplayStatus::Backoff, "RECONNECTING"),
        (DisplayStatus::GapSync, "SYNCING"),
        (DisplayStatus::Stopped, "STOPPED"),
        (DisplayStatus::Backfilling, "BACKFILLING"),
        (
            DisplayStatus::TerminalError,
            "ERROR: provider configuration is invalid: detail",
        ),
        (
            DisplayStatus::Disconnected,
            "DISCONNECTED: provider configuration is invalid: detail",
        ),
    ];
    for &(status, base) in &statuses {
        for (gate, expected) in [
            (
                RateGateState::Open,
                if matches!(
                    status,
                    DisplayStatus::TerminalError | DisplayStatus::Disconnected
                ) {
                    base.to_owned()
                } else {
                    format!("{base}: provider configuration is invalid: detail")
                },
            ),
            (
                RateGateState::TimedUntil(MonoInstant::from_millis(42).expect("deadline")),
                if matches!(
                    status,
                    DisplayStatus::TerminalError | DisplayStatus::Disconnected
                ) {
                    base.to_owned()
                } else {
                    "RATE LIMITED UNTIL 42ms: provider configuration is invalid: detail".to_owned()
                },
            ),
            (
                RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry),
                if matches!(
                    status,
                    DisplayStatus::TerminalError | DisplayStatus::Disconnected
                ) {
                    base.to_owned()
                } else {
                    "RATE BLOCKED: provider configuration is invalid: detail".to_owned()
                },
            ),
        ] {
            let mut current = snapshot(&series, state.clone(), RenderMode::Interactive);
            current.display_status = status;
            current.status_detail = Some(ProviderError::Configuration("detail"));
            current.rate_gate = gate;
            let buffer = render_with_sentinel(&current, layout, RenderPolicy::StyleFree);
            let first = row_text(&buffer, layout.header, layout.header.y);
            assert!(first.starts_with("binance Spot BTC/USDT 1m"), "{first:?}");
            assert!(
                first.trim_end().ends_with(
                    &expected[..expected
                        .len()
                        .min(usize::from(layout.header.width).saturating_sub(26))]
                ),
                "status={status:?}, gate={gate:?}, row={first:?}"
            );
        }
    }
}

#[test]
fn utc_and_price_overlays_are_exact_clamped_padded_and_do_not_spill() {
    let area = Rect::new(6, 4, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let count = usize::from(layout.main_plot.width);
    let visible_count = count.min(10_usize.max(count / 2));
    let first_visible = count - visible_count;
    let _ = series.replace(
        (0..count)
            .map(|index| {
                candle(
                    1_700_000_000_000 + i64::try_from(index).expect("small") * 1_000,
                    100.0,
                    112.0,
                    92.0,
                    101.0,
                    1.0,
                )
            })
            .collect(),
    );
    for (open_time, expected_start, expected_label) in [
        (
            1_700_000_000_000 + i64::try_from(first_visible).expect("small") * 1_000,
            layout.utc_axis.x,
            "22:13:53",
        ),
        (
            1_700_000_000_000 + i64::try_from(count - 1).expect("small") * 1_000,
            layout.utc_axis.right() - 8,
            "22:14:24",
        ),
    ] {
        for policy in [RenderPolicy::Color, RenderPolicy::StyleFree] {
            let mut state =
                ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
            state.set_coordinate_hover(
                &series,
                Some(CoordinateHover {
                    open_time,
                    price: 100.0,
                }),
            );
            let snapshot = snapshot(&series, state, RenderMode::Interactive);
            let buffer = render_with_sentinel(&snapshot, layout, policy);
            assert_eq!(
                (expected_start..expected_start + 8)
                    .map(|x| symbol(&buffer, x, layout.utc_axis.y))
                    .collect::<String>(),
                expected_label
            );
            // Ratatui materializes a complete reset style in each Cell; assert the
            // concrete representation instead of comparing partial Style patches.
            for x in expected_start..expected_start + 8 {
                let cell = &buffer[(x, layout.utc_axis.y)];
                if policy == RenderPolicy::Color {
                    assert_complete_style(cell, Color::Black, Color::Yellow);
                } else {
                    assert_complete_style(cell, Color::Reset, Color::Reset);
                }
            }
            assert_eq!(
                symbol(&buffer, layout.utc_axis.right(), layout.utc_axis.y),
                " "
            );
            let y = layout.main_plot.y
                + ((113.0 - 100.0) / 22.0 * f64::from(layout.main_plot.height - 1)).round() as u16;
            let price = row_text(&buffer, layout.price_axis, y);
            assert_eq!(
                price,
                format!(
                    "{:<width$}",
                    "100",
                    width = usize::from(layout.price_axis.width)
                )
            );
            for x in layout.price_axis.x..layout.price_axis.right() {
                let cell = &buffer[(x, y)];
                if policy == RenderPolicy::Color {
                    assert_complete_style(cell, Color::Black, Color::Yellow);
                } else {
                    assert_complete_style(cell, Color::Reset, Color::Reset);
                }
            }
            assert_eq!(symbol(&buffer, layout.price_axis.x - 1, y), "┄");
            assert_eq!(symbol(&buffer, layout.price_axis.right(), y), "X");
        }
    }
}

#[test]
fn horizontal_grid_fills_every_tick_row_and_never_draws_vertical_columns() {
    let area = Rect::new(2, 2, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let series = series();
    let mut state = ChartViewState::snapshot(&series, usize::from(layout.main_plot.width));
    for _ in 0..32 {
        state.pan_y_up();
    }

    for policy in [RenderPolicy::Color, RenderPolicy::StyleFree] {
        let rendered = render_with_sentinel(
            &snapshot(&series, state.clone(), RenderMode::Snapshot),
            layout,
            policy,
        );
        let grid_fg = if policy == RenderPolicy::Color {
            Color::DarkGray
        } else {
            Color::Reset
        };
        let tick_rows: Vec<_> = (layout.main_plot.y..layout.main_plot.bottom())
            .filter(|&y| {
                (layout.price_axis.x..layout.price_axis.right())
                    .any(|x| symbol(&rendered, x, y) != " ")
            })
            .collect();
        assert!(!tick_rows.is_empty(), "controlled range must produce ticks");

        for y in layout.main_plot.y..layout.main_plot.bottom() {
            for x in layout.main_plot.x..layout.main_plot.right() {
                let cell = &rendered[(x, y)];
                if tick_rows.contains(&y) {
                    assert_eq!(cell.symbol(), "─", "tick row ({x}, {y})");
                    assert_complete_style(cell, grid_fg, Color::Reset);
                } else {
                    assert_eq!(cell.symbol(), " ", "non-tick row ({x}, {y})");
                    assert_complete_style(cell, Color::Reset, Color::Reset);
                }
            }
        }
        for x in layout.main_plot.x..layout.main_plot.right() {
            let grid_rows_in_column = (layout.main_plot.y..layout.main_plot.bottom())
                .filter(|&y| symbol(&rendered, x, y) == "─")
                .collect::<Vec<_>>();
            assert_eq!(grid_rows_in_column, tick_rows, "vertical column {x}");
        }
    }
}

#[test]
fn candles_and_volume_overwrite_only_their_exact_cells_on_horizontal_grid() {
    let area = Rect::new(2, 2, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let series = series();

    for policy in [RenderPolicy::Color, RenderPolicy::StyleFree] {
        let state = ChartViewState::snapshot(&series, usize::from(layout.main_plot.width));
        let rendered = render_with_sentinel(
            &snapshot(&series, state, RenderMode::Snapshot),
            layout,
            policy,
        );
        let grid_fg = if policy == RenderPolicy::Color {
            Color::DarkGray
        } else {
            Color::Reset
        };
        let tick_rows: Vec<_> = (layout.main_plot.y..layout.main_plot.bottom())
            .filter(|&y| {
                (layout.price_axis.x..layout.price_axis.right())
                    .any(|x| symbol(&rendered, x, y) != " ")
            })
            .collect();
        assert!(!tick_rows.is_empty());

        for y in layout.main_plot.y..layout.main_plot.bottom() {
            for x in layout.main_plot.x..layout.main_plot.right() {
                let cell = &rendered[(x, y)];
                match cell.symbol() {
                    "─" => {
                        assert!(tick_rows.contains(&y), "grid outside tick row ({x}, {y})");
                        assert_complete_style(cell, grid_fg, Color::Reset);
                    }
                    "│" | "┃" | "╷" | "╵" | "╻" | "╹" | "╽" | "╿" | "█" | "▓" | "▀" | "▄" | "━"
                    | "┄" | "╌" => {}
                    " " => assert!(
                        !tick_rows.contains(&y),
                        "unowned hole in tick row ({x}, {y})"
                    ),
                    glyph => panic!("unexpected main-plot glyph {glyph:?} at ({x}, {y})"),
                }
            }
        }

        for y in layout.volume.y..layout.volume.bottom() {
            for x in layout.volume.x..layout.volume.right() {
                let cell = &rendered[(x, y)];
                match (policy, cell.symbol()) {
                    (RenderPolicy::Color, "█") => assert!(matches!(
                        cell.style().fg,
                        Some(Color::Rgb(52, 208, 88) | Color::Rgb(234, 74, 90) | Color::Reset)
                    )),
                    (RenderPolicy::Color, " ") => {
                        assert_complete_style(cell, Color::Reset, Color::Reset)
                    }
                    (RenderPolicy::StyleFree, "█" | "▓" | "━" | " ") => {
                        assert_complete_style(cell, Color::Reset, Color::Reset)
                    }
                    (_, glyph) => panic!("unexpected volume glyph {glyph:?} at ({x}, {y})"),
                }
            }
        }
    }
}

#[test]
fn exact_style_and_overwrite_table_resets_hostile_cells_for_both_policies() {
    let area = Rect::new(2, 2, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let series = series();
    for policy in [RenderPolicy::Color, RenderPolicy::StyleFree] {
        let mut state = ChartViewState::interactive(&series, usize::from(layout.main_plot.width));
        state.set_coordinate_hover(
            &series,
            Some(CoordinateHover {
                open_time: 1_700_000_060_000,
                price: 100.0,
            }),
        );
        let snapshot = snapshot(&series, state, RenderMode::Interactive);
        let buffer = render_with_sentinel(&snapshot, layout, policy);
        let default = Style::default();
        let grid = if policy == RenderPolicy::Color {
            default.fg(Color::DarkGray)
        } else {
            default
        };
        let crosshair = if policy == RenderPolicy::Color {
            default.fg(Color::Yellow)
        } else {
            default
        };
        let geometry =
            fccli::chart::CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, 3)
                .expect("geometry");
        let bull_x = u16::try_from(geometry.slot(0).expect("bull").start())
            .expect("valid layout slot coordinate fits u16");
        let bear_x = u16::try_from(geometry.slot(1).expect("bear").start())
            .expect("valid layout slot coordinate fits u16");
        let doji_x = u16::try_from(geometry.slot(2).expect("doji").start())
            .expect("valid layout slot coordinate fits u16");
        assert_complete_style(
            &buffer[(bull_x, layout.volume.bottom() - 1)],
            if policy == RenderPolicy::Color {
                Color::Rgb(52, 208, 88)
            } else {
                Color::Reset
            },
            Color::Reset,
        );
        assert_complete_style(
            &buffer[(bear_x, layout.volume.bottom() - 1)],
            if policy == RenderPolicy::Color {
                Color::Rgb(234, 74, 90)
            } else {
                Color::Reset
            },
            Color::Reset,
        );
        assert_complete_style(
            &buffer[(doji_x, layout.volume.bottom() - 1)],
            Color::Reset,
            Color::Reset,
        );
        assert_eq!(symbol(&buffer, bull_x, layout.volume.bottom() - 1), "█");
        assert_eq!(
            symbol(&buffer, bear_x, layout.volume.bottom() - 1),
            if policy == RenderPolicy::Color {
                "█"
            } else {
                "▓"
            }
        );
        assert_eq!(
            symbol(&buffer, doji_x, layout.volume.bottom() - 1),
            if policy == RenderPolicy::Color {
                "█"
            } else {
                "━"
            }
        );
        let grid_overwritten = (layout.main_plot.y..layout.main_plot.bottom())
            .find_map(|y| {
                let axis_has_tick = (layout.price_axis.x..layout.price_axis.right())
                    .any(|x| symbol(&buffer, x, y) != " ");
                axis_has_tick
                    .then(|| {
                        (layout.main_plot.x..layout.main_plot.right())
                            .find(|&x| {
                                matches!(
                                    symbol(&buffer, x, y),
                                    "█" | "▓"
                                        | "━"
                                        | "│"
                                        | "┃"
                                        | "╽"
                                        | "╿"
                                        | "╷"
                                        | "╵"
                                        | "╻"
                                        | "╹"
                                )
                            })
                            .map(|x| (x, y))
                    })
                    .flatten()
            })
            .expect("candle overwrites a horizontal grid row");
        if policy == RenderPolicy::Color {
            assert_ne!(style(&buffer, grid_overwritten.0, grid_overwritten.1), grid);
        } else {
            assert_ne!(symbol(&buffer, grid_overwritten.0, grid_overwritten.1), "─");
            assert_complete_style(
                &buffer[(grid_overwritten.0, grid_overwritten.1)],
                Color::Reset,
                Color::Reset,
            );
        }
        let grid_cell = (layout.main_plot.y..layout.main_plot.bottom())
            .flat_map(|y| (layout.main_plot.x..layout.main_plot.right()).map(move |x| (x, y)))
            .find(|&(x, y)| symbol(&buffer, x, y) == "─")
            .expect("grid-only cell");
        assert_complete_style(
            &buffer[(grid_cell.0, grid_cell.1)],
            grid.fg.unwrap_or(Color::Reset),
            Color::Reset,
        );
        let crosshair_x = geometry.center(1).expect("hovered candle center");
        let crosshair_y = layout.main_plot.y
            + ((112.0 - 100.0) / 20.0 * f64::from(layout.main_plot.height - 1)).round() as u16;
        assert!(is_price_candle_glyph(symbol(
            &buffer,
            crosshair_x,
            crosshair_y
        )));
        let vertical_overlay_y = (layout.main_plot.y..layout.main_plot.bottom())
            .find(|&y| symbol(&buffer, crosshair_x, y) == "┆")
            .expect("crosshair vertical overlay on non-candle cell");
        assert_complete_style(
            &buffer[(crosshair_x, vertical_overlay_y)],
            crosshair.fg.unwrap_or(Color::Reset),
            Color::Reset,
        );
        for y in layout.frame.y..layout.frame.bottom() {
            for x in layout.frame.x..layout.frame.right() {
                let cell = &buffer[(x, y)];
                assert_ne!(cell.symbol(), "X");
                assert_ne!(cell.style().fg, Some(Color::Magenta));
                assert_ne!(cell.style().bg, Some(Color::Blue));
                assert!(!cell.style().add_modifier.contains(Modifier::BOLD));
                assert!(!cell.style().add_modifier.contains(Modifier::UNDERLINED));
            }
        }
    }
}

#[test]
fn one_column_slots_preserve_bull_and_bear_direction() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let mut candles = CandleSeries::new(Timeframe::Minute1);
    let _ = candles.replace(
        (0..45)
            .map(|index| {
                candle(
                    1_701_000_000_000 + i64::from(index) * 60_000,
                    100.0,
                    110.0,
                    90.0,
                    if index % 2 == 0 { 105.0 } else { 95.0 },
                    1.0,
                )
            })
            .collect(),
    );
    let rendered = render_with_sentinel(
        &snapshot(
            &candles,
            ChartViewState::snapshot(&candles, 45),
            RenderMode::Snapshot,
        ),
        layout,
        RenderPolicy::StyleFree,
    );
    let geometry =
        fccli::chart::CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, 45)
            .expect("geometry");
    for index in 0..45 {
        let x = geometry.center(index).expect("candle center");
        let expected = if index % 2 == 0 { "█" } else { "▓" };
        assert!(
            (layout.main_plot.y..layout.main_plot.bottom())
                .any(|y| symbol(&rendered, x, y) == expected),
            "candle {index} did not preserve direction as {expected}"
        );
    }
}

#[test]
fn multi_column_slots_share_dynamic_width_between_price_and_volume() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let value_at_half = |half: u16| (10.5 - f64::from(half) * 11.0 / 23.0).clamp(0.0, 10.0);
    let mut candles = CandleSeries::new(Timeframe::Minute1);
    let _ = candles.replace(vec![
        candle(
            1_700_000_000_000,
            value_at_half(17),
            10.0,
            0.0,
            value_at_half(4),
            1.0,
        ),
        candle(1_700_000_060_000, 5.0, 10.0, 0.0, 5.0, 1.0),
        candle(1_700_000_120_000, 5.0, 10.0, 0.0, 5.0, 1.0),
    ]);
    let rendered = render_with_sentinel(
        &snapshot(
            &candles,
            ChartViewState::snapshot(&candles, 45),
            RenderMode::Snapshot,
        ),
        layout,
        RenderPolicy::Color,
    );
    let slot = fccli::chart::CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, 3)
        .expect("geometry")
        .slot(0)
        .expect("slot");
    let center = slot.center();
    assert!(
        (layout.main_plot.y..layout.main_plot.bottom())
            .any(|y| symbol(&rendered, center, y) == "┃"),
        "center preserves half-cell projection"
    );
    let body_row = (layout.main_plot.y..layout.main_plot.bottom())
        .find(|&y| symbol(&rendered, center, y) == "┃")
        .expect("body row");
    for x in slot.painted_range() {
        let x = u16::try_from(x).expect("valid layout slot coordinate fits u16");
        if x != center {
            assert_eq!(symbol(&rendered, x, body_row), "█");
        }
        assert_eq!(symbol(&rendered, x, layout.volume.bottom() - 1), "█");
    }
    let gap = u16::try_from(slot.end() - 1).expect("gap column");
    assert!(!is_price_candle_glyph(symbol(&rendered, gap, body_row)));
    assert_ne!(symbol(&rendered, gap, layout.volume.bottom() - 1), "█");
}

#[test]
fn zero_volume_candle_owns_no_volume_cell() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let mut candles = CandleSeries::new(Timeframe::Minute1);
    let _ = candles.replace(vec![
        candle(1_700_000_000_000, 100.0, 110.0, 90.0, 105.0, 0.0),
        candle(1_700_000_060_000, 100.0, 110.0, 90.0, 95.0, 10.0),
    ]);
    let rendered = render_with_sentinel(
        &snapshot(
            &candles,
            ChartViewState::snapshot(&candles, 45),
            RenderMode::Snapshot,
        ),
        layout,
        RenderPolicy::StyleFree,
    );
    let geometry = fccli::chart::CandleSlotGeometry::new(layout.volume.x, layout.volume.width, 2)
        .expect("geometry");
    let zero = geometry.slot(0).expect("zero slot");
    assert!((layout.volume.y..layout.volume.bottom()).all(|y| {
        (zero.start()..zero.end()).all(|x| {
            symbol(
                &rendered,
                u16::try_from(x).expect("valid layout slot coordinate fits u16"),
                y,
            ) == " "
        })
    }));
}

#[test]
fn footer_names_h_and_v_zoom_bindings_in_both_cases() {
    let area = Rect::new(0, 0, 120, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let rendered = render_with_sentinel(
        &snapshot(
            &candles,
            ChartViewState::interactive(&candles, usize::from(layout.main_plot.width)),
            RenderMode::Interactive,
        ),
        layout,
        RenderPolicy::StyleFree,
    );
    let footer = row_text(
        &rendered,
        layout.footer.expect("footer"),
        layout.footer.expect("footer").y,
    );
    assert!(footer.contains("h/H time"));
    assert!(footer.contains("v/V price"));
    assert!(footer.contains(": market/timeframe"));
}

#[test]
fn minimum_width_header_keeps_market_identity_status_and_all_ohlcv_labels() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut input = snapshot(
        &candles,
        ChartViewState::interactive(&candles, 45),
        RenderMode::Interactive,
    );
    input.display_status = DisplayStatus::Connecting;
    input.status_detail = Some(ProviderError::Configuration("detail"));
    let rendered = render_with_sentinel(&input, layout, RenderPolicy::StyleFree);
    let first = row_text(&rendered, layout.header, layout.header.y);
    assert!(first.starts_with("binance Spot BTC/USDT 1m"));
    assert!(
        first
            .trim_end()
            .ends_with("CONNECTING: provider configuration")
    );
    let second = row_text(&rendered, layout.header, layout.header.y + 1);
    for label in ["O:", "H:", "L:", "C:", "V:"] {
        assert!(second.contains(label), "{second:?}");
    }
}

#[test]
fn effective_gate_precedence_drives_both_status_text_and_style() {
    let area = Rect::new(0, 0, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut input = snapshot(
        &candles,
        ChartViewState::interactive(&candles, 65),
        RenderMode::Interactive,
    );
    input.display_status = DisplayStatus::Connected;
    input.rate_gate = RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry);
    let rendered = render_with_sentinel(&input, layout, RenderPolicy::Color);
    let row = row_text(&rendered, layout.header, layout.header.y);
    let start = layout.header.right() - "RATE BLOCKED".len() as u16;
    assert!(row.ends_with("RATE BLOCKED"));
    assert_complete_style(
        &rendered[(start, layout.header.y)],
        Color::Red,
        Color::Reset,
    );
}

#[test]
fn terminal_status_precedes_gate_for_text_and_style() {
    let area = Rect::new(0, 0, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut input = snapshot(
        &candles,
        ChartViewState::interactive(&candles, 65),
        RenderMode::Interactive,
    );
    input.display_status = DisplayStatus::TerminalError;
    input.rate_gate = RateGateState::TimedUntil(MonoInstant::from_millis(42).expect("deadline"));
    let rendered = render_with_sentinel(&input, layout, RenderPolicy::Color);
    let row = row_text(&rendered, layout.header, layout.header.y);
    assert!(row.ends_with("ERROR"));
    assert_complete_style(
        &rendered[(layout.header.right() - 5, layout.header.y)],
        Color::Red,
        Color::Reset,
    );
}

#[test]
fn hover_utc_precision_is_selected_from_visible_span() {
    let area = Rect::new(0, 0, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let mut candles = CandleSeries::new(Timeframe::Minute1);
    let _ = candles.replace(vec![
        candle(1_600_000_000_000, 1.0, 2.0, 0.5, 1.5, 1.0),
        candle(1_700_000_000_000, 1.0, 2.0, 0.5, 1.5, 1.0),
        candle(1_700_000_001_000, 1.0, 2.0, 0.5, 1.5, 1.0),
    ]);
    let mut state = ChartViewState::interactive(&candles, 2);
    state.set_coordinate_hover(
        &candles,
        Some(CoordinateHover {
            open_time: 1_700_000_000_000,
            price: 1.0,
        }),
    );
    let rendered = render_with_sentinel(
        &snapshot(&candles, state, RenderMode::Interactive),
        layout,
        RenderPolicy::StyleFree,
    );
    assert!(row_text(&rendered, layout.header, layout.header.y + 1).starts_with("22:13:20Z"));
}

#[test]
fn snapshot_ignores_injected_hover_for_header_and_crosshair() {
    let area = Rect::new(0, 0, 80, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut state = ChartViewState::snapshot(&candles, 65);
    state.set_coordinate_hover(
        &candles,
        Some(CoordinateHover {
            open_time: 1_700_000_000_000,
            price: 100.0,
        }),
    );
    let rendered = render_with_sentinel(
        &snapshot(&candles, state, RenderMode::Snapshot),
        layout,
        RenderPolicy::Color,
    );
    let detail = row_text(&rendered, layout.header, layout.header.y + 1);
    assert!(
        detail.contains("O:101"),
        "latest candle expected: {detail:?}"
    );
    assert!(!detail.contains('Z'));
    assert!(
        !symbols_in(&rendered, layout.main_plot)
            .iter()
            .any(|glyph| matches!(glyph.as_str(), "┆" | "┼"))
    );
}

#[test]
fn snapshot_effective_status_ignores_gate_error_and_remains_green() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut input = snapshot(
        &candles,
        ChartViewState::snapshot(&candles, 45),
        RenderMode::Snapshot,
    );
    input.display_status = DisplayStatus::TerminalError;
    input.status_detail = Some(ProviderError::Configuration("detail"));
    input.rate_gate = RateGateState::ProcessBlocked(ProcessBlocker::InvalidBanExpiry);
    let rendered = render_with_sentinel(&input, layout, RenderPolicy::Color);
    let row = row_text(&rendered, layout.header, layout.header.y);
    assert!(row.ends_with("SNAPSHOT"));
    assert_complete_style(
        &rendered[(layout.header.right() - 8, layout.header.y)],
        Color::Green,
        Color::Reset,
    );
}

#[test]
fn wholly_offscreen_candles_are_culled_instead_of_ghosting_at_edges() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let base = series();
    let state = ChartViewState::snapshot(&base, 45);
    let mut input = snapshot(&base, state, RenderMode::Snapshot);
    input.candles = vec![
        candle(1_700_000_000_000, 200.0, 220.0, 190.0, 210.0, 1.0),
        candle(1_700_000_060_000, 2.0, 3.0, 1.0, 1.5, 1.0),
        candle(1_700_000_120_000, 100.0, 120.0, 90.0, 105.0, 1.0),
    ]
    .into();
    let rendered = render_with_sentinel(&input, layout, RenderPolicy::StyleFree);
    let geometry =
        fccli::chart::CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, 3)
            .expect("geometry");
    for index in [0, 1] {
        let slot = geometry.slot(index).expect("slot");
        assert!((layout.main_plot.y..layout.main_plot.bottom()).all(|y| {
            (slot.start()..slot.end()).all(|x| {
                !matches!(
                    symbol(
                        &rendered,
                        u16::try_from(x).expect("valid layout slot coordinate fits u16"),
                        y,
                    ),
                    "█" | "▓" | "━" | "│" | "┃" | "╷" | "╵" | "╻" | "╹" | "╽" | "╿"
                )
            })
        }));
    }
}

#[test]
fn intersecting_body_is_clipped_while_retaining_visible_direction() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let base = series();
    let state = ChartViewState::snapshot(&base, 45);
    let mut input = snapshot(&base, state, RenderMode::Snapshot);
    input.candles = vec![
        candle(1_700_000_000_000, 200.0, 210.0, 100.0, 105.0, 1.0),
        candle(1_700_000_060_000, 100.0, 110.0, 90.0, 105.0, 1.0),
        candle(1_700_000_120_000, 100.0, 110.0, 90.0, 105.0, 1.0),
    ]
    .into();
    let rendered = render_with_sentinel(&input, layout, RenderPolicy::StyleFree);
    let slot = fccli::chart::CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, 3)
        .expect("geometry")
        .slot(0)
        .expect("slot");
    assert!((slot.start()..slot.end()).any(|x| {
        let x = u16::try_from(x).expect("valid layout slot coordinate fits u16");
        symbol(&rendered, x, layout.main_plot.y) == "▓"
            || symbol(&rendered, x, layout.main_plot.y) == "┃"
    }));
}

#[test]
fn offscreen_doji_body_is_not_clamped_into_view_when_only_wick_intersects() {
    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Snapshot)
    else {
        panic!("adequate")
    };
    let base = series();
    let state = ChartViewState::snapshot(&base, 45);
    let mut input = snapshot(&base, state, RenderMode::Snapshot);
    input.candles = vec![
        candle(1_700_000_000_000, 200.0, 200.0, 100.0, 200.0, 1.0),
        candle(1_700_000_060_000, 100.0, 110.0, 90.0, 105.0, 1.0),
        candle(1_700_000_120_000, 100.0, 110.0, 90.0, 105.0, 1.0),
    ]
    .into();
    let rendered = render_with_sentinel(&input, layout, RenderPolicy::StyleFree);
    let slot = fccli::chart::CandleSlotGeometry::new(layout.main_plot.x, layout.main_plot.width, 3)
        .expect("geometry")
        .slot(0)
        .expect("slot");
    assert!((layout.main_plot.y..layout.main_plot.bottom()).all(|y| {
        (slot.start()..slot.end()).all(|x| {
            symbol(
                &rendered,
                u16::try_from(x).expect("valid layout slot coordinate fits u16"),
                y,
            ) != "━"
        })
    }));
    let center = slot.center();
    assert!((layout.main_plot.y..layout.main_plot.bottom()).any(|y| {
        symbol(&rendered, center, y) == "│"
            || symbol(&rendered, center, y) == "╷"
            || symbol(&rendered, center, y) == "╵"
    }));
}

#[test]
fn render_policy_detection_uses_captured_tty_and_no_color_presence_only() {
    use std::ffi::OsStr;

    use fccli::chart::{detect_render_policy, no_color_present};

    assert!(!no_color_present(None));
    assert!(no_color_present(Some(OsStr::new(""))));
    assert!(no_color_present(Some(OsStr::new("1"))));
    assert!(no_color_present(Some(OsStr::new("禁止"))));

    #[cfg(unix)]
    {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};

        let non_unicode = OsString::from_vec(vec![0xff, 0xfe]);
        assert!(no_color_present(Some(non_unicode.as_os_str())));
    }

    assert_eq!(detect_render_policy(true, false), RenderPolicy::Color);
    assert_eq!(detect_render_policy(true, true), RenderPolicy::StyleFree);
    assert_eq!(detect_render_policy(false, false), RenderPolicy::StyleFree);
    assert_eq!(detect_render_policy(false, true), RenderPolicy::StyleFree);
}

#[test]
fn footer_help_renders_default_bindings() {
    use fccli::chart::FooterPresentation;

    let area = Rect::new(0, 0, 120, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut snap = snapshot(
        &candles,
        ChartViewState::interactive(&candles, usize::from(layout.main_plot.width)),
        RenderMode::Interactive,
    );
    snap.footer = FooterPresentation::Help;
    let rendered = render_with_sentinel(&snap, layout, RenderPolicy::StyleFree);
    let footer = row_text(
        &rendered,
        layout.footer.expect("footer"),
        layout.footer.expect("footer").y,
    );
    assert!(footer.contains("q quit"));
    assert!(footer.contains(": market/timeframe"));
}

#[test]
fn footer_editing_renders_prompt_and_cursor_marker() {
    use fccli::chart::FooterPresentation;

    let area = Rect::new(0, 0, 120, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut snap = snapshot(
        &candles,
        ChartViewState::interactive(&candles, usize::from(layout.main_plot.width)),
        RenderMode::Interactive,
    );
    snap.footer = FooterPresentation::Editing {
        text: "btc/usdt 1m".to_owned(),
        cursor: 3,
    };
    let rendered = render_with_sentinel(&snap, layout, RenderPolicy::StyleFree);
    let footer = row_text(
        &rendered,
        layout.footer.expect("footer"),
        layout.footer.expect("footer").y,
    );
    assert!(
        footer.starts_with(":btc│/usdt 1m"),
        "footer was: {footer:?}"
    );
}

#[test]
fn footer_preparing_renders_target_label() {
    use fccli::chart::FooterPresentation;

    let area = Rect::new(0, 0, 120, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut snap = snapshot(
        &candles,
        ChartViewState::interactive(&candles, usize::from(layout.main_plot.width)),
        RenderMode::Interactive,
    );
    snap.footer = FooterPresentation::Preparing {
        target: "BTCUSDT 1m".to_owned(),
    };
    let rendered = render_with_sentinel(&snap, layout, RenderPolicy::StyleFree);
    let footer = row_text(
        &rendered,
        layout.footer.expect("footer"),
        layout.footer.expect("footer").y,
    );
    assert!(
        footer.contains("Preparing BTCUSDT 1m"),
        "footer was: {footer:?}"
    );
}

#[test]
fn footer_error_renders_message() {
    use fccli::chart::FooterPresentation;

    let area = Rect::new(0, 0, 120, 24);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut snap = snapshot(
        &candles,
        ChartViewState::interactive(&candles, usize::from(layout.main_plot.width)),
        RenderMode::Interactive,
    );
    snap.footer = FooterPresentation::Error {
        message: "unsupported provider".to_owned(),
    };
    let rendered = render_with_sentinel(&snap, layout, RenderPolicy::StyleFree);
    let footer = row_text(
        &rendered,
        layout.footer.expect("footer"),
        layout.footer.expect("footer").y,
    );
    assert!(
        footer.contains("Error: unsupported provider"),
        "footer was: {footer:?}"
    );
}

#[test]
fn footer_editing_is_clipped_to_footer_width() {
    use fccli::chart::FooterPresentation;

    let area = Rect::new(0, 0, 60, 18);
    let ChartLayoutResult::Ready { layout } = calculate_chart_layout(area, LayoutMode::Interactive)
    else {
        panic!("adequate")
    };
    let candles = series();
    let mut snap = snapshot(
        &candles,
        ChartViewState::interactive(&candles, usize::from(layout.main_plot.width)),
        RenderMode::Interactive,
    );
    let long_text = "btc/usdt 1m ".repeat(20);
    snap.footer = FooterPresentation::Editing {
        text: long_text.clone(),
        cursor: long_text.len(),
    };
    let rendered = render_with_sentinel(&snap, layout, RenderPolicy::StyleFree);
    let footer_rect = layout.footer.expect("footer");
    let footer = row_text(&rendered, footer_rect, footer_rect.y);
    assert!(
        footer.chars().count() <= usize::from(footer_rect.width),
        "footer exceeded width: {footer:?}"
    );
}
