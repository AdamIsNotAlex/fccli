use fccli::model::{
    Candle, CandleSeries, FinalityAuthority, MutationKind, ResolvedMutation, Timeframe,
};

const MINUTE: i64 = 60_000;
const BASE: i64 = 1_700_000_040_000;

fn rest(open_time: i64, marker: f64) -> Candle {
    Candle::from_rest(
        open_time,
        open_time + MINUTE - 1,
        marker,
        marker + 2.0,
        marker - 1.0,
        marker + 1.0,
        marker * 10.0,
    )
    .expect("test REST candle is valid")
}

fn ws(open_time: i64, marker: f64, closed: bool) -> Candle {
    Candle::from_ws(
        open_time,
        open_time + MINUTE - 1,
        marker,
        marker + 2.0,
        marker - 1.0,
        marker + 1.0,
        marker * 10.0,
        closed,
    )
    .expect("test WS candle is valid")
}

fn open_times(series: &CandleSeries) -> Vec<i64> {
    series.iter().map(Candle::open_time).collect()
}

fn assert_strict_chronology(series: &CandleSeries) {
    let times = open_times(series);
    assert!(times.windows(2).all(|pair| pair[0] < pair[1]), "{times:?}");
}

#[test]
fn empty_singleton_and_current_open_series_are_safe() {
    let mut series = CandleSeries::new(Timeframe::Minute1);

    assert!(series.is_empty());
    assert_eq!(series.len(), 0);
    assert_eq!(series.oldest_open_time(), None);
    assert_eq!(series.newest_open_time(), None);
    assert_eq!(series.get(0), None);
    assert_eq!(series.index_of_open_time(BASE), None);
    assert_eq!(series.candle_at_open_time(BASE), None);
    assert_eq!(series.range(0..0).expect("empty range is valid").count(), 0);
    let reversed_start = series.len() + 1;
    let reversed_end = series.len();
    assert!(series.range(reversed_start..reversed_end).is_none());
    assert!(series.range(0..1).is_none());
    assert!(series.is_contiguous());
    assert!(series.is_range_contiguous(0..0));
    assert!(!series.is_contiguous_through(BASE, BASE));

    let summary = series.append(rest(BASE, 10.0));
    assert_eq!(
        (summary.inserted, summary.replaced, summary.unchanged),
        (1, 0, 0)
    );
    assert_eq!(series.oldest_open_time(), Some(BASE));
    assert_eq!(series.newest_open_time(), Some(BASE));
    assert_eq!(
        series.get(0).map(Candle::authority),
        Some(FinalityAuthority::RestProvisionalOpen)
    );
    assert!(!series.get(0).expect("singleton exists").is_closed());
    assert!(series.is_contiguous());
    assert!(series.is_contiguous_through(BASE, BASE));
    assert_eq!(series.range(0..1).expect("singleton range").count(), 1);
}

#[test]
fn unsorted_overlapping_duplicates_are_stably_normalized() {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let _ = series.replace(vec![rest(BASE + MINUTE, 11.0), rest(BASE, 10.0)]);

    let summary = series.merge(vec![
        rest(BASE + 3 * MINUTE, 30.0),
        rest(BASE + MINUTE, 20.0),
        rest(BASE + 2 * MINUTE, 25.0),
        rest(BASE + MINUTE, 21.0),
        rest(BASE, 9.0),
    ]);

    assert_eq!(
        open_times(&series),
        vec![BASE, BASE + MINUTE, BASE + 2 * MINUTE, BASE + 3 * MINUTE]
    );
    assert_eq!(
        series
            .candle_at_open_time(BASE + MINUTE)
            .expect("deduplicated candle")
            .open(),
        21.0
    );
    assert_eq!(
        series
            .candle_at_open_time(BASE)
            .expect("overlap candle")
            .open(),
        9.0
    );
    assert_eq!(
        (summary.inserted, summary.replaced, summary.unchanged),
        (2, 2, 0)
    );
    assert_eq!(summary.resolved.len(), 4, "equal input keys resolve once");
    assert_eq!(summary.old_to_new, vec![0, 1]);
    assert!(!summary.empty_input);
    assert!(!summary.duplicate_only);
    assert!(!summary.no_progress);
    assert_strict_chronology(&series);
    assert!(series.is_contiguous());
}

#[test]
fn authority_and_later_arrival_merge_table_never_regresses() {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let _ = series.merge(vec![rest(BASE, 10.0), rest(BASE + MINUTE, 11.0)]);
    assert_eq!(
        series.get(0).expect("predecessor").authority(),
        FinalityAuthority::RestProvisionalClosed
    );

    let later_rest = series.upsert(rest(BASE, 20.0));
    assert_eq!(later_rest.replaced, 1);
    let candle = series.get(0).expect("REST replacement");
    assert_eq!(candle.open(), 20.0, "later same-provenance payload wins");
    assert_eq!(candle.authority(), FinalityAuthority::RestProvisionalClosed);

    let ws_open = series.upsert(ws(BASE, 30.0, false));
    assert_eq!((ws_open.replaced, ws_open.unchanged), (1, 0));
    assert_eq!(
        series.get(0).expect("WS open").authority(),
        FinalityAuthority::WsAuthoritativeOpen
    );
    assert_eq!(series.get(0).expect("WS open").open(), 30.0);

    let rejected_rest = series.upsert(rest(BASE, 40.0));
    assert_eq!((rejected_rest.replaced, rejected_rest.unchanged), (0, 1));
    assert_eq!(series.get(0).expect("authoritative candle").open(), 30.0);

    let later_ws_open = series.upsert(ws(BASE, 50.0, false));
    assert_eq!((later_ws_open.replaced, later_ws_open.unchanged), (1, 0));
    assert_eq!(series.get(0).expect("later WS open").open(), 50.0);
    let ws_closed = series.upsert(ws(BASE, 60.0, true));
    assert_eq!((ws_closed.replaced, ws_closed.unchanged), (1, 0));
    assert_eq!(
        series.get(0).expect("WS closed").authority(),
        FinalityAuthority::WsAuthoritativeClosed
    );
    assert_eq!(series.get(0).expect("WS closed").open(), 60.0);

    for regressive in [
        ws(BASE, 70.0, false),
        rest(BASE, 80.0),
        ws(BASE, 90.0, true),
    ] {
        let expected_open = if regressive.authority() == FinalityAuthority::WsAuthoritativeClosed {
            90.0
        } else {
            60.0
        };
        let summary = series.upsert(regressive);
        if expected_open == 90.0 {
            assert_eq!((summary.replaced, summary.unchanged), (1, 0));
        } else {
            assert_eq!((summary.replaced, summary.unchanged), (0, 1));
        }
        assert_eq!(
            series
                .get(0)
                .expect("closed authority retained")
                .authority(),
            FinalityAuthority::WsAuthoritativeClosed
        );
        assert_eq!(series.get(0).expect("closed payload").open(), expected_open);
    }
}

#[test]
fn rest_adjacency_promotes_and_demotes_across_page_boundaries() {
    let mut series = CandleSeries::new(Timeframe::Minute1);

    let _ = series.merge(vec![rest(BASE + MINUTE, 11.0)]);
    assert_eq!(
        series.get(0).expect("current row").authority(),
        FinalityAuthority::RestProvisionalOpen
    );
    let prepended = series.prepend(vec![rest(BASE, 10.0)]);
    assert_eq!((prepended.inserted, prepended.replaced), (1, 0));
    assert_eq!(
        series.get(0).expect("older page boundary").authority(),
        FinalityAuthority::RestProvisionalClosed
    );
    assert_eq!(
        series.get(1).expect("current row").authority(),
        FinalityAuthority::RestProvisionalOpen
    );

    let _ = series.replace(vec![rest(BASE, 10.0), rest(BASE + 2 * MINUTE, 12.0)]);
    assert_eq!(
        series.get(0).expect("gap predecessor").authority(),
        FinalityAuthority::RestProvisionalOpen
    );
    assert!(!series.is_contiguous());
    let gap_fill = series.merge(vec![rest(BASE + MINUTE, 11.0)]);
    assert_eq!((gap_fill.inserted, gap_fill.replaced), (1, 0));
    assert_eq!(
        series.get(0).expect("first predecessor").authority(),
        FinalityAuthority::RestProvisionalClosed
    );
    assert_eq!(
        series.get(1).expect("second predecessor").authority(),
        FinalityAuthority::RestProvisionalClosed
    );
    assert_eq!(
        series.get(2).expect("current row").authority(),
        FinalityAuthority::RestProvisionalOpen
    );

    let mut boundary = CandleSeries::new(Timeframe::Minute1);
    let later_page = (1..=1_000)
        .map(|offset| rest(BASE + i64::from(offset) * MINUTE, 100.0 + f64::from(offset)))
        .collect();
    let _ = boundary.merge(later_page);
    let boundary_prepend = boundary.prepend(vec![rest(BASE, 10.0)]);
    assert_eq!(
        (boundary_prepend.inserted, boundary_prepend.replaced),
        (1, 0)
    );
    assert_eq!(boundary.len(), 1_001);
    assert_eq!(
        boundary.get(0).expect("page predecessor").authority(),
        FinalityAuthority::RestProvisionalClosed
    );
    assert_eq!(
        boundary
            .get(999)
            .expect("within page predecessor")
            .authority(),
        FinalityAuthority::RestProvisionalClosed
    );
    assert_eq!(
        boundary.get(1_000).expect("latest row").authority(),
        FinalityAuthority::RestProvisionalOpen
    );
    assert!(boundary.is_contiguous());
}

#[test]
fn ws_open_and_closed_precedence_is_independent_of_rest_adjacency() {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let _ = series.merge(vec![ws(BASE, 10.0, false), rest(BASE + MINUTE, 11.0)]);
    assert_eq!(
        series.get(0).expect("WS open").authority(),
        FinalityAuthority::WsAuthoritativeOpen
    );

    let rejected_rest = series.upsert(rest(BASE, 20.0));
    assert_eq!((rejected_rest.replaced, rejected_rest.unchanged), (0, 1));
    assert_eq!(
        series.get(0).expect("WS beats REST").authority(),
        FinalityAuthority::WsAuthoritativeOpen
    );
    assert_eq!(series.get(0).expect("WS payload").open(), 10.0);

    let ws_closed = series.upsert(ws(BASE, 30.0, true));
    assert_eq!((ws_closed.replaced, ws_closed.unchanged), (1, 0));
    assert_eq!(
        series.get(0).expect("closed WS").authority(),
        FinalityAuthority::WsAuthoritativeClosed
    );
    let _ = series.replace(vec![series.get(0).expect("closed WS").clone()]);
    assert_eq!(
        series.get(0).expect("isolated WS").authority(),
        FinalityAuthority::WsAuthoritativeClosed
    );
    assert!(series.get(0).expect("isolated WS").is_closed());
}

#[test]
fn prepend_middle_append_report_exact_mapping_and_resolutions() {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let _ = series.replace(vec![
        rest(BASE + MINUTE, 11.0),
        rest(BASE + 3 * MINUTE, 13.0),
    ]);

    let summary = series.merge(vec![
        rest(BASE + 4 * MINUTE, 14.0),
        rest(BASE, 10.0),
        rest(BASE + 2 * MINUTE, 12.0),
    ]);
    assert_eq!(summary.old_to_new, vec![1, 3]);
    assert_eq!(
        summary.resolved,
        vec![
            ResolvedMutation {
                open_time: BASE,
                final_index: 0,
                kind: MutationKind::Inserted
            },
            ResolvedMutation {
                open_time: BASE + 2 * MINUTE,
                final_index: 2,
                kind: MutationKind::Inserted
            },
            ResolvedMutation {
                open_time: BASE + 4 * MINUTE,
                final_index: 4,
                kind: MutationKind::Inserted
            },
        ]
    );
    assert_eq!(
        (summary.inserted, summary.replaced, summary.unchanged),
        (3, 0, 0)
    );

    let update = series.upsert(ws(BASE + 2 * MINUTE, 22.0, false));
    assert_eq!(update.old_to_new, vec![0, 1, 2, 3, 4]);
    assert_eq!(
        update.resolved,
        vec![ResolvedMutation {
            open_time: BASE + 2 * MINUTE,
            final_index: 2,
            kind: MutationKind::Replaced,
        }]
    );
    assert_eq!(
        (update.inserted, update.replaced, update.unchanged),
        (0, 1, 0)
    );
    assert_strict_chronology(&series);
}

#[test]
fn empty_duplicate_and_no_progress_signals_are_distinct() {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let _ = series.replace(vec![rest(BASE, 10.0), rest(BASE + MINUTE, 11.0)]);

    let empty = series.merge(Vec::new());
    assert!(empty.empty_input);
    assert!(!empty.duplicate_only);
    assert!(empty.no_progress);
    assert_eq!(empty.old_to_new, vec![0, 1]);
    assert!(empty.resolved.is_empty());

    let duplicate = series.merge(vec![rest(BASE, 10.0), rest(BASE, 10.0)]);
    assert!(!duplicate.empty_input);
    assert!(duplicate.duplicate_only);
    assert!(duplicate.no_progress);
    assert_eq!(
        (duplicate.inserted, duplicate.replaced, duplicate.unchanged),
        (0, 0, 1)
    );
    assert_eq!(duplicate.resolved.len(), 1);
    assert_eq!(duplicate.resolved[0].kind, MutationKind::Unchanged);

    let replacement_without_insertion = series.upsert(ws(BASE, 20.0, false));
    assert!(!replacement_without_insertion.duplicate_only);
    assert!(replacement_without_insertion.no_progress);
    assert_eq!(
        (
            replacement_without_insertion.inserted,
            replacement_without_insertion.replaced
        ),
        (0, 1)
    );
}

#[test]
fn chronology_lookup_and_continuity_detect_middle_gaps() {
    let mut series = CandleSeries::new(Timeframe::Minute1);
    let _ = series.merge(vec![
        rest(BASE + 3 * MINUTE, 13.0),
        rest(BASE, 10.0),
        rest(BASE + MINUTE, 11.0),
    ]);

    assert_strict_chronology(&series);
    assert_eq!(series.index_of_open_time(BASE), Some(0));
    assert_eq!(series.index_of_open_time(BASE + MINUTE), Some(1));
    assert_eq!(series.index_of_open_time(BASE + 2 * MINUTE), None);
    assert_eq!(series.index_of_open_time(BASE + 3 * MINUTE), Some(2));
    assert!(!series.is_contiguous());
    assert!(series.is_range_contiguous(0..2));
    assert!(!series.is_range_contiguous(1..3));
    assert!(!series.is_contiguous_through(BASE, BASE + 3 * MINUTE));

    let gap_fill = series.upsert(rest(BASE + 2 * MINUTE, 12.0));
    assert_eq!((gap_fill.inserted, gap_fill.replaced), (1, 0));
    assert!(series.is_contiguous());
    assert!(series.is_contiguous_through(BASE, BASE + 3 * MINUTE));
    assert!(!series.is_contiguous_through(BASE + 3 * MINUTE, BASE));
}

#[test]
fn monthly_continuity_uses_calendar_successors_not_fixed_durations() {
    const JAN_2024: i64 = 1_704_067_200_000;
    const FEB_2024: i64 = 1_706_745_600_000;
    const MAR_2024: i64 = 1_709_251_200_000;

    let monthly = |open_time, marker| {
        Candle::from_rest(
            open_time,
            open_time + 1,
            marker,
            marker + 2.0,
            marker - 1.0,
            marker + 1.0,
            1.0,
        )
        .expect("valid monthly test candle")
    };
    let mut series = CandleSeries::new(Timeframe::Month1);
    let _ = series.merge(vec![
        monthly(MAR_2024, 12.0),
        monthly(JAN_2024, 10.0),
        monthly(FEB_2024, 11.0),
    ]);

    assert!(series.is_contiguous());
    assert!(series.is_contiguous_through(JAN_2024, MAR_2024));
    assert_eq!(
        series.get(0).expect("January").authority(),
        FinalityAuthority::RestProvisionalClosed
    );
    assert_eq!(
        series.get(1).expect("February").authority(),
        FinalityAuthority::RestProvisionalClosed
    );
    assert_eq!(
        series.get(2).expect("March").authority(),
        FinalityAuthority::RestProvisionalOpen
    );
}
