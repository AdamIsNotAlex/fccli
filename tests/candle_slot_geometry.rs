use fccli::chart::CandleSlotGeometry;

#[test]
fn rejects_empty_and_invalid_geometry() {
    assert_eq!(CandleSlotGeometry::new(7, 0, 0), None);
    assert_eq!(CandleSlotGeometry::new(7, 0, 1), None);
    assert_eq!(CandleSlotGeometry::new(7, 5, 0), None);
    assert_eq!(CandleSlotGeometry::new(7, 5, 6), None);
    assert_eq!(CandleSlotGeometry::new(2, u16::MAX, 1), None);
}

#[test]
fn partitions_nondivisible_width_around_nearest_ideal_boundaries() {
    let geometry = CandleSlotGeometry::new(10, 11, 4).expect("valid geometry");
    let slots = (0..4)
        .map(|index| geometry.slot(index).expect("valid slot"))
        .collect::<Vec<_>>();

    assert_eq!(
        slots
            .iter()
            .map(|slot| (slot.start(), slot.end(), slot.width(), slot.center()))
            .collect::<Vec<_>>(),
        vec![
            (10, 13, 3, 11),
            (13, 16, 3, 14),
            (16, 18, 2, 16),
            (18, 21, 3, 19)
        ]
    );
    assert_eq!(geometry.origin(), 10);
    assert_eq!(geometry.width(), 11);
    assert_eq!(geometry.visible_count(), 4);
    assert_eq!(geometry.slot(4), None);
    assert_eq!(geometry.center(4), None);
}

#[test]
fn every_internal_boundary_stays_within_half_a_cell_of_ideal_spacing() {
    for width in 1_u16..=128 {
        for visible_count in 1..=usize::from(width) {
            let geometry =
                CandleSlotGeometry::new(19, width, visible_count).expect("valid geometry");
            let count = u64::try_from(visible_count).expect("small count");
            let width = u64::from(width);

            for boundary in 1..visible_count {
                let actual = u64::from(
                    geometry
                        .slot(boundary)
                        .expect("internal boundary slot")
                        .start()
                        - u32::from(geometry.origin()),
                );
                let scaled_error = actual
                    .saturating_mul(count)
                    .abs_diff(u64::try_from(boundary).expect("small boundary") * width);
                assert!(
                    scaled_error.saturating_mul(2) <= count,
                    "width={width}, count={count}, boundary={boundary}, actual={actual}"
                );
            }
        }
    }
}

#[test]
fn painted_ranges_reserve_one_right_column_when_possible() {
    let geometry = CandleSlotGeometry::new(10, 15, 5).expect("valid geometry");

    for index in 0..5 {
        let slot = geometry.slot(index).expect("valid slot");
        let painted = slot.painted_range();
        assert_eq!(painted, slot.start()..slot.end() - 1);
        assert_eq!(painted.end - painted.start, u32::from(slot.width() - 1));
        assert!(painted.contains(&u32::from(slot.center())));
        assert_eq!(
            geometry.index_at_x(u16::try_from(slot.end() - 1).expect("gap cell")),
            Some(index),
            "visual gaps retain slot ownership"
        );
    }
}

#[test]
fn inverse_mapping_obeys_half_open_edges_and_nonzero_origin() {
    let geometry = CandleSlotGeometry::new(37, 8, 3).expect("valid geometry");

    assert_eq!(geometry.index_at_x(36), None);
    assert_eq!(geometry.index_at_x(37), Some(0));
    assert_eq!(geometry.index_at_x(39), Some(0));
    assert_eq!(geometry.index_at_x(40), Some(1));
    assert_eq!(geometry.index_at_x(42), Some(2));
    assert_eq!(geometry.index_at_x(43), Some(2));
    assert_eq!(geometry.index_at_x(44), Some(2));
    assert_eq!(geometry.index_at_x(45), None);
}

#[test]
fn exclusive_bound_65536_round_trips_without_overflow() {
    let maximum_span = CandleSlotGeometry::new(1, u16::MAX, 1).expect("valid maximum span");
    let maximum_slot = maximum_span.slot(0).expect("single maximum slot");

    assert_eq!((maximum_slot.start(), maximum_slot.end()), (1, 65_536));
    assert_eq!(maximum_slot.width(), u16::MAX);
    assert_eq!(maximum_span.center(0), Some(32_768));
    assert_eq!(maximum_span.index_at_x(0), None);
    assert_eq!(maximum_span.index_at_x(1), Some(0));
    assert_eq!(maximum_span.index_at_x(u16::MAX), Some(0));
    let maximum_density = CandleSlotGeometry::new(1, u16::MAX, usize::from(u16::MAX))
        .expect("valid maximum-density geometry");
    let final_index = usize::from(u16::MAX) - 1;
    let final_slot = maximum_density
        .slot(final_index)
        .expect("final maximum-density slot");
    assert_eq!((final_slot.start(), final_slot.end()), (65_535, 65_536));
    assert_eq!(maximum_density.center(final_index), Some(u16::MAX));
    assert_eq!(maximum_density.index_at_x(u16::MAX), Some(final_index));

    let last_cell = CandleSlotGeometry::new(u16::MAX, 1, 1).expect("valid final cell");
    let last_slot = last_cell.slot(0).expect("single final slot");
    assert_eq!((last_slot.start(), last_slot.end()), (65_535, 65_536));
    assert_eq!(last_slot.center(), u16::MAX);
    assert!(last_slot.contains(u16::MAX));
    assert_eq!(last_cell.index_at_x(u16::MAX), Some(0));
}

#[test]
fn every_plot_column_has_exactly_one_owner_with_no_overlap() {
    for width in 1_u16..=64 {
        for visible_count in 1..=usize::from(width) {
            let geometry =
                CandleSlotGeometry::new(91, width, visible_count).expect("valid geometry");
            let mut ownership = vec![0_u8; usize::from(width)];

            for index in 0..visible_count {
                let slot = geometry.slot(index).expect("valid slot");
                for x in slot.start()..slot.end() {
                    let cell_x = u16::try_from(x).expect("slot cells are u16 coordinates");
                    ownership[usize::try_from(x - u32::from(geometry.origin()))
                        .expect("small offset")] += 1;
                    assert_eq!(geometry.index_at_x(cell_x), Some(index));
                }
                assert_eq!(
                    geometry.index_at_x(u16::try_from(slot.start()).expect("valid first cell")),
                    Some(index)
                );
                assert_eq!(geometry.center(index), Some(slot.center()));
            }

            assert!(ownership.iter().all(|count| *count == 1));
            assert_eq!(geometry.index_at_x(geometry.origin() - 1), None);
            assert_eq!(
                geometry.index_at_x(geometry.origin().checked_add(width).expect("small bound")),
                None
            );
        }
    }
}

#[test]
fn single_column_slots_round_trip() {
    let geometry = CandleSlotGeometry::new(5, 7, 7).expect("valid geometry");

    for index in 0..7 {
        let x = 5 + u16::try_from(index).expect("small index");
        let slot = geometry.slot(index).expect("valid slot");
        assert_eq!(
            (slot.start(), slot.end(), slot.center()),
            (u32::from(x), u32::from(x) + 1, x)
        );
        assert_eq!(geometry.index_at_x(x), Some(index));
        assert_eq!(slot.painted_range(), slot.start()..slot.end());
    }
}
