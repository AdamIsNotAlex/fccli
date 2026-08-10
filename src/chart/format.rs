use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};

const MIN_UTC_LABELS: usize = 4;
const MAX_UTC_LABELS: usize = 8;
const SCIENTIFIC_PRECISION_LIMIT: usize = 9;
const MAX_PRICE_STEP_COARSENINGS: usize =
    ((f64::MAX_EXP - f64::MIN_EXP) as usize + f64::MANTISSA_DIGITS as usize + 1) * 3;

#[derive(Clone, Debug, PartialEq)]
pub struct PriceTick {
    pub value: f64,
    pub label: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UtcLabelFormat {
    TimeWithSeconds,
    Time,
    MonthDayTime,
    Date,
    YearMonth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UtcLabel {
    pub candle_index: usize,
    pub x: u16,
    pub text: String,
}

/// Generates bounded, finite price ticks on a 1/2/5 × 10^n grid.
///
/// Invalid or empty ranges return no ticks. All returned ticks have globally
/// distinct, nonempty labels that fit `label_width`; the 1/2/5 step is
/// coarsened or no ticks are returned.
#[must_use]
pub fn price_ticks(low: f64, high: f64, row_count: u16, label_width: u16) -> Vec<PriceTick> {
    if !low.is_finite() || !high.is_finite() || low >= high || row_count == 0 || label_width == 0 {
        return Vec::new();
    }

    let target = usize::from(row_count.clamp(2, 8));
    let span = high - low;
    if !span.is_finite() || span <= 0.0 {
        return Vec::new();
    }
    let Some(mut step) = nice_step(span / (target.saturating_sub(1) as f64)) else {
        return Vec::new();
    };
    let width = usize::from(label_width);

    // Each complete 1/2/5 cycle advances by one decimal exponent. The bound
    // covers the full finite f64 exponent range plus its subnormal mantissa;
    // numeric overflow or a failure to make progress normally terminates first.
    let mut coarsenings = 0;
    loop {
        let values = price_tick_values(low, high, step, target);
        if !values.is_empty()
            && let Some(labels) = fit_distinct_price_labels(&values, step, width)
        {
            return values
                .into_iter()
                .zip(labels)
                .map(|(value, label)| PriceTick { value, label })
                .collect();
        }
        if coarsenings == MAX_PRICE_STEP_COARSENINGS {
            break;
        }
        let Some(coarser) = next_nice_step(step) else {
            break;
        };
        step = coarser;
        coarsenings += 1;
    }
    Vec::new()
}

fn price_tick_values(low: f64, high: f64, step: f64, target: usize) -> Vec<f64> {
    let first = (low / step).ceil() * step;
    if !first.is_finite() {
        return Vec::new();
    }

    let mut values = Vec::with_capacity(target + 1);
    let mut value = first;
    while value <= high && values.len() <= target {
        if value >= low && value.is_finite() {
            let value = normalize_zero(value);
            if values.last().is_none_or(|previous| *previous < value) {
                values.push(value);
            }
        }
        let next = value + step;
        if !next.is_finite() || next <= value {
            break;
        }
        value = next;
    }
    values
}

fn nice_step(raw: f64) -> Option<f64> {
    if !raw.is_finite() || raw <= 0.0 {
        return None;
    }
    let exponent = raw.log10().floor();
    let scale = 10.0_f64.powf(exponent);
    if !scale.is_finite() || scale <= 0.0 {
        return None;
    }
    let fraction = raw / scale;
    let multiplier = if fraction <= 1.0 {
        1.0
    } else if fraction <= 2.0 {
        2.0
    } else if fraction <= 5.0 {
        5.0
    } else {
        10.0
    };
    let step = multiplier * scale;
    step.is_finite().then_some(step)
}
fn next_nice_step(step: f64) -> Option<f64> {
    if !step.is_finite() || step <= 0.0 {
        return None;
    }
    let exponent = step.log10().floor();
    let scale = 10.0_f64.powf(exponent);
    if !scale.is_finite() || scale <= 0.0 {
        return None;
    }
    let fraction = step / scale;
    let next = if fraction < 1.5 {
        2.0 * scale
    } else if fraction < 3.5 {
        5.0 * scale
    } else {
        10.0 * scale
    };
    (next.is_finite() && next > step).then_some(next)
}

fn fit_distinct_price_labels(values: &[f64], step: f64, width: usize) -> Option<Vec<String>> {
    let fixed_precision = decimal_places(step).min(SCIENTIFIC_PRECISION_LIMIT);
    for precision in fixed_precision..=SCIENTIFIC_PRECISION_LIMIT {
        let labels: Vec<_> = values
            .iter()
            .map(|value| format!("{value:.precision$}"))
            .collect();
        if labels_fit_and_are_distinct(&labels, width) {
            return Some(labels);
        }
    }
    for precision in 0..=SCIENTIFIC_PRECISION_LIMIT {
        let labels: Vec<_> = values
            .iter()
            .map(|value| format!("{value:.precision$e}"))
            .collect();
        if labels_fit_and_are_distinct(&labels, width) {
            return Some(labels);
        }
    }
    None
}

fn decimal_places(step: f64) -> usize {
    if step >= 1.0 {
        0
    } else {
        (-step.log10().floor()).max(0.0) as usize
    }
}

fn labels_fit_and_are_distinct(labels: &[String], width: usize) -> bool {
    labels
        .iter()
        .all(|label| !label.is_empty() && label.len() <= width)
        && labels
            .iter()
            .enumerate()
            .all(|(index, label)| !labels[..index].contains(label))
}

fn bounded_scientific(value: f64, width: usize) -> String {
    for precision in (0..=SCIENTIFIC_PRECISION_LIMIT).rev() {
        let candidate = format!("{value:.precision$e}");
        if candidate.len() <= width {
            return candidate;
        }
    }
    String::new()
}

fn normalize_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

/// Formats base-asset volume without changing or rescaling the stored value.
#[must_use]
pub fn format_base_volume(value: f64, width: usize) -> String {
    if !value.is_finite() || width == 0 {
        return String::new();
    }
    let magnitude = value.abs();
    for (scale, suffix) in [
        (1_000_000_000_000.0, "T"),
        (1_000_000_000.0, "B"),
        (1_000_000.0, "M"),
        (1_000.0, "K"),
    ] {
        if magnitude >= scale {
            for precision in (0..=2).rev() {
                let candidate = format!("{:.precision$}{suffix}", value / scale);
                if candidate.len() <= width {
                    return candidate;
                }
            }
            break;
        }
    }
    for precision in (0..=2).rev() {
        let candidate = format!("{value:.precision$}");
        if candidate.len() <= width {
            return candidate;
        }
    }
    bounded_scientific(value, width)
}

#[must_use]
pub fn utc_label_format(first_ms: i64, last_ms: i64) -> UtcLabelFormat {
    let span_ms = last_ms.saturating_sub(first_ms).unsigned_abs();
    if span_ms < 60 * 60 * 1_000 {
        UtcLabelFormat::TimeWithSeconds
    } else if span_ms < 24 * 60 * 60 * 1_000 {
        UtcLabelFormat::Time
    } else if span_ms < 90 * 24 * 60 * 60 * 1_000 {
        UtcLabelFormat::MonthDayTime
    } else if span_ms < 2 * 366 * 24 * 60 * 60 * 1_000 {
        UtcLabelFormat::Date
    } else {
        UtcLabelFormat::YearMonth
    }
}

#[must_use]
pub fn format_utc_timestamp(timestamp_ms: i64, format: UtcLabelFormat) -> Option<String> {
    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000).ok()?;
    let description: &[FormatItem<'static>] = match format {
        UtcLabelFormat::TimeWithSeconds => format_description!("[hour]:[minute]:[second]"),
        UtcLabelFormat::Time => format_description!("[hour]:[minute]"),
        UtcLabelFormat::MonthDayTime => {
            format_description!("[month]-[day] [hour]:[minute]")
        }
        UtcLabelFormat::Date => format_description!("[year]-[month]-[day]"),
        UtcLabelFormat::YearMonth => format_description!("[year]-[month]"),
    };
    timestamp.format(description).ok()
}

/// Selects four to eight complete, non-overlapping UTC labels when the axis can
/// accommodate them. A label that cannot be placed whole is omitted.
#[must_use]
pub fn select_utc_labels(
    open_times_ms: &[i64],
    centers: &[u16],
    axis_x: u16,
    axis_width: u16,
) -> Vec<UtcLabel> {
    if open_times_ms.is_empty() || open_times_ms.len() != centers.len() || axis_width == 0 {
        return Vec::new();
    }
    let axis_end = u32::from(axis_x) + u32::from(axis_width);
    let format = utc_label_format(open_times_ms[0], open_times_ms[open_times_ms.len() - 1]);

    for desired in (MIN_UTC_LABELS..=MAX_UTC_LABELS.min(open_times_ms.len())).rev() {
        let mut labels = Vec::with_capacity(desired);
        let mut previous_end = u32::from(axis_x);
        for position in 0..desired {
            let index = if desired == 1 {
                0
            } else {
                position * (open_times_ms.len() - 1) / (desired - 1)
            };
            let Some(text) = format_utc_timestamp(open_times_ms[index], format) else {
                continue;
            };
            let text_width = u32::try_from(text.len()).unwrap_or(u32::MAX);
            let center = u32::from(centers[index]);
            let start = center.saturating_sub(text_width / 2);
            let end = start.saturating_add(text_width);
            if start < u32::from(axis_x) || end > axis_end || start < previous_end {
                continue;
            }
            labels.push(UtcLabel {
                candle_index: index,
                x: u16::try_from(start).expect("axis coordinates fit u16"),
                text,
            });
            previous_end = end.saturating_add(1);
        }
        if labels.len() >= MIN_UTC_LABELS {
            return labels;
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_ticks_are_finite_monotonic_distinct_and_bounded() {
        for (low, high) in [(0.000_000_001, 0.000_000_009), (1.0e290, 1.8e290)] {
            let ticks = price_ticks(low, high, 20, 14);
            assert!(!ticks.is_empty());
            assert!(
                ticks
                    .iter()
                    .all(|tick| tick.value.is_finite() && tick.label.len() <= 14)
            );
            assert!(ticks.windows(2).all(|pair| pair[0].value < pair[1].value));
            assert!(ticks.iter().enumerate().all(|(index, tick)| {
                !tick.label.is_empty()
                    && !ticks[..index]
                        .iter()
                        .any(|previous| previous.label.as_str() == tick.label.as_str())
            }));
            assert!(
                ticks
                    .iter()
                    .all(|tick| tick.value >= low && tick.value <= high)
            );
        }
    }

    #[test]
    fn huge_narrow_ranges_coarsen_to_a_finite_fitting_tick_or_omit_it() {
        let low: f64 = 1.0e300;
        let high = low.next_up().next_up().next_up().next_up();
        let ticks = price_ticks(low, high, 8, 5);

        assert_eq!(
            ticks.len(),
            1,
            "coarsening must outlive the old 32-step cutoff"
        );
        assert!(ticks[0].value.is_finite());
        assert!((low..=high).contains(&ticks[0].value));
        assert!(!ticks[0].label.is_empty());
        assert!(ticks[0].label.len() <= 5);
        assert_eq!(ticks[0].label, "1e300");

        assert!(
            price_ticks(low, high, 8, 4).is_empty(),
            "an axis narrower than the shortest finite label must omit the tick"
        );
    }

    #[test]
    fn empty_and_invalid_price_inputs_have_no_labels() {
        assert!(price_ticks(1.0, 1.0, 8, 14).is_empty());
        assert!(price_ticks(f64::NAN, 2.0, 8, 14).is_empty());
        assert!(price_ticks(1.0, 2.0, 0, 14).is_empty());
        assert!(price_ticks(1.0, 2.0, 8, 0).is_empty());
    }

    #[test]
    fn volume_is_compact_bounded_and_does_not_mutate_input() {
        let value = 12_345_678.0;
        assert_eq!(format_base_volume(value, 8), "12.35M");
        assert_eq!(value, 12_345_678.0);
        assert!(format_base_volume(1.0e100, 9).len() <= 9);
        assert_eq!(format_base_volume(f64::INFINITY, 8), "");
        assert_eq!(format_base_volume(1.0, 0), "");
    }

    #[test]
    fn utc_formats_are_exact() {
        let timestamp = 1_704_164_645_000_i64;
        assert_eq!(
            format_utc_timestamp(timestamp, UtcLabelFormat::TimeWithSeconds).as_deref(),
            Some("03:04:05")
        );
        assert_eq!(
            format_utc_timestamp(timestamp, UtcLabelFormat::Time).as_deref(),
            Some("03:04")
        );
        assert_eq!(
            format_utc_timestamp(timestamp, UtcLabelFormat::MonthDayTime).as_deref(),
            Some("01-02 03:04")
        );
        assert_eq!(
            format_utc_timestamp(timestamp, UtcLabelFormat::Date).as_deref(),
            Some("2024-01-02")
        );
        assert_eq!(
            format_utc_timestamp(timestamp, UtcLabelFormat::YearMonth).as_deref(),
            Some("2024-01")
        );
    }

    #[test]
    fn utc_selection_is_whole_nonoverlapping_and_omits_when_too_narrow() {
        let times: Vec<_> = (0..9)
            .map(|index| 1_704_164_645_000 + index * 60_000)
            .collect();
        let centers: Vec<_> = (0..9).map(|index| 5 + index * 12).collect();
        let labels = select_utc_labels(&times, &centers, 0, 110);
        assert!((4..=8).contains(&labels.len()));
        assert!(
            labels
                .windows(2)
                .all(|pair| u32::from(pair[0].x) + (pair[0].text.len() as u32)
                    < u32::from(pair[1].x))
        );
        assert!(
            labels
                .iter()
                .all(|label| u32::from(label.x) + label.text.len() as u32 <= 110)
        );
        assert!(select_utc_labels(&times, &centers, 0, 20).is_empty());
        assert!(select_utc_labels(&[], &[], 0, 100).is_empty());
    }
}
