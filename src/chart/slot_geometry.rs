//! Shared candle-slot projection for rendering and pointer interaction.

use std::ops::Range;

/// One candle's half-open horizontal slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandleSlot {
    start: u32,
    end: u32,
}

impl CandleSlot {
    /// First column owned by this slot.
    #[must_use]
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Exclusive end column of this slot. This may be `65536`, one past the
    /// largest terminal cell coordinate.
    #[must_use]
    pub const fn end(self) -> u32 {
        self.end
    }

    /// Number of columns owned by this slot.
    #[must_use]
    pub const fn width(self) -> u16 {
        (self.end - self.start) as u16
    }

    /// Column shared by candle wicks and nearest-candle projection.
    #[must_use]
    pub const fn center(self) -> u16 {
        (self.start + (self.end - self.start - 1) / 2) as u16
    }

    /// Columns painted by the candle body and its volume bar.
    ///
    /// One- and two-column slots remain fully painted. Wider slots reserve
    /// their rightmost column as a visual gap. Pointer ownership remains the
    /// complete slot.
    #[must_use]
    pub const fn painted_range(self) -> Range<u32> {
        let end = if self.end - self.start >= 3 {
            self.end - 1
        } else {
            self.end
        };
        self.start..end
    }

    /// Whether `x` belongs to this half-open slot.
    #[must_use]
    pub const fn contains(self, x: u16) -> bool {
        let x = x as u32;
        self.start <= x && x < self.end
    }
}

/// Equal partition of a plot's columns among visible candles.
///
/// Remainder columns are assigned from the left. Construction rejects empty
/// geometry, more candles than columns, and coordinate ranges whose exclusive
/// end exceeds `65536` (one past the largest `u16` terminal coordinate).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandleSlotGeometry {
    origin: u16,
    width: u16,
    visible_count: u16,
    quotient: u16,
    remainder: u16,
}

impl CandleSlotGeometry {
    /// Builds geometry for `visible_count` candles in `[origin, origin + width)`.
    #[must_use]
    pub fn new(origin: u16, width: u16, visible_count: usize) -> Option<Self> {
        if width == 0 || visible_count == 0 || visible_count > usize::from(width) {
            return None;
        }
        if u32::from(origin) + u32::from(width) > u32::from(u16::MAX) + 1 {
            return None;
        }
        let visible_count = u16::try_from(visible_count).ok()?;

        Some(Self {
            origin,
            width,
            visible_count,
            quotient: width / visible_count,
            remainder: width % visible_count,
        })
    }

    #[must_use]
    pub const fn origin(self) -> u16 {
        self.origin
    }

    #[must_use]
    pub const fn width(self) -> u16 {
        self.width
    }

    #[must_use]
    pub const fn visible_count(self) -> usize {
        self.visible_count as usize
    }

    /// Returns the exact half-open slot for a visible-candle index.
    #[must_use]
    pub fn slot(self, index: usize) -> Option<CandleSlot> {
        let index = u16::try_from(index).ok()?;
        if index >= self.visible_count {
            return None;
        }

        let index = u32::from(index);
        let quotient = u32::from(self.quotient);
        let remainder = u32::from(self.remainder);
        let offset = index * quotient + index.min(remainder);
        let next = index + 1;
        let next_offset = next * quotient + next.min(remainder);
        Some(CandleSlot {
            start: u32::from(self.origin) + offset,
            end: u32::from(self.origin) + next_offset,
        })
    }

    /// Returns the visible-candle index owning `x`.
    #[must_use]
    pub fn index_at_x(self, x: u16) -> Option<usize> {
        let offset = x.checked_sub(self.origin)?;
        if offset >= self.width {
            return None;
        }

        let quotient = u32::from(self.quotient);
        let remainder = u32::from(self.remainder);
        let offset = u32::from(offset);
        let index = if remainder == 0 {
            offset.checked_div(quotient)?
        } else {
            let wide_width = quotient.checked_add(1)?;
            let wide_columns = wide_width.checked_mul(remainder)?;
            if offset < wide_columns {
                offset.checked_div(wide_width)?
            } else {
                remainder.checked_add((offset - wide_columns).checked_div(quotient)?)?
            }
        };
        usize::try_from(index).ok()
    }

    /// Returns the center column for a visible-candle index.
    #[must_use]
    pub fn center(self, index: usize) -> Option<u16> {
        self.slot(index).map(CandleSlot::center)
    }
}
