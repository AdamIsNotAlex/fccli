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
    /// Multi-column slots reserve their rightmost column as a visual gap.
    /// Single-column slots remain fully painted because no nonzero gap can fit.
    /// Pointer ownership remains the complete slot.
    #[must_use]
    pub const fn painted_range(self) -> Range<u32> {
        let end = if self.end - self.start > 1 {
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

/// Equal-width, right-aligned projection of visible candles onto plot columns.
///
/// Every candle receives the same integer slot width. Columns left over after
/// division form inert visual padding on the left rather than changing gaps
/// between candles. Pointer projection maps that padding to the first visible
/// candle so the complete plot remains interactive. Construction rejects empty
/// geometry, more candles than columns, and coordinate ranges whose exclusive
/// end exceeds `65536` (one past the largest `u16` terminal coordinate).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandleSlotGeometry {
    origin: u16,
    width: u16,
    visible_count: u16,
    slot_width: u16,
    left_padding: u16,
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
            slot_width: width / visible_count,
            left_padding: width % visible_count,
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

        let offset = u32::from(self.left_padding) + u32::from(index) * u32::from(self.slot_width);
        let start = u32::from(self.origin) + offset;
        Some(CandleSlot {
            start,
            end: start + u32::from(self.slot_width),
        })
    }

    /// Returns the visible-candle index owning `x`.
    #[must_use]
    pub fn index_at_x(self, x: u16) -> Option<usize> {
        let offset = x.checked_sub(self.origin)?;
        if offset >= self.width {
            return None;
        }
        if offset < self.left_padding {
            return Some(0);
        }

        let index = (offset - self.left_padding) / self.slot_width;
        debug_assert!(index < self.visible_count);
        Some(usize::from(index))
    }

    /// Returns the center column for a visible-candle index.
    #[must_use]
    pub fn center(self, index: usize) -> Option<u16> {
        self.slot(index).map(CandleSlot::center)
    }
}
