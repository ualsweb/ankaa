//! L2 order-book reconstruction from absolute-quantity level updates.

use std::collections::BTreeMap;

/// Book side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    /// Bids (descending by price).
    Bid,
    /// Asks (ascending by price).
    Ask,
}

/// A price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    /// Price ticks.
    pub price: i64,
    /// Quantity ticks.
    pub size: i64,
}

/// Best bid/ask snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TopOfBook {
    /// Best bid, if any.
    pub bid: Option<Level>,
    /// Best ask, if any.
    pub ask: Option<Level>,
}

impl TopOfBook {
    /// Mid price in ticks ×2 (to stay integral), or `None` when one side is empty.
    #[must_use]
    pub fn mid_x2(&self) -> Option<i64> {
        Some(self.bid?.price + self.ask?.price)
    }

    /// Spread in ticks, or `None` when one side is empty.
    #[must_use]
    pub fn spread(&self) -> Option<i64> {
        Some(self.ask?.price - self.bid?.price)
    }
}

/// One instrument's L2 book. Levels are keyed by price in a `BTreeMap` per side; a zero-size
/// update deletes a level. (A flat sorted `Vec` was measured at 2× slower for 1,000-level books
/// because of the tail memmove on every insert/delete; see `docs/benchmarks/`.)
#[derive(Debug, Default, Clone)]
pub struct L2Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    /// Last applied exchange update id.
    pub last_update_id: i64,
    /// Updates applied.
    pub updates: u64,
}

impl L2Book {
    /// Empty book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one absolute level update (`qty` = new quantity, 0 deletes the level). Returns
    /// true when the top of book changed.
    pub fn apply(&mut self, side: BookSide, price: i64, qty: i64, update_id: i64) -> bool {
        let before = self.top();
        let map = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        if qty == 0 {
            map.remove(&price);
        } else {
            map.insert(price, qty);
        }
        self.last_update_id = update_id;
        self.updates += 1;
        self.top() != before
    }

    /// True when best bid >= best ask. Meaningful only between exchange messages (levels of one
    /// message are applied one by one and may cross transiently).
    #[must_use]
    pub fn is_crossed(&self) -> bool {
        matches!((self.top().bid, self.top().ask), (Some(b), Some(a)) if b.price >= a.price)
    }

    /// Replace the whole book from a snapshot (Binance REST `depth` or an exchange snapshot).
    pub fn load_snapshot(&mut self, bids: &[Level], asks: &[Level], update_id: i64) {
        self.bids.clear();
        self.asks.clear();
        self.bids.extend(
            bids.iter()
                .filter(|l| l.size > 0)
                .map(|l| (l.price, l.size)),
        );
        self.asks.extend(
            asks.iter()
                .filter(|l| l.size > 0)
                .map(|l| (l.price, l.size)),
        );
        self.last_update_id = update_id;
    }

    /// Best bid and ask.
    #[must_use]
    pub fn top(&self) -> TopOfBook {
        TopOfBook {
            bid: self
                .bids
                .iter()
                .next_back()
                .map(|(&price, &size)| Level { price, size }),
            ask: self
                .asks
                .iter()
                .next()
                .map(|(&price, &size)| Level { price, size }),
        }
    }

    /// Top `n` levels of a side, best first.
    #[must_use]
    pub fn levels(&self, side: BookSide, n: usize) -> Vec<Level> {
        match side {
            BookSide::Bid => self
                .bids
                .iter()
                .rev()
                .take(n)
                .map(|(&price, &size)| Level { price, size })
                .collect(),
            BookSide::Ask => self
                .asks
                .iter()
                .take(n)
                .map(|(&price, &size)| Level { price, size })
                .collect(),
        }
    }

    /// Number of levels per side.
    #[must_use]
    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    /// Total quantity within `levels` best levels of a side.
    #[must_use]
    pub fn size_within(&self, side: BookSide, levels: usize) -> i128 {
        self.levels(side, levels)
            .iter()
            .map(|l| i128::from(l.size))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_and_top() {
        let mut b = L2Book::new();
        assert!(b.apply(BookSide::Bid, 100, 5, 1));
        assert!(b.apply(BookSide::Ask, 102, 7, 2));
        assert!(!b.apply(BookSide::Bid, 99, 1, 3)); // not top
        assert_eq!(
            b.top(),
            TopOfBook {
                bid: Some(Level {
                    price: 100,
                    size: 5
                }),
                ask: Some(Level {
                    price: 102,
                    size: 7
                })
            }
        );
        assert_eq!(b.top().spread(), Some(2));
        assert_eq!(b.top().mid_x2(), Some(202));
        assert!(b.apply(BookSide::Bid, 100, 0, 4)); // delete best bid
        assert_eq!(b.top().bid, Some(Level { price: 99, size: 1 }));
        assert_eq!(b.depth(), (1, 1));
        assert_eq!(
            b.levels(BookSide::Bid, 5),
            vec![Level { price: 99, size: 1 }]
        );
        assert!(!b.is_crossed());
        b.apply(BookSide::Ask, 98, 1, 5);
        assert!(b.is_crossed());
        b.load_snapshot(
            &[Level { price: 10, size: 1 }, Level { price: 11, size: 0 }],
            &[Level { price: 12, size: 3 }],
            9,
        );
        assert_eq!(b.depth(), (1, 1));
        assert_eq!(b.last_update_id, 9);
    }
}
