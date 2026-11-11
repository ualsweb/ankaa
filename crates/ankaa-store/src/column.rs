//! Typed column storage for one chunk.

use crate::schema::ColumnType;

/// A single cell value (used for appends and tests; the hot path appends whole vectors).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value {
    /// `i64` (timestamps, fixed-point ticks, plain ints).
    I64(i64),
    /// Dictionary id.
    U32(u32),
    /// Flag byte.
    U8(u8),
    /// Float feature.
    F64(f64),
    /// No value (only in a nullable column).
    Null,
}

/// Min/max statistics for a column (zone-map entry).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnStats {
    /// Integer-typed columns.
    I64 {
        /// Minimum.
        min: i64,
        /// Maximum.
        max: i64,
    },
    /// Dictionary ids.
    U32 {
        /// Minimum.
        min: u32,
        /// Maximum.
        max: u32,
    },
    /// Flags.
    U8 {
        /// Minimum.
        min: u8,
        /// Maximum.
        max: u8,
    },
    /// Floats (NaN excluded; `None` when all values are NaN or the column is empty).
    F64 {
        /// Minimum.
        min: Option<f64>,
        /// Maximum.
        max: Option<f64>,
    },
}

/// Column data. Each variant is a plain, contiguous vector so kernels can take slices.
#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    /// `TimestampNs`, `FixedPoint`, `Int64`.
    I64(Vec<i64>),
    /// `DictU32`.
    U32(Vec<u32>),
    /// `U8`.
    U8(Vec<u8>),
    /// `Float64`.
    F64(Vec<f64>),
}

impl Column {
    /// Empty column of the given type with reserved capacity.
    #[must_use]
    pub fn with_capacity(ty: ColumnType, cap: usize) -> Self {
        match ty {
            ColumnType::TimestampNs | ColumnType::FixedPoint { .. } | ColumnType::Int64 => {
                Self::I64(Vec::with_capacity(cap))
            }
            ColumnType::DictU32 => Self::U32(Vec::with_capacity(cap)),
            ColumnType::U8 => Self::U8(Vec::with_capacity(cap)),
            ColumnType::Float64 => Self::F64(Vec::with_capacity(cap)),
        }
    }

    /// Append the values of `other`; `false` (nothing appended) when it holds another type.
    pub fn extend_from(&mut self, other: &Self) -> bool {
        match (self, other) {
            (Self::I64(a), Self::I64(b)) => a.extend_from_slice(b),
            (Self::U32(a), Self::U32(b)) => a.extend_from_slice(b),
            (Self::U8(a), Self::U8(b)) => a.extend_from_slice(b),
            (Self::F64(a), Self::F64(b)) => a.extend_from_slice(b),
            _ => return false,
        }
        true
    }

    /// A copy of rows `rows` (clamped to the column's length).
    #[must_use]
    pub fn copy_range(&self, rows: core::ops::Range<usize>) -> Self {
        fn cut<T: Copy>(v: &[T], rows: &core::ops::Range<usize>) -> Vec<T> {
            let end = rows.end.min(v.len());
            v[rows.start.min(end)..end].to_vec()
        }
        match self {
            Self::I64(v) => Self::I64(cut(v, &rows)),
            Self::U32(v) => Self::U32(cut(v, &rows)),
            Self::U8(v) => Self::U8(cut(v, &rows)),
            Self::F64(v) => Self::F64(cut(v, &rows)),
        }
    }

    /// Release capacity beyond the current length.
    pub fn shrink_to_fit(&mut self) {
        match self {
            Self::I64(v) => v.shrink_to_fit(),
            Self::U32(v) => v.shrink_to_fit(),
            Self::U8(v) => v.shrink_to_fit(),
            Self::F64(v) => v.shrink_to_fit(),
        }
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::I64(v) => v.len(),
            Self::U32(v) => v.len(),
            Self::U8(v) => v.len(),
            Self::F64(v) => v.len(),
        }
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append one value; returns false (and appends nothing) on a type mismatch.
    pub fn push(&mut self, v: Value) -> bool {
        match (self, v) {
            (Self::I64(c), Value::I64(x)) => c.push(x),
            (Self::U32(c), Value::U32(x)) => c.push(x),
            (Self::U8(c), Value::U8(x)) => c.push(x),
            (Self::F64(c), Value::F64(x)) => c.push(x),
            _ => return false,
        }
        true
    }

    /// Append the placeholder stored under a `NULL` (zero; the chunk's validity says it is
    /// not a value).
    pub fn push_null(&mut self) {
        match self {
            Self::I64(c) => c.push(0),
            Self::U32(c) => c.push(0),
            Self::U8(c) => c.push(0),
            Self::F64(c) => c.push(0.0),
        }
    }

    /// Insert the `NULL` placeholder at `row`.
    pub fn insert_null(&mut self, row: usize) {
        match self {
            Self::I64(c) => c.insert(row, 0),
            Self::U32(c) => c.insert(row, 0),
            Self::U8(c) => c.insert(row, 0),
            Self::F64(c) => c.insert(row, 0.0),
        }
    }

    /// Insert one value at `row` (shifting later values); false (nothing inserted) on a type
    /// mismatch.
    pub fn insert(&mut self, row: usize, v: Value) -> bool {
        match (self, v) {
            (Self::I64(c), Value::I64(x)) => c.insert(row, x),
            (Self::U32(c), Value::U32(x)) => c.insert(row, x),
            (Self::U8(c), Value::U8(x)) => c.insert(row, x),
            (Self::F64(c), Value::F64(x)) => c.insert(row, x),
            _ => return false,
        }
        true
    }

    /// Value at `row`.
    #[must_use]
    pub fn get(&self, row: usize) -> Option<Value> {
        match self {
            Self::I64(v) => v.get(row).map(|&x| Value::I64(x)),
            Self::U32(v) => v.get(row).map(|&x| Value::U32(x)),
            Self::U8(v) => v.get(row).map(|&x| Value::U8(x)),
            Self::F64(v) => v.get(row).map(|&x| Value::F64(x)),
        }
    }

    /// Bytes per value for this column's physical type.
    #[must_use]
    pub fn value_size(&self) -> usize {
        match self {
            Self::I64(_) | Self::F64(_) => 8,
            Self::U32(_) => 4,
            Self::U8(_) => 1,
        }
    }

    /// [`Self::stats`] over the rows `valid` marks (all rows when `None`).
    #[must_use]
    pub fn stats_where(&self, valid: Option<&[bool]>) -> ColumnStats {
        fn fold<T: Copy + Ord>(v: &[T], valid: &[bool], lo: T, hi: T) -> (T, T) {
            v.iter()
                .zip(valid)
                .filter(|(_, ok)| **ok)
                .fold((lo, hi), |(a, b), (&x, _)| (a.min(x), b.max(x)))
        }
        let Some(valid) = valid else {
            return self.stats();
        };
        match self {
            Self::I64(v) => {
                let (min, max) = fold(v, valid, i64::MAX, i64::MIN);
                ColumnStats::I64 { min, max }
            }
            Self::U32(v) => {
                let (min, max) = fold(v, valid, u32::MAX, u32::MIN);
                ColumnStats::U32 { min, max }
            }
            Self::U8(v) => {
                let (min, max) = fold(v, valid, u8::MAX, u8::MIN);
                ColumnStats::U8 { min, max }
            }
            Self::F64(v) => {
                let mut min = None;
                let mut max = None;
                for (&x, _) in v.iter().zip(valid).filter(|(x, ok)| **ok && !x.is_nan()) {
                    min = Some(min.map_or(x, |m: f64| m.min(x)));
                    max = Some(max.map_or(x, |m: f64| m.max(x)));
                }
                ColumnStats::F64 { min, max }
            }
        }
    }

    /// Compute min/max over `rows..` (used when sealing and for incremental zone maps).
    #[must_use]
    pub fn stats(&self) -> ColumnStats {
        match self {
            Self::I64(v) => {
                let (min, max) = v
                    .iter()
                    .fold((i64::MAX, i64::MIN), |(a, b), &x| (a.min(x), b.max(x)));
                ColumnStats::I64 { min, max }
            }
            Self::U32(v) => {
                let (min, max) = v
                    .iter()
                    .fold((u32::MAX, u32::MIN), |(a, b), &x| (a.min(x), b.max(x)));
                ColumnStats::U32 { min, max }
            }
            Self::U8(v) => {
                let (min, max) = v
                    .iter()
                    .fold((u8::MAX, u8::MIN), |(a, b), &x| (a.min(x), b.max(x)));
                ColumnStats::U8 { min, max }
            }
            Self::F64(v) => {
                let mut min = None;
                let mut max = None;
                for &x in v.iter().filter(|x| !x.is_nan()) {
                    min = Some(min.map_or(x, |m: f64| m.min(x)));
                    max = Some(max.map_or(x, |m: f64| m.max(x)));
                }
                ColumnStats::F64 { min, max }
            }
        }
    }

    /// Borrow as `&[i64]` when integer-typed.
    #[must_use]
    pub fn as_i64(&self) -> Option<&[i64]> {
        if let Self::I64(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Borrow as `&[u32]`.
    #[must_use]
    pub fn as_u32(&self) -> Option<&[u32]> {
        if let Self::U32(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Borrow as `&[u8]`.
    #[must_use]
    pub fn as_u8(&self) -> Option<&[u8]> {
        if let Self::U8(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Borrow as `&[f64]`.
    #[must_use]
    pub fn as_f64(&self) -> Option<&[f64]> {
        if let Self::F64(v) = self {
            Some(v)
        } else {
            None
        }
    }
}
