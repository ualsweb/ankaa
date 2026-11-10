//! Selection vectors.

/// Rows per kernel invocation: the `FastLanes` virtual-1024-bit vector unit.
pub const VECTOR_LEN: usize = 1024;

/// Backing capacity of a [`SelVec`]: `VECTOR_LEN` plus slack so SIMD kernels may store a
/// whole register unconditionally at the current tail and advance by the popcount. The slack
/// covers the widest SVE register (2048 bits = 64 × 32-bit lanes).
pub const SEL_CAPACITY: usize = VECTOR_LEN + 64;

/// Indices of selected rows within one 1,024-row vector.
///
/// Cache-line aligned, fixed capacity, no heap. Indices are `u16` because
/// `VECTOR_LEN <= u16::MAX`.
#[derive(Clone)]
#[repr(C, align(64))]
pub struct SelVec {
    len: u32,
    idx: [u16; SEL_CAPACITY],
}

impl Default for SelVec {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for SelVec {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SelVec")
            .field("len", &self.len)
            .field("idx", &self.as_slice())
            .finish()
    }
}

impl PartialEq for SelVec {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl SelVec {
    /// Empty selection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            len: 0,
            idx: [0; SEL_CAPACITY],
        }
    }

    /// Number of selected rows.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// True when nothing is selected.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Selected indices, ascending.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u16] {
        &self.idx[..self.len as usize]
    }

    /// Reset to empty.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Append an index. Caller guarantees `len < VECTOR_LEN`.
    #[inline]
    pub fn push(&mut self, i: u16) {
        debug_assert!((self.len as usize) < VECTOR_LEN);
        self.idx[self.len as usize] = i;
        self.len += 1;
    }

    /// Full backing storage (`SEL_CAPACITY` entries), for kernels that write indices directly
    /// then call [`Self::set_len`].
    #[inline]
    pub fn storage_mut(&mut self) -> &mut [u16; SEL_CAPACITY] {
        &mut self.idx
    }

    /// Set the length after a kernel wrote `n` indices into [`Self::storage_mut`].
    #[inline]
    pub fn set_len(&mut self, n: usize) {
        assert!(n <= VECTOR_LEN, "selection longer than VECTOR_LEN");
        self.len = n as u32;
    }

    /// Build from a slice of indices (test helper).
    #[must_use]
    pub fn from_indices(indices: &[u16]) -> Self {
        let mut s = Self::new();
        for &i in indices {
            s.push(i);
        }
        s
    }
}
