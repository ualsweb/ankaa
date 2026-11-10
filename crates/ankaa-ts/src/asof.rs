//! Streaming ASOF join.
//!
//! Both inputs are time-sorted (non-decreasing). For each left row the result is the index of the
//! right row with the greatest timestamp `<= left_ts` (backward, the default) or the smallest
//! `>= left_ts` (forward), optionally within a tolerance and matching a partition key.
//! Complexity O(n + m) with per-key cursors; memory O(keys). This is the kdb/Polars approach;
//! sort-based ASOF (`DuckDB`) needs memory proportional to the probe side.

use std::collections::HashMap;

/// Match direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsofDirection {
    /// Latest right row with `ts <= left_ts`.
    Backward,
    /// Earliest right row with `ts >= left_ts`.
    Forward,
}

/// Sentinel for "no match" in the output index array.
pub const NO_MATCH: u32 = u32::MAX;

/// Unkeyed ASOF join. `out[i]` = index into `right` or [`NO_MATCH`].
/// `tolerance` (ns) bounds `|left_ts - right_ts|`; `None` = unbounded.
///
/// # Panics
/// Panics if `out.len() < left.len()` or if either input is not sorted (debug builds only).
pub fn asof_join(
    left: &[i64],
    right: &[i64],
    dir: AsofDirection,
    tolerance: Option<i64>,
    out: &mut [u32],
) -> usize {
    debug_assert!(left.windows(2).all(|w| w[0] <= w[1]), "left not sorted");
    debug_assert!(right.windows(2).all(|w| w[0] <= w[1]), "right not sorted");
    assert!(out.len() >= left.len(), "asof_join: output too small");
    let mut matched = 0usize;
    match dir {
        AsofDirection::Backward => {
            let mut j = 0usize; // first right index with ts > current left ts
            for (i, &lt) in left.iter().enumerate() {
                while j < right.len() && right[j] <= lt {
                    j += 1;
                }
                out[i] = if j == 0 {
                    NO_MATCH
                } else {
                    let k = j - 1;
                    if tolerance.is_none_or(|tol| lt - right[k] <= tol) {
                        matched += 1;
                        k as u32
                    } else {
                        NO_MATCH
                    }
                };
            }
        }
        AsofDirection::Forward => {
            let mut j = 0usize; // first right index with ts >= current left ts
            for (i, &lt) in left.iter().enumerate() {
                while j < right.len() && right[j] < lt {
                    j += 1;
                }
                out[i] = if j < right.len() && tolerance.is_none_or(|tol| right[j] - lt <= tol) {
                    matched += 1;
                    j as u32
                } else {
                    NO_MATCH
                };
            }
        }
    }
    matched
}

/// Keyed ASOF join (e.g. per symbol): rows match only when `left_key[i] == right_key[j]`.
/// One cursor per key; inputs sorted by time (keys interleaved arbitrarily).
#[allow(clippy::too_many_lines)]
pub fn asof_join_keyed(
    left: &[i64],
    left_key: &[u32],
    right: &[i64],
    right_key: &[u32],
    dir: AsofDirection,
    tolerance: Option<i64>,
    out: &mut [u32],
) -> usize {
    assert_eq!(left.len(), left_key.len());
    assert_eq!(right.len(), right_key.len());
    assert!(out.len() >= left.len(), "asof_join_keyed: output too small");
    debug_assert!(left.windows(2).all(|w| w[0] <= w[1]), "left not sorted");
    debug_assert!(right.windows(2).all(|w| w[0] <= w[1]), "right not sorted");
    let mut matched = 0usize;
    match dir {
        AsofDirection::Backward => {
            // Last seen right index per key, advanced lazily as left time moves forward. Keys are
            // dense dictionary ids, so a Vec indexed by key beats hashing (SipHash per probe showed
            // in profiles); a hash map is used only when a key exceeds the dense bound.
            const DENSE_MAX: usize = 1 << 16;
            let max_key = left_key
                .iter()
                .chain(right_key)
                .copied()
                .max()
                .map_or(0, |k| k as usize);
            if max_key < DENSE_MAX {
                let mut last: Vec<u32> = vec![NO_MATCH; max_key + 1];
                let mut j = 0usize;
                for (i, &lt) in left.iter().enumerate() {
                    while j < right.len() && right[j] <= lt {
                        last[right_key[j] as usize] = j as u32;
                        j += 1;
                    }
                    let k = last[left_key[i] as usize];
                    out[i] = if k != NO_MATCH
                        && tolerance.is_none_or(|tol| lt - right[k as usize] <= tol)
                    {
                        matched += 1;
                        k
                    } else {
                        NO_MATCH
                    };
                }
            } else {
                let mut last: HashMap<u32, u32> = HashMap::new();
                let mut j = 0usize;
                for (i, &lt) in left.iter().enumerate() {
                    while j < right.len() && right[j] <= lt {
                        last.insert(right_key[j], j as u32);
                        j += 1;
                    }
                    out[i] = match last.get(&left_key[i]) {
                        Some(&k) if tolerance.is_none_or(|tol| lt - right[k as usize] <= tol) => {
                            matched += 1;
                            k
                        }
                        _ => NO_MATCH,
                    };
                }
            }
        }
        AsofDirection::Forward => {
            // Per key, the first right row at or after lt: per-key index lists built once (O(m)),
            // then one cursor per key. Dense keys use Vecs; sparse keys fall back to hash maps.
            const DENSE_MAX: usize = 1 << 16;
            let max_key = left_key
                .iter()
                .chain(right_key)
                .copied()
                .max()
                .map_or(0, |k| k as usize);
            if max_key < DENSE_MAX {
                let mut lists: Vec<Vec<u32>> = vec![Vec::new(); max_key + 1];
                for (j, &k) in right_key.iter().enumerate() {
                    lists[k as usize].push(j as u32);
                }
                let mut cursors = vec![0usize; max_key + 1];
                for (i, &lt) in left.iter().enumerate() {
                    let key = left_key[i] as usize;
                    let list = &lists[key];
                    let c = &mut cursors[key];
                    while *c < list.len() && right[list[*c] as usize] < lt {
                        *c += 1;
                    }
                    out[i] = NO_MATCH;
                    if *c < list.len() {
                        let k = list[*c];
                        if tolerance.is_none_or(|tol| right[k as usize] - lt <= tol) {
                            matched += 1;
                            out[i] = k;
                        }
                    }
                }
            } else {
                let mut lists: HashMap<u32, Vec<u32>> = HashMap::new();
                for (j, &k) in right_key.iter().enumerate() {
                    lists.entry(k).or_default().push(j as u32);
                }
                let mut cursors: HashMap<u32, usize> = HashMap::new();
                for (i, &lt) in left.iter().enumerate() {
                    let key = left_key[i];
                    out[i] = NO_MATCH;
                    if let Some(list) = lists.get(&key) {
                        let c = cursors.entry(key).or_insert(0);
                        while *c < list.len() && right[list[*c] as usize] < lt {
                            *c += 1;
                        }
                        if *c < list.len() {
                            let k = list[*c];
                            if tolerance.is_none_or(|tol| right[k as usize] - lt <= tol) {
                                matched += 1;
                                out[i] = k;
                            }
                        }
                    }
                }
            }
        }
    }
    matched
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn naive(left: &[i64], right: &[i64], dir: AsofDirection, tol: Option<i64>) -> Vec<u32> {
        left.iter()
            .map(|&lt| {
                let cand: Option<(usize, i64)> = match dir {
                    AsofDirection::Backward => right
                        .iter()
                        .copied()
                        .enumerate()
                        .filter(|(_, r)| *r <= lt)
                        .max_by_key(|(j, r)| (*r, *j)),
                    AsofDirection::Forward => right
                        .iter()
                        .copied()
                        .enumerate()
                        .filter(|(_, r)| *r >= lt)
                        .min_by_key(|(j, r)| (*r, *j)),
                };
                match cand {
                    Some((j, r)) if tol.is_none_or(|t| (lt - r).abs() <= t) => j as u32,
                    _ => NO_MATCH,
                }
            })
            .collect()
    }

    fn sorted(v: Vec<i64>) -> Vec<i64> {
        let mut v = v;
        v.sort_unstable();
        v
    }

    #[test]
    fn basic_backward_and_forward() {
        let quotes = [10, 20, 20, 30];
        let trades = [5, 10, 15, 20, 25, 35];
        let mut out = vec![0u32; trades.len()];
        asof_join(&trades, &quotes, AsofDirection::Backward, None, &mut out);
        assert_eq!(out, [NO_MATCH, 0, 0, 2, 2, 3]); // ties: latest index among equal ts
        asof_join(&trades, &quotes, AsofDirection::Forward, None, &mut out);
        assert_eq!(out, [0, 0, 1, 1, 3, NO_MATCH]);
        asof_join(&trades, &quotes, AsofDirection::Backward, Some(3), &mut out);
        assert_eq!(out, [NO_MATCH, 0, NO_MATCH, 2, NO_MATCH, NO_MATCH]);
    }

    proptest! {
        #[test]
        fn matches_naive(l in prop::collection::vec(0i64..1000, 0..200), r in prop::collection::vec(0i64..1000, 0..200), fwd in any::<bool>(), tol in prop::option::of(0i64..100)) {
            let l = sorted(l); let r = sorted(r);
            let dir = if fwd { AsofDirection::Forward } else { AsofDirection::Backward };
            let mut out = vec![0u32; l.len()];
            let m = asof_join(&l, &r, dir, tol, &mut out);
            let want = naive(&l, &r, dir, tol);
            // Backward ties resolve to the latest equal row in both; forward ties to the earliest.
            prop_assert_eq!(&out, &want);
            prop_assert_eq!(m, want.iter().filter(|&&x| x != NO_MATCH).count());
        }

        #[test]
        fn keyed_matches_per_key_naive(
            l in prop::collection::vec((0i64..500, 0u32..3), 0..150),
            r in prop::collection::vec((0i64..500, 0u32..3), 0..150),
            fwd in any::<bool>(),
        ) {
            let mut l = l; l.sort_unstable(); let mut r = r; r.sort_unstable();
            let (lt, lk): (Vec<i64>, Vec<u32>) = l.iter().copied().unzip();
            let (rt, rk): (Vec<i64>, Vec<u32>) = r.iter().copied().unzip();
            let dir = if fwd { AsofDirection::Forward } else { AsofDirection::Backward };
            let mut out = vec![0u32; lt.len()];
            asof_join_keyed(&lt, &lk, &rt, &rk, dir, None, &mut out);
            for (i, &k) in lk.iter().enumerate() {
                // naive per key: filter right rows of this key, run unkeyed naive on the single left ts
                let idx: Vec<usize> = rk.iter().enumerate().filter(|(_, x)| **x == k).map(|(j, _)| j).collect();
                let rsub: Vec<i64> = idx.iter().map(|&j| rt[j]).collect();
                let want = naive(&[lt[i]], &rsub, dir, None)[0];
                let want = if want == NO_MATCH { NO_MATCH } else { idx[want as usize] as u32 };
                prop_assert_eq!(out[i], want, "row {} key {}", i, k);
            }
        }
    }
}
