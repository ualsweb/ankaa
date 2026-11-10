//! Snapshot/diff alignment for incremental order books.
//!
//! Exchanges publish a diff stream with per-message `(first_id, last_id)` and a REST snapshot
//! with `last_update_id`. The documented rule (Binance, and equivalent elsewhere):
//! 1. buffer diffs until the snapshot arrives;
//! 2. drop every buffered diff with `last_id <= snapshot_id`;
//! 3. the first diff applied must satisfy `first_id <= snapshot_id + 1 <= last_id`;
//! 4. every later diff must satisfy `first_id == previous_last_id + 1`, otherwise resynchronise.
//!
//! [`BookSync`] implements that state machine without knowing anything about payloads: the
//! caller stores whatever it wants per message (`M`) and gets back the messages to apply.

/// Per-symbol synchronisation state.
#[derive(Debug)]
pub struct BookSync<M> {
    state: State<M>,
    /// Resynchronisations requested (gaps after sync, misaligned snapshots).
    pub resyncs: u64,
    /// Diffs dropped as older than the snapshot.
    pub dropped_stale: u64,
}

#[derive(Debug)]
enum State<M> {
    /// No snapshot expected: apply everything (replay of captures without snapshots).
    Cold { last_id: Option<i64> },
    /// Waiting for a snapshot; diffs are buffered.
    Awaiting { buffer: Vec<(i64, i64, M)> },
    /// Aligned; `last_id` is the last applied message id. `fresh` is true right after a snapshot
    /// whose id may fall inside the next message's range (straddle rule, step 3).
    Synced { last_id: i64, fresh: bool },
}

/// What the caller should do after feeding a diff or a snapshot.
#[derive(Debug, PartialEq, Eq)]
pub struct Decision<M> {
    /// Messages to apply now, in order.
    pub apply: Vec<M>,
    /// True when the book must be reset from the snapshot before applying `apply`.
    pub load_snapshot: bool,
    /// True when the caller must fetch a new snapshot (state went back to awaiting).
    pub need_snapshot: bool,
}

impl<M> Decision<M> {
    fn none() -> Self {
        Self {
            apply: Vec::new(),
            load_snapshot: false,
            need_snapshot: false,
        }
    }
}

impl<M> BookSync<M> {
    /// `expect_snapshot = false` applies diffs immediately (cold start); `true` buffers until
    /// [`Self::on_snapshot`] is called and reports `need_snapshot` right away.
    #[must_use]
    pub fn new(expect_snapshot: bool) -> Self {
        let state = if expect_snapshot {
            State::Awaiting { buffer: Vec::new() }
        } else {
            State::Cold { last_id: None }
        };
        Self {
            state,
            resyncs: 0,
            dropped_stale: 0,
        }
    }

    /// Go back to buffering until the next snapshot (reconnect / restart: the stream restarts
    /// and the held book must be re-based). Not counted as a resync.
    pub fn expect_new_snapshot(&mut self) {
        if !matches!(self.state, State::Cold { .. }) {
            self.state = State::Awaiting { buffer: Vec::new() };
        }
    }

    /// True while diffs are being buffered.
    #[must_use]
    pub fn awaiting_snapshot(&self) -> bool {
        matches!(self.state, State::Awaiting { .. })
    }

    /// Feed one diff message with its id range.
    pub fn on_diff(&mut self, first_id: i64, last_id: i64, msg: M) -> Decision<M> {
        match &mut self.state {
            State::Cold { last_id: prev } => {
                *prev = Some(last_id);
                Decision {
                    apply: vec![msg],
                    load_snapshot: false,
                    need_snapshot: false,
                }
            }
            State::Awaiting { buffer } => {
                buffer.push((first_id, last_id, msg));
                // keep the buffer bounded: a snapshot that never arrives must not eat memory
                if buffer.len() > 10_000 {
                    buffer.drain(..buffer.len() - 10_000);
                }
                Decision::none()
            }
            State::Synced {
                last_id: prev,
                fresh,
            } => {
                let next = prev.checked_add(1);
                if last_id <= *prev {
                    // older than what we hold (e.g. a diff that raced the snapshot): ignore
                    self.dropped_stale += 1;
                    return Decision::none();
                }
                // first message after a snapshot may straddle the snapshot id (rule 3); afterwards
                // exact contiguity (rule 4)
                let aligned = if *fresh {
                    next.is_some_and(|n| first_id <= n && last_id >= n)
                } else {
                    Some(first_id) == next
                };
                if aligned {
                    *prev = last_id;
                    *fresh = false;
                    Decision {
                        apply: vec![msg],
                        load_snapshot: false,
                        need_snapshot: false,
                    }
                } else {
                    // gap or replayed message: resynchronise, keep this diff for after the snapshot
                    self.resyncs += 1;
                    self.state = State::Awaiting {
                        buffer: vec![(first_id, last_id, msg)],
                    };
                    Decision {
                        apply: Vec::new(),
                        load_snapshot: false,
                        need_snapshot: true,
                    }
                }
            }
        }
    }

    /// Feed a snapshot with its `last_update_id`. Returns the buffered diffs that apply on top.
    pub fn on_snapshot(&mut self, snapshot_id: i64) -> Decision<M> {
        let buffer = match std::mem::replace(
            &mut self.state,
            State::Synced {
                last_id: snapshot_id,
                fresh: true,
            },
        ) {
            State::Awaiting { buffer } => buffer,
            // Cold or already synced: a snapshot resets the book anyway.
            State::Cold { .. } | State::Synced { .. } => Vec::new(),
        };
        let mut apply = Vec::new();
        let mut last = snapshot_id;
        for (first, lastid, msg) in buffer {
            if lastid <= snapshot_id {
                self.dropped_stale += 1;
                continue;
            }
            let ok = if apply.is_empty() {
                first <= snapshot_id.saturating_add(1) && lastid >= snapshot_id.saturating_add(1)
            } else {
                Some(first) == last.checked_add(1)
            };
            if !ok {
                // the snapshot is too old for the buffered stream (or a gap inside it): fetch again
                self.resyncs += 1;
                self.state = State::Awaiting { buffer: Vec::new() };
                return Decision {
                    apply: Vec::new(),
                    load_snapshot: true,
                    need_snapshot: true,
                };
            }
            last = lastid;
            apply.push(msg);
        }
        // nothing applied from the buffer: the next live message may straddle the snapshot id
        self.state = State::Synced {
            last_id: last,
            fresh: apply.is_empty(),
        };
        Decision {
            apply,
            load_snapshot: true,
            need_snapshot: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_applies_everything() {
        let mut s = BookSync::<&str>::new(false);
        assert_eq!(s.on_diff(1, 3, "a").apply, vec!["a"]);
        assert_eq!(s.on_diff(9, 9, "b").apply, vec!["b"]); // no gap tracking in cold mode
    }

    #[test]
    fn documented_alignment_rule() {
        let mut s = BookSync::<&str>::new(true);
        assert_eq!(s.on_diff(100, 105, "d1").apply, Vec::<&str>::new());
        assert_eq!(s.on_diff(106, 110, "d2").apply, Vec::<&str>::new());
        assert_eq!(s.on_diff(111, 112, "d3").apply, Vec::<&str>::new());
        // snapshot at 107: d1 is stale (105 <= 107); d2 must satisfy 106 <= 108 <= 110; then d3 continues
        let d = s.on_snapshot(107);
        assert!(d.load_snapshot && !d.need_snapshot);
        assert_eq!(d.apply, vec!["d2", "d3"]);
        assert_eq!(s.dropped_stale, 1);
        // synced: contiguous applies, gap triggers resync and buffers the diff
        assert_eq!(s.on_diff(113, 120, "d4").apply, vec!["d4"]);
        let d = s.on_diff(125, 126, "d5");
        assert!(d.apply.is_empty() && d.need_snapshot);
        assert_eq!(s.resyncs, 1);
        // new snapshot at 124 aligns with the buffered d5 (125 <= 125 <= 126)
        let d = s.on_snapshot(124);
        assert_eq!(d.apply, vec!["d5"]);
        assert!(!d.need_snapshot);
    }

    #[test]
    fn snapshot_newer_than_buffer_then_straddling_live_message() {
        // Typical live start: buffered diffs are all older than the REST snapshot; the first live
        // message after it straddles the snapshot id and must be applied, then exact contiguity.
        let mut s = BookSync::<&str>::new(true);
        s.on_diff(100, 105, "old1");
        s.on_diff(106, 110, "old2");
        let d = s.on_snapshot(117);
        assert!(d.apply.is_empty() && d.load_snapshot && !d.need_snapshot);
        assert_eq!(s.dropped_stale, 2);
        assert_eq!(s.on_diff(111, 118, "straddle").apply, vec!["straddle"]); // 111 <= 118 <= 118
        assert_eq!(s.on_diff(119, 125, "next").apply, vec!["next"]);
        let d = s.on_diff(127, 130, "gap");
        assert!(d.need_snapshot);
        // a diff older than the held state is ignored, never a resync
        let mut s2 = BookSync::<&str>::new(true);
        s2.on_snapshot(50);
        assert_eq!(s2.on_diff(40, 45, "stale"), Decision::none());
        assert_eq!(s2.resyncs, 0);
    }

    #[test]
    fn snapshot_too_old_requests_another() {
        let mut s = BookSync::<&str>::new(true);
        s.on_diff(200, 201, "x");
        let d = s.on_snapshot(100); // first buffered diff starts at 200 > 101: cannot align
        assert!(d.apply.is_empty() && d.need_snapshot);
        assert!(s.awaiting_snapshot());
    }
}
