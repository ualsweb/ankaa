//! Single-producer single-consumer ring of `Copy` descriptors.
//!
//! Protocol (PRD v4.1 §4.4; v3.2 RFC-009), exactly four rules:
//! 1. Slots are written/read with `ptr::write`/`ptr::read` (plain, not atomic): a slot is
//!    only ever touched by one side at a time, which the indices guarantee.
//! 2. A side publishes its **own** index with a `Release` store.
//! 3. A side observes the **other** side's index with an `Acquire` load.
//! 4. `SeqCst` and explicit fences are never used. Loading one's own index is `Relaxed`
//!    (the owner is the only writer).
//!
//! Capacity is a power of two; indices are free-running `usize` counters masked on access,
//! so wrap-around is handled by `wrapping_sub` (v3.2: no mirrored mmap, no modulo).
//!
//! Verified by `loom` (`cargo loom`, see `tests/loom.rs`); the `ARMv8` message-passing litmus test
//! for this protocol is in `docs/herd7/spsc-mp.litmus` (run manually with `herd7`).
#![cfg_attr(not(loom), no_std)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;

#[cfg(not(loom))]
use core::sync::atomic::{AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared ring state. Head is written by the producer, tail by the consumer; each lives on
/// its own cache line (Neoverse L1 lines are 64 B; `CachePadded` uses 128 B on aarch64,
/// which also defeats adjacent-line prefetch pairs).
struct Shared<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    mask: usize,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// SAFETY: slots are only accessed by one side at a time according to the index protocol.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

/// Producer handle (not `Clone`: single producer).
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
    /// Cached view of the consumer's tail; refreshed only when the ring looks full.
    tail_cache: usize,
}

/// Consumer handle (not `Clone`: single consumer).
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    /// Cached view of the producer's head; refreshed only when the ring looks empty.
    head_cache: usize,
}

impl<T> core::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Producer")
            .field("capacity", &(self.shared.mask + 1))
            .finish_non_exhaustive()
    }
}
impl<T> core::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Consumer")
            .field("capacity", &(self.shared.mask + 1))
            .finish_non_exhaustive()
    }
}

/// Create a ring with `capacity` slots (rounded up to a power of two, minimum 2).
#[must_use]
pub fn ring<T: Copy>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let cap = capacity.max(2).next_power_of_two();
    let slots = (0..cap)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect::<Box<[_]>>();
    let shared = Arc::new(Shared {
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicUsize::new(0)),
        mask: cap - 1,
        slots,
    });
    (
        Producer {
            shared: shared.clone(),
            tail_cache: 0,
        },
        Consumer {
            shared,
            head_cache: 0,
        },
    )
}

impl<T: Copy> Producer<T> {
    /// Capacity in slots.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.shared.mask + 1
    }

    /// Push one descriptor; returns it back if the ring is full. Never blocks.
    #[inline]
    pub fn try_push(&mut self, val: T) -> Result<(), T> {
        let s = &*self.shared;
        let head = s.head.load(Ordering::Relaxed); // own index
        if head.wrapping_sub(self.tail_cache) == s.mask + 1 {
            // looks full: refresh the consumer's index (Acquire pairs with its Release)
            self.tail_cache = s.tail.load(Ordering::Acquire);
            if head.wrapping_sub(self.tail_cache) == s.mask + 1 {
                return Err(val);
            }
        }
        // SAFETY: slot `head & mask` is owned by the producer until `head` is published:
        // the consumer only reads slots below `head` (Acquire on head).
        unsafe {
            (*s.slots[head & s.mask].get()).as_mut_ptr().write(val);
        }
        s.head.store(head.wrapping_add(1), Ordering::Release); // publish
        Ok(())
    }

    /// Number of free slots as last observed (may be stale-low).
    #[must_use]
    pub fn free_hint(&self) -> usize {
        let s = &*self.shared;
        (s.mask + 1) - s.head.load(Ordering::Relaxed).wrapping_sub(self.tail_cache)
    }
}

impl<T: Copy> Consumer<T> {
    /// Capacity in slots.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.shared.mask + 1
    }

    /// Pop one descriptor, or `None` if empty. Never blocks.
    #[inline]
    pub fn try_pop(&mut self) -> Option<T> {
        let s = &*self.shared;
        let tail = s.tail.load(Ordering::Relaxed); // own index
        if tail == self.head_cache {
            // looks empty: refresh the producer's index (Acquire pairs with its Release)
            self.head_cache = s.head.load(Ordering::Acquire);
            if tail == self.head_cache {
                return None;
            }
        }
        // SAFETY: slot `tail & mask` was initialised by the producer before it published a
        // head above `tail`; the Acquire load above makes that write visible. The producer
        // will not overwrite it until we publish `tail + 1`.
        let val = unsafe { (*s.slots[tail & s.mask].get()).assume_init_read() };
        s.tail.store(tail.wrapping_add(1), Ordering::Release); // release the slot
        Some(val)
    }

    /// Pop up to `out.len()` descriptors; returns how many were written.
    pub fn pop_batch(&mut self, out: &mut [T]) -> usize {
        let mut n = 0;
        while n < out.len() {
            match self.try_pop() {
                Some(v) => {
                    out[n] = v;
                    n += 1;
                }
                None => break,
            }
        }
        n
    }

    /// Number of readable slots as last observed (may be stale-low).
    #[must_use]
    pub fn len_hint(&self) -> usize {
        let s = &*self.shared;
        self.head_cache.wrapping_sub(s.tail.load(Ordering::Relaxed))
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn fifo_and_wraparound() {
        let (mut p, mut c) = ring::<u64>(4);
        assert_eq!(p.capacity(), 4);
        for round in 0..10u64 {
            for i in 0..4 {
                p.try_push(round * 10 + i).unwrap();
            }
            assert_eq!(p.try_push(99), Err(99), "full");
            for i in 0..4 {
                assert_eq!(c.try_pop(), Some(round * 10 + i));
            }
            assert_eq!(c.try_pop(), None, "empty");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn two_threads_10k_messages_in_order() {
        let (mut p, mut c) = ring::<u32>(64);
        let n = 10_000u32;
        let t = std::thread::spawn(move || {
            let mut i = 0;
            while i < n {
                if p.try_push(i).is_ok() {
                    i += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        let mut expect = 0;
        while expect < n {
            if let Some(v) = c.try_pop() {
                assert_eq!(v, expect);
                expect += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        t.join().unwrap();
    }
}
