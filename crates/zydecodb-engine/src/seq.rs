//! Monotonic sequence number allocator. Single source of `seq` for the engine.
//! In Sprint 3 this becomes the MVCC TxID source.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct SeqAllocator {
    next: AtomicU64,
}

impl SeqAllocator {
    /// Create an allocator whose first handed-out value is `start`.
    pub fn new(start: u64) -> Self {
        SeqAllocator {
            next: AtomicU64::new(start),
        }
    }

    /// Allocate the next sequence number. Strictly monotonic across threads.
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst)
    }

    /// Peek the next value without consuming it.
    pub fn peek(&self) -> u64 {
        self.next.load(Ordering::SeqCst)
    }

    /// Ensure the allocator will hand out at least `floor` next. Used on recovery
    /// to seed `max(seq) + 1`.
    pub fn bump_to_at_least(&self, floor: u64) {
        let mut cur = self.next.load(Ordering::SeqCst);
        while cur < floor {
            match self
                .next
                .compare_exchange(cur, floor, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

impl Default for SeqAllocator {
    fn default() -> Self {
        SeqAllocator::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn monotonic_single_thread() {
        let a = SeqAllocator::new(1);
        assert_eq!(a.next(), 1);
        assert_eq!(a.next(), 2);
        assert_eq!(a.next(), 3);
        assert_eq!(a.peek(), 4);
    }

    #[test]
    fn bump_to_at_least_only_increases() {
        let a = SeqAllocator::new(10);
        a.bump_to_at_least(5);
        assert_eq!(a.peek(), 10);
        a.bump_to_at_least(100);
        assert_eq!(a.peek(), 100);
    }

    #[test]
    fn strictly_monotonic_under_contention() {
        let a = Arc::new(SeqAllocator::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let a = a.clone();
            handles.push(thread::spawn(move || {
                let mut v = Vec::with_capacity(10_000);
                for _ in 0..10_000 {
                    v.push(a.next());
                }
                v
            }));
        }
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        let len = all.len();
        all.dedup();
        assert_eq!(all.len(), len, "no duplicate seq numbers");
        assert_eq!(all.len(), 80_000);
    }
}
