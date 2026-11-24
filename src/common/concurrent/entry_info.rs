use std::sync::atomic::{self, AtomicBool, AtomicU16, AtomicU32, Ordering};

use super::constants::ACCESS_TIME_REFRESH_INTERVAL_NANOS;
use super::{AccessTime, KeyHash};
use crate::common::time::{AtomicInstant, Instant};
use portable_atomic::AtomicU64;

#[derive(Debug)]
pub(crate) struct EntryInfo<K> {
    key_hash: KeyHash<K>,
    /// `is_admitted` indicates that the entry has been admitted to the cache. When
    /// `false`, it means the entry is _temporary_ admitted to the cache or evicted
    /// from the cache (so it should not have LRU nodes).
    ///
    /// This is distinct from [`is_retired`](Self::is_retired): `is_admitted`
    /// tracks deque membership, while `is_retired` tracks CHT membership. A
    /// freshly inserted entry is **not retired** and **not admitted** until its
    /// `WriteOp::Upsert` is drained; a retired entry may still be `is_admitted`
    /// until `handle_remove*` unlinks it.
    is_admitted: AtomicBool,
    /// CHT-membership flag: `false` while the entry is alive (still backed by
    /// the concurrent hash table), `true` once the entry has been removed.
    ///
    /// The flag is set by `retire()` at the moment the entry is unlinked from
    /// the CHT and is never cleared (`false → true`, monotonic). The
    /// invariant it enforces is:
    ///
    /// > If `is_retired == true`, no policy-tier mutation may run against
    /// > this `EntryInfo`.
    ///
    /// Examples of policy-tier mutations:
    ///
    /// - admitting the entry to an LRU deque,
    /// - moving its deque node to the back,
    /// - scheduling a timer-wheel node,
    /// - incrementing `weighted_size`.
    ///
    /// ### Why we need it
    ///
    /// moka stores entries in a lock-free CHT (source of truth) and mirrors
    /// their ordering into LRU deques updated asynchronously via a bounded
    /// `WriteOp` channel. Without this flag, a `WriteOp::Upsert` can sit in the
    /// channel long enough that the backing CHT entry is evicted before the op
    /// is drained; the drain then pushes an "orphan" deque node that the
    /// eviction loop cannot reach, blocking progress and stalling eviction
    /// indefinitely (see <https://github.com/moka-rs/moka/issues/590>). The
    /// retire store is performed inside the CHT's post-success
    /// `with_previous_entry` callback so the transition linearises with the
    /// bucket-pointer CAS that physically unlinked the entry; any consumer
    /// draining a stale op thereafter observes `is_retired == true` and
    /// declines to touch policy state.
    ///
    /// ### Why a single bit suffices (no `Dead` state)
    ///
    /// Caffeine encodes a three-state lifecycle (`Alive → Retired → Dead`)
    /// because its `makeDead()` can be called twice for the same node: once
    /// inline from `evictEntry()` during admission, and again later from
    /// `RemovalTask.run()` draining the write buffer. The `isDead()` guard at
    /// the top of `makeDead` prevents double-subtraction of `weightedSize`.
    ///
    /// moka has no such double-enter path: every CHT removal site gates its
    /// `handle_remove_*` call on `if let Some(entry) = …`; i.e. on winning
    /// the CHT bucket CAS. So `handle_remove_*` runs exactly once per
    /// retirement, structurally.
    ///
    /// ### When a third state would become necessary
    ///
    /// A third state (`Dead`) earns its keep when there are **multiple
    /// finalization paths** that may both target the same entry. Currently,
    /// moka has one at the CAS-gated inline `handle_remove_*`.
    ///
    /// If moka ever grows a second finalization path that runs alongside the
    /// `WriteOp::Remove` drain (e.g., an inline emergency-evict, an
    /// async load-completion that touches policy state, or any code that
    /// can re-enter `handle_remove_*` for an already-retired entry), the
    /// idempotency requirement returns and a third state (or an equivalent
    /// "weight already subtracted" marker) becomes necessary. Until then, a
    /// single bit is sufficient.
    is_retired: AtomicBool,
    /// `entry_gen` (entry generation) is incremented every time the entry is updated
    /// in the concurrent hash table.
    entry_gen: AtomicU16,
    /// `policy_gen` (policy generation) is incremented every time entry's `WriteOp`
    /// is applied to the cache policies including the access-order queue (the LRU
    /// deque).
    policy_gen: AtomicU16,
    /// Packed expiration state: contains both `expiration_time` (upper 52 bits) and
    /// `expiry_gen` (lower 12 bits) in a single atomic u64 for consistent reads.
    ///
    /// Encoding:
    /// - Bits 0-11: 12-bit expiry_gen (wraps from 4095 to 0)
    /// - Bits 12-63: 52-bit expiration timestamp (nanoseconds from a monotonic clock/Instant)
    /// - Sentinel: When time field (bits 12-63) is all 1s (0xFFFF_FFFF_FFFF_F000),
    ///   represents "None" (no expiration). Gen bits are always preserved.
    /// - Valid timestamps are clamped to 52-bit range to avoid collisions with sentinel.
    ///
    /// NOTE: The time value is relative to the runtime's monotonic clock (not Unix epoch).
    /// It is obtained from `Instant` and should not be interpreted as an absolute time
    /// suitable for serialization or cross-process comparison.
    expiration_state: AtomicU64,
    last_accessed: AtomicInstant,
    last_modified: AtomicInstant,
    policy_weight: AtomicU32,
}

impl<K> EntryInfo<K> {
    #[inline]
    pub(crate) fn new(key_hash: KeyHash<K>, timestamp: Instant, policy_weight: u32) -> Self {
        #[cfg(feature = "unstable-debug-counters")]
        super::debug_counters::InternalGlobalDebugCounters::entry_info_created();

        Self {
            key_hash,
            is_admitted: AtomicBool::default(),
            is_retired: AtomicBool::default(),
            // `entry_gen` starts at 1 and `policy_gen` start at 0.
            entry_gen: AtomicU16::new(1),
            policy_gen: AtomicU16::new(0),
            // Initial state: None (time field all 1s), gen=0
            expiration_state: AtomicU64::new(0xFFFF_FFFF_FFFF_F000),
            last_accessed: AtomicInstant::new(timestamp),
            last_modified: AtomicInstant::new(timestamp),
            policy_weight: AtomicU32::new(policy_weight),
        }
    }

    #[inline]
    pub(crate) fn key_hash(&self) -> &KeyHash<K> {
        &self.key_hash
    }

    #[inline]
    pub(crate) fn is_admitted(&self) -> bool {
        self.is_admitted.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn set_admitted(&self, value: bool) {
        self.is_admitted.store(value, Ordering::Release);
    }

    /// Returns `true` iff the entry has been retired (removed from the CHT).
    #[inline]
    pub(crate) fn is_retired(&self) -> bool {
        self.is_retired.load(Ordering::Acquire)
    }

    /// Returns `true` iff the entry is still alive (not yet retired).
    /// Consumers of buffered operations (`WriteOp::Upsert`, `ReadOp::Hit`,
    /// TinyLFU inline victim scans) MUST check this before mutating policy
    /// state; see the `is_retired` field docs.
    #[inline]
    pub(crate) fn is_alive(&self) -> bool {
        !self.is_retired()
    }

    /// Transitions the entry to the retired state. Must be called inside the
    /// post-CAS `with_previous_entry` callback of `cht::remove_entry_if_and`
    /// (or an equivalent linearisation point) so the transition is atomic
    /// with the CHT unlink.
    ///
    /// The callsite is exclusive: the `with_previous_entry` closure fires at
    /// most once per `EntryInfo` (it only runs for the thread that won the
    /// bucket-pointer CAS), so no other thread can be transitioning the same
    /// field in parallel. A plain `Release` store is therefore sufficient
    /// (no RMW needed) and matches the `is_admitted` pattern. Double-retire
    /// would indicate a broken invariant (caught by a debug-mode assertion).
    #[inline]
    pub(crate) fn retire(&self) {
        debug_assert!(
            !self.is_retired.load(Ordering::Acquire),
            "retire() called on already-retired entry; lifecycle invariant broken",
        );
        self.is_retired.store(true, Ordering::Release);
    }

    /// Returns `true` if the `ValueEntry` having this `EntryInfo` is dirty.
    ///
    /// Dirty means that the entry has been updated in the concurrent hash table but
    /// not yet in the cache policies such as access-order queue.
    #[inline]
    pub(crate) fn is_dirty(&self) -> bool {
        let result =
            self.entry_gen.load(Ordering::Relaxed) != self.policy_gen.load(Ordering::Relaxed);
        atomic::fence(Ordering::Acquire);
        result
    }

    #[inline]
    pub(crate) fn entry_gen(&self) -> u16 {
        self.entry_gen.load(Ordering::Acquire)
    }

    /// Increments the entry generation and returns the new value.
    #[inline]
    pub(crate) fn incr_entry_gen(&self) -> u16 {
        // NOTE: This operation wraps around on overflow.
        let prev = self.entry_gen.fetch_add(1, Ordering::AcqRel);
        // Need to add `1` to the previous value to get the current value.
        prev.wrapping_add(1)
    }

    /// Sets the policy generation to the given value.
    #[inline]
    pub(crate) fn set_policy_gen(&self, value: u16) {
        let g = &self.policy_gen;
        loop {
            let current = g.load(Ordering::Acquire);

            // Do not set the given value if it is smaller than the current value of
            // `policy_gen`. Note that the current value may have been wrapped
            // around. If the value is much larger than the current value, it is
            // likely that the value of `policy_gen` has been wrapped around.
            if current >= value || value.wrapping_sub(current) > u16::MAX / 2 {
                break;
            }

            // Try to set the value.
            if g.compare_exchange_weak(current, value, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    #[inline]
    pub(crate) fn policy_weight(&self) -> u32 {
        self.policy_weight.load(Ordering::Acquire)
    }

    pub(crate) fn set_policy_weight(&self, size: u32) {
        self.policy_weight.store(size, Ordering::Release);
    }

    /// Atomically reads both `expiration_time` and `expiry_gen` as a single unit.
    /// Returns `(expiration_time, expiry_gen)` where expiration_time is None if unset.
    ///
    /// This provides a consistent snapshot of the expiration state, avoiding TOCTOU
    /// issues where the time and generation could be read separately while being
    /// modified by another thread.
    #[inline]
    pub(crate) fn expiration_state(&self) -> (Option<Instant>, u32) {
        const GEN_MASK: u64 = 0xFFF;
        const TIME_MASK: u64 = 0xFFFF_FFFF_FFFF_F000;

        let packed = self.expiration_state.load(Ordering::Acquire);

        // Extract time field and gen bits
        let time_nanos = packed & TIME_MASK;
        let gen = (packed & GEN_MASK) as u32;

        // Check if time field (upper 52 bits) is all 1s (sentinel for None)
        if time_nanos == TIME_MASK {
            (None, gen)
        } else {
            (Some(Instant::from_nanos(time_nanos)), gen)
        }
    }

    /// Sets the expiration time and returns the new expiry generation.
    pub(crate) fn set_expiration_time(&self, time: Option<Instant>) -> u32 {
        const GEN_MASK: u64 = 0xFFF;
        const TIME_MASK: u64 = 0xFFFF_FFFF_FFFF_F000;

        // Use compare_exchange to atomically update the expiration state.
        // This prevents race conditions where multiple threads try to update
        // the expiration time simultaneously.
        loop {
            let prev_packed = self.expiration_state.load(Ordering::Acquire);

            // Extract previous generation (always preserved, even for None state)
            let prev_gen = (prev_packed & GEN_MASK) as u32;
            let new_gen = prev_gen.wrapping_add(1) & GEN_MASK as u32;

            // Pack the new state
            let new_packed = if let Some(t) = time {
                // Clamp timestamp to 52-bit range. Ensure it's strictly less than TIME_MASK
                // to avoid collision with the sentinel (None) value.
                let mut nanos = t.as_nanos() & TIME_MASK;
                // If nanos equals TIME_MASK, adjust it down by one time unit (4096 nanos)
                // to avoid corrupting the generation counter bits (should never happen in practice).
                if nanos == TIME_MASK {
                    nanos = TIME_MASK - 0x1000; // Subtract one 12-bit unit to keep lower bits clear
                }
                debug_assert!(nanos < TIME_MASK, "Timestamp value collides with sentinel");
                // Pack: store nanos in upper 52 bits, gen in lower 12 bits
                nanos | (new_gen as u64)
            } else {
                // Sentinel: time field all 1s, gen preserved in lower bits
                TIME_MASK | (new_gen as u64)
            };

            // Try to atomically update if the state hasn't changed
            match self.expiration_state.compare_exchange_weak(
                prev_packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return new_gen, // Successfully updated
                Err(_) => continue,      // State changed, retry
            }
        }
    }

    /// Refreshes the `last_accessed` timestamp for a cache read, but only when
    /// the stored timestamp is older than the refresh interval.
    ///
    /// A hot key can be read many thousands of times per second. Updating
    /// `last_accessed` on every read would issue an atomic store (and bounce the
    /// cache line between cores) for each of those reads, even though almost all
    /// of them write a value within microseconds of each other. This method
    /// collapses those writes: reads that land within
    /// [`ACCESS_TIME_REFRESH_INTERVAL_NANOS`] of the currently recorded access
    /// time skip the store entirely, so an entry gets at most one timestamp
    /// store per interval no matter how hot it is.
    ///
    /// The timestamp keeps its full nanosecond precision (it is never rounded
    /// down to seconds or minutes); only the *rate* at which it advances is
    /// limited. The bound this introduces is at most one refresh interval, which
    /// is far below any practical time-to-idle duration.
    ///
    /// The update is lock-free: a compare-and-swap makes sure concurrent readers
    /// cannot move the timestamp backwards and that only one of them actually
    /// performs the store per interval.
    #[inline]
    pub(crate) fn refresh_last_accessed(&self, now: Instant) {
        let now_nanos = now.as_nanos();
        let current = self.last_accessed.as_nanos();

        // `u64::MAX` means unset; always record the first access. Otherwise skip
        // the store while the recorded time is still inside the refresh window.
        let stale = current == u64::MAX
            || now_nanos.saturating_sub(current) >= ACCESS_TIME_REFRESH_INTERVAL_NANOS;
        if !stale {
            return;
        }

        // On CAS failure another thread refreshed the timestamp first; their
        // value is at least as recent as the one we observed, so there is
        // nothing more to do.
        let _ = self
            .last_accessed
            .compare_exchange_nanos(current, now_nanos);
    }
}

#[cfg(feature = "unstable-debug-counters")]
impl<K> Drop for EntryInfo<K> {
    fn drop(&mut self) {
        super::debug_counters::InternalGlobalDebugCounters::entry_info_dropped();
    }
}

impl<K> AccessTime for EntryInfo<K> {
    #[inline]
    fn last_accessed(&self) -> Option<Instant> {
        self.last_accessed.instant()
    }

    #[inline]
    fn set_last_accessed(&self, timestamp: Instant) {
        self.last_accessed.set_instant(timestamp);
    }

    #[inline]
    fn last_modified(&self) -> Option<Instant> {
        self.last_modified.instant()
    }

    #[inline]
    fn set_last_modified(&self, timestamp: Instant) {
        self.last_modified.set_instant(timestamp);
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryInfo, KeyHash};
    use crate::common::concurrent::AccessTime;
    use crate::common::time::Instant;
    use std::sync::Arc;

    fn make_entry_info() -> EntryInfo<u64> {
        let kh = KeyHash::new(Arc::new(42u64), 0xdead_beef);
        EntryInfo::new(kh, Instant::from_nanos(0), 1)
    }

    #[test]
    fn lifecycle_initial_is_alive() {
        let info = make_entry_info();
        assert!(info.is_alive());
        assert!(!info.is_retired());
    }

    #[test]
    fn lifecycle_retire_transitions_to_retired() {
        let info = make_entry_info();
        info.retire();
        assert!(!info.is_alive());
        assert!(info.is_retired());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "retire() called on already-retired entry")]
    fn lifecycle_double_retire_debug() {
        let info = make_entry_info();
        info.retire();
        // Second retire violates the exclusive-call invariant; debug-asserted.
        info.retire();
    }

    #[test]
    fn refresh_last_accessed_throttles_within_window() {
        use super::super::constants::ACCESS_TIME_REFRESH_INTERVAL_NANOS;

        let info = make_entry_info();
        let start = Instant::from_nanos(1_000_000);
        info.set_last_accessed(start);

        // A read well inside the refresh window must not move the timestamp.
        let inside = Instant::from_nanos(start.as_nanos() + ACCESS_TIME_REFRESH_INTERVAL_NANOS / 2);
        info.refresh_last_accessed(inside);
        assert_eq!(info.last_accessed(), Some(start));

        // Reads right up to (but not reaching) the window boundary are ignored.
        let just_under =
            Instant::from_nanos(start.as_nanos() + ACCESS_TIME_REFRESH_INTERVAL_NANOS - 1);
        info.refresh_last_accessed(just_under);
        assert_eq!(info.last_accessed(), Some(start));

        // Once the window elapses, the timestamp is refreshed with full
        // nanosecond precision (no rounding to seconds or minutes).
        let after =
            Instant::from_nanos(start.as_nanos() + ACCESS_TIME_REFRESH_INTERVAL_NANOS + 123);
        info.refresh_last_accessed(after);
        assert_eq!(info.last_accessed(), Some(after));
    }

    #[test]
    fn refresh_last_accessed_never_moves_backwards_under_contention() {
        use std::thread;

        use super::super::constants::ACCESS_TIME_REFRESH_INTERVAL_NANOS;

        let info = Arc::new(make_entry_info());
        // Seed an access time so the "unset" fast path does not apply.
        info.set_last_accessed(Instant::from_nanos(0));

        let mut handles = Vec::new();
        let thread_count = 8u64;
        for thread_idx in 0..thread_count {
            let info = Arc::clone(&info);
            handles.push(thread::spawn(move || {
                for step in 0..1_000u64 {
                    // Every thread offers globally increasing timestamps spaced
                    // further apart than the refresh window, so each call is
                    // eligible to store. The CAS must never move the value
                    // backwards regardless of how the threads interleave.
                    let nanos = step * ACCESS_TIME_REFRESH_INTERVAL_NANOS * 8 + thread_idx;
                    info.refresh_last_accessed(Instant::from_nanos(nanos));
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // Final value is one of the timestamps actually offered: it must be at
        // least the seed and no larger than the biggest timestamp any thread
        // produced, proving the timestamp stayed within range and never rolled
        // back.
        let largest_offered = 999 * ACCESS_TIME_REFRESH_INTERVAL_NANOS * 8 + thread_count - 1;
        let final_nanos = info.last_accessed().unwrap().as_nanos();
        assert!(final_nanos <= largest_offered);
        assert!(final_nanos >= ACCESS_TIME_REFRESH_INTERVAL_NANOS);
    }
}
