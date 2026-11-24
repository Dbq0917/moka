use crate::common::time::Instant;

use portable_atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// `AtomicInstant` is a wrapper around `AtomicU64` that provides thread-safe access
/// to an `Instant`.
///
/// `u64::MAX` is used to represent an unset `Instant`.
#[derive(Debug)]
pub(crate) struct AtomicInstant {
    instant: AtomicU64,
}

impl Default for AtomicInstant {
    /// Creates a new `AtomicInstant` with an unset `Instant`.
    fn default() -> Self {
        Self {
            instant: AtomicU64::new(u64::MAX),
        }
    }
}

impl AtomicInstant {
    /// Creates a new `AtomicInstant` with the given `Instant`.
    pub(crate) fn new(instant: Instant) -> Self {
        // Ensure the `Instant` is not `u64::MAX`, which means unset.
        debug_assert!(instant.as_nanos() != u64::MAX);

        Self {
            instant: AtomicU64::new(instant.as_nanos()),
        }
    }

    /// Returns `true` if the `Instant` is set.
    pub(crate) fn is_set(&self) -> bool {
        self.instant.load(Ordering::Acquire) != u64::MAX
    }

    /// Returns the `Instant` if it is set, otherwise `None`.
    pub(crate) fn instant(&self) -> Option<Instant> {
        let ts = self.instant.load(Ordering::Acquire);
        if ts == u64::MAX {
            None
        } else {
            Some(Instant::from_nanos(ts))
        }
    }

    /// Returns the raw nanosecond value, or `u64::MAX` if the `Instant` is unset.
    #[inline]
    pub(crate) fn as_nanos(&self) -> u64 {
        self.instant.load(Ordering::Acquire)
    }

    /// Sets the `Instant`.
    pub(crate) fn set_instant(&self, instant: Instant) {
        // Ensure the `Instant` is not `u64::MAX`, which means unset.
        debug_assert!(instant.as_nanos() != u64::MAX);

        self.instant.store(instant.as_nanos(), Ordering::Release);
    }

    /// Stores `nanos` if its current value equals `current`.
    ///
    /// Returns `Ok(())` on success, or `Err(actual)` with the value observed at
    /// the failed comparison. This is a thin wrapper around
    /// [`AtomicU64::compare_exchange`], used to refresh a timestamp only when no
    /// other thread has done so in the meantime.
    #[inline]
    pub(crate) fn compare_exchange_nanos(&self, current: u64, nanos: u64) -> Result<(), u64> {
        self.instant
            .compare_exchange(current, nanos, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
    }
}
