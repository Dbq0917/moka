pub(crate) const DEFAULT_MAX_LOG_SYNC_REPEATS: usize = 4;
pub(crate) const LOG_SYNC_INTERVAL_MILLIS: u64 = 300;

pub(crate) const READ_LOG_FLUSH_POINT: usize = 64;
pub(crate) const WRITE_LOG_FLUSH_POINT: usize = 64;

/// The minimum interval between two refreshes of an entry's `last_accessed`
/// timestamp that are triggered by cache reads.
///
/// Without this, a hot key that is read tens of thousands of times per second
/// would perform an atomic store (and bounce the cache line holding
/// `last_accessed` between cores) on every single read. Reads within this window
/// skip the store and keep the previously recorded timestamp, collapsing the
/// write rate to at most one store per entry per window regardless of the read
/// rate.
///
/// The window is sub-millisecond on purpose: it keeps the access-time precision
/// far below the second/minute granularity that would be visible to expiration,
/// while still removing the overwhelming majority of the contended atomic
/// writes. The bounded lag is only observable as an entry expiring up to this
/// much earlier than its nominal time-to-idle, which is within the cache's
/// best-effort expiration semantics.
pub(crate) const ACCESS_TIME_REFRESH_INTERVAL_NANOS: u64 = 100_000;

// 384 elements
pub(crate) const READ_LOG_CH_SIZE: usize =
    READ_LOG_FLUSH_POINT * (DEFAULT_MAX_LOG_SYNC_REPEATS + 2);

// 384 elements
pub(crate) const WRITE_LOG_CH_SIZE: usize =
    WRITE_LOG_FLUSH_POINT * (DEFAULT_MAX_LOG_SYNC_REPEATS + 2);

// TODO: Calculate the batch size based on the number of entries in the cache (or an
// estimated number of entries to evict)
pub(crate) const DEFAULT_EVICTION_BATCH_SIZE: u32 = WRITE_LOG_CH_SIZE as u32;

/// The default timeout duration for the `run_pending_tasks` method.
pub(crate) const DEFAULT_MAINTENANCE_TASK_TIMEOUT_MILLIS: u64 = 100;

#[cfg(feature = "sync")]
pub(crate) const WRITE_RETRY_INTERVAL_MICROS: u64 = 50;
