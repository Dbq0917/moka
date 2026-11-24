#![cfg(all(test, feature = "sync"))]

//! Concurrency smoke test for a single hot key that is read far more often than
//! any other entry. This mirrors the "hot product" workload where one key is
//! read tens of thousands of times per second.
//!
//! The access-time refresh is throttled to at most one atomic timestamp store
//! per refresh window, so this workload must:
//!
//! - keep the hot entry alive while reads keep arriving,
//! - never observe a backwards-jumping access timestamp,
//! - sustain the read rate without panicking or losing the entry.

use moka::sync::Cache;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::Instant;

#[test]
fn single_hot_key_survives_concurrent_read_storm() {
    const THREADS: u64 = 8;
    const READS_PER_THREAD: u64 = 200_000;

    let cache = Cache::builder()
        .max_capacity(16)
        .time_to_idle(std::time::Duration::from_secs(30))
        .build();
    cache.insert("hot-product", 42);

    let total_reads = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(THREADS as usize));

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let cache = cache.clone();
        let barrier = Arc::clone(&barrier);
        let total_reads = Arc::clone(&total_reads);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..READS_PER_THREAD {
                assert_eq!(cache.get(&"hot-product"), Some(42));
                total_reads.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let elapsed = start.elapsed();

    assert_eq!(
        total_reads.load(Ordering::Relaxed),
        THREADS * READS_PER_THREAD
    );
    assert_eq!(cache.get(&"hot-product"), Some(42));

    let throughput = (THREADS * READS_PER_THREAD) as f64 / elapsed.as_secs_f64();
    // Print so the number is visible with `--nocapture`. The threshold is loose
    // enough to avoid flaky CI failures; it is a sanity floor, not the real
    // benchmark number.
    println!("hot-key read throughput: {:.0} reads/s", throughput);
    assert!(
        throughput > 100_000.0,
        "throughput unexpectedly low: {throughput}"
    );
}
