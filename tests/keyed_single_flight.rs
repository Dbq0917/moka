#![cfg(feature = "sync")]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Barrier,
    },
    thread,
    time::Duration,
};

use moka::sync::Cache;

#[test]
fn same_key_runs_only_one_loader() {
    let cache = Cache::<&'static str, &'static str>::new(100);
    let db_queries = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(2));

    let threads = (0..2)
        .map(|_| {
            let cache = cache.clone();
            let db_queries = Arc::clone(&db_queries);
            let start = Arc::clone(&start);

            thread::spawn(move || {
                start.wait();
                cache.get_with("user:1", || {
                    db_queries.fetch_add(1, Ordering::AcqRel);
                    thread::sleep(Duration::from_millis(50));
                    "fresh-value"
                })
            })
        })
        .collect::<Vec<_>>();

    for handle in threads {
        assert_eq!(
            handle.join().expect("loader thread panicked"),
            "fresh-value"
        );
    }

    assert_eq!(db_queries.load(Ordering::Acquire), 1);
    assert_eq!(cache.get("user:1"), Some("fresh-value"));
}

#[test]
fn different_keys_do_not_serialize_loaders() {
    let cache = Cache::<&'static str, u8>::new(100);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();

    let key_a = {
        let cache = cache.clone();
        let started_tx = started_tx.clone();
        thread::spawn(move || {
            cache.get_with("a", || {
                started_tx.send("a").expect("started receiver disappeared");
                release_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("release signal timed out");
                1
            })
        })
    };

    assert_eq!(
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("first loader did not start"),
        "a"
    );

    let key_b = {
        let cache = cache.clone();
        thread::spawn(move || {
            cache.get_with("b", || {
                started_tx.send("b").expect("started receiver disappeared");
                2
            })
        })
    };

    assert_eq!(
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("different key was serialized behind the first loader"),
        "b"
    );

    release_tx.send(()).expect("first loader already released");
    assert_eq!(key_a.join().expect("key a loader panicked"), 1);
    assert_eq!(key_b.join().expect("key b loader panicked"), 2);
}
