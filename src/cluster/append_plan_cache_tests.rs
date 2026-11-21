// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn plan(bucket: &str, key: &str) -> PlacementAssignment {
    PlacementAssignment {
        bucket: bucket.into(),
        key: key.into(),
        length: 4096,
        node_ids: vec!["data-1".into()],
        skipped_draining: 0,
        skipped_offline: 0,
        skipped_capacity: 0,
        plan_checksum: "checked".into(),
    }
}
fn get(cache: &mut AppendPlanCache, key: &str, now: Instant) -> bool {
    cache
        .lookup("bucket", key, 4096, 1, Duration::from_secs(1), now)
        .is_some()
}
fn remember(cache: &mut AppendPlanCache, key: &str, now: Instant) {
    cache.remember(&plan("bucket", key), &[], 4096, 1, now);
}

#[test]
fn alternating_log_and_index_keep_both_plans_without_growing_the_cache() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "wal", now);
    remember(&mut cache, "index", now);
    for _ in 0..64 {
        assert!(get(&mut cache, "wal", now));
        assert!(get(&mut cache, "index", now));
    }
    assert_eq!(cache.entries.iter().flatten().count(), 2);
    assert!(cache
        .lookup(
            "another-bucket",
            "wal",
            4096,
            1,
            Duration::from_secs(1),
            now
        )
        .is_none());
}

#[test]
fn cache_hits_do_not_extend_the_original_deadline() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "wal", now);
    for millis in [100, 500, 999] {
        assert!(get(&mut cache, "wal", now + Duration::from_millis(millis)));
    }
    assert!(!get(&mut cache, "wal", now + Duration::from_secs(1)));
}

#[test]
fn expiration_is_independent_for_each_object_and_zero_disables_hits() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "wal", now);
    remember(&mut cache, "index", now + Duration::from_millis(500));
    assert!(!get(&mut cache, "wal", now + Duration::from_secs(1)));
    assert!(get(&mut cache, "index", now + Duration::from_secs(1)));
    assert!(cache
        .lookup(
            "bucket",
            "index",
            4096,
            1,
            Duration::ZERO,
            now + Duration::from_secs(1)
        )
        .is_none());
}

#[test]
fn a_plan_only_serves_its_group_count_and_smaller_payloads() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "wal", now);
    assert!(cache
        .lookup("bucket", "wal", 4097, 1, Duration::from_secs(1), now)
        .is_none());
    assert!(cache
        .lookup("bucket", "wal", 4096, 2, Duration::from_secs(1), now)
        .is_none());
    assert!(cache
        .lookup("bucket", "wal", 1, 1, Duration::from_secs(1), now)
        .is_some());
}

#[test]
fn a_third_object_evicts_the_least_recently_used_plan() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "wal", now);
    remember(&mut cache, "index", now);
    assert!(get(&mut cache, "wal", now));
    remember(&mut cache, "other", now);
    assert!(get(&mut cache, "wal", now));
    assert!(get(&mut cache, "other", now));
    assert!(!get(&mut cache, "index", now));
    assert_eq!(cache.entries.iter().flatten().count(), 2);
}

#[test]
fn replacing_one_objects_plan_preserves_the_other_objects_deadline() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "wal", now);
    remember(&mut cache, "index", now);
    cache.remember(
        &plan("bucket", "wal"),
        &[],
        8192,
        1,
        now + Duration::from_millis(500),
    );
    assert!(cache
        .lookup(
            "bucket",
            "wal",
            8192,
            1,
            Duration::from_secs(1),
            now + Duration::from_secs(1)
        )
        .is_some());
    assert!(!get(&mut cache, "index", now + Duration::from_secs(1)));
    assert_eq!(cache.entries.iter().flatten().count(), 1);
}

#[test]
fn wide_or_large_routing_plans_bypass_the_cache() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "index", now);
    let mut wide = plan("bucket", "wal");
    wide.node_ids = vec!["group".into(); 33];
    cache.remember(&wide, &[], 4096, 33, now);
    assert!(cache
        .lookup("bucket", "wal", 4096, 33, Duration::from_secs(1), now)
        .is_none());
    let route = ChunkGroupRoute {
        fence: vec![0; MAX_CACHED_PLAN_BYTES],
        ..Default::default()
    };
    cache.remember(&plan("bucket", "wal"), &[route], 4096, 1, now);
    assert!(!get(&mut cache, "wal", now));
    assert!(get(&mut cache, "index", now));
    let long = plan("bucket", &"x".repeat(MAX_CACHED_PLAN_BYTES));
    cache.remember(&long, &[], 4096, 1, now);
    assert_eq!(cache.entries.iter().flatten().count(), 1);
}

#[test]
fn clearing_retires_both_plans_and_moving_keeps_absolute_deadlines() {
    let mut cache = AppendPlanCache::default();
    let now = Instant::now();
    remember(&mut cache, "wal", now);
    remember(&mut cache, "index", now);
    let mut moved = std::mem::take(&mut cache);
    assert!(!get(&mut cache, "wal", now));
    assert!(get(&mut moved, "wal", now));
    assert!(!get(&mut moved, "index", now + Duration::from_secs(1)));
    remember(&mut moved, "index", now + Duration::from_secs(1));
    moved.clear();
    assert!(!get(&mut moved, "wal", now + Duration::from_secs(1)));
    assert!(!get(&mut moved, "index", now + Duration::from_secs(1)));
}
