// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::{MembershipOperation as Op, MembershipRequest};
use std::sync::atomic::{AtomicBool, Ordering};

fn replace_member(meta: &mut RemoteMetaClient, group: &mut Group, recycle: bool) {
    let placement = group.placement_id();
    let route = meta.read_chunk_route(&placement).unwrap();
    let identity = route.group.as_ref().unwrap().identity.as_ref().unwrap();
    let mut fence = route.membership_fence().unwrap().unwrap();
    let leader = group.leader();
    assert!(fence.outgoing_voters().is_empty());
    assert!(fence.learners().is_empty());
    let added = (1..=4).find(|id| !fence.voters().contains(id)).unwrap();
    let removed = *fence
        .voters()
        .iter()
        .find(|id| **id != leader as u64 + 1)
        .unwrap();
    let mut client = meta
        .connect_chunk_group(&placement, &chunk_options())
        .unwrap();
    for (op, id) in [
        (Op::AddLearner, added),
        (Op::Promote, added),
        (Op::LeaveJoint, 0),
        (Op::Remove, removed),
        (Op::LeaveJoint, 0),
    ] {
        let request = MembershipRequest {
            version: 1,
            bootstrap_id: identity.bootstrap_id,
            operation: op as i32,
            node_id: id,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            match client.chunk_membership(request.clone()) {
                Ok(status) => break status,
                Err(error)
                    if op == Op::Promote && error.to_string().contains("caught-up learner") =>
                {
                    assert!(
                        Instant::now() < deadline,
                        "learner did not catch up: {error}"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => panic!("membership operation {op:?} failed for {placement}: {error}"),
            }
        };
        let previous_floor = fence.applied_floor();
        fence = meta
            .refresh_chunk_membership(&placement, &chunk_options())
            .unwrap();
        assert!(fence.applied_floor() >= status.applied_index);
        assert!(fence.applied_floor() >= previous_floor);
        assert_eq!(fence.voters(), status.voters);
        assert_eq!(fence.outgoing_voters(), status.outgoing_voters);
        assert_eq!(fence.learners(), status.learners);
        let route = meta.read_chunk_route(&placement).unwrap();
        assert_eq!(route.membership_fence().unwrap().unwrap(), fence);
        if op == Op::AddLearner {
            assert!(!route
                .active_members()
                .unwrap()
                .iter()
                .any(|(_, id)| *id == added));
            let joining = added as usize - 1;
            assert!(group.nodes[joining].is_none());
            group.start(joining);
        }
    }
    assert_eq!(fence.voters().len(), 3);
    assert!(fence.voters().contains(&added));
    assert!(!fence.voters().contains(&removed));
    if recycle {
        group.kill(removed as usize - 1);
    }
}
fn replacement_load(group_count: usize, writes: u64, concurrent: bool, rounds: u64) {
    assert!(rounds > 0);
    let mut groups: Vec<_> = (0..group_count)
        .map(|i| {
            let mut group = Group::configured_voters(64 * 1024 * 1024, 4);
            group.group_id = i as u64 + 1;
            group.membership = true;
            group.max_pages = 2048;
            group.checkpoint_bytes = 16 * 1024;
            group.heartbeat_ms = "1000";
            group
        })
        .collect();
    for group in &mut groups {
        for i in 0..3 {
            group.start(i);
        }
    }
    let mut metadata = MetadataGroup::for_groups_with_threads(&groups, 16);
    for group in &mut groups {
        group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
        for i in 0..3 {
            group.kill(i);
        }
        for i in 0..3 {
            group.start(i);
        }
    }
    metadata.ready();
    for group in &groups {
        group.leader();
    }
    group_usage(&metadata.api, &groups, &[]);
    let mut saved = load_batch_sized(
        &LoadEndpoint {
            api: &metadata.api,
            tls: None,
        },
        0,
        writes,
        4,
        true,
        2,
        16384,
    );
    group_usage(&metadata.api, &groups, &saved);
    for group in &groups {
        assert!(saved
            .iter()
            .any(|(_, p)| p.node_ids.contains(&group.placement_id())));
    }
    let stop = AtomicBool::new(false);
    let mut controller_peak = 1;
    let mut added_objects = Vec::new();
    let reads = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let started = Instant::now();
            let mut count = 0;
            let mut meta =
                RemoteMetaClient::connect_catalog(&metadata.api, &load_options()).unwrap();
            while !stop.load(Ordering::Acquire) {
                assert!(
                    started.elapsed() < Duration::from_secs(180),
                    "replacement reader exceeded bound"
                );
                let (n, placement) = &saved[count % saved.len()];
                let (manifest, data) = meta
                    .read_object(
                        &placement.bucket,
                        &placement.key,
                        placement.length as usize,
                        &chunk_options(),
                    )
                    .unwrap();
                assert_eq!(data, load_data_sized(*n, placement.length));
                assert_eq!(manifest.generation, placement.version);
                count += 1;
            }
            count
        });
        // Always release the reader even if a membership assertion unwinds.
        struct Stop<'a>(&'a AtomicBool);
        impl Drop for Stop<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let guard = Stop(&stop);
        for round in 0..rounds {
            if concurrent {
                // Separate controller connections exercise independent group updates
                // contending for the same metadata quorum while reads continue.
                std::thread::scope(|controllers| {
                    let handles: Vec<_> = groups
                        .iter_mut()
                        .map(|group| {
                            let api = &metadata.api;
                            controllers.spawn(move || {
                                let mut meta =
                                    RemoteMetaClient::connect_catalog(api, &load_options())
                                        .unwrap();
                                let started = Instant::now();
                                replace_member(&mut meta, group, rounds > 1);
                                (started, Instant::now())
                            })
                        })
                        .collect();
                    let mut intervals = Vec::new();
                    for handle in handles {
                        let (start, end) = handle.join().unwrap();
                        intervals.push((start, 1isize));
                        intervals.push((end, -1isize));
                    }
                    intervals.sort_unstable();
                    let mut active = 0isize;
                    let mut round_peak = 0;
                    for (_, delta) in intervals {
                        active += delta;
                        round_peak = round_peak.max(active as usize);
                    }
                    assert!(round_peak > 1, "membership controllers did not overlap");
                    controller_peak = controller_peak.max(round_peak);
                });
            } else {
                let mut meta = metadata.ready();
                for group in &mut groups {
                    replace_member(&mut meta, group, rounds > 1);
                }
            }
            if rounds > 1 {
                let expected: Vec<_> = saved.iter().chain(added_objects.iter()).cloned().collect();
                group_usage(&metadata.api, &groups, &expected);
                let start = writes + round * (writes / 4);
                added_objects.extend(load_batch_sized(
                    &LoadEndpoint {
                        api: &metadata.api,
                        tls: None,
                    },
                    start,
                    start + writes / 4,
                    4,
                    true,
                    2,
                    16384,
                ));
            }
        }
        drop(guard);
        reader.join().unwrap()
    });
    assert!(reads > 0);
    saved.extend(added_objects);
    group_usage(&metadata.api, &groups, &saved);
    if rounds == 1 {
        saved.extend(load_batch_sized(
            &LoadEndpoint {
                api: &metadata.api,
                tls: None,
            },
            writes,
            writes + writes / 4,
            4,
            true,
            2,
            16384,
        ));
    }
    let fences: Vec<_> = groups
        .iter()
        .map(|group| {
            metadata
                .ready()
                .read_chunk_route(&group.placement_id())
                .unwrap()
                .membership_fence()
                .unwrap()
                .unwrap()
        })
        .collect();
    let meta_leader = metadata.ready().endpoint().to_string();
    metadata.kill(metadata.api.iter().position(|a| *a == meta_leader).unwrap());
    for group in &mut groups {
        let leader = group.leader();
        group.kill(leader);
    }
    metadata.ready();
    for group in &groups {
        group.leader();
    }
    verify_load(&metadata.api, &saved, true);
    for group in &mut groups {
        for i in 0..4 {
            if group.nodes[i].is_some() {
                group.kill(i);
            }
        }
    }
    for i in 0..3 {
        if metadata.nodes[i].is_some() {
            metadata.kill(i);
        }
    }
    for i in 0..3 {
        metadata.start(i);
    }
    for group in &mut groups {
        for i in 0..4 {
            group.start(i);
        }
    }
    let mut meta = metadata.ready();
    for (group, fence) in groups.iter().zip(fences) {
        group.leader();
        let route = meta.read_chunk_route(&group.placement_id()).unwrap();
        assert_eq!(route.membership_fence().unwrap().unwrap(), fence);
        assert_eq!(
            route
                .active_members()
                .unwrap()
                .iter()
                .map(|(_, id)| *id)
                .collect::<Vec<_>>(),
            fence.voters()
        );
    }
    group_usage(&metadata.api, &groups, &saved);
    verify_load(&metadata.api, &saved, true);
    println!(
        "DYNAMIC_GROUPS groups={group_count} peak_processes={} objects={} concurrent_reads={reads} concurrent_controllers={concurrent} peak_controllers={controller_peak} replacement_rounds={rounds}",
        group_count * 4 + 3,
        saved.len()
    );
}
#[test]
fn three_dynamic_groups_replace_voters_during_reads_and_recover() {
    replacement_load(3, 48, false, 1);
}
#[test]
#[ignore = "67-process dynamic replacement workload; run explicitly in WSL"]
fn sixteen_dynamic_groups_replace_voters_during_reads_and_recover() {
    replacement_load(16, 256, false, 1);
}

#[test]
fn three_dynamic_groups_replace_voters_concurrently_during_reads_and_recover() {
    replacement_load(3, 48, true, 1);
}
#[test]
#[ignore = "67-process concurrent dynamic replacement workload; run explicitly in WSL"]
fn sixteen_dynamic_groups_replace_voters_concurrently_during_reads_and_recover() {
    replacement_load(16, 256, true, 1);
}

#[test]
fn three_dynamic_groups_rejoin_removed_voters_across_three_rounds() {
    replacement_load(3, 48, true, 3);
}
#[test]
#[ignore = "67-process repeated membership workload; run explicitly in WSL"]
fn sixteen_dynamic_groups_rejoin_removed_voters_across_four_rounds() {
    replacement_load(16, 256, true, 4);
}
