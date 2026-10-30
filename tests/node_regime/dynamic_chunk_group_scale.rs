// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::{
    MembershipOperation as Op, MembershipRequest,
};
use std::sync::atomic::{AtomicBool, Ordering};

fn replace_member(meta: &mut RemoteMetaClient, group: &mut Group) {
    let placement = group.placement_id();
    let route = meta.read_chunk_route(&placement).unwrap();
    let identity = route.group.as_ref().unwrap().identity.as_ref().unwrap();
    let mut fence = route.membership_fence().unwrap().unwrap();
    let leader = group.leader();
    let removed = (leader + 1) % 3;
    let mut client = meta
        .connect_chunk_group(&placement, &chunk_options())
        .unwrap();
    for (op, id) in [
        (Op::AddLearner, 4),
        (Op::Promote, 4),
        (Op::LeaveJoint, 0),
        (Op::Remove, removed as u64 + 1),
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
        fence = meta.refresh_chunk_membership(&placement, &chunk_options()).unwrap();
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
                .any(|(_, id)| *id == 4));
            group.start(3);
        }
    }
    assert_eq!(fence.voters().len(), 3);
    assert!(fence.voters().contains(&4));
    assert!(!fence.voters().contains(&(removed as u64 + 1)));
}
fn replacement_load(group_count: usize, writes: u64) {
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
        let mut meta = metadata.ready();
        for group in &mut groups {
            replace_member(&mut meta, group);
        }
        drop(guard);
        reader.join().unwrap()
    });
    assert!(reads > 0);
    group_usage(&metadata.api, &groups, &saved);
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
        "DYNAMIC_GROUPS groups={group_count} peak_processes={} objects={} concurrent_reads={reads}",
        group_count * 4 + 3,
        saved.len()
    );
}
#[test]
fn three_dynamic_groups_replace_voters_during_reads_and_recover() {
    replacement_load(3, 48);
}
#[test]
#[ignore = "67-process dynamic replacement workload; run explicitly in WSL"]
fn sixteen_dynamic_groups_replace_voters_during_reads_and_recover() {
    replacement_load(16, 256);
}
