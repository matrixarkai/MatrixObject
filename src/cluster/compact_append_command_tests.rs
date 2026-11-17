// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn compact_append_fixture(
    now: u64,
) -> (
    MatrixMetaService,
    ChunkGroupCatalog,
    crate::staged_allocation::StagedAllocationRequest,
) {
    use crate::{
        object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest},
        staged_allocation::StagedAllocationRequest,
    };
    let (mut service, groups) = fixture(1, 0);
    let base = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "append".into(),
        generation: 1,
        ..Default::default()
    };
    PlacementOutcome::publish_manifest(
        &service,
        ManifestOutcome::decide(None, base.clone()).unwrap(),
        0,
    )
    .unwrap()
    .apply(&mut service)
    .unwrap();
    for command in [
        StagedAllocationRequest {
            version: 1,
            action: 6,
            ..Default::default()
        },
        StagedAllocationRequest {
            version: 2,
            action: 1,
            bucket: "bucket".into(),
            key: "append".into(),
            expected_generation: 1,
            append: true,
            page_count: 1,
            ttl_ms: 60_000,
            ..Default::default()
        },
    ] {
        super::super::staged_allocation_commands::prepare_at(
            &service, &groups, &command, 8192, now,
        )
        .unwrap()
        .0
        .apply(&mut service)
        .unwrap();
    }
    let id = service.placement_commit_index;
    let allocation = service.object_manifests.staged_allocation(id).unwrap();
    let page = allocation.allocation().unwrap().page(0).unwrap();
    let after = ObjectManifest {
        generation: 2,
        length: 1,
        extents: vec![ObjectExtent {
            group: "group-0".into(),
            volume_id: page.chunk_id.volume_id(),
            chunk_id: page.chunk_id.id(),
            page_index: page.page_index,
            length: 1,
            write_sequence: 1,
            data_sha256: vec![7; 32],
            ..Default::default()
        }],
        ..base.clone()
    };
    let append = crate::manifest_append::ManifestAppend::decide(
        &ManifestOutcome::decide(Some(base), after).unwrap(),
    )
    .unwrap();
    (
        service,
        groups,
        StagedAllocationRequest {
            version: 4,
            action: 11,
            allocation_id: id,
            manifest_append: Some(append),
            ..Default::default()
        },
    )
}

#[test]
fn compact_append_rejects_forgery_and_preserves_atomic_state() {
    use crate::staged_allocation::{StagedAllocationReply, StagedAllocationRequest};
    let (mut service, groups, command) = compact_append_fixture(1000);
    let saved = service.export_snapshot().to_bytes().unwrap();
    let decide = |service: &MatrixMetaService, command: &StagedAllocationRequest, limit, now| {
        super::super::staged_allocation_commands::prepare_at(service, &groups, command, limit, now)
    };
    assert!(decide(&service, &command, 1, 1500).is_err());
    assert!(decide(&service, &command, 8192, 61_000).is_err());
    for field in 0..17 {
        let mut wrong = command.clone();
        let delta = wrong.manifest_append.as_mut().unwrap();
        match field {
            0 => delta.before_sha256[0] ^= 1,
            1 => delta.after_sha256[0] ^= 1,
            2 => delta.before_generation += 1,
            3 => delta.before_length += 1,
            4 => delta.before_extents += 1,
            5 => delta.suffix[0].chunk_id += 1,
            6 => delta.suffix[0].page_index += 1,
            7 => delta.suffix[0].group = "missing".into(),
            8 => delta.suffix[0].object_offset += 1,
            9 => delta.suffix[0].write_sequence += 1,
            10 => delta.suffix[0].page_offset += 1,
            11 => delta.suffix[0].data_sha256.pop().map(|_| ()).unwrap(),
            12 => delta.suffix.clear(),
            13 => wrong.allocation_id += 1,
            14 => wrong.version = 3,
            15 => wrong.action = 4,
            _ => {
                wrong.outcome = Some(
                    delta
                        .expand(
                            service
                                .object_manifests
                                .get(&crate::ObjectId::new("bucket", "append"))
                                .unwrap(),
                        )
                        .unwrap(),
                )
            }
        }
        if (5..=7).contains(&field) {
            let delta = wrong.manifest_append.as_mut().unwrap();
            let mut candidate = service
                .object_manifests
                .get(&crate::ObjectId::new("bucket", "append"))
                .unwrap()
                .clone();
            candidate.generation += 1;
            candidate.length = delta.after_length;
            candidate.extents.extend(delta.suffix.clone());
            delta.after_sha256 = crate::read_session_outcome::manifest_sha256(&candidate).unwrap();
        }
        assert!(
            decide(&service, &wrong, 8192, 1500).is_err(),
            "field={field}"
        );
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
    }
    let (outcome, reply) = decide(&service, &command, 8192, 1500).unwrap();
    assert_eq!(outcome.version, 12);
    assert!(outcome.manifest.is_none());
    assert!(outcome.to_payload().unwrap().len() < 1024);
    let MetaReply::StagedAllocation(reply) = reply else {
        panic!()
    };
    assert!(reply.encoded_len() <= 47);
    assert_eq!(
        StagedAllocationReply::from_payload(&reply.encode_to_vec()).unwrap(),
        reply
    );
    for field in 0..6 {
        let mut wrong = reply.clone();
        match field {
            0 => wrong.version = 1,
            1 => wrong.committed_allocation_id = 0,
            2 => wrong.manifest_sha256.clear(),
            3 => wrong.publication_fence = 1,
            4 => wrong.allocation_cursor = 1,
            _ => wrong.has_more = true,
        }
        assert!(StagedAllocationReply::from_payload(&wrong.encode_to_vec()).is_err());
    }
    let mut duplicate = command.encode_to_vec();
    duplicate.extend_from_slice(&[8, 4]);
    assert!(StagedAllocationRequest::from_payload(&duplicate).is_err());
    let mut downgraded = outcome.clone();
    downgraded.version = 4;
    assert!(downgraded.to_payload().is_err());
    // Capacity refusal uses the same full publication admission as legacy requests.
    service.nodes.get_mut("group-0").unwrap().capacity_bytes = 0;
    assert!(decide(&service, &command, 8192, 1500).is_err());
    service.nodes.get_mut("group-0").unwrap().capacity_bytes = 1 << 30;
    let expected = command
        .manifest_append
        .as_ref()
        .unwrap()
        .expand(
            service
                .object_manifests
                .get(&crate::ObjectId::new("bucket", "append"))
                .unwrap(),
        )
        .unwrap();
    let legacy = StagedAllocationRequest {
        version: 1,
        action: 4,
        allocation_id: command.allocation_id,
        outcome: Some(expected.clone()),
        ..Default::default()
    };
    let full = decide(&service, &legacy, 8192, 1500).unwrap().0;
    let mut other = service.clone();
    full.apply(&mut other).unwrap();
    PlacementOutcome::from_payload(&outcome.to_payload().unwrap())
        .unwrap()
        .apply(&mut service)
        .unwrap();
    assert_eq!(
        service.export_snapshot().to_bytes().unwrap(),
        other.export_snapshot().to_bytes().unwrap()
    );
    let committed = service.export_snapshot().to_bytes().unwrap();
    assert!(decide(&service, &command, 8192, 1500).is_err());
    assert!(outcome.apply(&mut service).is_err());
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), committed);
}

#[test]
fn compact_append_restores_speculative_state_and_waits_for_commit() {
    let (_files, mut peer) = batch_peer();
    let (mut service, groups, command) =
        compact_append_fixture(super::super::staged_lease_sweeper::now_ms().unwrap());
    let saved = service.export_snapshot().to_bytes().unwrap();
    let id = crate::ObjectId::new("bucket", "append");
    let original_entry = service.object_manifests.entry_arc(&id).unwrap();
    let (request, receiver) = queued(
        MetaRequest::StagedAllocation(command.clone()),
        Instant::now() + Duration::from_secs(10),
    );
    let mut requests = VecDeque::from([request]);
    let mut pending = VecDeque::new();
    propose_batch(
        &mut peer,
        &mut service,
        &groups,
        8192,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
    assert!(std::sync::Arc::ptr_eq(&original_entry, &service.object_manifests.entry_arc(&id).unwrap()));
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    peer.process_ready(|id, bytes| {
        apply_committed(&mut service, &groups, 8192, &mut pending, id, bytes)
    })
    .unwrap();
    let MetaReply::StagedAllocation(reply) = receiver.try_recv().unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(reply.version, 3);
    assert_eq!(
        reply.manifest_sha256,
        command.manifest_append.unwrap().after_sha256
    );
    assert!(service
        .object_manifests
        .staged_allocation(command.allocation_id)
        .is_none());
    assert_eq!(service.nodes["group-0"].committed_bytes, 1);
    assert!(!std::sync::Arc::ptr_eq(&original_entry, &service.object_manifests.entry_arc(&id).unwrap()));
}

#[test]
fn compact_append_mixed_journal_reopens_without_clocked_revalidation() {
    let (mut service, groups, command) = compact_append_fixture(1000);
    let root = std::env::temp_dir().join(format!("compact-append-journal-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let (mut storage, _) =
        super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
    storage.persist(&service, &groups).unwrap();
    let compact = super::super::staged_allocation_commands::prepare_at(
        &service, &groups, &command, 8192, 1500,
    )
    .unwrap()
    .0;
    storage.append_outcome(&compact).unwrap();
    compact.apply(&mut service).unwrap();
    let mut next = service
        .object_manifests
        .get(&crate::ObjectId::new("bucket", "append"))
        .unwrap()
        .clone();
    let prior = next.clone();
    next.generation += 1;
    next.deleted = true;
    next.extents.clear();
    next.length = 0;
    let legacy = PlacementOutcome::publish_manifest(
        &service,
        crate::object_manifest::ManifestOutcome::decide(Some(prior), next).unwrap(),
        0,
    )
    .unwrap();
    storage.append_outcome(&legacy).unwrap();
    legacy.apply(&mut service).unwrap();
    let expected = service.export_snapshot().to_bytes().unwrap();
    drop(storage);
    let mut restored = MatrixMetaService::new();
    let (_storage, recovered) =
        super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut restored).unwrap();
    assert!(recovered);
    assert_eq!(restored.export_snapshot().to_bytes().unwrap(), expected);
}

#[test]
fn an_append_validates_whole_manifests_a_bounded_number_of_times() {
    let (mut service, groups, command) = compact_append_fixture(1000);
    crate::object_manifest::reset_validate_calls();
    crate::manifest_chain::reset_full_chains();
    let (outcome, _) = super::super::staged_allocation_commands::prepare_at(
        &service, &groups, &command, 8192, 1500,
    )
    .unwrap();
    let after_prepare = crate::object_manifest::validate_calls();
    outcome.apply(&mut service).unwrap();
    let total = crate::object_manifest::validate_calls();
    // The stored manifest was validated when it was stored; an append validates what it adds
    // and nothing else, so neither preparing nor applying walks a whole manifest.
    assert_eq!(after_prepare, 0, "prepare validated {after_prepare} whole manifests");
    assert_eq!(total, 0, "prepare and apply validated {total} whole manifests");
    // Nor is any manifest hashed whole: the stored chain is computed once, the first time the
    // object is appended to, and everything after that extends it.
    assert_eq!(crate::manifest_chain::full_chains(), 1, "whole-manifest chains computed");
}

/// Time the metadata state machine's work for one append (reserving the pages, then
/// publishing the digest-bound suffix) against an object that already holds N extents. No
/// network and no fsync: the CPU cost the metaserver adds per append as the object grows.
/// Run with `cargo test --release --lib append_cpu_cost -- --ignored --nocapture`.
#[test]
#[ignore = "local measurement: metadata state machine cost per append by extent count"]
fn append_cpu_cost_by_extent_count() {
    use crate::{
        object_manifest::{ManifestOutcome, ObjectManifest},
        staged_allocation::StagedAllocationRequest,
    };
    crate::manifest_chain::set_soundness_checks(false);
    let (mut service, groups) = fixture(1, 0);
    let mut stored_manifest = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "append".into(),
        generation: 1,
        ..Default::default()
    };
    PlacementOutcome::publish_manifest(
        &service,
        ManifestOutcome::decide(None, stored_manifest.clone()).unwrap(),
        0,
    )
    .unwrap()
    .apply(&mut service)
    .unwrap();
    super::super::staged_allocation_commands::prepare_at(
        &service,
        &groups,
        &StagedAllocationRequest { version: 1, action: 6, ..Default::default() },
        8192,
        1,
    )
    .unwrap()
    .0
    .apply(&mut service)
    .unwrap();
    let windows = [250usize, 1000, 2000, 3000, 4000];
    let per_window = 100usize;
    let mut appended = 0usize;
    for target in windows {
        while appended < target.min(4095 - per_window) {
            append_once(&mut service, &groups, &mut stored_manifest);
            appended += 1;
        }
        let started = std::time::Instant::now();
        for _ in 0..per_window {
            append_once(&mut service, &groups, &mut stored_manifest);
            appended += 1;
        }
        let micros = started.elapsed().as_secs_f64() * 1e6 / per_window as f64;
        println!("extents {:>5}: {:>8.1} microseconds per append", appended - per_window, micros);
    }
}

fn append_once(
    service: &mut MatrixMetaService,
    groups: &ChunkGroupCatalog,
    manifest: &mut crate::object_manifest::ObjectManifest,
) {
    use crate::{object_manifest::ObjectExtent, staged_allocation::StagedAllocationRequest};
    let prepare = |service: &MatrixMetaService, command: &StagedAllocationRequest| {
        super::super::staged_allocation_commands::prepare_at(service, groups, command, 8192, 1500).unwrap().0
    };
    prepare(
        service,
        &StagedAllocationRequest {
            version: 2,
            action: 1,
            bucket: "bucket".into(),
            key: "append".into(),
            expected_generation: manifest.generation,
            append: true,
            page_count: 1,
            ttl_ms: 600_000,
            ..Default::default()
        },
    )
    .apply(service)
    .unwrap();
    let id = service.placement_commit_index;
    let page = service.object_manifests.staged_allocation(id).unwrap().allocation().unwrap().page(0).unwrap();
    let extent = ObjectExtent {
        group: "group-0".into(),
        volume_id: page.chunk_id.volume_id(),
        chunk_id: page.chunk_id.id(),
        page_index: page.page_index,
        object_offset: manifest.length,
        length: 1,
        write_sequence: 1,
        data_sha256: vec![7; 32],
        ..Default::default()
    };
    let object_id = crate::ObjectId::new("bucket", "append");
    let (_, stored) = service.object_manifests.get_with_digest(&object_id).unwrap().unwrap();
    let before_sha256 = stored.digest.clone();
    let chain = stored.chain.extended(std::slice::from_ref(&extent));
    let mut after = manifest.clone();
    after.generation += 1;
    after.length += 1;
    after.extents.push(extent.clone());
    let append = crate::manifest_append::ManifestAppend {
        bucket: "bucket".into(),
        key: "append".into(),
        before_generation: manifest.generation,
        before_length: manifest.length,
        before_extents: manifest.extents.len() as u32,
        before_sha256,
        suffix: vec![extent],
        after_length: after.length,
        after_sha256: chain.digest(&after),
    };
    prepare(
        service,
        &StagedAllocationRequest {
            version: 4,
            action: 11,
            allocation_id: id,
            manifest_append: Some(append),
            ..Default::default()
        },
    )
    .apply(service)
    .unwrap();
    *manifest = after;
}

/// One metadata state machine, an object published with `BENCH_PREFILL` extents (default 250),
/// then `BENCH_APPENDS` (default 20) appends timed and printed. Meant for a profiler.
#[test]
#[ignore = "local measurement for a profiler"]
fn append_cpu_cost_at_extents() {
    use crate::{
        object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest},
        staged_allocation::StagedAllocationRequest,
    };
    let prefill: usize = std::env::var("BENCH_PREFILL").ok().and_then(|v| v.parse().ok()).unwrap_or(250);
    let appends: usize = std::env::var("BENCH_APPENDS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    crate::manifest_chain::set_soundness_checks(false);
    let (mut service, groups) = fixture(1, 0);
    let mut manifest = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "append".into(),
        generation: 1,
        length: prefill as u64,
        extents: (0..prefill)
            .map(|index| ObjectExtent {
                group: "group-0".into(),
                volume_id: 7,
                chunk_id: index as u64 + 1,
                page_index: 0,
                object_offset: index as u64,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![(index % 251) as u8; 32],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    PlacementOutcome::publish_manifest(&service, ManifestOutcome::decide(None, manifest.clone()).unwrap(), 0)
        .unwrap()
        .apply(&mut service)
        .unwrap();
    super::super::staged_allocation_commands::prepare_at(
        &service,
        &groups,
        &StagedAllocationRequest { version: 1, action: 6, ..Default::default() },
        8192,
        1,
    )
    .unwrap()
    .0
    .apply(&mut service)
    .unwrap();
    // the first append computes the stored chain; keep it out of the timing
    append_once(&mut service, &groups, &mut manifest);
    let started = std::time::Instant::now();
    for _ in 0..appends {
        append_once(&mut service, &groups, &mut manifest);
    }
    println!(
        "extents {:>5}: {:>8.1} microseconds per append",
        prefill,
        started.elapsed().as_secs_f64() * 1e6 / appends as f64
    );
}
