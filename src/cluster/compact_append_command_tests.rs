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
                            &service
                                .object_manifests
                                .get_manifest(&crate::ObjectId::new("bucket", "append"))
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
                .get_manifest(&crate::ObjectId::new("bucket", "append"))
                .unwrap();
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
            &service
                .object_manifests
                .get_manifest(&crate::ObjectId::new("bucket", "append"))
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
        .get_manifest(&crate::ObjectId::new("bucket", "append"))
        .unwrap();
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
    append_once_with(service, groups, manifest, false);
}

/// One reserve-and-publish append. With `slow` the publication is applied the long way: the
/// expansion prepared for it is forgotten and another holder keeps the catalog entry shared, so
/// the apply expands for itself and builds a new entry. Returns the two outcomes as they would be
/// made durable.
fn append_once_with(
    service: &mut MatrixMetaService,
    groups: &ChunkGroupCatalog,
    manifest: &mut crate::object_manifest::ObjectManifest,
    slow: bool,
) -> Vec<Vec<u8>> {
    let pending = begin_append(service, groups, manifest, |_| {});
    let durable = pending.durable.clone();
    finish_append(service, pending, manifest, slow).unwrap();
    durable
}

/// An append that has been reserved and prepared but not applied.
struct PendingAppend {
    publish: PlacementOutcome,
    after: crate::object_manifest::ObjectManifest,
    durable: Vec<Vec<u8>>,
}

/// Reserve the page and prepare the publication of one more extent; the allocation is applied,
/// the publication is not. `tweak` may alter the new extent before it is published.
/// Panics if the reservation or the preparation is refused.
fn begin_append(
    service: &mut MatrixMetaService,
    groups: &ChunkGroupCatalog,
    manifest: &crate::object_manifest::ObjectManifest,
    tweak: impl FnOnce(&mut crate::object_manifest::ObjectExtent),
) -> PendingAppend {
    try_begin_append(service, groups, manifest, tweak).unwrap()
}

fn try_begin_append(
    service: &mut MatrixMetaService,
    groups: &ChunkGroupCatalog,
    manifest: &crate::object_manifest::ObjectManifest,
    tweak: impl FnOnce(&mut crate::object_manifest::ObjectExtent),
) -> Result<PendingAppend, crate::ObjectError> {
    use crate::{object_manifest::ObjectExtent, staged_allocation::StagedAllocationRequest};
    let mut durable = Vec::new();
    let prepare = |service: &MatrixMetaService, command: &StagedAllocationRequest| {
        super::super::staged_allocation_commands::prepare_at(service, groups, command, 8192, 1500).map(|prepared| prepared.0)
    };
    let reserve = prepare(
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
    )?;
    durable.push(reserve.to_payload().unwrap());
    reserve.apply(service).unwrap();
    let id = service.placement_commit_index;
    let page = service.object_manifests.staged_allocation(id).unwrap().allocation().unwrap().page(0).unwrap();
    let mut extent = ObjectExtent {
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
    tweak(&mut extent);
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
    let publish = prepare(
        service,
        &StagedAllocationRequest {
            version: 4,
            action: 11,
            allocation_id: id,
            manifest_append: Some(append),
            ..Default::default()
        },
    )?;
    durable.push(publish.to_payload().unwrap());
    Ok(PendingAppend { publish, after, durable })
}

/// Apply a prepared append. With `slow` the expansion made while preparing it is forgotten and
/// another holder keeps the catalog entry shared, so the apply builds everything itself.
fn finish_append(
    service: &mut MatrixMetaService,
    pending: PendingAppend,
    manifest: &mut crate::object_manifest::ObjectManifest,
    slow: bool,
) -> Result<(), crate::ObjectError> {
    let object_id = crate::ObjectId::new("bucket", "append");
    let _shared = if slow {
        service.object_manifests.entry_arc(&object_id)
    } else {
        None
    };
    pending.publish.apply(service)?;
    *manifest = pending.after;
    Ok(())
}

/// A catalog holding an object of `prefill` one-byte extents, with the publication fence on, and
/// the manifest as published.
fn prefilled_log(prefill: usize) -> (MatrixMetaService, ChunkGroupCatalog, crate::object_manifest::ObjectManifest) {
    use crate::{
        object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest},
        staged_allocation::StagedAllocationRequest,
    };
    crate::manifest_chain::set_soundness_checks(false);
    let (mut service, groups) = fixture(1, 0);
    let manifest = ObjectManifest {
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
    (service, groups, manifest)
}

/// Extending the catalog entry in place, reusing the expansion made while preparing, and
/// skipping the comparisons of the stored extents with themselves must change nothing that is
/// observable: the outcomes made durable, the catalog image, and the state a replay of those
/// outcomes rebuilds are byte for byte those of the long way round.
#[test]
fn growing_an_entry_in_place_is_byte_identical_to_rebuilding_it() {
    const STEPS: usize = 40;
    let (mut quick, groups, mut quick_manifest) = prefilled_log(300);
    let (mut slow, _, mut slow_manifest) = prefilled_log(300);
    let (mut replayed, _, _) = prefilled_log(300);
    let mut quick_durable = Vec::new();
    let mut slow_durable = Vec::new();
    for _ in 0..STEPS {
        quick_durable.extend(append_once_with(&mut quick, &groups, &mut quick_manifest, false));
        slow_durable.extend(append_once_with(&mut slow, &groups, &mut slow_manifest, true));
    }
    assert_eq!(quick_durable, slow_durable, "the outcomes made durable differ");
    assert_eq!(quick_manifest, slow_manifest);
    assert_eq!(
        quick.object_manifests.to_bytes().unwrap(),
        slow.object_manifests.to_bytes().unwrap(),
        "the catalog images differ"
    );
    // A restart or a follower knows only the durable outcomes: replaying them must rebuild the
    // same catalog, whatever was or was not prepared on this thread.
    for payload in &quick_durable {
        PlacementOutcome::from_payload(payload).unwrap().apply(&mut replayed).unwrap();
    }
    assert_eq!(
        replayed.object_manifests.to_bytes().unwrap(),
        quick.object_manifests.to_bytes().unwrap(),
        "a replay of the durable outcomes rebuilds a different catalog"
    );
    let id = crate::ObjectId::new("bucket", "append");
    assert_eq!(replayed.object_manifests.get_manifest(&id), Some(quick_manifest.clone()));
    assert_eq!(quick.object_manifests.get_manifest(&id), Some(quick_manifest.clone()));
}

/// The expansion kept from preparing an append may only be used against the manifest it was made
/// from. Here the object moves on between preparing and applying (by a publication that does not
/// go through the compact path, so the kept expansion is still waiting): the apply must be
/// refused and leave the catalog as it was.
#[test]
fn a_waiting_expansion_is_not_used_after_the_stored_manifest_changed() {
    use crate::object_manifest::ManifestOutcome;
    let (mut service, groups, mut manifest) = prefilled_log(100);
    let stale = begin_append(&mut service, &groups, &manifest, |_| {});
    // Same extents, next generation: a publication the fence allows without owned pages.
    let mut moved = manifest.clone();
    moved.generation += 1;
    PlacementOutcome::publish_manifest(&service, ManifestOutcome::decide(Some(manifest.clone()), moved.clone()).unwrap(), 0)
        .unwrap()
        .apply(&mut service)
        .unwrap();
    let image = service.object_manifests.to_bytes().unwrap();
    let error = finish_append(&mut service, stale, &mut manifest, false).unwrap_err();
    assert!(error.to_string().contains("prior manifest changed"), "{error}");
    assert_eq!(service.object_manifests.to_bytes().unwrap(), image, "a refused append changed the catalog");
    assert_eq!(service.object_manifests.get_manifest(&crate::ObjectId::new("bucket", "append")), Some(moved.clone()));
}

/// An append interleaved with another one to the same object: the second prepare replaces the
/// kept expansion, so the first apply expands for itself and is refused because the stored
/// manifest it was decided against is gone once the second is applied.
#[test]
fn an_interleaved_append_to_the_same_object_refuses_the_older_one() {
    let (mut service, groups, mut manifest) = prefilled_log(100);
    let older = begin_append(&mut service, &groups, &manifest, |_| {});
    let mut newer_base = manifest.clone();
    let newer = begin_append(&mut service, &groups, &newer_base, |_| {});
    finish_append(&mut service, newer, &mut newer_base, false).unwrap();
    let image = service.object_manifests.to_bytes().unwrap();
    assert!(finish_append(&mut service, older, &mut manifest, false).is_err());
    assert_eq!(service.object_manifests.to_bytes().unwrap(), image);
    assert_eq!(service.object_manifests.get_manifest(&crate::ObjectId::new("bucket", "append")), Some(newer_base.clone()));
}

/// Two catalogs on one thread, appends prepared for both before either is applied: each apply
/// finds the kept expansion belongs to the other (or is gone) and still ends in the state the
/// long way round reaches.
#[test]
fn two_catalogs_alternating_on_one_thread_never_borrow_each_others_expansion() {
    const ROUNDS: usize = 15;
    let (mut a, groups, mut a_manifest) = prefilled_log(300);
    let (mut b, _, mut b_manifest) = prefilled_log(301);
    let (mut slow_a, _, mut slow_a_manifest) = prefilled_log(300);
    let (mut slow_b, _, mut slow_b_manifest) = prefilled_log(301);
    for _ in 0..ROUNDS {
        let pending_a = begin_append(&mut a, &groups, &a_manifest, |_| {});
        let pending_b = begin_append(&mut b, &groups, &b_manifest, |_| {});
        finish_append(&mut a, pending_a, &mut a_manifest, false).unwrap();
        finish_append(&mut b, pending_b, &mut b_manifest, false).unwrap();
        append_once_with(&mut slow_a, &groups, &mut slow_a_manifest, true);
        append_once_with(&mut slow_b, &groups, &mut slow_b_manifest, true);
    }
    assert_eq!(a.object_manifests.to_bytes().unwrap(), slow_a.object_manifests.to_bytes().unwrap());
    assert_eq!(b.object_manifests.to_bytes().unwrap(), slow_b.object_manifests.to_bytes().unwrap());
    assert_eq!(a_manifest.extents.len(), 300 + ROUNDS);
    assert_eq!(b_manifest.extents.len(), 301 + ROUNDS);
}

/// The same measurement as `append_cpu_cost_at_extents` along the path a leader takes: prepare,
/// apply the decoded record on the undo draft (which holds a second Arc handle on the catalog
/// entry) and restore it, then apply the committed record to the live catalog. Meant for a
/// profiler: `BENCH_PREFILL` extents (default 250), `BENCH_APPENDS` appends (default 20).
#[test]
#[ignore = "local measurement for a profiler"]
fn append_cpu_cost_through_the_leaders_draft() {
    let prefill: usize = std::env::var("BENCH_PREFILL").ok().and_then(|v| v.parse().ok()).unwrap_or(250);
    let appends: usize = std::env::var("BENCH_APPENDS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let (mut service, groups, mut manifest) = prefilled_log(prefill);
    let object_id = crate::ObjectId::new("bucket", "append");
    let leader_append = |service: &mut MatrixMetaService, manifest: &mut crate::object_manifest::ObjectManifest| {
        let pending = begin_append(service, &groups, manifest, |_| {});
        let record = pending.durable.last().unwrap().clone();
        {
            let nodes: Vec<String> = service.nodes.keys().cloned().collect();
            let mut draft = PlacementDraft::with_records(service, nodes, vec![object_id.clone()]);
            let allocation = pending.publish.staging.as_ref().unwrap().before.as_ref().unwrap().allocation_id;
            draft.staging_allocations =
                vec![(allocation, draft.service.object_manifests.staged_allocation(allocation).cloned())];
            PlacementOutcome::from_payload(&record).unwrap().apply(draft.service).unwrap();
        }
        PlacementOutcome::from_payload(&record).unwrap().apply(service).unwrap();
        *manifest = pending.after;
    };
    // the first append computes the stored chain; keep it out of the timing
    leader_append(&mut service, &mut manifest);
    let started = std::time::Instant::now();
    for _ in 0..appends {
        leader_append(&mut service, &mut manifest);
    }
    println!(
        "extents {:>5}: {:>8.1} microseconds per append (leader path)",
        prefill,
        started.elapsed().as_secs_f64() * 1e6 / appends as f64
    );
}

/// What the writing client does with the manifest for one compact append, apart from any I/O: it
/// builds the published manifest from the prior one, decides the outcome, extends the remembered
/// chain and makes the bounded `ManifestAppend`. Meant for a profiler, like the leader-path
/// measurement above: `BENCH_PREFILL` extents, `BENCH_APPENDS` appends.
#[test]
#[ignore = "profiling aid: run under callgrind"]
fn client_append_cpu_cost_at_extents() {
    use crate::object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest, StoredDigest};
    let prefill: usize = std::env::var("BENCH_PREFILL").ok().and_then(|v| v.parse().ok()).unwrap_or(250);
    let appends: usize = std::env::var("BENCH_APPENDS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let (_service, _groups, mut manifest) = prefilled_log(prefill);
    let mut prior = StoredDigest::of(&manifest);
    let step = |manifest: &mut ObjectManifest, prior: &mut StoredDigest| {
        let expected = std::mem::take(manifest);
        let new = ObjectManifest {
            format_version: 2,
            bucket: expected.bucket.clone(),
            key: expected.key.clone(),
            generation: expected.generation + 1,
            length: 1,
            extents: vec![ObjectExtent {
                group: "group-0".into(),
                volume_id: 7,
                chunk_id: 1_000_000 + expected.generation,
                object_offset: 0,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![7; 32],
                ..Default::default()
            }],
            ..Default::default()
        };
        // the published manifest, as `write_object_inner` builds it for an append
        let base = expected.length;
        let mut combined = new.clone();
        combined.length += base;
        for extent in &mut combined.extents {
            extent.object_offset += base;
        }
        let mut extents = expected.extents[..expected.extents.len()].to_vec();
        extents.extend(combined.extents);
        combined.extents = extents;
        let published = combined;
        let outcome = ManifestOutcome::decide_append(expected, published.clone()).unwrap();
        // `decide_compact_append`
        let before = outcome.before.as_ref().unwrap();
        let after = outcome.after.as_ref().unwrap();
        assert!(after.extents.starts_with(&before.extents));
        let chain = prior.chain.extended(&after.extents[before.extents.len()..]);
        let result = StoredDigest { digest: chain.digest(after), chain };
        crate::manifest_append::ManifestAppend::decide_with_digests_checked(
            &outcome,
            prior.digest.clone(),
            result.digest.clone(),
            true,
        )
        .unwrap();
        *prior = result;
        *manifest = published;
    };
    step(&mut manifest, &mut prior);
    let started = std::time::Instant::now();
    for _ in 0..appends {
        step(&mut manifest, &mut prior);
    }
    println!(
        "extents {:>5}: {:>8.1} microseconds per append (client side)",
        prefill,
        started.elapsed().as_secs_f64() * 1e6 / appends as f64
    );
}

/// The work of reading a range of an object with `BENCH_PREFILL` extents, without I/O, in the
/// phases of a ranged read. `BENCH_PHASE` selects one: `server_read` (the metaserver reads the
/// manifest and encodes its reply), `client_decode` (the client decodes the reply),
/// `client_prepare` (the client validates the manifest, digests it for the read session, validates
/// it again and picks the extents of a one-page range), `open_session` (the metaserver prepares
/// the read session for the digest). `BENCH_APPENDS` repetitions.
#[test]
#[ignore = "profiling aid: run under callgrind"]
fn read_cpu_cost_at_extents() {
    use prost::Message;
    let prefill: usize = std::env::var("BENCH_PREFILL").ok().and_then(|v| v.parse().ok()).unwrap_or(250);
    let reps: usize = std::env::var("BENCH_APPENDS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let phase = std::env::var("BENCH_PHASE").unwrap_or_else(|_| "server_read".into());
    let (service, _groups, manifest) = prefilled_log(prefill);
    let id = crate::ObjectId::new("bucket", "append");
    let bytes = manifest.encode_to_vec();
    let digest = crate::read_session_outcome::manifest_sha256(&manifest).unwrap();
    let range_start = manifest.length / 2;
    let one = |phase: &str| match phase {
        "server_read" => {
            let value = service.object_manifests.get_manifest(&id).unwrap();
            let reply = super::super::meta_message::encode_meta_reply(&super::super::meta_message::MetaReply::Manifest(value));
            std::hint::black_box(reply.len());
        }
        "client_decode" => {
            let decoded = crate::object_manifest::ObjectManifest::decode(bytes.as_slice()).unwrap();
            std::hint::black_box(decoded.extents.len());
        }
        "client_prepare" => {
            manifest.validate().unwrap();
            let sha = crate::read_session_outcome::manifest_sha256(&manifest).unwrap();
            manifest.validate().unwrap();
            let picked = manifest
                .extents
                .iter()
                .filter(|extent| extent.object_offset < range_start + 1 && extent.object_offset + extent.length > range_start)
                .count();
            std::hint::black_box((sha.len(), picked));
        }
        "open_session" => {
            let request = crate::manifest_retention::ManifestRetentionRequest {
                version: 3,
                action: 8,
                bucket: "bucket".into(),
                key: "append".into(),
                generation: manifest.generation,
                ttl_ms: 60_000,
                manifest_sha256: digest.clone(),
                ..Default::default()
            };
            let prepared = super::super::read_session_commands::prepare_at(&service, &request, 1 << 20, 1000).unwrap();
            std::hint::black_box(prepared.1);
        }
        other => panic!("unknown BENCH_PHASE {other}"),
    };
    one(&phase);
    for _ in 0..reps {
        one(&phase);
    }
}

/// The leader collects the nodes a queued compact append touches from the object's placement
/// record instead of walking every stored extent. That is only the same set while the record's
/// nodes are exactly the groups of all the stored extents, whatever sequence of appends built it:
/// checked here over random sequences across three groups.
#[test]
fn the_placement_nodes_of_an_appended_object_are_the_groups_of_its_extents() {
    use crate::object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest};
    let mut state = 0x2545f4914f6cdd1du64;
    let mut next = |bound: u64| {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) % bound
    };
    for round in 0..6u64 {
        let (mut service, groups) = fixture(3, 0);
        let prefill = (round as usize) * 3;
        let mut manifest = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "append".into(),
            generation: 1,
            length: prefill as u64,
            extents: (0..prefill)
                .map(|index| ObjectExtent {
                    group: format!("group-{}", next(3)),
                    volume_id: 7,
                    chunk_id: index as u64 + 1,
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
            &crate::staged_allocation::StagedAllocationRequest { version: 1, action: 6, ..Default::default() },
            8192,
            1,
        )
        .unwrap()
        .0
        .apply(&mut service)
        .unwrap();
        let id = crate::ObjectId::new("bucket", "append");
        for _ in 0..25 {
            let group = format!("group-{}", next(3));
            let pending = begin_append(&mut service, &groups, &manifest, |extent| extent.group = group.clone());
            finish_append(&mut service, pending, &mut manifest, false).unwrap();
            let from_extents: std::collections::BTreeSet<String> =
                manifest.extents.iter().map(|extent| extent.group.clone()).collect();
            let recorded: std::collections::BTreeSet<String> =
                service.placements.get(&id).unwrap().node_ids.iter().cloned().collect();
            assert_eq!(recorded, from_extents, "round {round}, {} extents", manifest.extents.len());
            let stored: std::collections::BTreeSet<String> = service
                .object_manifests
                .get(&id)
                .unwrap()
                .extents
                .iter()
                .map(|extent| extent.group.clone())
                .collect();
            assert_eq!(stored, from_extents);
        }
    }
}

/// The durable outcomes and the catalog image of a fixed sequence of appends are pinned, so a
/// change of representation cannot change a byte of either: the digests below were taken from the
/// implementation that copies the manifest (before the shared extent list).
#[test]
fn a_fixed_sequence_of_appends_makes_the_same_bytes_as_before() {
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let sha = |parts: &[&[u8]]| {
        let mut context = ring::digest::Context::new(&ring::digest::SHA256);
        for part in parts {
            context.update(part);
        }
        hex(context.finish().as_ref())
    };
    let (mut service, groups, mut manifest) = prefilled_log(100);
    let mut durable = Vec::new();
    for _ in 0..40 {
        let pending = begin_append(&mut service, &groups, &manifest, |_| {});
        durable.extend(pending.durable.clone());
        finish_append(&mut service, pending, &mut manifest, false).unwrap();
    }
    let durable_digest = sha(&durable.iter().map(Vec::as_slice).collect::<Vec<_>>());
    let image_digest = sha(&[&service.object_manifests.to_bytes().unwrap()]);
    assert_eq!(durable_digest, "3547898c44502f48dfc78140af761e271ac183fb9afdca448816df958616ffd6", "the outcomes made durable changed");
    assert_eq!(image_digest, "582f02ab0853f46a4de2905aa2a1e581adea70bf82fc21ee54159e464c8f40ac", "the catalog image changed");
}

/// An append that names a page the object already holds is refused when it is prepared, by the
/// check of the new extents against the stored ones, and nothing is stored.
#[test]
fn an_append_naming_a_stored_page_again_is_refused_before_anything_is_published() {
    let (mut service, groups, manifest) = prefilled_log(50);
    let image = service.object_manifests.to_bytes().unwrap();
    let stored = manifest.extents[7].clone();
    let refused = try_begin_append(&mut service, &groups, &manifest, |extent| {
        extent.volume_id = stored.volume_id;
        extent.chunk_id = stored.chunk_id;
        extent.page_index = stored.page_index;
    });
    let error = match refused {
        Err(error) => error,
        Ok(_) => panic!("an append that repeats a stored page was prepared"),
    };
    assert!(error.to_string().contains("extent ordering, identity or bounds"), "{error}");
    // The reservation was applied before the refusal; the object itself is untouched.
    let after = service.object_manifests.to_bytes().unwrap();
    assert_eq!(
        service.object_manifests.get_manifest(&crate::ObjectId::new("bucket", "append")),
        Some(manifest.clone())
    );
    let _ = (image, after);
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
