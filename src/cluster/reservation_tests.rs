// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Publishing an append and reserving the next append's allocation in one durable record.
// Included beside `compact_append_command_tests.rs`, whose fixture it uses.

/// Build and apply the compact append that publishes one new extent on `allocation`, asking the
/// metadata plane to reserve the next allocation in the same commit when `reserve` is set.
/// Returns the reservation. The manifest is the caller's view of the object.
fn publish_on(
    service: &mut MatrixMetaService,
    groups: &ChunkGroupCatalog,
    manifest: &mut crate::object_manifest::ObjectManifest,
    allocation: &crate::staged_allocation::StagedAllocation,
    reserve: bool,
) -> Option<crate::staged_allocation::StagedAllocation> {
    let (command, after) = build_append(service, manifest, allocation, reserve);
    let (outcome, reply) =
        super::super::staged_allocation_commands::prepare_at(service, groups, &command, 8192, 1500).unwrap();
    outcome.apply(service).unwrap();
    *manifest = after;
    match reply {
        MetaReply::StagedAllocation(reply) => reply.allocation,
        _ => panic!("unexpected reply"),
    }
}

/// The command that appends one extent on `allocation`, and the manifest it results in.
fn build_append(
    service: &MatrixMetaService,
    manifest: &crate::object_manifest::ObjectManifest,
    allocation: &crate::staged_allocation::StagedAllocation,
    reserve: bool,
) -> (crate::staged_allocation::StagedAllocationRequest, crate::object_manifest::ObjectManifest) {
    use crate::{object_manifest::ObjectExtent, staged_allocation::StagedAllocationRequest};
    let page = allocation.allocation().unwrap().page(0).unwrap();
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
    let command = if reserve {
        StagedAllocationRequest {
            version: 5,
            action: 12,
            allocation_id: allocation.allocation_id,
            manifest_append: Some(append),
            page_count: 1,
            ttl_ms: 60_000,
            ..Default::default()
        }
    } else {
        StagedAllocationRequest {
            version: 4,
            action: 11,
            allocation_id: allocation.allocation_id,
            manifest_append: Some(append),
            ..Default::default()
        }
    };
    (command, after)
}

fn reserving_fixture() -> (
    MatrixMetaService,
    ChunkGroupCatalog,
    crate::staged_allocation::StagedAllocationRequest,
) {
    let (service, groups, mut command) = compact_append_fixture(1000);
    command.version = 5;
    command.action = 12;
    command.page_count = 1;
    command.ttl_ms = 60_000;
    (service, groups, command)
}

#[test]
fn one_record_publishes_an_append_consumes_its_allocation_and_reserves_the_next() {
    let (mut service, groups, command) = reserving_fixture();
    let consumed = command.allocation_id;
    let before_index = service.placement_commit_index;
    let (outcome, reply) =
        super::super::staged_allocation_commands::prepare_at(&service, &groups, &command, 8192, 1500).unwrap();
    assert_eq!(outcome.version, 13);
    assert!(outcome.manifest.is_none() && outcome.manifest_append.is_some());
    assert_eq!(outcome.after_index, before_index + 1, "one metadata position for both changes");
    let reserved = outcome.reserved.clone().unwrap();
    assert_eq!(reserved.allocation_id, outcome.after_index);
    assert_eq!(reserved.expires_at_ms, 1500 + 60_000);
    let MetaReply::StagedAllocation(reply) = reply else { panic!() };
    assert_eq!(reply.version, 5);
    assert_eq!(reply.allocation.as_ref(), Some(&reserved));
    assert_eq!(
        crate::staged_allocation::StagedAllocationReply::from_payload(&reply.encode_to_vec()).unwrap(),
        reply
    );
    assert!(outcome.to_payload().unwrap().len() < 1024, "the record carries a decision, not manifests");
    outcome.apply(&mut service).unwrap();
    // both changes landed together
    assert!(service.object_manifests.staged_allocation(consumed).is_none());
    assert_eq!(service.object_manifests.staged_allocation(reserved.allocation_id), Some(&reserved));
    let published = service.object_manifests.get_manifest(&crate::ObjectId::new("bucket", "append")).unwrap();
    assert_eq!(published.generation, 2);
    assert_eq!(reserved.expected_generation, published.generation);
}

#[test]
fn replaying_the_single_record_reproduces_the_live_state_and_a_torn_record_leaves_the_old_one() {
    let (mut live, groups, command) = reserving_fixture();
    let (mut replica, _, _) = reserving_fixture();
    let (untouched, _, _) = reserving_fixture();
    let before = untouched.export_snapshot().to_bytes().unwrap();
    let (outcome, _) =
        super::super::staged_allocation_commands::prepare_at(&live, &groups, &command, 8192, 1500).unwrap();
    let record = outcome.to_payload().unwrap();
    // The record is the unit of durability. A crash before it is durable leaves the old state:
    // the consumed allocation active, nothing published, nothing reserved.
    assert_eq!(replica.export_snapshot().to_bytes().unwrap(), before);
    assert!(replica.object_manifests.staged_allocation(command.allocation_id).is_some());
    // A torn or damaged record is refused whole, never half applied.
    for cut in [1usize, 8, record.len() / 2, record.len() - 1] {
        assert!(PlacementOutcome::from_payload(&record[..record.len() - cut]).is_err(), "cut {cut}");
    }
    let mut damaged = record.clone();
    let middle = damaged.len() / 2;
    damaged[middle] ^= 1;
    assert!(PlacementOutcome::from_payload(&damaged).is_err());
    assert_eq!(replica.export_snapshot().to_bytes().unwrap(), before);
    // A durable record replays to exactly the state the live node reached, both changes together.
    outcome.apply(&mut live).unwrap();
    PlacementOutcome::from_payload(&record).unwrap().apply(&mut replica).unwrap();
    assert_eq!(live.export_snapshot().to_bytes().unwrap(), replica.export_snapshot().to_bytes().unwrap());
    assert!(replica.object_manifests.staged_allocation(command.allocation_id).is_none());
    assert!(replica.object_manifests.staged_allocation(outcome.after_index).is_some());
}

#[test]
fn a_publication_cannot_reserve_without_consuming_or_reserve_for_the_wrong_object_or_position() {
    let (service, groups, command) = reserving_fixture();
    let saved = service.export_snapshot().to_bytes().unwrap();
    let (good, _) =
        super::super::staged_allocation_commands::prepare_at(&service, &groups, &command, 8192, 1500).unwrap();
    let mut tampered: Vec<(&str, PlacementOutcome)> = Vec::new();
    let mut outcome = good.clone();
    outcome.staging = None;
    tampered.push(("publication without consuming its allocation", outcome));
    let mut outcome = good.clone();
    outcome.reserved = None;
    tampered.push(("version 13 without a reservation", outcome));
    let mut outcome = good.clone();
    outcome.version = 12;
    tampered.push(("a reservation in a version 12 record", outcome));
    let mut outcome = good.clone();
    outcome.reserved.as_mut().unwrap().allocation_id += 1;
    tampered.push(("a reservation at another position", outcome));
    let mut outcome = good.clone();
    outcome.reserved.as_mut().unwrap().expected_generation = 1;
    tampered.push(("a reservation for the generation already published", outcome));
    let mut outcome = good.clone();
    outcome.reserved.as_mut().unwrap().key = "other".into();
    tampered.push(("a reservation for another object", outcome));
    let mut outcome = good.clone();
    outcome.reserved.as_mut().unwrap().append = false;
    tampered.push(("a reservation that is not an append", outcome));
    for (name, outcome) in tampered {
        let mut copy = service.clone();
        assert!(outcome.apply(&mut copy).is_err(), "{name}");
        assert_eq!(copy.export_snapshot().to_bytes().unwrap(), saved, "{name} changed state");
    }
    // The allocation reserved for the next append is for the generation just published: it
    // cannot publish over the manifest it was reserved after.
    let old = service.object_manifests.get_manifest(&crate::ObjectId::new("bucket", "append")).unwrap();
    let mut newer = old.clone();
    newer.generation += 1;
    let reserved = good.reserved.clone().unwrap();
    let stale = crate::object_manifest::ManifestOutcome::decide(Some(old), newer).unwrap();
    assert!(reserved.check_publication(&stale).is_err());
}

#[test]
fn reserved_allocations_chain_through_many_appends_and_survive_a_snapshot() {
    let (mut service, groups, command) = compact_append_fixture(1000);
    let object_id = crate::ObjectId::new("bucket", "append");
    let mut manifest = service.object_manifests.get_manifest(&object_id).unwrap();
    // the fixture's allocation is already staged for generation 1
    let mut allocation = service.object_manifests.staged_allocation(command.allocation_id).unwrap().clone();
    for step in 0..40usize {
        let reserved = publish_on(&mut service, &groups, &mut manifest, &allocation, true)
            .unwrap_or_else(|| panic!("append {step} returned no reservation"));
        assert_eq!(reserved.expected_generation, manifest.generation);
        assert_eq!(reserved.allocation_id, service.placement_commit_index);
        allocation = reserved;
    }
    assert_eq!(manifest.extents.len(), 40);
    // exactly the reservation of the last append is active
    assert_eq!(service.object_manifests.staged_allocation(allocation.allocation_id), Some(&allocation));
    let restored =
        crate::object_manifest::ManifestCatalog::from_bytes(&service.object_manifests.to_bytes().unwrap()).unwrap();
    assert_eq!(restored.staged_allocation(allocation.allocation_id), Some(&allocation));
    // an append that does not ask for a reservation consumes it and leaves none
    assert!(publish_on(&mut service, &groups, &mut manifest, &allocation, false).is_none());
    assert!(service.object_manifests.staged_allocation(allocation.allocation_id).is_none());
}

#[test]
fn a_full_table_still_takes_a_publication_and_frees_its_slot() {
    use crate::staged_allocation::StagedAllocationRequest;
    let (mut service, groups, command) = reserving_fixture();
    let allocate = |service: &mut MatrixMetaService, key: &str| -> Result<(), ObjectError> {
        let (outcome, _) = super::super::staged_allocation_commands::prepare_at(
            service,
            &groups,
            &StagedAllocationRequest {
                version: 2,
                action: 1,
                bucket: "bucket".into(),
                key: key.into(),
                page_count: 1,
                ttl_ms: 600_000,
                ..Default::default()
            },
            8192,
            1500,
        )?;
        outcome.apply(service)
    };
    // fill the metadata plane's allocation table: the fixture's own allocation plus 127 more
    for index in 0..127usize {
        allocate(&mut service, &format!("filler-{index}")).unwrap();
    }
    assert!(allocate(&mut service, "one-too-many").is_err(), "the table is full");
    let (outcome, _) =
        super::super::staged_allocation_commands::prepare_at(&service, &groups, &command, 8192, 1500).unwrap();
    // At a full table the append is published without a reservation, so the slot its consumed
    // allocation held is free for another object.
    assert!(outcome.reserved.is_none());
    outcome.apply(&mut service).unwrap();
    allocate(&mut service, "fits-now").unwrap();
    assert!(allocate(&mut service, "full-again").is_err(), "the freed slot is the only one");
}

/// Fill the table with allocations for other objects until it holds `total`.
fn fill_table_to(service: &mut MatrixMetaService, groups: &ChunkGroupCatalog, total: usize) {
    use crate::staged_allocation::StagedAllocationRequest;
    let mut index = 0;
    while service.object_manifests.staged_count() < total {
        let create = StagedAllocationRequest {
            version: 2,
            action: 1,
            page_count: 1,
            ttl_ms: 60_000,
            bucket: "bucket".into(),
            key: format!("other-{index}"),
            ..Default::default()
        };
        index += 1;
        let (outcome, _) =
            super::super::staged_allocation_commands::prepare_at(service, groups, &create, 8192, 1500).unwrap();
        outcome.apply(service).unwrap();
    }
}

#[test]
fn a_publication_reserves_only_while_the_table_has_room_for_other_objects() {
    use crate::staged_allocation::{MAX_STAGED_ALLOCATIONS, MAX_STAGED_FOR_RESERVATION};
    // At the limit the publication still reserves.
    let (mut service, groups, command) = reserving_fixture();
    fill_table_to(&mut service, &groups, MAX_STAGED_FOR_RESERVATION);
    let (outcome, reply) =
        super::super::staged_allocation_commands::prepare_at(&service, &groups, &command, 8192, 1500).unwrap();
    assert!(outcome.reserved.is_some());
    let MetaReply::StagedAllocation(reply) = reply else { panic!() };
    assert!(reply.allocation.is_some());
    // One past it the append is published and nothing is reserved: the allocation it consumed
    // is the only change to the table, so the table keeps room for other objects.
    let (mut service, groups, command) = reserving_fixture();
    fill_table_to(&mut service, &groups, MAX_STAGED_FOR_RESERVATION + 1);
    let held = service.object_manifests.staged_count();
    let (outcome, reply) =
        super::super::staged_allocation_commands::prepare_at(&service, &groups, &command, 8192, 1500).unwrap();
    assert!(outcome.reserved.is_none(), "no reservation past the limit");
    assert!(outcome.manifest_append.is_some(), "the append is still published");
    let MetaReply::StagedAllocation(reply) = reply else { panic!() };
    assert_eq!(reply.version, 5);
    assert!(reply.allocation.is_none());
    assert_eq!(
        crate::staged_allocation::StagedAllocationReply::from_payload(&reply.encode_to_vec()).unwrap(),
        reply,
        "a reply without a reservation is a valid acknowledgement"
    );
    outcome.apply(&mut service).unwrap();
    assert_eq!(service.object_manifests.staged_count(), held - 1);
    let published = service.object_manifests.get_manifest(&crate::ObjectId::new("bucket", "append")).unwrap();
    assert_eq!(published.generation, 2);
    // A table filled by reservations leaves room: even a full-size table is never what a
    // reservation fills, so another object's allocation is not refused for want of a slot.
    assert!(MAX_STAGED_FOR_RESERVATION + 1 < MAX_STAGED_ALLOCATIONS);
}

/// The placement record of an append is the prior record extended by the new extents, so once the
/// record has per-group bytes an append makes no pass over the object's extents for it: the count
/// of whole-extent-list passes does not move across 50 appends (it was several per append).
#[test]
fn appends_after_the_first_make_no_whole_extent_list_pass_for_the_placement_record() {
    let (mut service, groups, command) = reserving_fixture();
    let object_id = crate::ObjectId::new("bucket", "append");
    let mut manifest = service.object_manifests.get_manifest(&object_id).unwrap();
    let mut allocation = service.object_manifests.staged_allocation(command.allocation_id).unwrap().clone();
    // the first append gives the record its per-group bytes from the whole manifest
    for _ in 0..3 {
        allocation = publish_on(&mut service, &groups, &mut manifest, &allocation, true).unwrap();
    }
    let before = crate::placement_outcome::full_derivations();
    for _ in 0..50 {
        allocation = publish_on(&mut service, &groups, &mut manifest, &allocation, true).unwrap();
    }
    assert_eq!(manifest.extents.len(), 53);
    assert_eq!(
        crate::placement_outcome::full_derivations(),
        before,
        "50 appends made whole-extent-list passes for the placement record"
    );
    // and the record is exactly what the whole manifest gives
    let placement = service.placements.get(&object_id).unwrap();
    assert_eq!(placement.node_ids, vec!["group-0".to_string()]);
    assert_eq!(placement.group_bytes, vec![53]);
    assert_eq!(placement.version, manifest.generation);
    assert_eq!(placement.length, manifest.length);
}

/// A publication whose resulting placement names a group the suffix does not, or charges a group
/// differently than the prior record plus the suffix, or states another length, is refused.
#[test]
fn an_append_whose_placement_differs_from_the_prior_record_plus_its_suffix_is_refused() {
    let (mut service, groups, command) = reserving_fixture();
    let object_id = crate::ObjectId::new("bucket", "append");
    let mut manifest = service.object_manifests.get_manifest(&object_id).unwrap();
    let mut allocation = service.object_manifests.staged_allocation(command.allocation_id).unwrap().clone();
    for _ in 0..3 {
        allocation = publish_on(&mut service, &groups, &mut manifest, &allocation, true).unwrap();
    }
    let (next, _) = build_append(&service, &manifest, &allocation, true);
    let (good, _) =
        super::super::staged_allocation_commands::prepare_at(&service, &groups, &next, 8192, 1500).unwrap();
    // the untouched decision applies
    good.clone().apply(&mut service.clone()).unwrap();
    let tampers: [(&str, fn(&mut crate::placement_outcome::PlacementValue)); 3] = [
        ("a group the suffix does not name", |p| p.node_ids.push("group-9".into())),
        ("a different charge", |p| p.group_bytes[0] += 1),
        ("a different length", |p| p.length += 1),
    ];
    for (what, tamper) in tampers {
        let mut outcome = good.clone();
        tamper(outcome.placements[0].after.as_mut().unwrap());
        let refused = match PlacementOutcome::from_payload(&match outcome.to_payload() {
            Ok(payload) => payload,
            Err(_) => continue, // refused already when the record is encoded
        }) {
            Ok(decoded) => decoded.apply(&mut service.clone()).is_err(),
            Err(_) => true,
        };
        assert!(refused, "a placement with {what} was accepted");
    }
}
