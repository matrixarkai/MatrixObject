// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
mod unified_log_writer_command_tests {
    use super::*;
    use crate::chunk_catalog::ChunkOutcome;
    use crate::chunk_manifest::{
        ChunkManifest, CHUNK_KIND_LOG, CHUNK_KIND_PAGED, CHUNK_MANIFEST_FORMAT,
    };
    use crate::cluster::log_chunk_commands::{
        self as lcc, Decision, LogChunkReply, LogChunkRequest,
    };
    use crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS;
    const NOW: u64 = 5_000_000;
    const TTL: u64 = 30_000;
    fn setup() -> (MatrixMetaService, ChunkGroupCatalog) {
        let (mut service, groups) = fixture(1, 0);
        service.cluster_features.raised = FEATURE_UNIFIED_LOG_WRITERS;
        (service, groups)
    }
    fn grant(key: &str, holder: &str) -> LogChunkRequest {
        LogChunkRequest::grant("bucket", key, holder, TTL, "group-0").unified()
    }
    fn answer(reply: MetaReply) -> LogChunkReply {
        match reply {
            MetaReply::LogChunk(r) => r,
            other => panic!("unexpected {other:?}"),
        }
    }
    fn run(
        service: &mut MatrixMetaService,
        groups: &ChunkGroupCatalog,
        request: LogChunkRequest,
        now: u64,
    ) -> Result<(LogChunkReply, bool), ObjectError> {
        match lcc::prepare(service, groups, &request, now)? {
            Decision::Unchanged(reply) => Ok((answer(reply), false)),
            Decision::Change(outcome, reply) => {
                assert_eq!(outcome.version, 17);
                assert!(outcome.manifest.is_none());
                let payload = outcome.to_payload()?;
                PlacementOutcome::from_payload(&payload)?.apply(service)?;
                Ok((answer(reply), true))
            }
        }
    }
    fn state(s: &MatrixMetaService) -> (Vec<u8>, u64, String) {
        (
            s.export_snapshot().to_bytes().unwrap(),
            s.chunk_catalog.revision(),
            format!(
                "{:?}|{:?}|{:?}|{}",
                s.object_manifests, s.placements, s.nodes, s.placement_commit_index
            ),
        )
    }
    fn manifest(s: &MatrixMetaService, key: &str) -> ChunkManifest {
        (**s.chunk_catalog.get(&ObjectId::new("bucket", key)).unwrap()).clone()
    }
    fn switch(key: &str, epoch: u64, bytes: u64) -> LogChunkRequest {
        LogChunkRequest::switch(
            "bucket",
            key,
            "writer-a",
            epoch,
            "group-0",
            bytes,
            7,
            vec![3; 32],
        )
        .unified()
    }
    #[test]
    fn version_two_requests_and_typed_replies_round_trip_canonically() {
        let request = grant("log", "writer-a");
        assert_eq!(
            LogChunkRequest::from_payload(&request.encode_to_vec()).unwrap(),
            request
        );
        let (mut service, groups) = setup();
        let (reply, _) = run(&mut service, &groups, request.clone(), NOW).unwrap();
        assert_eq!(
            lcc::reply_from_payload(&reply.encode_to_vec()).unwrap(),
            reply
        );
        assert_eq!(reply.version, 2);
        assert!(reply.open.is_none());
        assert_eq!(reply.chunk_open.as_ref().unwrap().kind, CHUNK_KIND_LOG);
        println!(
            "UNIFIED_WRITER_REQUEST_HEX {}",
            request
                .encode_to_vec()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        println!(
            "UNIFIED_WRITER_REPLY_HEX {}",
            reply
                .encode_to_vec()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let mut padded = request.encode_to_vec();
        padded.extend_from_slice(&[0xf8, 0x06, 1]);
        assert!(LogChunkRequest::from_payload(&padded).is_err());
    }
    #[test]
    fn inactive_unified_commands_leave_both_catalogs_and_leases_untouched() {
        let (mut service, groups) = fixture(1, 0);
        let before = state(&service);
        assert!(run(&mut service, &groups, grant("log", "writer-a"), NOW).is_err());
        assert!(lcc::read(&service, &LogChunkRequest::read("bucket", "log").unified()).is_err());
        assert_eq!(state(&service), before);
    }
    #[test]
    fn grant_publishes_only_the_unified_catalog_with_its_lease_and_index() {
        let (mut service, groups) = setup();
        let index = service.placement_commit_index + 1;
        let (reply, changed) = run(&mut service, &groups, grant("log", "writer-a"), NOW).unwrap();
        assert!(changed);
        assert_eq!(service.placement_commit_index, index);
        assert_eq!(reply.lease.as_ref().unwrap().epoch, index);
        let m = manifest(&service, "log");
        assert_eq!(m.generation, 1);
        assert_eq!(m.page_bytes, 0);
        assert_eq!(m.length, 0);
        assert_eq!(
            (m.extents[0].volume_id, m.extents[0].chunk_id),
            crate::object_manifest::chunk_identity_of_index(index)
        );
        assert!(service.object_manifests.is_empty());
    }
    #[test]
    fn live_holder_retry_requires_no_entry_or_catalog_revision_change() {
        let (mut service, groups) = setup();
        let first = run(&mut service, &groups, grant("log", "writer-a"), NOW)
            .unwrap()
            .0;
        let before = state(&service);
        let (again, changed) =
            run(&mut service, &groups, grant("log", "writer-a"), NOW + 1).unwrap();
        assert!(!changed);
        assert_eq!(again, first);
        assert_eq!(state(&service), before);
    }
    #[test]
    fn expired_takeover_preserves_the_tail_arc_and_advances_its_epoch_only() {
        let (mut service, groups) = setup();
        run(&mut service, &groups, grant("log", "writer-a"), NOW).unwrap();
        let id = ObjectId::new("bucket", "log");
        let arc = service.chunk_catalog.entry(&id).unwrap();
        let revision = service.chunk_catalog.revision();
        let (reply, changed) = run(
            &mut service,
            &groups,
            grant("log", "writer-b"),
            NOW + TTL + 1,
        )
        .unwrap();
        assert!(changed);
        assert_eq!(reply.generation, 1);
        assert_eq!(reply.lease.unwrap().holder, "writer-b");
        assert_eq!(revision, service.chunk_catalog.revision());
        assert!(std::sync::Arc::ptr_eq(
            &arc,
            service.chunk_catalog.get(&id).unwrap()
        ));
    }
    #[test]
    fn switch_keeps_attributes_and_seals_the_tail_under_the_same_epoch() {
        let (mut service, groups) = setup();
        let first = run(&mut service, &groups, grant("log", "writer-a"), NOW)
            .unwrap()
            .0;
        let before = manifest(&service, "log");
        let mut after = before.clone();
        after.generation += 1;
        after.content_type = "application/binary".into();
        service
            .chunk_catalog
            .apply(&ChunkOutcome::decide(Some(before), after).unwrap())
            .unwrap();
        let epoch = first.lease.unwrap().epoch;
        let (reply, changed) =
            run(&mut service, &groups, switch("log", epoch, 9), NOW + 1).unwrap();
        assert!(changed);
        assert_eq!(reply.length, 9);
        assert_eq!(reply.lease.unwrap().epoch, epoch);
        let m = manifest(&service, "log");
        assert_eq!(m.content_type, "application/binary");
        assert_eq!(m.extents.len(), 2);
        assert_eq!(m.extents[0].length, 9);
        assert_eq!(m.extents[0].footer_crc, 7);
        assert_eq!(m.extents[0].footer_sha256, vec![3; 32]);
        assert_eq!(m.extents[1].object_offset, 9);
        assert_eq!(m.extents[1].length, 0);
    }
    #[test]
    fn rival_and_expired_switches_are_refused_before_any_mutation() {
        let (mut service, groups) = setup();
        let epoch = run(&mut service, &groups, grant("log", "writer-a"), NOW)
            .unwrap()
            .0
            .lease
            .unwrap()
            .epoch;
        let before = state(&service);
        assert!(run(&mut service, &groups, grant("log", "writer-b"), NOW + 1).is_err());
        let mut wrong = switch("log", epoch, 9);
        wrong.holder = "writer-b".into();
        assert!(run(&mut service, &groups, wrong, NOW + 1).is_err());
        assert!(run(
            &mut service,
            &groups,
            switch("log", epoch, 9),
            NOW + TTL + 1
        )
        .is_err());
        assert_eq!(state(&service), before);
    }
    #[test]
    fn unknown_groups_and_exhausted_indexes_do_not_create_objects() {
        let (mut service, groups) = setup();
        let before = state(&service);
        let mut request = grant("log", "writer-a");
        request.group = "missing".into();
        assert!(run(&mut service, &groups, request, NOW).is_err());
        assert_eq!(state(&service), before);
        service.placement_commit_index = u64::MAX;
        assert!(run(&mut service, &groups, grant("log", "writer-a"), NOW).is_err());
        assert!(service.chunk_catalog.is_empty());
        assert!(service.writer_leases.is_empty());
    }
    #[test]
    fn each_catalog_rejects_grants_to_objects_owned_by_the_other() {
        let (mut service, groups) = setup();
        service.cluster_features.raised |= crate::cluster_features::FEATURE_CHUNK_MANIFEST
            | crate::cluster_features::FEATURE_LOG_CHUNK_LEASES;
        let legacy = LogChunkRequest::grant("bucket", "older", "writer-a", TTL, "group-0");
        let Decision::Change(outcome, _) = lcc::prepare(&service, &groups, &legacy, NOW).unwrap()
        else {
            panic!("no grant")
        };
        outcome.apply(&mut service).unwrap();
        let before = state(&service);
        assert!(run(&mut service, &groups, grant("older", "writer-a"), NOW).is_err());
        assert_eq!(state(&service), before);
        run(&mut service, &groups, grant("newer", "writer-a"), NOW).unwrap();
        let before = state(&service);
        assert!(lcc::prepare(
            &service,
            &groups,
            &LogChunkRequest::grant("bucket", "newer", "writer-a", TTL, "group-0"),
            NOW
        )
        .is_err());
        assert!(lcc::read(&service, &LogChunkRequest::read("bucket", "newer")).is_err());
        assert_eq!(state(&service), before);
    }
    #[test]
    fn unified_tombstones_cannot_be_reopened_through_the_earlier_catalog() {
        let (mut service, groups) = setup();
        service.cluster_features.raised |= crate::cluster_features::FEATURE_CHUNK_MANIFEST
            | crate::cluster_features::FEATURE_LOG_CHUNK_LEASES;
        run(&mut service, &groups, grant("log", "writer-a"), NOW).unwrap();
        let prior = manifest(&service, "log");
        let mut tombstone = prior.clone();
        tombstone.generation += 1;
        tombstone.deleted = true;
        tombstone.length = 0;
        tombstone.extents.clear();
        tombstone.kind = 0;
        service
            .chunk_catalog
            .apply(&ChunkOutcome::decide(Some(prior), tombstone).unwrap())
            .unwrap();
        let before = state(&service);
        assert!(lcc::prepare(
            &service,
            &groups,
            &LogChunkRequest::grant("bucket", "log", "writer-a", TTL, "group-0"),
            NOW
        )
        .is_err());
        assert_eq!(state(&service), before);
        let (reply, _) = run(&mut service, &groups, grant("log", "writer-a"), NOW + 1).unwrap();
        assert_eq!(reply.generation, 3);
        assert!(service.object_manifests.is_empty());
    }
    #[test]
    fn paged_objects_cannot_be_leased_as_logs() {
        let (mut service, groups) = setup();
        let m = ChunkManifest {
            format_version: CHUNK_MANIFEST_FORMAT,
            bucket: "bucket".into(),
            key: "paged".into(),
            generation: 1,
            kind: CHUNK_KIND_PAGED,
            page_bytes: 4096,
            ..Default::default()
        };
        service
            .chunk_catalog
            .apply(&ChunkOutcome::decide(None, m).unwrap())
            .unwrap();
        let before = state(&service);
        assert!(run(&mut service, &groups, grant("paged", "writer-a"), NOW).is_err());
        assert!(lcc::read(
            &service,
            &LogChunkRequest::read("bucket", "paged").unified()
        )
        .is_err());
        assert_eq!(state(&service), before);
    }
    #[test]
    fn typed_replies_cannot_mix_catalogs_and_renewals_keep_the_shared_protocol() {
        let (mut service, groups) = setup();
        let reply = run(&mut service, &groups, grant("log", "writer-a"), NOW)
            .unwrap()
            .0;
        let epoch = reply.lease.as_ref().unwrap().epoch;
        let mut mixed = reply.clone();
        mixed.version = 1;
        assert!(lcc::reply_from_payload(&mixed.encode_to_vec()).is_err());
        let mut mixed = reply;
        mixed.open = Some(crate::object_manifest::ObjectExtent::default());
        assert!(lcc::reply_from_payload(&mixed.encode_to_vec()).is_err());
        let renewal = LogChunkRequest::renew(
            "writer-a",
            TTL,
            vec![crate::log_chunk_outcome::LeaseRef {
                bucket: "bucket".into(),
                key: "log".into(),
                epoch,
            }],
        );
        assert!(renewal.clone().unified().validate().is_err());
        let revision = service.chunk_catalog.revision();
        let Decision::Change(outcome, _) =
            lcc::prepare(&service, &groups, &renewal, NOW + 100).unwrap()
        else {
            panic!("no renewal")
        };
        assert_eq!(outcome.version, 16);
        outcome.apply(&mut service).unwrap();
        assert_eq!(service.chunk_catalog.revision(), revision);
        assert_eq!(
            answer(lcc::read(&service, &LogChunkRequest::read("bucket", "log").unified()).unwrap())
                .lease
                .unwrap()
                .expires_at_ms,
            NOW + 100 + TTL
        );
    }
    fn queue(requests: Vec<LogChunkRequest>) -> VecDeque<Request> {
        requests
            .into_iter()
            .map(|command| {
                let (reply, _receiver) = std::sync::mpsc::sync_channel(1);
                Request {
                    request: MetaRequest::LogChunk(command),
                    reply,
                    progress: RequestProgress::new(false),
                    deadline: Instant::now() + Duration::from_secs(60),
                }
            })
            .collect()
    }
    fn membership() -> matrixraft::peer_runtime::MembershipState {
        matrixraft::peer_runtime::MembershipState {
            voters: vec![1],
            outgoing_voters: vec![],
            learners: vec![],
            next_learners: vec![],
            auto_leave: false,
        }
    }
    fn commit(service: &mut MatrixMetaService, payloads: &[Vec<u8>]) {
        for payload in payloads {
            let entry = Entry::decode(payload.as_slice()).unwrap();
            PlacementOutcome::from_payload(&entry.outcome)
                .unwrap()
                .apply(service)
                .unwrap();
        }
    }
    #[test]
    fn batched_grants_and_switch_restore_the_live_catalog_before_commit() {
        let (mut leader, groups) = setup();
        let mut follower = leader.clone();
        let before = state(&leader);
        let epoch = leader.placement_commit_index + 1;
        let mut requests = queue(vec![
            grant("a", "writer-a"),
            grant("b", "writer-a"),
            switch("a", epoch, 9),
        ]);
        let (payloads, prepared) = prepare_batch(
            &mut leader,
            &groups,
            1 << 20,
            &mut requests,
            None,
            &membership(),
        )
        .unwrap();
        assert!(requests.is_empty());
        assert_eq!(payloads.len(), 3);
        assert!(prepared
            .iter()
            .all(|(_, r)| matches!(r, Some(Ok(MetaReply::LogChunk(_))))));
        assert_eq!(state(&leader), before);
        commit(&mut leader, &payloads);
        commit(&mut follower, &payloads);
        assert_eq!(state(&leader), state(&follower));
        assert_eq!(manifest(&leader, "a").extents.len(), 2);
        assert_eq!(manifest(&leader, "b").extents.len(), 1);
    }
    #[test]
    fn a_failed_draft_apply_restores_the_catalog_revision_and_keeps_the_prepared_prefix() {
        let (mut leader, groups) = setup();
        let mut follower = leader.clone();
        let before = state(&leader);
        let mut requests = queue(vec![grant("a", "writer-a"), grant("b", "writer-a")]);
        APPLY_FAULT.with(|fault| fault.set(Some(1)));
        let result = prepare_batch(
            &mut leader,
            &groups,
            1 << 20,
            &mut requests,
            None,
            &membership(),
        );
        APPLY_FAULT.with(|fault| fault.set(None));
        let (payloads, _) = result.unwrap();
        assert_eq!(payloads.len(), 1);
        assert_eq!(state(&leader), before);
        commit(&mut leader, &payloads);
        commit(&mut follower, &payloads);
        assert_eq!(state(&leader), state(&follower));
        assert!(leader
            .chunk_catalog
            .get(&ObjectId::new("bucket", "a"))
            .is_some());
        assert!(leader
            .chunk_catalog
            .get(&ObjectId::new("bucket", "b"))
            .is_none());
    }
}
