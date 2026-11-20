// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// The leader's proposal pipeline as it really runs: each prepared outcome is applied to a draft of
// the live service so the next request sees the prefix, then the draft is dropped to restore the
// live service, and only then is the entry committed (applied once more, by the leader and by every
// follower). A test that applies outcomes to a plain service never sees a field the draft forgets to
// restore: the commit-apply then fails on a service that already holds the change.
mod log_chunk_pipeline_tests {
    use super::*;
    use crate::cluster::log_chunk_commands::LogChunkRequest;
    use crate::cluster::FeaturesRequest;
    use crate::log_chunk_outcome::{LeaseRef, WriterLease};

    /// Everything the service holds that a snapshot writes, plus the fields a test can name that a
    /// snapshot does not carry. The snapshot half is what the existing preparation tests compare;
    /// the second half is what they did not (features, leases, manifests, placements, nodes).
    fn full_state(service: &MatrixMetaService) -> (Vec<u8>, String, String) {
        (
            service.export_snapshot().to_bytes().unwrap(),
            service.placement_state_fingerprint(),
            format!(
                "{:?}|{:?}|{:?}|{}|{:?}|{:?}",
                service.object_manifests,
                service.nodes,
                service.placements,
                service.placement_commit_index,
                service.cluster_features,
                service.writer_leases,
            ),
        )
    }
    fn queue_of(requests: Vec<MetaRequest>) -> VecDeque<Request> {
        requests
            .into_iter()
            .map(|request| {
                let (reply, _kept) = std::sync::mpsc::sync_channel(1);
                Request {
                    progress: RequestProgress::new(false),
                    request,
                    reply,
                    deadline: Instant::now() + Duration::from_secs(60),
                }
            })
            .collect()
    }
    /// Apply the payloads of a batch as the state machine applies a committed entry.
    fn commit(service: &mut MatrixMetaService, payloads: &[Vec<u8>]) {
        for payload in payloads {
            let entry = Entry::decode(payload.as_slice()).unwrap();
            if entry.outcome.is_empty() {
                continue;
            }
            PlacementOutcome::from_payload(&entry.outcome).unwrap().apply(service).unwrap();
        }
    }
    /// One batch through the real pipeline: the live service must be exactly as it was after the
    /// draft is dropped, and the committed entries must then apply on the leader and on a follower
    /// that never saw a draft, leaving them identical.
    fn run_batch(
        name: &str,
        leader: &mut MatrixMetaService,
        follower: &mut MatrixMetaService,
        groups: &ChunkGroupCatalog,
        requests: Vec<MetaRequest>,
        expect_ok: bool,
    ) {
        let before = full_state(leader);
        let mut queue = queue_of(requests);
        let (payloads, prepared) = prepare_batch(leader, groups, 1 << 20, &mut queue, None, &matrixraft::peer_runtime::MembershipState { voters: vec![1], outgoing_voters: vec![], learners: vec![], next_learners: vec![], auto_leave: false }).unwrap();
        assert!(queue.is_empty(), "{name}: the batch did not take every request");
        assert_eq!(full_state(leader), before, "{name}: the draft left something behind in the live service");
        for (_, reply) in &prepared {
            match reply {
                Some(Ok(_)) => assert!(expect_ok, "{name}: a request that should be refused was prepared"),
                Some(Err(error)) => assert!(!expect_ok, "{name}: {error:?}"),
                None => {}
            }
        }
        commit(leader, &payloads);
        commit(follower, &payloads);
        assert_eq!(full_state(leader), full_state(follower), "{name}: the leader and a follower diverge");
        if expect_ok {
            assert!(!payloads.is_empty() || name.contains("unchanged"), "{name}: nothing was proposed");
        } else {
            assert!(payloads.iter().all(|payload| Entry::decode(payload.as_slice()).unwrap().outcome.is_empty()), "{name}: a refused request proposed a change");
        }
    }
    fn lease_ref(service: &MatrixMetaService, key: &str) -> LeaseRef {
        LeaseRef { bucket: "bucket".into(), key: key.into(), epoch: service.writer_leases.get("bucket", key).unwrap().epoch }
    }

    #[test]
    fn every_outcome_kind_survives_the_leaders_draft_and_the_commit_that_follows() {
        let (mut leader, groups) = fixture(1, 0);
        let mut follower = leader.clone();
        let both = crate::cluster_features::FEATURE_CHUNK_MANIFEST | crate::cluster_features::FEATURE_LOG_CHUNK_LEASES;
        let grant = |key: &str, holder: &str| MetaRequest::LogChunk(LogChunkRequest::grant("bucket", key, holder, 30_000, "group-0"));

        // the feature set: declare, then raise, the second seeing the first through the draft
        run_batch(
            "features: declare and raise",
            &mut leader,
            &mut follower,
            &groups,
            vec![
                MetaRequest::Features(FeaturesRequest::declare(1, crate::cluster_features::SUPPORTED_FEATURES)),
                MetaRequest::Features(FeaturesRequest::raise(both, vec![1])),
            ],
            true,
        );
        assert!(leader.cluster_features.has(both) && follower.cluster_features.has(both));

        // two grants (a new object each: manifest, placement, capacity, lease) and a page allocation
        run_batch(
            "grants and an allocation",
            &mut leader,
            &mut follower,
            &groups,
            vec![grant("o1", "gw-a"), grant("o2", "gw-a"), MetaRequest::AllocateObjectPages { page_count: 3 }],
            true,
        );
        assert_eq!(leader.writer_leases.len(), 2);

        // a retried grant is decided to change nothing; a grant of a third object follows it
        run_batch(
            "grant unchanged then a third object",
            &mut leader,
            &mut follower,
            &groups,
            vec![grant("o1", "gw-a"), grant("o3", "gw-b")],
            true,
        );

        // switches see the leases the grants made; one renewal then renews them all
        let (e1, e2) = (lease_ref(&leader, "o1"), lease_ref(&leader, "o2"));
        let switch = |key: &str, epoch: u64, holder: &str| {
            MetaRequest::LogChunk(LogChunkRequest::switch("bucket", key, holder, epoch, "group-0", 4096, 9, Vec::new()))
        };
        run_batch(
            "switches and a batched renewal",
            &mut leader,
            &mut follower,
            &groups,
            vec![
                switch("o1", e1.epoch, "gw-a"),
                switch("o2", e2.epoch, "gw-a"),
                switch("o1", e1.epoch, "gw-a"),
                MetaRequest::LogChunk(LogChunkRequest::renew("gw-a", 60_000, vec![e1.clone(), e2.clone()])),
            ],
            true,
        );

        // a renewal alone: nothing else in the batch names those objects, so the draft saves their
        // leases only because the renewal's own entries were collected
        run_batch(
            "a renewal alone",
            &mut leader,
            &mut follower,
            &groups,
            vec![MetaRequest::LogChunk(LogChunkRequest::renew("gw-a", 90_000, vec![e1.clone(), e2.clone()]))],
            true,
        );

        // an expired lease is dropped by a sweep and taken over by another writer in one batch
        for service in [&mut leader, &mut follower] {
            service.writer_leases.set("bucket", "o2", &WriterLease { holder: "gw-a".into(), epoch: e2.epoch, expires_at_ms: 1 });
            service.writer_leases.set("bucket", "o1", &WriterLease { holder: "gw-a".into(), epoch: e1.epoch, expires_at_ms: 1 });
        }
        let swept = LeaseRef { bucket: "bucket".into(), key: "o2".into(), epoch: e2.epoch };
        run_batch(
            "a sweep and a takeover",
            &mut leader,
            &mut follower,
            &groups,
            vec![MetaRequest::LogChunk(LogChunkRequest::sweep(vec![swept])), grant("o1", "gw-b")],
            true,
        );
        assert!(leader.writer_leases.get("bucket", "o2").is_none());
        assert_eq!(leader.writer_leases.get("bucket", "o1").unwrap().holder, "gw-b");

        // refused requests change nothing at all, in the draft or after it
        run_batch(
            "refusals",
            &mut leader,
            &mut follower,
            &groups,
            vec![switch("o1", 1, "gw-b"), grant("o1", "gw-c"), MetaRequest::LogChunk(LogChunkRequest::grant("bucket", "o8", "gw-b", 30_000, "no-such-group"))],
            false,
        );
    }

    #[test]
    fn a_switch_into_another_group_restores_the_capacity_of_the_group_it_leaves() {
        // two groups: the object holds a sealed chunk in group-0 and its next chunk opens in group-1.
        // The capacity the sealed bytes charge group-0 is saved only because the draft names the
        // groups of the placement the object already has; the request names group-1 alone.
        let (mut leader, groups) = fixture(2, 0);
        let mut follower = leader.clone();
        let both = crate::cluster_features::FEATURE_CHUNK_MANIFEST | crate::cluster_features::FEATURE_LOG_CHUNK_LEASES;
        run_batch(
            "features",
            &mut leader,
            &mut follower,
            &groups,
            vec![
                MetaRequest::Features(FeaturesRequest::declare(1, crate::cluster_features::SUPPORTED_FEATURES)),
                MetaRequest::Features(FeaturesRequest::raise(both, vec![1])),
            ],
            true,
        );
        run_batch(
            "a grant in group-0",
            &mut leader,
            &mut follower,
            &groups,
            vec![MetaRequest::LogChunk(LogChunkRequest::grant("bucket", "move", "gw-a", 30_000, "group-0"))],
            true,
        );
        let epoch = leader.writer_leases.get("bucket", "move").unwrap().epoch;
        run_batch(
            "a switch into group-1",
            &mut leader,
            &mut follower,
            &groups,
            vec![MetaRequest::LogChunk(LogChunkRequest::switch("bucket", "move", "gw-a", epoch, "group-1", 4096, 9, Vec::new()))],
            true,
        );
        let charged = |service: &MatrixMetaService, node: &str| service.nodes[node].committed_bytes;
        assert!(charged(&leader, "group-0") > 0, "the sealed chunk charges the group it lives in");
        assert_eq!(charged(&leader, "group-1"), 0);
    }
}
