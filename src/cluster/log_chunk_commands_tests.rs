// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// The leader's commands for log chunks: grant, switch, batched renewal, the read and the sweep.
mod log_chunk_commands_tests {
    use super::*;
    use crate::cluster::log_chunk_commands::{self as lcc, Decision, LogChunkReply, LogChunkRequest};
    use crate::log_chunk_outcome::{LeaseRef, WriterLease, MAX_LEASE_TTL_MS};
    use crate::object_manifest::{ObjectManifest, EXTENT_FLAG_SEALED, EXTENT_FLAG_TAIL};

    const NOW: u64 = 5_000_000;
    const TTL: u64 = 30_000;

    fn raise(service: &mut MatrixMetaService) {
        service.cluster_features = crate::cluster_features::ClusterFeatures {
            raised: crate::cluster_features::FEATURE_CHUNK_MANIFEST | crate::cluster_features::FEATURE_LOG_CHUNK_LEASES,
            declared: [(1, 0x17)].into(),
        };
    }
    fn stored(service: &MatrixMetaService, key: &str) -> Option<ObjectManifest> {
        service
            .object_manifests
            .get(&crate::ObjectId::new("bucket".to_string(), key.to_string()))
            .map(|stored| stored.to_manifest())
    }
    fn reply_of(reply: crate::cluster::MetaReply) -> LogChunkReply {
        match reply {
            crate::cluster::MetaReply::LogChunk(reply) => reply,
            other => panic!("unexpected reply {other:?}"),
        }
    }
    /// Decide `request` at `now`, and if it changes anything commit it the way the log does:
    /// encode, decode, apply. Returns the reply and whether an entry was committed.
    fn run(
        service: &mut MatrixMetaService,
        groups: &ChunkGroupCatalog,
        request: LogChunkRequest,
        now: u64,
    ) -> Result<(LogChunkReply, bool), ObjectError> {
        match lcc::prepare(service, groups, &request, now)? {
            Decision::Unchanged(reply) => Ok((reply_of(reply), false)),
            Decision::Change(outcome, reply) => {
                let bytes = outcome.to_payload()?;
                let read = PlacementOutcome::from_payload(&bytes)?;
                assert_eq!(read, outcome);
                read.apply(service)?;
                Ok((reply_of(reply), true))
            }
        }
    }
    fn grant(service: &mut MatrixMetaService, groups: &ChunkGroupCatalog, key: &str, holder: &str, now: u64) -> LogChunkReply {
        run(service, groups, LogChunkRequest::grant("bucket", key, holder, TTL, "group-0"), now).unwrap().0
    }
    fn lref(key: &str, epoch: u64) -> LeaseRef {
        LeaseRef { bucket: "bucket".into(), key: key.into(), epoch }
    }

    #[test]
    fn nothing_leases_or_emits_a_chunked_manifest_until_an_operator_raises_the_feature() {
        let (mut service, groups) = fixture(1, 0);
        assert_eq!(service.cluster_features.raised, 0, "the feature is never raised by default");
        let request = LogChunkRequest::grant("bucket", "log", "gateway-a", TTL, "group-0");
        let error = run(&mut service, &groups, request.clone(), NOW).unwrap_err();
        assert!(matches!(error, ObjectError::PreconditionFailed(_)), "{error:?}");
        assert!(service.writer_leases.is_empty());
        assert!(stored(&service, "log").is_none());
        let switch = LogChunkRequest::switch("bucket", "log", "gateway-a", 5, "group-0", 10, 0, Vec::new());
        assert!(run(&mut service, &groups, switch, NOW).is_err());
        // the general publication path still refuses a format 3 manifest, feature or not
        let chunked = ObjectManifest { format_version: 3, bucket: "bucket".into(), key: "x".into(), generation: 1, ..Default::default() };
        assert!(crate::object_manifest::ManifestOutcome::decide(None, chunked).is_err());
        // a build may read format 3 manifests and not the outcomes that make them: one bit is not enough
        service.cluster_features = crate::cluster_features::ClusterFeatures {
            raised: crate::cluster_features::FEATURE_CHUNK_MANIFEST,
            declared: [(1, 0x17)].into(),
        };
        assert!(run(&mut service, &groups, request.clone(), NOW).is_err());
        service.cluster_features.raised = crate::cluster_features::FEATURE_LOG_CHUNK_LEASES;
        assert!(run(&mut service, &groups, request.clone(), NOW).is_err());
        raise(&mut service);
        run(&mut service, &groups, request, NOW).unwrap();
        assert_eq!(service.writer_leases.len(), 1);
    }

    #[test]
    fn a_grant_opens_a_chunk_named_by_its_own_index_and_a_retry_costs_no_entry() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let index = service.placement_commit_index + 1;
        let (reply, committed) = run(&mut service, &groups, LogChunkRequest::grant("bucket", "log", "gateway-a", TTL, "group-0"), NOW).unwrap();
        assert!(committed);
        assert_eq!(reply.lease, Some(WriterLease { holder: "gateway-a".into(), epoch: index, expires_at_ms: NOW + TTL }));
        let open = reply.open.clone().unwrap();
        assert_eq!((open.volume_id, open.chunk_id), lcc::chunk_identity(index));
        assert_eq!((open.object_offset, open.length, open.write_sequence, open.flags), (0, 0, index, EXTENT_FLAG_TAIL));
        let manifest = stored(&service, "log").unwrap();
        assert_eq!((manifest.format_version, manifest.generation, manifest.length), (3, 1, 0));
        // the same writer asking again while its lease is live: the same answer, no entry
        let before = service.placement_commit_index;
        let (again, committed) = run(&mut service, &groups, LogChunkRequest::grant("bucket", "log", "gateway-a", TTL, "group-0"), NOW + 5).unwrap();
        assert!(!committed);
        assert_eq!(again.lease, reply.lease);
        assert_eq!(again.open, reply.open);
        assert_eq!(service.placement_commit_index, before);
        // two grants never name one chunk
        let other = grant(&mut service, &groups, "log2", "gateway-a", NOW);
        assert_ne!(other.open.unwrap().chunk_id, open.chunk_id);
        // a group nobody configured is refused
        let error = run(&mut service, &groups, LogChunkRequest::grant("bucket", "log3", "gateway-a", TTL, "group-9"), NOW).unwrap_err();
        assert!(matches!(error, ObjectError::Corruption(_) | ObjectError::InvalidArgument(_) | ObjectError::PreconditionFailed(_)));
        assert!(service.writer_leases.get("bucket", "log3").is_none());
    }

    #[test]
    fn a_live_lease_blocks_another_writer_and_an_expired_one_is_fenced_by_a_later_epoch() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let first = grant(&mut service, &groups, "log", "gateway-a", NOW).lease.unwrap();
        let rival = LogChunkRequest::grant("bucket", "log", "gateway-b", TTL, "group-0");
        assert!(run(&mut service, &groups, rival.clone(), NOW + 1).is_err(), "live lease of another writer");
        // after the term the rival takes over: a later epoch, no second chunk (one is open)
        let manifest_before = stored(&service, "log").unwrap();
        let (taken, committed) = run(&mut service, &groups, rival, NOW + TTL + 1).unwrap();
        assert!(committed);
        let second = taken.lease.unwrap();
        assert_eq!(second.holder, "gateway-b");
        assert!(second.epoch > first.epoch);
        assert_eq!(stored(&service, "log").unwrap(), manifest_before, "the chunk that was open stays open");
        // the old writer holds nothing now: its switch is refused
        let stale = LogChunkRequest::switch("bucket", "log", "gateway-a", first.epoch, "group-0", 10, 0, Vec::new());
        assert!(run(&mut service, &groups, stale, NOW + TTL + 2).is_err());
    }

    #[test]
    fn a_switch_seals_and_opens_the_next_in_one_entry_and_refuses_a_writer_without_the_live_lease() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let lease = grant(&mut service, &groups, "log", "gateway-a", NOW).lease.unwrap();
        let before = service.placement_commit_index;
        let request = LogChunkRequest::switch("bucket", "log", "gateway-a", lease.epoch, "group-0", 4096, 0xdead_beef, vec![7; 32]);
        let (reply, committed) = run(&mut service, &groups, request, NOW + 10).unwrap();
        assert!(committed);
        assert_eq!(service.placement_commit_index, before + 1, "one entry seals and opens");
        let manifest = stored(&service, "log").unwrap();
        assert_eq!((manifest.generation, manifest.length, manifest.extents.len()), (2, 4096, 2));
        let sealed = &manifest.extents[0];
        assert_eq!((sealed.length, sealed.footer_crc), (4096, 0xdead_beef));
        assert_eq!(sealed.flags & EXTENT_FLAG_SEALED, EXTENT_FLAG_SEALED);
        assert_eq!(sealed.footer_sha256, vec![7; 32]);
        let open = reply.open.unwrap();
        assert_eq!((open.volume_id, open.chunk_id), lcc::chunk_identity(before + 1));
        assert_eq!((open.object_offset, open.length), (4096, 0));
        // the lease is the same lease (same holder, same epoch) renewed by the switch itself: it
        // runs to now + ttl, which is later than it did
        let renewed = service.writer_leases.get("bucket", "log").unwrap();
        assert_eq!((renewed.holder.as_str(), renewed.epoch), (lease.holder.as_str(), lease.epoch));
        assert_eq!(renewed.expires_at_ms, NOW + 10 + TTL);
        assert!(renewed.expires_at_ms > lease.expires_at_ms);
        assert_eq!(reply.lease, Some(renewed));
        // not the holder, not the epoch, a lease that ran out, an object that is not leased
        for (holder, epoch, now, key) in [
            ("gateway-b", lease.epoch, NOW + 20, "log"),
            ("gateway-a", lease.epoch + 1, NOW + 20, "log"),
            ("gateway-a", lease.epoch, NOW + 10 + TTL + 1, "log"),
            ("gateway-a", lease.epoch, NOW + 20, "nothing"),
        ] {
            let attempt = LogChunkRequest::switch("bucket", key, holder, epoch, "group-0", 10, 0, Vec::new());
            // refused by the leader itself (a precondition), not only by the outcome reader behind it
            let error = run(&mut service, &groups, attempt, now).unwrap_err();
            assert!(matches!(error, ObjectError::PreconditionFailed(_)), "{holder} {epoch} {now} {key}: {error:?}");
        }
        assert_eq!(service.placement_commit_index, before + 1);
        // a switch that asks for a shorter term never shortens the lease
        let before_expiry = service.writer_leases.get("bucket", "log").unwrap().expires_at_ms;
        let short = LogChunkRequest::switch("bucket", "log", "gateway-a", lease.epoch, "group-0", 8, 0, Vec::new()).with_ttl(lcc::MIN_TTL_MS);
        run(&mut service, &groups, short, NOW + 11).unwrap();
        assert_eq!(service.writer_leases.get("bucket", "log").unwrap().expires_at_ms, before_expiry);
        // an empty seal is not a request
        assert!(LogChunkRequest::switch("bucket", "log", "gateway-a", 5, "group-0", 0, 0, Vec::new()).validate().is_err());
    }

    #[test]
    fn an_object_at_the_entry_cap_cannot_open_another_chunk_and_may_still_fill_the_open_one() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let lease = grant(&mut service, &groups, "log", "gateway-a", NOW).lease.unwrap();
        // one entry now; every switch seals one and opens one, so each adds one entry
        for i in 1..lcc::MAX_OBJECT_ENTRIES {
            let request = LogChunkRequest::switch("bucket", "log", "gateway-a", lease.epoch, "group-0", 1, 0, Vec::new());
            run(&mut service, &groups, request, NOW + i as u64 % 10).unwrap();
        }
        assert_eq!(stored(&service, "log").unwrap().extents.len(), lcc::MAX_OBJECT_ENTRIES);
        let before = service.placement_commit_index;
        let request = LogChunkRequest::switch("bucket", "log", "gateway-a", lease.epoch, "group-0", 1, 0, Vec::new());
        let error = run(&mut service, &groups, request, NOW + 1).unwrap_err();
        assert!(matches!(error, ObjectError::PreconditionFailed(ref message) if message.contains("most entries")), "{error:?}");
        assert_eq!(service.placement_commit_index, before);
        // the writer still holds a live lease and an open chunk: it may keep appending to it
        let (kept, committed) = run(&mut service, &groups, LogChunkRequest::grant("bucket", "log", "gateway-a", TTL, "group-0"), NOW + 2).unwrap();
        assert!(!committed);
        assert!(kept.open.is_some());
    }

    #[test]
    fn one_entry_renews_every_lease_a_writer_holds_and_reports_the_ones_it_does_not() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let a = grant(&mut service, &groups, "a", "gateway-a", NOW).lease.unwrap();
        let b = grant(&mut service, &groups, "b", "gateway-a", NOW).lease.unwrap();
        let c = grant(&mut service, &groups, "c", "gateway-a", NOW).lease.unwrap();
        let d = grant(&mut service, &groups, "d", "gateway-b", NOW).lease.unwrap();
        let before = service.placement_commit_index;
        let renewals = vec![lref("a", a.epoch), lref("b", b.epoch), lref("c", c.epoch + 1), lref("d", d.epoch), lref("zz", 3)];
        let (reply, committed) = run(&mut service, &groups, LogChunkRequest::renew("gateway-a", 2 * TTL, renewals), NOW + TTL / 2).unwrap();
        assert!(committed);
        assert_eq!(service.placement_commit_index, before + 1, "one entry for all of them");
        assert_eq!(reply.renewed, 2);
        assert_eq!(reply.lost, vec![lref("c", c.epoch + 1), lref("d", d.epoch), lref("zz", 3)]);
        let moved = NOW + TTL / 2 + 2 * TTL;
        assert_eq!(service.writer_leases.get("bucket", "a").unwrap(), WriterLease { expires_at_ms: moved, ..a.clone() });
        assert_eq!(service.writer_leases.get("bucket", "b").unwrap(), WriterLease { expires_at_ms: moved, ..b });
        // nobody else's lease moved, and a lease the writer does not hold stays as it was
        assert_eq!(service.writer_leases.get("bucket", "c").unwrap(), c);
        assert_eq!(service.writer_leases.get("bucket", "d").unwrap(), d);
        // a renewal that would shorten changes nothing and costs no entry
        let before = service.placement_commit_index;
        let (reply, committed) = run(&mut service, &groups, LogChunkRequest::renew("gateway-a", TTL, vec![lref("a", a.epoch)]), NOW + TTL / 2 + 1).unwrap();
        assert!(!committed);
        assert_eq!((reply.renewed, reply.lost.len()), (1, 0));
        // all lost: no entry
        let (reply, committed) = run(&mut service, &groups, LogChunkRequest::renew("gateway-a", TTL, vec![lref("d", d.epoch)]), NOW + TTL).unwrap();
        assert!(!committed);
        assert_eq!(reply.lost.len(), 1);
        assert_eq!(service.placement_commit_index, before);
    }

    #[test]
    fn the_sweep_drops_expired_leases_in_one_entry_and_a_later_grant_still_gets_a_later_epoch() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        assert!(lcc::expired(&service, NOW).is_empty(), "nothing leased, nothing to sweep");
        let a = grant(&mut service, &groups, "a", "gateway-a", NOW).lease.unwrap();
        grant(&mut service, &groups, "b", "gateway-a", NOW);
        grant(&mut service, &groups, "c", "gateway-a", NOW + TTL); // runs later than the others
        assert!(lcc::expired(&service, NOW + TTL).is_empty(), "none is over at the instant it ends");
        let commands = lcc::expired(&service, NOW + TTL + 1);
        assert_eq!(commands.len(), 1);
        let crate::cluster::MetaRequest::LogChunk(command) = commands[0].clone() else { panic!("not a log chunk command") };
        assert_eq!(command.drops.len(), 2, "a and b are over, c is not");
        let before = service.placement_commit_index;
        let (reply, committed) = run(&mut service, &groups, command.clone(), NOW + TTL + 1).unwrap();
        assert!(committed);
        assert_eq!(reply.dropped, 2);
        assert_eq!(service.placement_commit_index, before + 1);
        assert_eq!(service.writer_leases.len(), 1);
        assert!(service.writer_leases.get("bucket", "c").is_some());
        // a sweep that names a lease that is NOT over (c runs later) drops nothing and costs no entry
        let c = service.writer_leases.get("bucket", "c").unwrap();
        let live = LogChunkRequest::sweep(vec![lref("c", c.epoch)]);
        let before_live = service.placement_commit_index;
        let (reply, committed) = run(&mut service, &groups, live, NOW + TTL + 2).unwrap();
        assert!(!committed);
        assert_eq!(reply.dropped, 0);
        assert_eq!(service.placement_commit_index, before_live);
        assert!(service.writer_leases.get("bucket", "c").is_some());
        // a sweep of what is already gone, or whose lease changed since, is filtered out
        let (reply, committed) = run(&mut service, &groups, command, NOW + TTL + 2).unwrap();
        assert!(!committed);
        assert_eq!(reply.dropped, 0);
        // the object the sweep dropped is granted again: the epoch is the new index, later than before
        let again = grant(&mut service, &groups, "a", "gateway-b", NOW + TTL + 3).lease.unwrap();
        assert!(again.epoch > a.epoch);
    }

    #[test]
    fn a_read_reports_the_lease_and_the_open_chunk_and_changes_nothing() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let empty = run(&mut service, &groups, LogChunkRequest::read("bucket", "log"), NOW).unwrap();
        assert!(!empty.1);
        assert_eq!((empty.0.lease, empty.0.open), (None, None));
        let granted = grant(&mut service, &groups, "log", "gateway-a", NOW);
        let before = service.placement_commit_index;
        let (read, committed) = run(&mut service, &groups, LogChunkRequest::read("bucket", "log"), NOW + 1).unwrap();
        assert!(!committed);
        assert_eq!((read.lease, read.open, read.generation), (granted.lease, granted.open, 1));
        assert_eq!(service.placement_commit_index, before);
    }

    #[test]
    fn the_commands_round_trip_the_wire_and_malformed_ones_are_refused() {
        use prost::Message;
        let requests = [
            LogChunkRequest::grant("bucket", "log", "gateway-a", TTL, "group-0"),
            LogChunkRequest::switch("bucket", "log", "gateway-a", 9, "group-0", 4096, 7, vec![1; 32]),
            LogChunkRequest::renew("gateway-a", TTL, vec![lref("a", 3), lref("b", 4)]),
            LogChunkRequest::read("bucket", "log"),
            LogChunkRequest::sweep(vec![lref("a", 3)]),
        ];
        for request in &requests {
            let bytes = request.encode_to_vec();
            assert_eq!(&LogChunkRequest::from_payload(&bytes).unwrap(), request);
            let meta = crate::cluster::MetaRequest::LogChunk(request.clone());
            let wire = meta.encode_payload();
            let decoded = crate::cluster::MetaRequest::decode(crate::cluster::meta_wire::MetaRequestCode::LogChunk, &wire).unwrap();
            assert_eq!(decoded, meta);
            // an unknown trailing field is not canonical
            let mut padded = bytes.clone();
            padded.extend_from_slice(&[0xf8, 0x06, 0x01]);
            assert!(LogChunkRequest::from_payload(&padded).is_err());
        }
        let good = LogChunkRequest::grant("bucket", "log", "gateway-a", TTL, "group-0");
        let bad = [
            LogChunkRequest { version: 2, ..good.clone() },
            LogChunkRequest { action: 9, ..good.clone() },
            LogChunkRequest { ttl_ms: MIN_TTL_FOR_TESTS - 1, ..good.clone() },
            LogChunkRequest { ttl_ms: MAX_LEASE_TTL_MS + 1, ..good.clone() },
            LogChunkRequest { holder: String::new(), ..good.clone() },
            LogChunkRequest { holder: "has space".into(), ..good.clone() },
            LogChunkRequest { group: String::new(), ..good.clone() },
            LogChunkRequest { bucket: String::new(), ..good.clone() },
            LogChunkRequest { epoch: 4, ..good.clone() },
            LogChunkRequest { renewals: vec![lref("a", 3)], ..good.clone() },
            LogChunkRequest { footer_sha256: vec![1; 16], ..LogChunkRequest::switch("bucket", "log", "a", 9, "group-0", 5, 0, Vec::new()) },
            LogChunkRequest::renew("gateway-a", TTL, Vec::new()),
            LogChunkRequest::renew("gateway-a", TTL, vec![lref("b", 4), lref("a", 3)]),
            LogChunkRequest::renew("gateway-a", TTL, vec![lref("a", 0)]),
            LogChunkRequest { bucket: "bucket".into(), ..LogChunkRequest::renew("gateway-a", TTL, vec![lref("a", 3)]) },
            LogChunkRequest::sweep(Vec::new()),
            LogChunkRequest { holder: "gateway-a".into(), ..LogChunkRequest::sweep(vec![lref("a", 3)]) },
            LogChunkRequest { ttl_ms: TTL, ..LogChunkRequest::read("bucket", "log") },
        ];
        for request in bad {
            assert!(request.validate().is_err(), "{request:?}");
            assert!(LogChunkRequest::from_payload(&request.encode_to_vec()).is_err());
        }
        let reply = LogChunkReply { version: 1, renewed: 2, lost: vec![lref("a", 3)], ..Default::default() };
        assert_eq!(lcc::reply_from_payload(&reply.encode_to_vec()).unwrap(), reply);
        assert!(lcc::reply_from_payload(&LogChunkReply { version: 2, ..reply }.encode_to_vec()).is_err());
    }

    const MIN_TTL_FOR_TESTS: u64 = lcc::MIN_TTL_MS;

    #[test]
    fn the_dispatch_paths_reach_the_commands() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let apply = |service: &mut MatrixMetaService, request: LogChunkRequest| {
            crate::cluster::meta_server::apply_with_groups(service, crate::cluster::MetaRequest::LogChunk(request), 1 << 20, &groups)
        };
        let granted = reply_of(apply(&mut service, LogChunkRequest::grant("bucket", "log", "gateway-a", TTL, "group-0")).unwrap());
        let lease = granted.lease.unwrap();
        assert_eq!(service.writer_leases.get("bucket", "log"), Some(lease.clone()));
        let read = reply_of(apply(&mut service, LogChunkRequest::read("bucket", "log")).unwrap());
        assert_eq!(read.open, granted.open);
        let switched = reply_of(apply(&mut service, LogChunkRequest::switch("bucket", "log", "gateway-a", lease.epoch, "group-0", 100, 0, Vec::new())).unwrap());
        assert_eq!(stored(&service, "log").unwrap().extents.len(), 2);
        assert_eq!(switched.length, 100);
        let renewed = reply_of(apply(&mut service, LogChunkRequest::renew("gateway-a", 2 * TTL, vec![lref("log", lease.epoch)])).unwrap());
        assert_eq!(renewed.renewed, 1);
    }

    // ---- the chunk ids: one id space, named after the entry that creates the chunk ----

    #[test]
    fn a_page_and_a_grant_name_their_chunks_by_one_function_of_the_raft_index() {
        use crate::object_manifest::ObjectPageAllocation;
        let probes = [1u64, 2, 3, 1000, u32::MAX as u64 - 1, u32::MAX as u64, 1 << 32, (1 << 32) + 1, (5 << 32) | 7, (7 << 32) | u32::MAX as u64];
        let mut seen = std::collections::BTreeSet::new();
        for index in probes {
            let (volume, chunk) = lcc::chunk_identity(index);
            // the encoding a page allocation uses, field for field
            let page = ObjectPageAllocation { format_version: 1, allocation_id: index, page_count: 1 }.page(0).unwrap();
            assert_eq!((page.chunk_id.volume_id(), page.chunk_id.id()), (volume, chunk), "index {index}");
            // the id IS the index, so it is injective
            assert_eq!((volume << 32) | chunk, index);
            assert!(volume <= u32::MAX as u64 && chunk <= u32::MAX as u64, "the chunkserver store takes 32 bits");
            assert!(seen.insert((volume, chunk)), "index {index} repeats an id");
        }
    }

    /// Every chunk id the state has named so far, by the entry that named it: a page allocation's
    /// chunk, and every extent of every object (sealed ones stay in the manifest).
    fn named_ids(service: &MatrixMetaService, keys: &[String], pages: &[(u64, u64)]) -> Vec<(u64, u64, &'static str)> {
        let mut all: Vec<(u64, u64, &'static str)> = pages.iter().map(|(v, c)| (*v, *c, "page")).collect();
        for key in keys {
            if let Some(manifest) = stored(service, key) {
                all.extend(manifest.extents.iter().map(|extent| (extent.volume_id, extent.chunk_id, "tail")));
            }
        }
        all
    }

    #[test]
    fn interleaved_grants_switches_sweeps_and_allocations_never_name_a_chunk_twice_and_ids_only_go_up() {
        let (mut service, groups) = fixture(1, 0);
        raise(&mut service);
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |bound: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };
        let mut clock = NOW;
        let mut keys: Vec<String> = Vec::new();
        let mut pages: Vec<(u64, u64)> = Vec::new();
        let mut last_id = 0u64;
        let check_entry = |service: &MatrixMetaService, last_id: &mut u64, id: (u64, u64)| {
            // a chunk id is the index of the entry that created it, and entries only go up
            let combined = (id.0 << 32) | id.1;
            assert_eq!(combined, service.placement_commit_index, "id {id:?} is not named after its own entry");
            assert!(combined > *last_id, "ids went back: {combined} after {last_id}");
            *last_id = combined;
        };
        let mut restored = false;
        for round in 0..400u32 {
            clock += 400; // leases of one second run out all the time
            match next(6) {
                0 => {
                    // a page allocation: a different entry from any grant
                    let (outcome, reply) = crate::cluster::meta_server::decide_allocation(&service, 3, 1 << 20).unwrap();
                    outcome.apply(&mut service).unwrap();
                    let allocation = match reply {
                        crate::cluster::MetaReply::ObjectPages(allocation) => allocation,
                        other => panic!("unexpected reply {other:?}"),
                    };
                    let page = allocation.page(0).unwrap();
                    let id = (page.chunk_id.volume_id(), page.chunk_id.id());
                    check_entry(&service, &mut last_id, id);
                    for index in 1..3 {
                        assert_eq!(allocation.page(index).unwrap().chunk_id, page.chunk_id, "one allocation, one chunk");
                    }
                    pages.push(id);
                }
                1 => {
                    let key = format!("o{}", keys.len());
                    let granted = run(&mut service, &groups, LogChunkRequest::grant("bucket", &key, "gw", lcc::MIN_TTL_MS, "group-0"), clock).unwrap().0;
                    let open = granted.open.unwrap();
                    check_entry(&service, &mut last_id, (open.volume_id, open.chunk_id));
                    keys.push(key);
                }
                2 | 3 if !keys.is_empty() => {
                    let key = keys[next(keys.len() as u64) as usize].clone();
                    let lease = match service.writer_leases.get("bucket", &key) {
                        Some(lease) if lease.expires_at_ms > clock => lease,
                        _ => {
                            // the lease ran out or was swept: take it over (a later epoch, the open chunk stays open)
                            let again = run(&mut service, &groups, LogChunkRequest::grant("bucket", &key, "gw2", lcc::MIN_TTL_MS, "group-0"), clock).unwrap().0;
                            let lease = again.lease.unwrap();
                            assert_eq!(lease.epoch, service.placement_commit_index);
                            lease
                        }
                    };
                    let switch = LogChunkRequest::switch("bucket", &key, &lease.holder, lease.epoch, "group-0", 1 + next(1000), 0, Vec::new());
                    let switched = run(&mut service, &groups, switch.with_ttl(lcc::MIN_TTL_MS), clock).unwrap().0;
                    let open = switched.open.unwrap();
                    check_entry(&service, &mut last_id, (open.volume_id, open.chunk_id));
                }
                4 => {
                    for command in lcc::expired(&service, clock) {
                        let crate::cluster::MetaRequest::LogChunk(command) = command else { unreachable!() };
                        run(&mut service, &groups, command, clock).unwrap();
                    }
                }
                _ => {}
            }
            if round == 200 && !restored {
                // a restart: the state comes back from its snapshot and the ids carry on from it
                let bytes = service.export_snapshot().to_bytes().unwrap();
                let mut late = MatrixMetaService::new();
                late.install_snapshot(crate::MetaSnapshot::from_bytes(&bytes).unwrap()).unwrap();
                assert_eq!(late.placement_commit_index, service.placement_commit_index);
                service = late;
                restored = true;
            }
        }
        assert!(restored && pages.len() > 20 && keys.len() > 20, "the run exercised allocations and grants: {} {}", pages.len(), keys.len());
        // after all of it, no chunk id is named twice, across pages and every chunk of every object,
        // sealed ones included
        let all = named_ids(&service, &keys, &pages);
        let unique: std::collections::BTreeSet<(u64, u64)> = all.iter().map(|(v, c, _)| (*v, *c)).collect();
        assert_eq!(unique.len(), all.len(), "a chunk id is named twice");
        assert!(all.iter().any(|(_, _, kind)| *kind == "tail") && all.iter().any(|(_, _, kind)| *kind == "page"));
        let sealed = keys.iter().filter_map(|key| stored(&service, key)).map(|m| m.extents.iter().filter(|e| e.flags & EXTENT_FLAG_SEALED != 0).count()).sum::<usize>();
        assert!(sealed > 20, "the run sealed {sealed} chunks");
    }
}
