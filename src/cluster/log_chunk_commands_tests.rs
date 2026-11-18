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
            raised: crate::cluster_features::FEATURE_CHUNK_MANIFEST,
            declared: [(1, 7)].into(),
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
            assert!(run(&mut service, &groups, attempt, now).is_err(), "{holder} {epoch} {now} {key}");
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
}
