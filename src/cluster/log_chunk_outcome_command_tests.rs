// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Outcome version 15: the writer lease grant and the chunk switch, read and applied. Nothing in a
// build decides one yet; these tests build them by hand.
mod log_chunk_outcome_tests {
    use super::*;
    use crate::log_chunk_outcome::{LogChunkChange, WriterLease, ACTION_GRANT, ACTION_SEAL_SWITCH, MAX_LEASE_TTL_MS};
    use crate::object_manifest::{
        ManifestOutcome, ObjectExtent, ObjectManifest, EXTENT_FLAG_SEALED, EXTENT_FLAG_TAIL, EXTENT_KIND_LOG_CHUNK,
    };

    const NOW: u64 = 1_000_000;
    const TTL: u64 = 30_000;

    fn extent(chunk: u64, offset: u64, length: u64, flags: u32, sequence: u64) -> ObjectExtent {
        ObjectExtent {
            group: "group-0".into(),
            volume_id: 1,
            chunk_id: chunk,
            object_offset: offset,
            length,
            write_sequence: sequence,
            kind: EXTENT_KIND_LOG_CHUNK,
            flags,
            ..Default::default()
        }
    }
    fn manifest(generation: u64, length: u64, extents: Vec<ObjectExtent>) -> ObjectManifest {
        ObjectManifest {
            format_version: 3,
            bucket: "bucket".into(),
            key: "log".into(),
            generation,
            length,
            extents,
            ..Default::default()
        }
    }
    fn publication(before: Option<ObjectManifest>, after: ObjectManifest) -> ManifestOutcome {
        // `decide` refuses a chunked manifest: nothing publishes one yet
        ManifestOutcome { format_version: 1, before, after: Some(after) }
    }
    fn lease(holder: &str, epoch: u64, expires_at_ms: u64) -> WriterLease {
        WriterLease { holder: holder.into(), epoch, expires_at_ms }
    }
    fn grant_change(service: &MatrixMetaService, holder: &str, now_ms: u64) -> LogChunkChange {
        LogChunkChange {
            action: ACTION_GRANT,
            bucket: "bucket".into(),
            key: "log".into(),
            lease_before: service.writer_leases.get("bucket", "log"),
            lease_after: Some(lease(holder, service.placement_commit_index + 1, now_ms + TTL)),
            now_ms,
        }
    }
    fn switch_change(service: &MatrixMetaService, now_ms: u64) -> LogChunkChange {
        let held = service.writer_leases.get("bucket", "log");
        LogChunkChange {
            action: ACTION_SEAL_SWITCH,
            bucket: "bucket".into(),
            key: "log".into(),
            lease_before: held.clone(),
            lease_after: held,
            now_ms,
        }
    }
    fn stored(service: &MatrixMetaService) -> Option<ObjectManifest> {
        service
            .object_manifests
            .get(&crate::ObjectId::new("bucket".to_string(), "log".to_string()))
            .map(|stored| stored.to_manifest())
    }
    /// Encode, decode and apply: the path an entry of the log takes.
    fn commit(service: &mut MatrixMetaService, outcome: &PlacementOutcome) -> Result<(), ObjectError> {
        let bytes = outcome.to_payload()?;
        let read = PlacementOutcome::from_payload(&bytes)?;
        assert_eq!(&read, outcome);
        read.apply(service)
    }
    /// A grant of the first chunk to `holder`, committed.
    fn first_grant(service: &mut MatrixMetaService, holder: &str) -> PlacementOutcome {
        let index = service.placement_commit_index + 1;
        let change = grant_change(service, holder, NOW);
        let after = manifest(1, 0, vec![extent(7, 0, 0, EXTENT_FLAG_TAIL, index)]);
        let outcome =
            PlacementOutcome::log_chunk_for_tests(service, change, Some(publication(None, after))).unwrap();
        commit(service, &outcome).unwrap();
        outcome
    }
    /// A switch that seals chunk 7 at `sealed` bytes and opens chunk 8.
    fn switch_outcome(service: &MatrixMetaService, sealed: u64) -> PlacementOutcome {
        let index = service.placement_commit_index + 1;
        let before = stored(service).unwrap();
        let mut closed = before.extents[0].clone();
        closed.length = sealed;
        closed.flags = EXTENT_FLAG_SEALED;
        closed.footer_crc = 0xdead_beef;
        let after = manifest(before.generation + 1, sealed, vec![closed, extent(8, sealed, 0, EXTENT_FLAG_TAIL, index)]);
        PlacementOutcome::log_chunk_for_tests(service, switch_change(service, NOW + 1), Some(publication(Some(before), after)))
            .unwrap()
    }

    #[test]
    fn a_grant_leases_the_object_and_opens_an_empty_chunk_and_a_switch_seals_it_and_opens_the_next() {
        let (mut service, _groups) = fixture(1, 0);
        let before_index = service.placement_commit_index;
        let grant = first_grant(&mut service, "gateway-a");
        // the epoch of the lease is the index of the entry that granted it
        let held = service.writer_leases.get("bucket", "log").unwrap();
        assert_eq!(held, lease("gateway-a", before_index + 1, NOW + TTL));
        assert_eq!(service.placement_commit_index, before_index + 1);
        let opened = stored(&service).unwrap();
        assert_eq!((opened.format_version, opened.generation, opened.length), (3, 1, 0));
        assert_eq!(opened.extents.len(), 1);
        assert_eq!((opened.extents[0].length, opened.extents[0].flags), (0, EXTENT_FLAG_TAIL));
        assert_eq!(grant.version, 15);

        let switch = switch_outcome(&service, 4096);
        commit(&mut service, &switch).unwrap();
        let switched = stored(&service).unwrap();
        assert_eq!((switched.generation, switched.length), (2, 4096));
        assert_eq!(switched.extents.len(), 2);
        assert_eq!(switched.extents[0].flags, EXTENT_FLAG_SEALED);
        assert_eq!(switched.extents[0].length, 4096);
        assert_eq!((switched.extents[1].chunk_id, switched.extents[1].object_offset, switched.extents[1].length), (8, 4096, 0));
        // the switch does not touch the lease
        assert_eq!(service.writer_leases.get("bucket", "log").unwrap(), held);
        assert_eq!(service.placement_commit_index, before_index + 2);
    }

    #[test]
    fn a_grant_to_an_object_already_writing_a_chunk_changes_the_lease_and_no_manifest() {
        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        let manifest_before = stored(&service).unwrap();
        // the same holder opens again, and gets a new epoch: its previous writer is fenced
        let index = service.placement_commit_index + 1;
        let again = PlacementOutcome::log_chunk_for_tests(&service, grant_change(&service, "gateway-a", NOW + 5), None).unwrap();
        commit(&mut service, &again).unwrap();
        assert_eq!(service.writer_leases.get("bucket", "log").unwrap().epoch, index);
        assert_eq!(stored(&service).unwrap(), manifest_before);
        // an object with no chunk being written cannot take a lease-only grant
        let (mut fresh, _groups) = fixture(1, 0);
        let lone = PlacementOutcome::log_chunk_for_tests(&fresh, grant_change(&fresh, "gateway-a", NOW), None).unwrap();
        assert!(commit(&mut fresh, &lone).is_err());
        assert!(fresh.writer_leases.is_empty());
    }

    #[test]
    fn a_live_lease_held_by_another_writer_refuses_and_an_expired_one_is_taken_over() {
        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        // while the lease is live, another writer is refused when the outcome is built
        let live = grant_change(&service, "gateway-b", NOW + TTL - 1);
        assert!(PlacementOutcome::log_chunk_for_tests(&service, live, None).is_err());
        // after it ran out, anyone may take it over
        let taken = PlacementOutcome::log_chunk_for_tests(&service, grant_change(&service, "gateway-b", NOW + TTL), None).unwrap();
        commit(&mut service, &taken).unwrap();
        assert_eq!(service.writer_leases.get("bucket", "log").unwrap().holder, "gateway-b");
        // and the old holder is now the one refused
        let stale = grant_change(&service, "gateway-a", NOW + TTL + 1);
        assert!(PlacementOutcome::log_chunk_for_tests(&service, stale, None).is_err());
    }

    #[test]
    fn an_outcome_decided_from_a_lease_the_state_does_not_hold_fails_and_changes_nothing() {
        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        let decided = PlacementOutcome::log_chunk_for_tests(&service, grant_change(&service, "gateway-a", NOW + 5), None).unwrap();
        // a different grant lands first, so this one was decided from a lease that is gone
        let other = PlacementOutcome::log_chunk_for_tests(&service, grant_change(&service, "gateway-a", NOW + 6), None).unwrap();
        commit(&mut service, &other).unwrap();
        let snapshot = service.export_snapshot().to_bytes().unwrap();
        let error = commit(&mut service, &decided).unwrap_err();
        assert!(matches!(error, ObjectError::Corruption(_)), "{error:?}");
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), snapshot, "a refused entry changes nothing");
        // at the right index, but decided from a lease that is not the one the table holds
        let mut wrong = grant_change(&service, "gateway-a", NOW + 7);
        let held = wrong.lease_before.clone().unwrap();
        wrong.lease_before = Some(WriterLease { expires_at_ms: held.expires_at_ms + 1, ..held });
        let outcome = PlacementOutcome::log_chunk_for_tests(&service, wrong, None).unwrap();
        assert!(commit(&mut service, &outcome).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), snapshot);
    }

    #[test]
    fn a_grant_whose_epoch_is_not_the_index_of_its_entry_is_refused() {
        let (service, _groups) = fixture(1, 0);
        for epoch in [service.placement_commit_index, service.placement_commit_index + 2] {
            let mut change = grant_change(&service, "gateway-a", NOW);
            change.lease_after = Some(lease("gateway-a", epoch, NOW + TTL));
            let after = manifest(1, 0, vec![extent(7, 0, 0, EXTENT_FLAG_TAIL, service.placement_commit_index + 1)]);
            assert!(
                PlacementOutcome::log_chunk_for_tests(&service, change, Some(publication(None, after))).is_err(),
                "epoch {epoch}"
            );
        }
    }

    #[test]
    fn a_switch_must_keep_the_lease_and_seal_the_chunk_that_was_open() {
        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        let index = service.placement_commit_index + 1;
        let before = stored(&service).unwrap();
        let build = |sealed: u64, mutate: &dyn Fn(&mut Vec<ObjectExtent>), change: LogChunkChange| {
            let mut closed = before.extents[0].clone();
            closed.length = sealed;
            closed.flags = EXTENT_FLAG_SEALED;
            let mut extents = vec![closed, extent(8, sealed, 0, EXTENT_FLAG_TAIL, index)];
            mutate(&mut extents);
            let after = manifest(2, extents[0].length, extents);
            PlacementOutcome::log_chunk_for_tests(&service, change, Some(publication(Some(before.clone()), after)))
        };
        let good = switch_change(&service, NOW + 1);
        assert!(build(4096, &|_| {}, good.clone()).is_ok());
        // the lease may not change under a switch, and it must be live
        let mut moved = good.clone();
        moved.lease_after = Some(lease("gateway-b", index - 1, NOW + TTL));
        assert!(build(4096, &|_| {}, moved).is_err());
        let mut expired = good.clone();
        expired.now_ms = NOW + TTL;
        assert!(build(4096, &|_| {}, expired).is_err());
        // the next chunk: empty, owned by this entry, a chunk the object does not already name
        assert!(build(4096, &|e| e[1].length = 1, good.clone()).is_err());
        assert!(build(4096, &|e| e[1].write_sequence += 1, good.clone()).is_err());
        assert!(build(4096, &|e| e[1].chunk_id = 7, good.clone()).is_err());
        assert!(build(4096, &|e| e[1].flags = EXTENT_FLAG_SEALED, good.clone()).is_err());
        // the sealed chunk is the one that was open, still at its place
        assert!(build(4096, &|e| e[0].chunk_id = 9, good.clone()).is_err());
        assert!(build(4096, &|e| e[0].write_sequence += 1, good.clone()).is_err());
        assert!(build(4096, &|e| e[0].flags = EXTENT_FLAG_TAIL, good.clone()).is_err());
        // nothing else changes: no third entry, no extra bytes
        assert!(build(4096, &|e| e.push(extent(9, 4096, 0, EXTENT_FLAG_TAIL, index)), good).is_err());
    }

    #[test]
    fn a_switch_never_seals_a_chunk_shorter_than_the_length_recorded_for_it() {
        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        // record 100 bytes in the open chunk (a later slice's check-in does this); the lower bound
        // stands even though the stored lease and manifest are otherwise valid
        let mut recorded = stored(&service).unwrap();
        recorded.extents[0].length = 100;
        recorded.length = 100;
        recorded.generation = 2;
        let publish = PlacementOutcome::log_chunk_for_tests(
            &service,
            grant_change(&service, "gateway-a", NOW + 2),
            Some(publication(stored(&service), recorded.clone())),
        );
        // a grant adds an empty tail and nothing else, so growing the open chunk is not a grant
        assert!(publish.is_err());
        // directly: sealing at 99 when 100 is recorded is refused by the relation itself
        let mut shorter = recorded.clone();
        shorter.extents[0].length = 99;
        shorter.extents[0].flags = EXTENT_FLAG_SEALED;
        shorter.length = 99;
        shorter.generation = 3;
        shorter.extents.push(extent(8, 99, 0, EXTENT_FLAG_TAIL, 1));
        let change = LogChunkChange {
            action: ACTION_SEAL_SWITCH,
            bucket: "bucket".into(),
            key: "log".into(),
            lease_before: Some(lease("gateway-a", 1, NOW + TTL)),
            lease_after: Some(lease("gateway-a", 1, NOW + TTL)),
            now_ms: NOW,
        };
        assert!(change.check_manifest(1, Some(&recorded), &shorter).is_err());
        shorter.extents[0].length = 100;
        shorter.length = 100;
        shorter.extents[1].object_offset = 100;
        assert!(change.check_manifest(1, Some(&recorded), &shorter).is_ok());
    }

    #[test]
    fn malformed_log_chunk_outcomes_are_refused_before_they_can_apply() {
        let (service, _groups) = fixture(1, 0);
        let index = service.placement_commit_index + 1;
        let tail = || manifest(1, 0, vec![extent(7, 0, 0, EXTENT_FLAG_TAIL, index)]);
        let ok = |change: LogChunkChange| {
            PlacementOutcome::log_chunk_for_tests(&service, change, Some(publication(None, tail()))).is_ok()
        };
        let good = grant_change(&service, "gateway-a", NOW);
        assert!(ok(good.clone()));
        let mut bad = good.clone();
        bad.action = 3;
        assert!(!ok(bad), "an unknown action");
        let mut bad = good.clone();
        bad.now_ms = 0;
        assert!(!ok(bad), "no clock reading");
        let mut bad = good.clone();
        bad.lease_after = Some(lease("gateway-a", index, NOW + MAX_LEASE_TTL_MS + 1));
        assert!(!ok(bad), "a term past the bound");
        let mut bad = good.clone();
        bad.lease_after = Some(lease("gateway-a", index, NOW));
        assert!(!ok(bad), "a lease that is already over");
        let mut bad = good.clone();
        bad.lease_after = Some(lease("", index, NOW + TTL));
        assert!(!ok(bad), "no holder");
        let mut bad = good.clone();
        bad.lease_after = Some(lease("two words", index, NOW + TTL));
        assert!(!ok(bad), "a holder with a space");
        let mut bad = good.clone();
        bad.lease_after = None;
        assert!(!ok(bad), "no lease left behind");
        let mut bad = good.clone();
        bad.lease_before = Some(lease("gateway-a", index, NOW));
        assert!(!ok(bad), "a lease from the entry itself");
        let mut bad = good.clone();
        bad.bucket = String::new();
        assert!(!ok(bad), "no object");
        // the manifest it publishes
        let build = |after: ObjectManifest| {
            PlacementOutcome::log_chunk_for_tests(&service, good.clone(), Some(publication(None, after))).is_ok()
        };
        assert!(build(tail()));
        assert!(!build(ObjectManifest { format_version: 2, ..tail() }), "a grant publishes a chunked manifest");
        assert!(!build(manifest(1, 0, vec![extent(7, 0, 0, EXTENT_FLAG_TAIL, index + 1)])), "owned by another entry");
        assert!(!build(manifest(1, 0, vec![extent(7, 0, 0, 0, index)])), "an empty entry that is not a tail");
        assert!(!build(manifest(1, 0, vec![extent(7, 0, 0, EXTENT_FLAG_TAIL | EXTENT_FLAG_SEALED, index)])), "sealed and tail");
        assert!(!build(manifest(1, 5, vec![extent(7, 0, 5, EXTENT_FLAG_TAIL, index)])), "a grant carries no bytes");
        assert!(!build(ObjectManifest { key: "other".into(), ..tail() }), "another object");
        // a version 15 outcome that is not a log chunk outcome, and a log chunk change on another version
        let mut outcome =
            PlacementOutcome::log_chunk_for_tests(&service, good.clone(), Some(publication(None, tail()))).unwrap();
        let sound = outcome.clone();
        outcome.version = 2;
        assert!(outcome.to_payload().is_err(), "a log chunk change belongs to version 15");
        let mut outcome = sound.clone();
        outcome.log_chunk = None;
        assert!(outcome.to_payload().is_err(), "version 15 carries its change");
        let mut outcome = sound.clone();
        outcome.retention_deadline = Some(Default::default());
        assert!(outcome.to_payload().is_err(), "unrelated state changes");
        let mut outcome = sound.clone();
        outcome.features = Some(Default::default());
        assert!(outcome.to_payload().is_err(), "a feature change rides alone");
        let mut outcome = sound;
        outcome.before_index += 1;
        assert!(outcome.to_payload().is_err(), "indexes are consecutive");
    }

    fn contains(bytes: &[u8], needle: &[u8]) -> bool {
        bytes.windows(needle.len()).any(|window| window == needle)
    }
    struct Short<'a>(&'a [u8]);
    impl std::io::Read for Short<'_> {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            let n = out.len().min(5).min(self.0.len());
            out[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    #[test]
    fn no_lease_leaves_the_snapshot_encoding_as_it_was_and_a_lease_survives_it_and_a_late_joiner() {
        let none = MatrixMetaService::new().export_snapshot();
        assert!(none.writer_leases.is_empty());
        let bytes = none.to_bytes().unwrap();
        assert!(contains(&bytes[..32], b"MOSRSMETA18"), "no lease must keep the old magic");
        assert!(!contains(&bytes, b"MOSRSMETA22"));

        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        let snapshot = service.export_snapshot();
        assert_eq!(snapshot.writer_leases.len(), 1);
        let bytes = snapshot.to_bytes().unwrap();
        assert!(contains(&bytes[..32], b"MOSRSMETA22"));
        // the fixture's roster carries node-local fields a snapshot never holds, so compare what the
        // snapshot is about and the bytes it writes again
        let same = |read: &crate::MetaSnapshot| {
            assert_eq!(read.writer_leases, snapshot.writer_leases);
            assert_eq!(read.object_manifests, snapshot.object_manifests);
            assert_eq!(read.cluster_features, snapshot.cluster_features);
            assert_eq!(read.checksum, snapshot.checksum);
            assert_eq!(read.to_bytes().unwrap(), bytes);
        };
        same(&crate::MetaSnapshot::from_bytes(&bytes).unwrap());
        same(&crate::MetaSnapshot::read_from(&mut Short(&bytes), bytes.len() as u64).unwrap());
        // a late joiner installs it, holds the same lease, and applies the next entry as everyone does
        let mut late = MatrixMetaService::new();
        late.install_snapshot(crate::MetaSnapshot::from_bytes(&bytes).unwrap()).unwrap();
        assert_eq!(late.writer_leases.get("bucket", "log"), service.writer_leases.get("bucket", "log"));
        assert_eq!(stored(&late), stored(&service));
        // a switch then moves the leader's state on; the joiner's lease is the one it was decided from
        let next = switch_outcome(&service, 4096);
        commit(&mut service, &next).unwrap();
        assert_eq!(service.writer_leases.get("bucket", "log"), late.writer_leases.get("bucket", "log"));
        assert_eq!(stored(&service).unwrap().generation, 2);
        // the feature set and the leases ride together, and a feature set alone still writes 21
        service.cluster_features = crate::cluster_features::ClusterFeatures {
            raised: crate::cluster_features::FEATURE_CHUNK_MANIFEST,
            declared: [(1, 7)].into(),
        };
        let both = service.export_snapshot();
        let bytes = both.to_bytes().unwrap();
        assert!(contains(&bytes[..32], b"MOSRSMETA22"));
        let read = crate::MetaSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!((&read.writer_leases, &read.cluster_features), (&both.writer_leases, &both.cluster_features));
        assert_eq!(read.to_bytes().unwrap(), bytes);
        let mut features_only = MatrixMetaService::new();
        features_only.cluster_features = service.cluster_features.clone();
        assert!(contains(&features_only.export_snapshot().to_bytes().unwrap()[..32], b"MOSRSMETA21"));
    }

    #[test]
    fn the_leases_are_covered_by_the_checksum_and_damage_is_refused() {
        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        let bytes = service.export_snapshot().to_bytes().unwrap();
        // flip each byte of the holder name in turn: every one is refused
        let at = bytes.windows(9).position(|window| window == b"gateway-a").unwrap();
        for offset in 0..9 {
            let mut damaged = bytes.clone();
            damaged[at + offset] ^= 1;
            assert!(crate::MetaSnapshot::from_bytes(&damaged).is_err(), "byte {offset} of the holder");
        }
    }

    #[test]
    fn a_changed_lease_alone_changes_the_snapshot_checksum() {
        let (mut service, _groups) = fixture(1, 0);
        first_grant(&mut service, "gateway-a");
        let snapshot = service.export_snapshot();
        let base = snapshot.checksum_material_hash();
        assert_eq!(base, snapshot.checksum);
        let held = service.writer_leases.get("bucket", "log").unwrap();
        for changed in [
            WriterLease { holder: "gateway-b".into(), ..held.clone() },
            WriterLease { epoch: held.epoch + 1, ..held.clone() },
            WriterLease { expires_at_ms: held.expires_at_ms + 1, ..held.clone() },
        ] {
            let mut other = snapshot.clone();
            other.writer_leases.set("bucket", "log", &changed);
            assert_ne!(other.checksum_material_hash(), base, "{changed:?}");
        }
        let mut none = snapshot.clone();
        none.writer_leases = Default::default();
        assert_ne!(none.checksum_material_hash(), base, "no lease at all");
    }

    #[test]
    fn an_empty_entry_is_valid_only_as_the_last_log_chunk_entry_still_being_written() {
        let ok = manifest(1, 0, vec![extent(7, 0, 0, EXTENT_FLAG_TAIL, 3)]);
        ok.validate().unwrap();
        // not a tail
        assert!(manifest(1, 0, vec![extent(7, 0, 0, 0, 3)]).validate().is_err());
        // sealed
        assert!(manifest(1, 0, vec![extent(7, 0, 0, EXTENT_FLAG_SEALED, 3)]).validate().is_err());
        // not last
        assert!(manifest(1, 5, vec![extent(7, 0, 0, EXTENT_FLAG_TAIL, 3), extent(8, 0, 5, EXTENT_FLAG_TAIL, 4)]).validate().is_err());
        // a page extent is never empty
        let page = ObjectExtent {
            group: "group-0".into(),
            chunk_id: 9,
            length: 0,
            write_sequence: 1,
            data_sha256: vec![1; 32],
            ..Default::default()
        };
        assert!(ObjectManifest { format_version: 2, ..manifest(1, 0, vec![page]) }.validate().is_err());
    }
}
