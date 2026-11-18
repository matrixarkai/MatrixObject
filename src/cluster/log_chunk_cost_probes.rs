// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Local measurements, for a profiler: what the metadata state machine does for a 4 GiB object that
// is held as `BENCH_PREFILL` chunk entries (log model, 64 MiB each) or as 4096 page extents (page
// model, 1 MiB each). Nothing here runs by default.
//
//   BENCH_MODEL   chunk | page        (default chunk)
//   BENCH_PREFILL entries the object holds before the measurement (default 64)
//   BENCH_APPENDS repetitions (default 20)
//   BENCH_PHASE   switch      chunk model only: one grant-free chunk switch (seal + open next)
//                              = the ONLY metadata work the log model does, once per chunk
//                 server_read the metadata server answers a whole-manifest read
//                 ranged      the metadata server answers a read of 4 MiB in the middle
//                 client_decode the client decodes and validates the whole-manifest reply
//                 entries     print raft entries one append costs (page model: reserve + publish)
mod log_chunk_cost_probes {
    use super::*;
    use crate::cluster::log_chunk_commands::{self as lcc, Decision, LogChunkRequest};
    use crate::log_chunk_outcome::WriterLease;
    use crate::object_manifest::{
        ManifestOutcome, ObjectExtent, ObjectManifest, EXTENT_FLAG_SEALED, EXTENT_FLAG_TAIL, EXTENT_KIND_LOG_CHUNK,
    };

    const CHUNK: u64 = 64 << 20;
    const PAGE: u64 = 1 << 20;
    const NOW: u64 = 1_000_000_000;

    fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
        std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
    }

    fn chunk_manifest(entries: usize) -> ObjectManifest {
        let mut extents: Vec<ObjectExtent> = (0..entries - 1)
            .map(|index| ObjectExtent {
                group: "group-0".into(),
                volume_id: 1,
                chunk_id: index as u64 + 1,
                object_offset: index as u64 * CHUNK,
                length: CHUNK,
                write_sequence: index as u64 + 1,
                kind: EXTENT_KIND_LOG_CHUNK,
                flags: EXTENT_FLAG_SEALED,
                footer_crc: 1 + index as u32,
                ..Default::default()
            })
            .collect();
        extents.push(ObjectExtent {
            group: "group-0".into(),
            volume_id: 1,
            chunk_id: entries as u64,
            object_offset: (entries as u64 - 1) * CHUNK,
            length: 0,
            write_sequence: entries as u64,
            kind: EXTENT_KIND_LOG_CHUNK,
            flags: EXTENT_FLAG_TAIL,
            ..Default::default()
        });
        ObjectManifest {
            format_version: 3,
            bucket: "bucket".into(),
            key: "append".into(),
            generation: 1,
            length: (entries as u64 - 1) * CHUNK,
            extents,
            ..Default::default()
        }
    }
    fn page_manifest(entries: usize) -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "append".into(),
            generation: 1,
            length: entries as u64 * PAGE,
            extents: (0..entries)
                .map(|index| ObjectExtent {
                    group: "group-0".into(),
                    volume_id: 7,
                    chunk_id: index as u64 + 1,
                    object_offset: index as u64 * PAGE,
                    length: PAGE,
                    write_sequence: 1,
                    data_sha256: vec![(index % 251) as u8; 32],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    fn publish(service: &mut MatrixMetaService, manifest: &ObjectManifest) {
        PlacementOutcome::publish_manifest(service, ManifestOutcome::decide_chunked(None, manifest.clone()).unwrap(), 0)
            .unwrap()
            .apply(service)
            .unwrap();
    }

    #[test]
    #[ignore = "local measurement for a profiler"]
    fn log_chunk_cpu_cost_at_entries() {
        let model: String = env("BENCH_MODEL", "chunk".to_string());
        let phase: String = env("BENCH_PHASE", "server_read".to_string());
        let entries: usize = env("BENCH_PREFILL", 64);
        let reps: usize = env("BENCH_APPENDS", 20);
        crate::manifest_chain::set_soundness_checks(false);
        let (mut service, groups) = fixture(1, 0);
        let manifest = if model == "page" { page_manifest(entries) } else { chunk_manifest(entries) };
        publish(&mut service, &manifest);
        let id = crate::ObjectId::new("bucket", "append");
        match phase.as_str() {
            "switch" => {
                assert_eq!(model, "chunk");
                service.cluster_features = crate::cluster_features::ClusterFeatures {
                    raised: crate::cluster_features::FEATURE_CHUNK_MANIFEST,
                    declared: [(1, 7)].into(),
                };
                let epoch = service.placement_commit_index;
                service.writer_leases.set("bucket", "append", &WriterLease { holder: "gw".into(), epoch, expires_at_ms: NOW * 2 });
                let one = |service: &mut MatrixMetaService, n: u64| {
                    let request = LogChunkRequest::switch("bucket", "append", "gw", epoch, "group-0", CHUNK, n as u32 + 7, Vec::new());
                    match lcc::prepare(service, &groups, &request, NOW + n).unwrap() {
                        Decision::Change(outcome, _) => {
                            let bytes = outcome.to_payload().unwrap();
                            std::hint::black_box(bytes.len());
                            outcome.apply(service).unwrap();
                        }
                        Decision::Unchanged(_) => panic!("a switch changes the manifest"),
                    }
                };
                // the first switch is not measured: a callgrind run subtracts a run with reps = 0
                one(&mut service, 0);
                for n in 1..=reps as u64 {
                    one(&mut service, n);
                }
            }
            "server_read" => {
                for _ in 0..=reps {
                    let value = service.object_manifests.get_manifest(&id).unwrap();
                    let reply = crate::cluster::meta_message::encode_meta_reply(&MetaReply::Manifest(value));
                    std::hint::black_box(reply.len());
                }
            }
            "client_decode" => {
                use prost::Message;
                let bytes = manifest.encode_to_vec();
                for _ in 0..=reps {
                    let decoded = ObjectManifest::decode(bytes.as_slice()).unwrap();
                    decoded.validate().unwrap();
                    std::hint::black_box(decoded.extents.len());
                }
            }
            "ranged" => {
                let middle = manifest.length / 2;
                for _ in 0..=reps {
                    let request = MetaRequest::ReadManifestRange {
                        bucket: "bucket".into(),
                        key: "append".into(),
                        offset: middle,
                        length: 4 << 20,
                    };
                    let reply = crate::cluster::meta_server::apply_with_groups(&mut service, request, usize::MAX, &groups).unwrap();
                    match reply {
                        MetaReply::ManifestRange(entry) => {
                            std::hint::black_box(entry);
                        }
                        other => panic!("unexpected reply {other:?}"),
                    }
                }
            }
            "entries" => {
                assert_eq!(model, "page");
                // a page append reserves, then publishes; count the entries the state machine commits
                crate::cluster::staged_allocation_commands::prepare_at(
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
                let mut manifest = manifest.clone();
                let before = service.placement_commit_index;
                append_once(&mut service, &groups, &mut manifest);
                println!("page model: {} raft entries per append (object at {} extents)", service.placement_commit_index - before, entries);
            }
            other => panic!("unknown BENCH_PHASE {other}"),
        }
    }
}
