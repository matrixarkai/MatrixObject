// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Separate-process fixture for unified metadata leases and identified native records.
//! This does not exercise gateway settlement, client reply loss or object-plane defaults.
use matrixobjectstore_rs::chunk_manifest::{ChunkExtent, CHUNK_FLAG_SEALED, CHUNK_KIND_LOG};
use matrixobjectstore_rs::cluster::{
    AppendIdentity, ChunkLogWriter, LogCallError, LogOpRequest, MetaClientOptions,
    RemoteMetaClient, RemoteReplicaClient, ReplicaClientOptions,
};
use matrixobjectstore_rs::cluster_features::{FEATURE_UNIFIED_LOG_WRITERS, SUPPORTED_FEATURES};
use std::io::Write;
use std::time::{Duration, Instant};
const BUCKET: &str = "bucket";
const KEY: &str = "writer-probe";
const GROUP: &str = "group-a";
const BODY: &[u8] = b"same-body";
const GENERATION: u32 = 1;
// This fixture uses one isolated object incarnation, not a gateway incarnation policy.
fn request_identity(logical: u64, attempt: u64) -> AppendIdentity {
    let mut id = [0; 16];
    id[..8].copy_from_slice(&attempt.to_le_bytes());
    AppendIdentity {
        object_incarnation: [7; 16],
        attempt_id: id,
        request_offset: logical,
        request_length: BODY.len() as u64,
    }
}
fn tag(chunk: &ChunkExtent, physical: u64, logical: u64, attempt: u64) -> [u8; 16] {
    request_identity(logical, attempt)
        .record_tag(
            chunk.volume_id,
            chunk.chunk_id,
            GENERATION,
            physical,
            logical,
            BODY.len() as u32,
        )
        .unwrap()
}
fn native_writer(data: &[&str], chunk: &ChunkExtent, epoch: u64) -> ChunkLogWriter {
    let endpoints: Vec<String> = data.iter().map(|s| (*s).to_string()).collect();
    ChunkLogWriter::open_identified(
        &endpoints,
        chunk.volume_id,
        chunk.chunk_id,
        GENERATION,
        epoch,
        ReplicaClientOptions::default(),
    )
    .unwrap()
}
fn connect(endpoint: &str) -> RemoteReplicaClient {
    RemoteReplicaClient::connect(
        endpoint,
        &ReplicaClientOptions {
            connect_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap()
}
fn append(
    chunk: &ChunkExtent,
    epoch: u64,
    offset: u64,
    logical: u64,
    attempt: u64,
) -> LogOpRequest {
    LogOpRequest::append_identified(
        chunk.volume_id,
        chunk.chunk_id,
        GENERATION,
        epoch,
        offset,
        BODY.to_vec(),
        tag(chunk, offset, logical, attempt),
    )
}
fn identity(
    client: &mut RemoteReplicaClient,
    chunk: &ChunkExtent,
    offset: u64,
    logical: u64,
    attempt: u64,
) {
    let id = client
        .log_identity(chunk.volume_id, chunk.chunk_id, GENERATION, offset)
        .unwrap()
        .record_identity()
        .unwrap();
    assert_eq!(
        (id.offset, id.length, id.attempt_tag),
        (
            offset,
            BODY.len() as u32,
            Some(tag(chunk, offset, logical, attempt))
        )
    );
}
fn barrier(path: &str, phase: &str, epoch: u64, logical_length: u64) -> ! {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.write_all(format!(r#"{{"pid":{},"phase":"{phase}","epoch":{epoch},"logical_length":{logical_length},"replicas":3}}"#,std::process::id()).as_bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("controller did not terminate the fixture at {phase}");
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len()>=4,"usage: unified_log_writer_process_probe seed|recover|verify|old-probe|legacy-smoke META,META,META DATA,DATA,DATA [READY_FILE] [OLD_EPOCH]");
    let metas: Vec<String> = args[2].split(',').map(str::to_string).collect();
    let data: Vec<&str> = args[3].split(',').collect();
    assert_eq!(metas.len(), 3);
    assert_eq!(data.len(), 3);
    let mut client =
        RemoteMetaClient::connect_catalog(&metas, &MetaClientOptions::default()).unwrap();
    match args[1].as_str() {
        "old-probe" => {
            for endpoint in &data {
                assert!(matches!(
                    connect(endpoint).log_identity(37, 97, GENERATION, 0),
                    Err(LogCallError::Refused(_) | LogCallError::NotSpoken)
                ));
            }
            let endpoints: Vec<String> = data.iter().map(|s| (*s).to_string()).collect();
            assert!(ChunkLogWriter::open_identified(
                &endpoints,
                37,
                97,
                GENERATION,
                7,
                ReplicaClientOptions::default()
            )
            .is_err());
            println!(r#"{{"phase":"older-protocol-refused","new_payload_writes":0,"replicas":3}}"#);
        }
        "legacy-smoke" => {
            for endpoint in &data {
                let mut replica = connect(endpoint);
                assert_eq!(
                    replica
                        .log_call(LogOpRequest::append(
                            37,
                            97,
                            GENERATION,
                            7,
                            0,
                            b"old".to_vec()
                        ))
                        .unwrap()
                        .durable_length,
                    3
                );
                assert_eq!(
                    replica.log_read(37, 97, GENERATION, 0, 3).unwrap().data,
                    b"old"
                );
            }
            println!(r#"{{"phase":"older-legacy-verified","legacy_writes":3}}"#);
        }
        "seed" => {
            assert_eq!(args.len(), 5);
            assert!(!client
                .read_cluster_features()
                .unwrap()
                .has(FEATURE_UNIFIED_LOG_WRITERS));
            assert!(client
                .grant_chunk_writer(BUCKET, KEY, "writer-a", 1000, GROUP)
                .is_err());
            for voter in 1..=3 {
                client
                    .declare_voter_features(voter, SUPPORTED_FEATURES)
                    .unwrap();
            }
            client
                .raise_cluster_features(FEATURE_UNIFIED_LOG_WRITERS, vec![1, 2, 3])
                .unwrap();
            let first = client
                .grant_chunk_writer(BUCKET, KEY, "writer-a", 1000, GROUP)
                .unwrap();
            let epoch = first.lease.unwrap().epoch;
            let chunk = first.chunk_open.unwrap();
            assert!(first.open.is_none());
            assert_eq!(chunk.kind, CHUNK_KIND_LOG);
            let mut writer = native_writer(&data, &chunk, epoch);
            assert_eq!(
                writer
                    .append_identified(0, 0, BODY, request_identity(0, 1))
                    .unwrap(),
                BODY.len() as u64
            );
            for endpoint in &data {
                identity(&mut connect(endpoint), &chunk, 0, 0, 1);
            }
            barrier(&args[4], "first-record-durable", epoch, BODY.len() as u64);
        }
        "recover" => {
            assert_eq!(args.len(), 6);
            let old_epoch: u64 = args[5].parse().unwrap();
            assert!(client
                .read_cluster_features()
                .unwrap()
                .has(FEATURE_UNIFIED_LOG_WRITERS));
            let before = client.read_chunk_manifest(BUCKET, KEY).unwrap();
            assert_eq!(before.generation, 1);
            assert_eq!(before.extents.len(), 1);
            let resumed = client
                .grant_chunk_writer(BUCKET, KEY, "writer-b", 60_000, GROUP)
                .unwrap();
            let epoch = resumed.lease.unwrap().epoch;
            assert!(epoch > old_epoch);
            let chunk = resumed.chunk_open.unwrap();
            assert_eq!(chunk, before.extents[0]);
            let mut writer = native_writer(&data, &chunk, epoch);
            assert_eq!(writer.length(), BODY.len() as u64);
            for endpoint in &data {
                let mut replica = connect(endpoint);
                identity(&mut replica, &chunk, 0, 0, 1);
                let stale = replica.log_call(append(
                    &chunk,
                    old_epoch,
                    BODY.len() as u64,
                    BODY.len() as u64,
                    99,
                ));
                assert!(
                    matches!(&stale,Err(LogCallError::StaleEpoch { current }) if *current == epoch),
                    "stale writer result: {stale:?}"
                );
            }
            assert_eq!(
                writer
                    .append_identified(
                        BODY.len() as u64,
                        BODY.len() as u64,
                        BODY,
                        request_identity(BODY.len() as u64, 2)
                    )
                    .unwrap(),
                2 * BODY.len() as u64
            );
            let footer = writer.seal_footer(true).unwrap();
            assert_eq!(footer.length, 2 * BODY.len() as u64);
            let (crc, sha) = (footer.crc, footer.sha256);
            let switched = client
                .switch_chunk_writer(
                    BUCKET,
                    KEY,
                    "writer-b",
                    epoch,
                    GROUP,
                    2 * BODY.len() as u64,
                    crc,
                    sha,
                )
                .unwrap();
            assert_eq!(switched.lease.unwrap().epoch, epoch);
            assert_eq!(switched.generation, 2);
            let next = switched.chunk_open.unwrap();
            assert_ne!(
                (next.volume_id, next.chunk_id),
                (chunk.volume_id, chunk.chunk_id)
            );
            assert_eq!(next.object_offset, 2 * BODY.len() as u64);
            let mut next_writer = native_writer(&data, &next, epoch);
            assert_eq!(
                next_writer
                    .append_identified(
                        0,
                        2 * BODY.len() as u64,
                        BODY,
                        request_identity(2 * BODY.len() as u64, 3)
                    )
                    .unwrap(),
                BODY.len() as u64
            );
            barrier(
                &args[4],
                "switch-and-next-record-durable",
                epoch,
                3 * BODY.len() as u64,
            );
        }
        "verify" => {
            assert_eq!(args.len(), 4);
            let m = client.read_chunk_manifest(BUCKET, KEY).unwrap();
            assert_eq!(m.generation, 2);
            assert_eq!(m.length, 2 * BODY.len() as u64);
            assert_eq!(m.extents.len(), 2);
            assert_ne!(m.extents[0].flags & CHUNK_FLAG_SEALED, 0);
            let writer = client.read_chunk_writer(BUCKET, KEY).unwrap();
            assert_eq!(writer.chunk_open.as_ref(), Some(&m.extents[1]));
            let epoch = writer.lease.unwrap().epoch;
            for endpoint in &data {
                let mut replica = connect(endpoint);
                identity(&mut replica, &m.extents[0], 0, 0, 1);
                identity(
                    &mut replica,
                    &m.extents[0],
                    BODY.len() as u64,
                    BODY.len() as u64,
                    2,
                );
                identity(&mut replica, &m.extents[1], 0, 2 * BODY.len() as u64, 3);
                let sealed = replica
                    .log_tail(m.extents[0].volume_id, m.extents[0].chunk_id, GENERATION)
                    .unwrap();
                assert!(sealed.sealed);
                assert_eq!(sealed.epoch, epoch);
                assert_eq!(sealed.crc, m.extents[0].footer_crc);
                assert_eq!(sealed.sha256, m.extents[0].footer_sha256);
                assert_eq!(
                    replica
                        .log_read(
                            m.extents[0].volume_id,
                            m.extents[0].chunk_id,
                            GENERATION,
                            0,
                            (2 * BODY.len()) as u32
                        )
                        .unwrap()
                        .data,
                    [BODY, BODY].concat()
                );
                assert_eq!(
                    replica
                        .log_read(
                            m.extents[1].volume_id,
                            m.extents[1].chunk_id,
                            GENERATION,
                            0,
                            BODY.len() as u32
                        )
                        .unwrap()
                        .data,
                    BODY
                );
            }
            assert!(client
                .grant_log_chunk(BUCKET, KEY, "writer-b", 60_000, GROUP)
                .is_err());
            println!(
                r#"{{"phase":"verified","logical_length":{},"metadata_generation":2,"identified_records_per_replica":3,"stale_epoch_refusals":3,"replicas":3}}"#,
                3 * BODY.len()
            );
        }
        other => panic!("unknown mode {other}"),
    }
}
