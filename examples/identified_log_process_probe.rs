// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A client fixture for separate-process log record recovery checks.
//! Seed waits at a committed-record barrier so a controller can kill this client and replicas.
//! This exercises storage identity, not gateway outcome settlement or writer lease acquisition.
use matrixobjectstore_rs::cluster::{
    LogCallError, LogOpRequest, RemoteReplicaClient, ReplicaClientOptions,
};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};
const VOLUME: u64 = 37;
const CHUNK: u64 = 97;
const GENERATION: u32 = 1;
const SEED: &[u8] = b"seed\n";
const RECORD: &[u8] = b"same\n";
fn tag(offset: u64, attempt: u64) -> [u8; 16] {
    let mut context = [0; 64];
    context[..16].copy_from_slice(b"mol-process-id01");
    context[16..24].copy_from_slice(&VOLUME.to_le_bytes());
    context[24..32].copy_from_slice(&CHUNK.to_le_bytes());
    context[32..36].copy_from_slice(&GENERATION.to_le_bytes());
    context[36..44].copy_from_slice(&offset.to_le_bytes());
    context[44..52].copy_from_slice(&(RECORD.len() as u64).to_le_bytes());
    context[52..60].copy_from_slice(&attempt.to_le_bytes());
    ring::digest::digest(&ring::digest::SHA256, &context).as_ref()[..16]
        .try_into()
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
fn expect_identity(
    client: &mut RemoteReplicaClient,
    offset: u64,
    length: usize,
    expected: Option<[u8; 16]>,
) {
    let reply = client
        .log_identity(VOLUME, CHUNK, GENERATION, offset)
        .unwrap();
    let record = reply.record_identity().expect("complete durable record");
    assert_eq!(
        (record.offset, record.length, record.attempt_tag),
        (offset, length as u32, expected)
    );
}
fn append(offset: u64, epoch: u64, attempt: u64) -> LogOpRequest {
    LogOpRequest::append_identified(
        VOLUME,
        CHUNK,
        GENERATION,
        epoch,
        offset,
        RECORD.to_vec(),
        tag(offset, attempt),
    )
}
fn verify(endpoints: &[&str], second: bool) {
    let first = SEED.len() as u64;
    let next = first + RECORD.len() as u64;
    for endpoint in endpoints {
        let mut client = connect(endpoint);
        expect_identity(&mut client, 0, SEED.len(), None);
        expect_identity(&mut client, first, RECORD.len(), Some(tag(first, 1)));
        assert!(
            !client
                .log_identity(VOLUME, CHUNK, GENERATION, first + 1)
                .unwrap()
                .record_present
        );
        let mut expected = SEED.to_vec();
        expected.extend_from_slice(RECORD);
        if second {
            expect_identity(&mut client, next, RECORD.len(), Some(tag(next, 2)));
            expected.extend_from_slice(RECORD);
        }
        assert_eq!(
            client
                .log_read(VOLUME, CHUNK, GENERATION, 0, expected.len() as u32)
                .unwrap()
                .data,
            expected
        );
    }
}
fn refused(result: Result<matrixobjectstore_rs::cluster::LogOpReply, LogCallError>) {
    assert!(
        matches!(result, Err(LogCallError::Refused(_))),
        "expected definitive refusal: {result:?}"
    );
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "usage: identified_log_process_probe seed|recover|verify|old-probe EP,EP,EP [READY_FILE]"
    );
    let endpoints: Vec<&str> = args[2].split(',').collect();
    assert_eq!(endpoints.len(), 3);
    let first = SEED.len() as u64;
    let next = first + RECORD.len() as u64;
    match args[1].as_str() {
        "seed" => {
            assert_eq!(args.len(), 4);
            for endpoint in &endpoints {
                let mut client = connect(endpoint);
                assert!(
                    !client
                        .log_identity(VOLUME, CHUNK, GENERATION, first)
                        .unwrap()
                        .record_present
                );
                client
                    .log_call(LogOpRequest::append(
                        VOLUME,
                        CHUNK,
                        GENERATION,
                        7,
                        0,
                        SEED.to_vec(),
                    ))
                    .unwrap();
                assert_eq!(
                    client.log_call(append(first, 7, 1)).unwrap().durable_length,
                    next
                );
            }
            let ready = Path::new(&args[3]);
            let mut f = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(ready)
                .unwrap();
            f.write_all(format!(r#"{{"phase":"replicas_committed_client_waiting","pid":{},"durable_length":{},"replicas":3}}"#, std::process::id(), next).as_bytes()).unwrap();
            f.sync_all().unwrap();
            drop(f);
            let deadline = Instant::now() + Duration::from_secs(90);
            while Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            panic!("controller did not terminate the client at its barrier");
        }
        "recover" => {
            verify(&endpoints, false);
            for endpoint in &endpoints {
                let mut client = connect(endpoint);
                refused(client.log_call(append(first, 7, 999)));
                assert_eq!(
                    client.log_call(append(first, 7, 1)).unwrap().durable_length,
                    next
                );
            }
            // A fixture fence on every replica. This does not acquire a metadata writer lease.
            for endpoint in &endpoints {
                let mut client = connect(endpoint);
                assert_eq!(
                    client
                        .log_truncate(VOLUME, CHUNK, GENERATION, 8, next)
                        .unwrap()
                        .durable_length,
                    next
                );
                assert_eq!(
                    client.log_call(append(next, 7, 2)).unwrap_err(),
                    LogCallError::StaleEpoch { current: 8 }
                );
                assert!(
                    !client
                        .log_identity(VOLUME, CHUNK, GENERATION, next)
                        .unwrap()
                        .record_present
                );
            }
            for endpoint in &endpoints {
                let mut client = connect(endpoint);
                assert_eq!(
                    client.log_call(append(next, 8, 2)).unwrap().durable_length,
                    next + RECORD.len() as u64
                );
            }
            verify(&endpoints, true);
            println!(
                r#"{{"phase":"recovered","replicas":3,"wrong_attempt_refusals":3,"exact_retries":3,"late_epoch_refusals":3,"logical_length":{}}}"#,
                next + RECORD.len() as u64
            );
        }
        "verify" => {
            verify(&endpoints, true);
            println!(
                r#"{{"phase":"verified_after_second_restart","replicas":3,"identified_records_per_replica":2,"logical_length":{}}}"#,
                next + RECORD.len() as u64
            );
        }
        "old-probe" => {
            for endpoint in &endpoints {
                let mut client = connect(endpoint);
                assert_eq!(
                    client
                        .log_tail(VOLUME, CHUNK, GENERATION)
                        .unwrap()
                        .durable_length,
                    0
                );
                refused(client.log_identity(VOLUME, CHUNK, GENERATION, first));
                refused(client.log_call(LogOpRequest::append_identified(
                    VOLUME,
                    CHUNK,
                    GENERATION,
                    7,
                    0,
                    RECORD.to_vec(),
                    [0; 16],
                )));
                assert_eq!(
                    client
                        .log_tail(VOLUME, CHUNK, GENERATION)
                        .unwrap()
                        .durable_length,
                    0
                );
                client
                    .log_call(LogOpRequest::append(
                        VOLUME,
                        CHUNK,
                        GENERATION,
                        7,
                        0,
                        SEED.to_vec(),
                    ))
                    .unwrap();
                assert_eq!(
                    client
                        .log_read(VOLUME, CHUNK, GENERATION, 0, SEED.len() as u32)
                        .unwrap()
                        .data,
                    SEED
                );
            }
            println!(
                r#"{{"phase":"older_replicas_refused_new_version","replicas":3,"version_two_writes_applied":0,"legacy_writes_verified":3}}"#
            );
        }
        other => panic!("unknown fixture mode {other}"),
    }
}
