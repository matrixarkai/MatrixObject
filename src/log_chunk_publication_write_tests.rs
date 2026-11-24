// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "publication-write-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> PathBuf {
        self.0.join("chunk")
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn open(p: &Path) -> LogChunk {
    LogChunk::open(p, 7, 1024 * 1024).unwrap()
}
fn initialized() -> (Dir, LogChunk) {
    let d = Dir::new();
    let mut c = open(&d.path());
    c.initialize_publication(1, [7; 16]).unwrap();
    (d, c)
}
fn frame(kind: u8, offset: u64, body: &[u8], published: Option<u64>) -> Vec<u8> {
    let h = Header {
        kind,
        flags: 0,
        generation: 7,
        epoch: 1,
        offset,
        length: body.len() as u32,
        data_crc: crc32c(body),
        attempt_tag: Some([7; 16]),
        published_length: published,
    };
    let mut b = h.encode()[..h.encoded_len()].to_vec();
    b.extend_from_slice(body);
    b
}
#[test]
fn initialization_has_an_exact_control_record_and_cold_empty_prefix() {
    let (d, c) = initialized();
    assert_eq!(c.publication_initialization(), Some([7; 16]));
    assert_eq!(
        (c.length(), c.published_length(), c.durable_length()),
        (0, 0, 0)
    );
    let b = std::fs::read(d.path()).unwrap();
    assert_eq!(b.len(), 64);
    assert_eq!(&b[..4], b"MOL3");
    assert_eq!(b[4], KIND_INITIALIZE);
    assert_eq!(&b[36..52], &[7; 16]);
    assert_eq!(
        u32::from_le_bytes(b[60..64].try_into().unwrap()),
        crc32c(&b[..60])
    );
    drop(c);
    let mut c = open(&d.path());
    assert!(c.has_publication_records());
    assert_eq!(c.publication_initialization(), Some([7; 16]));
    assert_eq!(c.durable_published_length().unwrap(), 0);
    assert!(c.durable_publication_record().unwrap().is_none());
    let (mut sync, _) = c.sync_handle().unwrap();
    std::io::Seek::seek(&mut sync, std::io::SeekFrom::Start(0)).unwrap();
    c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
    c.commit().unwrap();
    drop(c);
    let c = open(&d.path());
    assert_eq!(c.publication_initialization(), Some([7; 16]));
    assert_eq!(c.durable_published_length().unwrap(), 3);
    assert_eq!(c.read_vec(0, 3).unwrap(), b"abc");
    assert_eq!(
        c.append_record_identity(0).unwrap().unwrap().attempt_tag,
        Some([1; 16])
    );
}
#[test]
fn exact_initialization_retry_preserves_existing_data_and_refuses_changed_context() {
    let (d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
    c.commit().unwrap();
    let before = std::fs::read(d.path()).unwrap();
    c.initialize_publication(1, [7; 16]).unwrap();
    assert!(matches!(
        c.initialize_publication(1, [8; 16]),
        Err(LogError::Overlap)
    ));
    assert!(c.initialize_publication(2, [7; 16]).is_err());
    assert_eq!(std::fs::read(d.path()).unwrap(), before);
    assert_eq!(c.published_length(), 3);
}
#[test]
fn initialization_refuses_legacy_and_uninitialized_new_format_history() {
    for new_format in [false, true] {
        let d = Dir::new();
        if new_format {
            std::fs::write(d.path(), frame(KIND_APPEND, 0, b"abc", Some(0))).unwrap();
        } else {
            let mut c = open(&d.path());
            c.append(1, 0, b"old").unwrap();
            c.commit().unwrap();
            c.truncate(1, 0).unwrap();
            c.commit().unwrap();
        }
        let before = std::fs::read(d.path()).unwrap();
        let mut c = open(&d.path());
        assert!(c.initialize_publication(1, [7; 16]).is_err());
        assert_eq!(std::fs::read(d.path()).unwrap(), before);
    }
    let d = Dir::new();
    let mut c = open(&d.path());
    assert!(c.initialize_publication(0, [7; 16]).is_err());
    assert_eq!(std::fs::metadata(d.path()).unwrap().len(), 0);
}
#[test]
fn publication_mutations_require_explicit_initialization() {
    let d = Dir::new();
    let mut c = open(&d.path());
    assert!(c.append_published(1, 0, b"abc", [1; 16], 3).is_err());
    assert!(c.fence_publication(1).is_err());
    assert!(c.seal_publication(1, false).is_err());
    assert_eq!(std::fs::metadata(d.path()).unwrap().len(), 0);
}
#[test]
fn fragments_advance_visibility_only_at_the_final_record() {
    let (d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 0).unwrap();
    c.commit().unwrap();
    assert_eq!((c.length(), c.durable_published_length().unwrap()), (3, 0));
    c.append_published(1, 3, b"def", [2; 16], 6).unwrap();
    assert_eq!(c.published_length(), 6);
    assert_eq!(c.durable_published_length().unwrap(), 0);
    c.commit().unwrap();
    drop(c);
    let c = open(&d.path());
    assert_eq!(c.read_vec(0, 6).unwrap(), b"abcdef");
    assert_eq!(c.durable_published_length().unwrap(), 6);
    assert_eq!(
        c.durable_publication_record()
            .unwrap()
            .unwrap()
            .identity
            .attempt_tag,
        Some([2; 16])
    );
}
#[test]
fn a_retry_cannot_change_a_record_publication_boundary() {
    let (d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 0).unwrap();
    let before = std::fs::read(d.path()).unwrap();
    assert!(matches!(
        c.append_published(1, 0, b"abc", [1; 16], 3),
        Err(LogError::Overlap)
    ));
    assert_eq!(c.append_published(1, 0, b"abc", [1; 16], 0).unwrap(), 3);
    assert_eq!(c.published_length(), 0);
    assert_eq!(std::fs::read(d.path()).unwrap(), before);
}
#[test]
fn retries_keep_exact_tags_payload_and_record_boundaries() {
    let (_d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 0).unwrap();
    c.append_published(1, 3, b"def", [2; 16], 6).unwrap();
    for (tag, body) in [([9; 16], b"abc".as_slice()), ([1; 16], b"xyz")] {
        assert!(matches!(
            c.append_published(1, 0, body, tag, 0),
            Err(LogError::Overlap)
        ));
    }
    assert!(matches!(
        c.append_published(1, 0, b"abcdef", [1; 16], 6),
        Err(LogError::Overlap)
    ));
    assert_eq!(c.append_published(1, 0, b"abc", [1; 16], 0).unwrap(), 6);
}
#[test]
fn reordered_fragments_retain_their_publication_boundary_until_drained() {
    let (_d, mut c) = initialized();
    assert_eq!(c.append_published(1, 3, b"def", [2; 16], 6).unwrap(), 0);
    assert_eq!(c.published_length(), 0);
    assert_eq!(c.append_published(1, 0, b"abc", [1; 16], 0).unwrap(), 6);
    c.commit().unwrap();
    assert_eq!(c.durable_published_length().unwrap(), 6);
    assert_eq!(c.read_vec(0, 6).unwrap(), b"abcdef");
}
#[test]
fn a_pending_record_cannot_be_retried_with_another_publication_phase() {
    let (_d, mut c) = initialized();
    c.append_published(1, 3, b"def", [2; 16], 6).unwrap();
    assert!(matches!(
        c.append_published(1, 3, b"def", [2; 16], 0),
        Err(LogError::Overlap)
    ));
    c.append_published(1, 0, b"abc", [1; 16], 0).unwrap();
    assert_eq!(c.published_length(), 6);
}
#[test]
fn invalid_boundaries_do_not_write_or_poison_the_chunk() {
    let (d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
    let before = std::fs::read(d.path()).unwrap();
    for invalid in [0, 1, 2, 4, 5, 7, u64::MAX] {
        assert!(matches!(
            c.append_published(1, 3, b"def", [2; 16], invalid),
            Err(LogError::Bounds(_))
        ));
        assert_eq!(std::fs::read(d.path()).unwrap(), before);
    }
    c.append_published(1, 3, b"def", [2; 16], 6).unwrap();
    assert_eq!(c.published_length(), 6);
}
#[test]
fn fencing_preserves_the_complete_prefix_and_discards_only_its_incomplete_suffix() {
    let (d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
    c.append_published(1, 3, b"part", [2; 16], 3).unwrap();
    c.commit().unwrap();
    assert_eq!(c.fence_publication(2).unwrap(), 3);
    c.commit().unwrap();
    assert_eq!((c.length(), c.published_length(), c.epoch()), (3, 3, 2));
    assert!(matches!(
        c.append_published(1, 3, b"new", [3; 16], 6),
        Err(LogError::Fenced { current: 2 })
    ));
    c.append_published(2, 3, b"new", [3; 16], 6).unwrap();
    c.commit().unwrap();
    drop(c);
    let c = open(&d.path());
    assert_eq!(c.read_vec(0, 6).unwrap(), b"abcnew");
    assert_eq!(c.publication_initialization(), Some([7; 16]));
}
#[test]
fn fencing_an_unfinished_first_group_retains_initialized_empty_state() {
    let (d, mut c) = initialized();
    c.append_published(1, 0, b"part", [1; 16], 0).unwrap();
    c.append_published(1, 8, b"waiting", [2; 16], 0).unwrap();
    assert_eq!(c.fence_publication(2).unwrap(), 0);
    c.commit().unwrap();
    drop(c);
    let mut c = open(&d.path());
    assert_eq!(c.length(), 0);
    assert!(c.has_publication_records());
    assert_eq!(c.publication_initialization(), Some([7; 16]));
    assert!(c.append(2, 0, b"legacy").is_err());
    c.append_published(2, 0, b"new", [3; 16], 3).unwrap();
}
#[test]
fn repeated_fences_do_not_add_journal_records() {
    let (d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
    c.commit().unwrap();
    assert_eq!(c.fence_publication(2).unwrap(), 3);
    c.commit().unwrap();
    let before = std::fs::read(d.path()).unwrap();
    assert_eq!(c.fence_publication(2).unwrap(), 3);
    c.commit().unwrap();
    assert_eq!(std::fs::read(d.path()).unwrap(), before);
}
#[test]
fn sealing_refuses_incomplete_applied_or_waiting_records() {
    for waiting in [false, true] {
        let (d, mut c) = initialized();
        if waiting {
            c.append_published(1, 3, b"def", [1; 16], 6).unwrap();
        } else {
            c.append_published(1, 0, b"abc", [1; 16], 0).unwrap();
        }
        let before = std::fs::read(d.path()).unwrap();
        assert!(matches!(
            c.seal_publication(1, false),
            Err(LogError::SealMismatch)
        ));
        assert_eq!(std::fs::read(d.path()).unwrap(), before);
    }
}
#[test]
fn complete_publications_seal_and_replay_cold_with_digest_and_identity() {
    for digest in [false, true] {
        let (d, mut c) = initialized();
        c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
        let sealed = c.seal_publication(1, digest).unwrap();
        assert_eq!(sealed.length, 3);
        assert_eq!(sealed.sha256.is_some(), digest);
        assert_eq!(c.resident_bytes(), 0);
        drop(c);
        let c = open(&d.path());
        assert_eq!(c.read_vec(0, 3).unwrap(), b"abc");
        assert_eq!(c.durable_published_length().unwrap(), 3);
        assert_eq!(c.publication_initialization(), Some([7; 16]));
        assert_eq!(
            c.append_record_identity(0).unwrap().unwrap().attempt_tag,
            Some([1; 16])
        );
    }
}
#[test]
fn every_torn_initialization_header_recovers_as_uninitialized_empty() {
    let (d, c) = initialized();
    let bytes = std::fs::read(d.path()).unwrap();
    drop(c);
    for cut in 0..bytes.len() {
        std::fs::write(d.path(), &bytes[..cut]).unwrap();
        let c = open(&d.path());
        assert_eq!(c.length(), 0);
        assert!(!c.has_publication_records());
        assert!(c.publication_initialization().is_none());
        assert_eq!(std::fs::metadata(d.path()).unwrap().len(), 0);
    }
}
#[test]
fn complete_initialization_header_damage_is_never_erased() {
    let (d, c) = initialized();
    let bytes = std::fs::read(d.path()).unwrap();
    drop(c);
    for at in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[at] ^= 1;
        std::fs::write(d.path(), &bad).unwrap();
        assert!(
            matches!(
                LogChunk::open(&d.path(), 7, 1024 * 1024),
                Err(LogError::Corrupt(_))
            ),
            "byte={at}"
        );
        assert_eq!(std::fs::read(d.path()).unwrap(), bad);
    }
}
#[test]
fn initialization_must_be_first_and_have_canonical_fields() {
    let (d, c) = initialized();
    let bytes = std::fs::read(d.path()).unwrap();
    drop(c);
    let repeated = [bytes.clone(), bytes.clone()].concat();
    std::fs::write(d.path(), &repeated).unwrap();
    assert!(matches!(
        LogChunk::open(&d.path(), 7, 1024 * 1024),
        Err(LogError::Corrupt(_))
    ));
    assert_eq!(std::fs::read(d.path()).unwrap(), repeated);
    for at in [5, 6, 12, 20, 28, 32, 52] {
        let mut bad = bytes.clone();
        if at == 12 {
            bad[12..20].fill(0);
        } else {
            bad[at] = 1;
        }
        let crc = crc32c(&bad[..60]);
        bad[60..64].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(d.path(), &bad).unwrap();
        assert!(
            matches!(
                LogChunk::open(&d.path(), 7, 1024 * 1024),
                Err(LogError::Corrupt(_))
            ),
            "field={at}"
        );
        assert_eq!(std::fs::read(d.path()).unwrap(), bad);
    }
}
#[test]
fn an_epoch_change_cannot_complete_an_old_partial_group_without_a_fence() {
    let (_d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 0).unwrap();
    assert!(matches!(
        c.append_published(2, 3, b"def", [2; 16], 6),
        Err(LogError::Bounds(_))
    ));
    assert_eq!((c.length(), c.published_length(), c.epoch()), (3, 0, 1));
    c.fence_publication(2).unwrap();
    c.append_published(2, 0, b"new", [3; 16], 3).unwrap();
    assert_eq!(c.published_length(), 3);
}
#[test]
fn failed_chunks_cannot_acknowledge_idempotent_or_new_publication_operations() {
    let (d, mut c) = initialized();
    let before = std::fs::read(d.path()).unwrap();
    c.failed = true;
    assert!(matches!(
        c.initialize_publication(1, [7; 16]),
        Err(LogError::Unavailable)
    ));
    assert!(matches!(c.fence_publication(1), Err(LogError::Unavailable)));
    assert!(matches!(
        c.append_published(1, 0, b"x", [1; 16], 1),
        Err(LogError::Unavailable)
    ));
    assert!(matches!(
        c.seal_publication(1, false),
        Err(LogError::Unavailable)
    ));
    assert_eq!(std::fs::read(d.path()).unwrap(), before);
}
#[test]
fn captured_sync_lengths_do_not_publish_later_records() {
    let (_d, mut c) = initialized();
    c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
    c.commit().unwrap();
    let (_file, length) = c.sync_handle().unwrap();
    c.append_published(1, 3, b"def", [2; 16], 6).unwrap();
    c.note_durable(length);
    assert_eq!(c.durable_published_length().unwrap(), 3);
    assert_eq!(
        c.durable_publication_record()
            .unwrap()
            .unwrap()
            .identity
            .attempt_tag,
        Some([1; 16])
    );
    c.commit().unwrap();
    assert_eq!(c.durable_published_length().unwrap(), 6);
}
#[cfg(feature = "replica-server")]
#[test]
fn replica_wire_reads_initialized_files_but_does_not_enable_a_write_emitter() {
    use crate::cluster::{
        LogOpRequest, RemoteReplicaClient, ReplicaClientOptions, ReplicaNodeOptions,
        ReplicaNodeServer,
    };
    let d = Dir::new();
    let logs = d.0.join("logchunks");
    std::fs::create_dir(&logs).unwrap();
    let path = logs.join("0000000000000003-000000000000000b.log");
    let mut c = open(&path);
    c.initialize_publication(1, [7; 16]).unwrap();
    c.append_published(1, 0, b"abc", [1; 16], 3).unwrap();
    c.commit().unwrap();
    drop(c);
    let before = std::fs::read(&path).unwrap();
    let server = ReplicaNodeServer::bind_with_journal(
        "127.0.0.1:0",
        ReplicaNodeOptions::default(),
        &d.0.join("journal"),
    )
    .unwrap();
    let addr = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve());
    let outcome = std::panic::catch_unwind(|| {
        let mut client =
            RemoteReplicaClient::connect(&addr, &ReplicaClientOptions::default()).unwrap();
        let r = client
            .log_call(LogOpRequest::publication_tail(3, 11, 7))
            .unwrap();
        assert_eq!(r.published_prefix().unwrap().length, 3);
        assert_eq!(
            client
                .log_call(LogOpRequest::publication_read(3, 11, 7, 0, 3))
                .unwrap()
                .data,
            b"abc"
        );
        assert!(client
            .log_call(LogOpRequest::append_identified(
                3,
                11,
                7,
                1,
                3,
                b"new".to_vec(),
                [2; 16]
            ))
            .is_err());
    });
    stop.stop();
    worker.join().unwrap().unwrap();
    outcome.unwrap();
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[cfg(unix)]
#[test]
fn initialization_directory_sync_failure_cannot_be_acknowledged_even_on_a_retry() {
    for already_initialized in [false, true] {
        let d = Dir::new();
        let mut c = open(&d.path());
        if already_initialized {
            c.initialize_publication(1, [7; 16]).unwrap();
        }
        c.path = d.0.join("missing-parent").join("chunk");
        assert!(matches!(
            c.initialize_publication(1, [7; 16]),
            Err(LogError::Io(_))
        ));
        assert!(matches!(
            c.initialize_publication(1, [7; 16]),
            Err(LogError::Unavailable)
        ));
        assert!(matches!(
            c.append_published(1, 0, b"x", [1; 16], 1),
            Err(LogError::Unavailable)
        ));
        assert_eq!(std::fs::metadata(d.path()).unwrap().len(), 64);
    }
}
