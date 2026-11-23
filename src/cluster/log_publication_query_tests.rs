// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::log_chunk::crc32c;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "publication-query-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn record(offset: u64, body: &[u8], published: u64) -> Vec<u8> {
    let mut b = vec![0u8; 64];
    b[..4].copy_from_slice(b"MOL3");
    b[4] = 1;
    b[8..12].copy_from_slice(&7u32.to_le_bytes());
    b[12..20].copy_from_slice(&1u64.to_le_bytes());
    b[20..28].copy_from_slice(&offset.to_le_bytes());
    b[28..32].copy_from_slice(&(body.len() as u32).to_le_bytes());
    b[32..36].copy_from_slice(&crc32c(body).to_le_bytes());
    b[36..52].fill(offset as u8);
    b[52..60].copy_from_slice(&published.to_le_bytes());
    let crc = crc32c(&b[..60]);
    b[60..64].copy_from_slice(&crc.to_le_bytes());
    b.extend_from_slice(body);
    b
}
fn fixture(frames: &[Vec<u8>]) -> (Dir, LogChunkStore, PathBuf) {
    let d = Dir::new();
    let store = LogChunkStore::open(&d.0).unwrap();
    let path = store.path(3, 11);
    std::fs::write(&path, frames.concat()).unwrap();
    (d, store, path)
}
fn tail(store: &mut LogChunkStore) -> LogOpReply {
    store
        .apply(&LogOpRequest::publication_tail(3, 11, 7))
        .unwrap()
}
fn valid_reply() -> LogOpReply {
    LogOpReply {
        length: 6,
        durable_length: 6,
        published_length: 3,
        publication_mode: true,
        publication_supported: true,
        chunk_present: true,
        record_present: true,
        record_identified: true,
        record_length: 3,
        record_publication: true,
        record_published_length: 3,
        ..Default::default()
    }
}
#[test]
fn read_only_version3_constructors_roundtrip_in_borrowed_and_owned_decoders() {
    for r in [
        LogOpRequest::publication_tail(3, 11, 7),
        LogOpRequest::publication_read(3, 11, 7, 1, 2),
        LogOpRequest::publication_identity(3, 11, 7, 3),
    ] {
        assert_eq!(r.version, 3);
        assert!(!r.is_mutation());
        let b = r.encode_to_vec();
        assert_eq!(LogOpRequest::from_payload(&b).unwrap(), r);
        assert_eq!(LogOpRequest::from_payload_owned(b).unwrap(), r);
    }
}
#[test]
fn version3_rejects_write_operations_and_creates_no_chunk() {
    let d = Dir::new();
    let mut store = LogChunkStore::open(&d.0).unwrap();
    for mut r in [
        LogOpRequest::append(3, 11, 7, 1, 0, b"x".to_vec()),
        LogOpRequest::append_identified(3, 11, 7, 1, 0, b"x".to_vec(), [7; 16]),
        LogOpRequest::truncate(3, 11, 7, 1, 0),
        LogOpRequest::seal(3, 11, 7, 1, false),
    ] {
        r.version = 3;
        assert!(r.validate().is_err());
        assert!(store.apply(&r).is_err());
        assert!(LogOpRequest::from_payload(&r.encode_to_vec()).is_err());
    }
    assert_eq!(std::fs::read_dir(&d.0).unwrap().count(), 0);
}
#[test]
fn an_absent_chunk_explicitly_reports_support_without_reserving_storage() {
    let d = Dir::new();
    let mut store = LogChunkStore::open(&d.0).unwrap();
    let r = tail(&mut store);
    assert!(r.has_publication_observation());
    assert!(!r.chunk_present);
    assert!(r.published_prefix().is_none());
    assert!(!r.publication_mode);
    assert_eq!(std::fs::read_dir(&d.0).unwrap().count(), 0);
    assert!(!LogOpReply::default().has_publication_observation());
    assert!(LogOpReply::default().published_prefix().is_none());
}
#[test]
fn an_incomplete_fragment_is_physical_only_but_keeps_exact_identity() {
    let (_d, mut store, path) = fixture(&[record(0, b"abc", 0)]);
    let before = std::fs::read(&path).unwrap();
    let r = tail(&mut store);
    assert_eq!((r.length, r.durable_length, r.published_length), (3, 3, 0));
    assert!(r.publication_mode && r.has_publication_observation());
    assert!(!r.record_present);
    assert_eq!(r.published_prefix().unwrap().length, 0);
    assert!(store
        .apply(&LogOpRequest::publication_read(3, 11, 7, 0, 3))
        .unwrap()
        .data
        .is_empty());
    let id = store
        .apply(&LogOpRequest::publication_identity(3, 11, 7, 0))
        .unwrap();
    assert!(id.record_publication && id.record_identified);
    assert_eq!(id.record_published_length, 0);
    assert_eq!(id.record_identity().unwrap().attempt_tag, Some([0; 16]));
    assert_eq!(std::fs::read(path).unwrap(), before);
}
#[test]
fn tail_identity_names_the_published_record_not_the_last_physical_record() {
    let (_d, mut store, _p) = fixture(&[record(0, b"abc", 3), record(3, b"def", 3)]);
    let r = tail(&mut store);
    assert_eq!(
        (
            r.length,
            r.published_length,
            r.record_offset,
            r.record_length
        ),
        (6, 3, 0, 3)
    );
    assert_eq!(r.record_published_length, 3);
    let p = r.published_prefix().unwrap();
    assert_eq!(p.length, 3);
    assert_eq!(p.tip.unwrap().attempt_tag, Some([0; 16]));
}
#[test]
fn a_multi_record_publication_returns_its_final_identity() {
    let (_d, mut store, _p) = fixture(&[record(0, b"abc", 0), record(3, b"def", 6)]);
    let r = tail(&mut store);
    let p = r.published_prefix().unwrap();
    assert_eq!(p.length, 6);
    let id = p.tip.unwrap();
    assert_eq!(
        (id.offset, id.length, id.attempt_tag),
        (3, 3, Some([3; 16]))
    );
    assert!(r.record_publication);
    assert_eq!(r.record_published_length, 6);
}
#[test]
fn version3_reads_never_return_unpublished_suffix_bytes() {
    let (_d, mut store, _p) = fixture(&[record(0, b"abc", 3), record(3, b"def", 3)]);
    for (offset, want) in [
        (0, b"abc".as_slice()),
        (1, b"bc"),
        (3, b""),
        (4, b""),
        (u64::MAX, b""),
    ] {
        let r = store
            .apply(&LogOpRequest::publication_read(3, 11, 7, offset, 100))
            .unwrap();
        assert_eq!(r.data, want);
        assert_eq!(r.published_length, 3);
        assert!(r.has_publication_observation());
    }
}
#[test]
fn version3_identity_observes_complete_unpublished_records_without_promising_settlement() {
    let (_d, mut store, _p) = fixture(&[record(0, b"abc", 3), record(3, b"def", 3)]);
    let r = store
        .apply(&LogOpRequest::publication_identity(3, 11, 7, 3))
        .unwrap();
    assert_eq!(r.record_offset, 3);
    assert_eq!(r.record_published_length, 3);
    assert!(r.record_present && r.record_publication);
    assert!(r.published_prefix().is_none());
    for offset in [4, 6, 7] {
        assert!(
            !store
                .apply(&LogOpRequest::publication_identity(3, 11, 7, offset))
                .unwrap()
                .record_present
        );
    }
    assert!(!store.has_dirty());
    assert_eq!(store.commits(), 0);
}
#[test]
fn version3_queries_do_not_enable_older_wire_access_to_new_files() {
    let (_d, mut store, _p) = fixture(&[record(0, b"abc", 3)]);
    assert!(tail(&mut store).has_publication_observation());
    for r in [
        LogOpRequest::tail(3, 11, 7),
        LogOpRequest::read(3, 11, 7, 0, 3),
        LogOpRequest::identity(3, 11, 7, 0),
    ] {
        assert!(store.apply(&r).is_err());
    }
}
#[test]
fn version3_storage_observations_do_not_upgrade_nonempty_legacy_files() {
    let d = Dir::new();
    let mut store = LogChunkStore::open(&d.0).unwrap();
    store
        .apply(&LogOpRequest::append(3, 11, 7, 1, 0, b"old".to_vec()))
        .unwrap();
    let before = std::fs::read(store.path(3, 11)).unwrap();
    let old = store.apply(&LogOpRequest::tail(3, 11, 7)).unwrap();
    assert!(!old.publication_supported);
    assert_eq!(old.published_length, 0);
    assert_eq!(
        old.encode_to_vec(),
        LogOpReply {
            length: 3,
            durable_length: 3,
            epoch: 1,
            crc: crc32c(b"old"),
            ..Default::default()
        }
        .encode_to_vec()
    );
    let new = tail(&mut store);
    assert!(new.has_publication_observation());
    assert!(!new.publication_mode);
    assert_eq!(new.published_length, 3);
    assert!(new.published_prefix().is_none());
    assert_eq!(std::fs::read(store.path(3, 11)).unwrap(), before);
}
#[test]
fn extended_reply_owned_decode_preserves_its_data_allocation() {
    let mut r = valid_reply();
    r.data = b"abc".to_vec();
    r.sha256 = vec![9; 32];
    let mut bytes = r.encode_to_vec();
    assert_eq!(LogOpReply::from_payload(&bytes).unwrap(), r);
    bytes.reserve(128);
    let pointer = bytes.as_ptr();
    let capacity = bytes.capacity();
    let got = LogOpReply::from_payload_owned(bytes).unwrap();
    assert_eq!(got, r);
    assert_eq!(got.data.as_ptr(), pointer);
    assert_eq!(got.data.capacity(), capacity);
    assert_eq!(got.published_prefix().unwrap().length, 3);
}
#[test]
fn invalid_publication_reply_shapes_are_refused_by_both_decoders() {
    let r = valid_reply();
    let bad = [
        LogOpReply {
            publication_supported: false,
            ..r.clone()
        },
        LogOpReply {
            published_length: 7,
            ..r.clone()
        },
        LogOpReply {
            durable_length: 7,
            ..r.clone()
        },
        LogOpReply {
            publication_mode: false,
            ..r.clone()
        },
        LogOpReply {
            record_identified: false,
            ..r.clone()
        },
        LogOpReply {
            record_offset: 1,
            record_published_length: 2,
            ..r.clone()
        },
        LogOpReply {
            record_published_length: 6,
            ..r.clone()
        },
        LogOpReply {
            record_length: 0,
            ..r.clone()
        },
        LogOpReply {
            record_present: false,
            ..r.clone()
        },
        LogOpReply {
            record_publication: false,
            ..r
        },
    ];
    for b in bad {
        let bytes = b.encode_to_vec();
        assert!(LogOpReply::from_payload(&bytes).is_err(), "{b:?}");
        assert!(LogOpReply::from_payload_owned(bytes).is_err(), "{b:?}");
    }
}
#[test]
fn extended_fields_still_require_canonical_order_defaults_and_boolean_widths() {
    let r = valid_reply();
    let bytes = r.encode_to_vec();
    for suffix in [
        vec![0x70, 1],
        vec![0x88, 1, 1],
        vec![0x98, 1, 1],
        vec![0xa0, 1, 1],
        vec![0x90, 1, 2],
    ] {
        let mut b = bytes.clone();
        b.extend(suffix);
        assert!(LogOpReply::from_payload(&b).is_err());
        assert!(LogOpReply::from_payload_owned(b).is_err());
    }
    for b in [vec![0x88, 1, 0], vec![0x88, 1, 2], vec![0x88, 1, 0x81, 0]] {
        assert!(LogOpReply::from_payload(&b).is_err());
        assert!(LogOpReply::from_payload_owned(b).is_err());
    }
    let mut request = LogOpRequest::publication_tail(3, 11, 7);
    request.attempt_hi = 1;
    assert!(request.validate().is_err());
}
#[test]
fn version3_prefix_queries_roundtrip_through_the_replica_server() {
    use crate::cluster::{
        RemoteReplicaClient, ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer,
    };
    let d = Dir::new();
    let logs = d.0.join("logchunks");
    std::fs::create_dir(&logs).unwrap();
    let path = logs.join("0000000000000003-000000000000000b.log");
    let before = [record(0, b"abc", 3), record(3, b"def", 3)].concat();
    std::fs::write(&path, &before).unwrap();
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
        let mut c = RemoteReplicaClient::connect(&addr, &ReplicaClientOptions::default()).unwrap();
        let r = c
            .log_call(LogOpRequest::publication_tail(3, 11, 7))
            .unwrap();
        assert_eq!(r.published_prefix().unwrap().length, 3);
        assert_eq!(
            c.log_call(LogOpRequest::publication_read(3, 11, 7, 0, 6))
                .unwrap()
                .data,
            b"abc"
        );
        let id = c
            .log_call(LogOpRequest::publication_identity(3, 11, 7, 3))
            .unwrap();
        assert_eq!(id.record_identity().unwrap().attempt_tag, Some([3; 16]));
        assert!(c.log_call(LogOpRequest::tail(3, 11, 7)).is_err());
    });
    stop.stop();
    worker.join().unwrap().unwrap();
    assert_eq!(std::fs::read(path).unwrap(), before);
    outcome.unwrap();
}
#[test]
fn a_truncated_empty_publication_file_reports_a_zero_prefix_without_downgrading() {
    let mut frames = vec![record(0, b"abc", 3)];
    let mut cut = vec![0; 40];
    cut[..4].copy_from_slice(b"MOL1");
    cut[4] = 3;
    cut[8..12].copy_from_slice(&7u32.to_le_bytes());
    cut[12..20].copy_from_slice(&2u64.to_le_bytes());
    let crc = crc32c(&cut[..36]);
    cut[36..40].copy_from_slice(&crc.to_le_bytes());
    frames.push(cut);
    let (_d, mut store, path) = fixture(&frames);
    let before = std::fs::read(&path).unwrap();
    let r = tail(&mut store);
    assert!(r.publication_mode && r.has_publication_observation());
    assert_eq!((r.length, r.published_length), (0, 0));
    assert!(r.published_prefix().unwrap().tip.is_none());
    assert!(store.apply(&LogOpRequest::tail(3, 11, 7)).is_err());
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn missing_and_uninitialized_members_never_supply_an_empty_prefix_vote() {
    let d = Dir::new();
    let mut store = LogChunkStore::open(&d.0).unwrap();
    let missing = tail(&mut store);
    assert!(missing.has_publication_observation());
    assert!(!missing.chunk_present);
    assert!(missing.published_prefix().is_none());
    store
        .apply(&LogOpRequest::truncate(3, 11, 7, 1, 0))
        .unwrap();
    let empty = tail(&mut store);
    assert!(empty.has_publication_observation() && empty.chunk_present);
    assert!(!empty.publication_mode);
    assert!(empty.published_prefix().is_none());
    for forged in [
        LogOpReply {
            chunk_present: false,
            ..valid_reply()
        },
        LogOpReply {
            publication_mode: true,
            ..missing
        },
    ] {
        let bytes = forged.encode_to_vec();
        assert!(LogOpReply::from_payload(&bytes).is_err());
        assert!(LogOpReply::from_payload_owned(bytes).is_err());
    }
}
