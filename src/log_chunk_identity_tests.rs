// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "identified-log-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self) -> PathBuf {
        self.0.join("chunk")
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn frame(offset: u64, data: &[u8], tag: Option<[u8; 16]>) -> Vec<u8> {
    let h = Header {
        kind: KIND_APPEND,
        flags: 0,
        generation: 7,
        epoch: 1,
        offset,
        length: data.len() as u32,
        data_crc: crc32c(data),
        attempt_tag: tag,
        published_length: None,
    };
    let mut bytes = h.encode()[..h.encoded_len()].to_vec();
    bytes.extend_from_slice(data);
    bytes
}
fn open(path: &Path) -> LogChunk {
    LogChunk::open(path, 7, 1 << 20).unwrap()
}
#[test]
fn identified_header_layout_and_replayed_identity_are_exact() {
    assert_eq!((LOG_HEADER_LEN, IDENTIFIED_LOG_HEADER_LEN), (40, 56));
    assert!(std::mem::size_of::<Extent>() <= 32);
    let d = Dir::new();
    let p = d.file();
    let tag = [0x5a; 16];
    let bytes = frame(0, b"hello", Some(tag));
    assert_eq!(&bytes[..4], IDENTIFIED_MAGIC);
    assert_eq!(&bytes[36..52], &tag);
    assert_eq!(
        u32::from_le_bytes(bytes[52..56].try_into().unwrap()),
        crc32c(&bytes[..52])
    );
    std::fs::write(&p, bytes).unwrap();
    let c = open(&p);
    assert_eq!(c.read_vec(0, 5).unwrap(), b"hello");
    assert_eq!(
        c.append_record_identity(0).unwrap(),
        Some(AppendRecordIdentity {
            offset: 0,
            length: 5,
            attempt_tag: Some(tag)
        })
    );
}
#[test]
fn mixed_headers_replay_and_existing_writer_keeps_legacy_encoding() {
    let d = Dir::new();
    let p = d.file();
    let mut bytes = frame(0, b"old", None);
    bytes.extend(frame(3, b"new", Some([9; 16])));
    std::fs::write(&p, &bytes).unwrap();
    let mut c = open(&p);
    c.append(1, 6, b"tail").unwrap();
    c.commit().unwrap();
    drop(c);
    assert_eq!(
        &std::fs::read(&p).unwrap()[bytes.len()..bytes.len() + 4],
        MAGIC
    );
    let c = open(&p);
    assert_eq!(c.read_vec(0, 10).unwrap(), b"oldnewtail");
    assert_eq!(
        c.append_record_identity(0).unwrap().unwrap().attempt_tag,
        None
    );
    assert_eq!(
        c.append_record_identity(3).unwrap().unwrap().attempt_tag,
        Some([9; 16])
    );
}
#[test]
fn interior_missing_and_truncated_records_have_no_complete_identity() {
    let d = Dir::new();
    let p = d.file();
    std::fs::write(&p, frame(0, b"hello", Some([1; 16]))).unwrap();
    let mut c = open(&p);
    assert_eq!(c.append_record_identity(1).unwrap(), None);
    assert_eq!(c.append_record_identity(5).unwrap(), None);
    c.truncate(1, 3).unwrap();
    c.commit().unwrap();
    assert_eq!(c.append_record_identity(0).unwrap(), None);
    drop(c);
    assert_eq!(open(&p).append_record_identity(0).unwrap(), None);
}
#[test]
fn identity_survives_sealing_and_cold_replay_without_payload_residency() {
    let d = Dir::new();
    let p = d.file();
    std::fs::write(&p, frame(0, b"hello", Some([2; 16]))).unwrap();
    let mut c = open(&p);
    c.seal(1, true).unwrap();
    c.commit().unwrap();
    drop(c);
    let c = open(&p);
    assert_eq!(c.resident_bytes(), 0);
    assert_eq!(c.read_vec(0, 5).unwrap(), b"hello");
    assert_eq!(
        c.append_record_identity(0).unwrap().unwrap().attempt_tag,
        Some([2; 16])
    );
}
#[test]
fn every_complete_header_byte_is_protected_without_destructive_recovery() {
    let d = Dir::new();
    let p = d.file();
    let original = frame(0, b"hello", Some([3; 16]));
    for i in 0..IDENTIFIED_LOG_HEADER_LEN {
        let mut bytes = original.clone();
        bytes[i] ^= 0x80;
        std::fs::write(&p, &bytes).unwrap();
        assert!(LogChunk::open(&p, 7, 1 << 20).is_err(), "byte {i}");
        assert_eq!(std::fs::read(&p).unwrap(), bytes, "byte {i}");
    }
    // A changed version byte must not turn a complete short legacy record into a torn tail.
    for len in 0..16 {
        let mut bytes = frame(0, &vec![7; len], None);
        bytes[..4].copy_from_slice(IDENTIFIED_MAGIC);
        std::fs::write(&p, &bytes).unwrap();
        assert!(
            LogChunk::open(&p, 7, 1 << 20).is_err(),
            "legacy payload length {len}"
        );
        assert_eq!(std::fs::read(&p).unwrap(), bytes);
    }
}
#[test]
fn each_incomplete_identified_tail_preserves_its_valid_predecessor() {
    let d = Dir::new();
    let p = d.file();
    let prefix = frame(0, b"old", None);
    let tail = frame(3, b"hello", Some([4; 16]));
    for cut in 1..tail.len() {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&tail[..cut]);
        std::fs::write(&p, bytes).unwrap();
        let c = open(&p);
        assert_eq!(c.read_vec(0, 3).unwrap(), b"old", "cut {cut}");
        drop(c);
        assert_eq!(std::fs::read(&p).unwrap(), prefix, "cut {cut}");
    }
}
#[test]
fn damaged_complete_payload_is_not_discarded_as_a_torn_tail() {
    let d = Dir::new();
    let p = d.file();
    let mut bytes = frame(0, b"hello", Some([5; 16]));
    bytes[56] ^= 1;
    std::fs::write(&p, &bytes).unwrap();
    assert!(LogChunk::open(&p, 7, 1 << 20).is_err());
    assert_eq!(std::fs::read(&p).unwrap(), bytes);
}
#[test]
fn uncommitted_records_have_no_durable_identity() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append(1, 0, b"hello").unwrap();
    assert_eq!(c.append_record_identity(0).unwrap(), None);
    c.commit().unwrap();
    assert_eq!(c.append_record_identity(0).unwrap().unwrap().length, 5);
}
#[test]
fn identity_lookup_rechecks_header_crc_and_index_binding() {
    let d = Dir::new();
    let p = d.file();
    let original = frame(0, b"hello", Some([6; 16]));
    std::fs::write(&p, &original).unwrap();
    let c = open(&p);
    let mut bytes = original.clone();
    bytes[36] ^= 1;
    std::fs::write(&p, &bytes).unwrap();
    assert!(c.append_record_identity(0).is_err());
    let mut bytes = original;
    bytes[8..12].copy_from_slice(&8u32.to_le_bytes());
    let crc = crc32c(&bytes[..52]);
    bytes[52..56].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&p, bytes).unwrap();
    assert!(c.append_record_identity(0).is_err());
}
