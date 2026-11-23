// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "identified-writes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
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
fn open(p: &Path) -> LogChunk {
    LogChunk::open(p, 3, 1 << 20).unwrap()
}
fn tag(n: u8) -> [u8; 16] {
    [n; 16]
}
#[test]
fn identified_write_is_durable_with_its_exact_record_identity_after_reopen() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    assert_eq!(c.append_identified(1, 0, b"hello", tag(1)).unwrap(), 5);
    assert_eq!(c.append_record_identity(0).unwrap(), None);
    c.commit().unwrap();
    drop(c);
    let c = open(&p);
    assert_eq!(std::fs::metadata(&p).unwrap().len(), 61);
    assert_eq!(
        c.append_record_identity(0).unwrap(),
        Some(AppendRecordIdentity {
            offset: 0,
            length: 5,
            attempt_tag: Some(tag(1))
        })
    );
    assert_eq!(c.read_vec(0, 5).unwrap(), b"hello");
}
#[test]
fn exact_attempt_retry_does_not_write_another_record_before_or_after_restart() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    let bytes = std::fs::read(&p).unwrap();
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    c.commit().unwrap();
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    drop(c);
    let mut c = open(&p);
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    c.commit().unwrap();
    assert_eq!(std::fs::read(&p).unwrap(), bytes);
    assert_eq!(c.extent_count(), 1);
}
#[test]
fn equal_bytes_with_another_attempt_tag_are_refused_without_mutation() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    c.commit().unwrap();
    let before = std::fs::read(&p).unwrap();
    assert!(matches!(
        c.append_identified(1, 0, b"hello", tag(2)),
        Err(LogError::Overlap)
    ));
    assert_eq!(std::fs::read(&p).unwrap(), before);
    assert_eq!(c.durable_length(), 5);
}
#[test]
fn one_tag_cannot_authorize_a_subrange_or_an_overlapping_extension() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    c.commit().unwrap();
    let before = std::fs::read(&p).unwrap();
    for (offset, data) in [
        (0, &b"hell"[..]),
        (1, &b"ello"[..]),
        (0, &b"hello!"[..]),
        (3, &b"lo!"[..]),
    ] {
        assert!(matches!(
            c.append_identified(1, offset, data, tag(1)),
            Err(LogError::Overlap)
        ));
        assert_eq!(std::fs::read(&p).unwrap(), before);
    }
}
#[test]
fn legacy_and_identified_retries_cannot_claim_each_others_records() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append(1, 0, b"old").unwrap();
    c.append_identified(1, 3, b"new", tag(1)).unwrap();
    c.commit().unwrap();
    let before = std::fs::read(&p).unwrap();
    assert!(matches!(
        c.append_identified(1, 0, b"old", tag(1)),
        Err(LogError::Overlap)
    ));
    assert!(matches!(c.append(1, 3, b"new"), Err(LogError::Overlap)));
    assert!(matches!(c.append(1, 4, b"ew!"), Err(LogError::Overlap)));
    assert_eq!(std::fs::read(&p).unwrap(), before);
    c.append(1, 6, b"tail").unwrap();
}
#[test]
fn out_of_order_records_keep_their_own_tags_when_the_gap_is_filled() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    assert_eq!(c.append_identified(1, 3, b"two", tag(2)).unwrap(), 0);
    assert_eq!(c.pending_records(), 1);
    assert_eq!(c.append_identified(1, 0, b"one", tag(1)).unwrap(), 6);
    c.commit().unwrap();
    drop(c);
    let c = open(&p);
    assert_eq!(c.read_vec(0, 6).unwrap(), b"onetwo");
    assert_eq!(
        c.append_record_identity(0).unwrap().unwrap().attempt_tag,
        Some(tag(1))
    );
    assert_eq!(
        c.append_record_identity(3).unwrap().unwrap().attempt_tag,
        Some(tag(2))
    );
}
#[test]
fn pending_attempts_reject_wrong_identity_and_crossing_records_before_writing() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(1, 3, b"two", tag(2)).unwrap();
    assert!(matches!(
        c.append_identified(1, 3, b"two", tag(3)),
        Err(LogError::Overlap)
    ));
    assert!(matches!(c.append(1, 0, b"onetwo"), Err(LogError::Overlap)));
    assert!(matches!(
        c.append_identified(1, 0, b"onetwo", tag(1)),
        Err(LogError::Overlap)
    ));
    assert_eq!(std::fs::metadata(&p).unwrap().len(), 0);
    assert_eq!(c.pending_records(), 1);
    c.append_identified(1, 3, b"two", tag(2)).unwrap();
    c.append_identified(1, 0, b"one", tag(1)).unwrap();
    c.commit().unwrap();
}
#[test]
fn tagged_pending_records_obey_the_existing_record_and_byte_limits() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.set_reorder_window(1, 3);
    c.append_identified(1, 3, b"two", tag(2)).unwrap();
    assert!(matches!(
        c.append_identified(1, 6, b"end", tag(3)),
        Err(LogError::Window)
    ));
    assert_eq!(c.pending_records(), 1);
    assert_eq!(c.pending_bytes, 3);
    c.append_identified(1, 0, b"one", tag(1)).unwrap();
    assert_eq!(c.pending_bytes, 0);
    c.commit().unwrap();
}
#[test]
fn a_new_epoch_must_fence_pending_attempts_instead_of_promoting_them() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(2, 3, b"old", tag(2)).unwrap();
    assert!(matches!(
        c.append_identified(1, 0, b"one", tag(1)),
        Err(LogError::Fenced { current: 2 })
    ));
    assert!(matches!(
        c.append_identified(3, 0, b"one", tag(1)),
        Err(LogError::Overlap)
    ));
    assert_eq!(std::fs::metadata(&p).unwrap().len(), 0);
    c.truncate(3, 0).unwrap();
    c.commit().unwrap();
    assert_eq!(c.pending_records(), 0);
    c.append_identified(3, 0, b"one", tag(1)).unwrap();
    c.commit().unwrap();
    assert!(matches!(
        c.append_identified(2, 3, b"old", tag(2)),
        Err(LogError::Fenced { current: 3 })
    ));
    drop(c);
    let c = open(&p);
    assert_eq!(c.read_vec(0, 3).unwrap(), b"one");
    assert_eq!(c.epoch(), 3);
}
#[test]
fn truncation_does_not_reassign_a_removed_attempt_to_replacement_bytes() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    c.commit().unwrap();
    c.truncate(2, 0).unwrap();
    c.commit().unwrap();
    c.append_identified(2, 0, b"hello", tag(2)).unwrap();
    c.commit().unwrap();
    drop(c);
    let mut c = open(&p);
    assert_eq!(
        c.append_record_identity(0).unwrap().unwrap().attempt_tag,
        Some(tag(2))
    );
    assert!(matches!(
        c.append_identified(2, 0, b"hello", tag(1)),
        Err(LogError::Overlap)
    ));
}
#[test]
fn sealed_identified_records_are_readable_but_refuse_further_writes() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    c.commit().unwrap();
    c.seal(1, true).unwrap();
    c.commit().unwrap();
    drop(c);
    let mut c = open(&p);
    assert_eq!(c.resident_bytes(), 0);
    assert_eq!(c.read_vec(0, 5).unwrap(), b"hello");
    assert!(matches!(
        c.append_identified(1, 5, b"world", tag(2)),
        Err(LogError::Sealed)
    ));
    assert_eq!(
        c.append_record_identity(0).unwrap().unwrap().attempt_tag,
        Some(tag(1))
    );
}
#[test]
fn a_retry_rechecks_the_persistent_attempt_header() {
    let d = Dir::new();
    let p = d.file();
    let mut c = open(&p);
    c.append_identified(1, 0, b"hello", tag(1)).unwrap();
    c.commit().unwrap();
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[36] ^= 1;
    std::fs::write(&p, &bytes).unwrap();
    assert!(matches!(
        c.append_identified(1, 0, b"hello", tag(1)),
        Err(LogError::Corrupt(_))
    ));
    assert_eq!(std::fs::read(&p).unwrap(), bytes);
}
