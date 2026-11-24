// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::log_chunk::crc32c;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "publication-protocol-{}-{}",
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
fn publication_record(published: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; 64];
    bytes[..4].copy_from_slice(b"MOL3");
    bytes[4] = 1;
    bytes[8..12].copy_from_slice(&7u32.to_le_bytes());
    bytes[12..20].copy_from_slice(&1u64.to_le_bytes());
    bytes[28..32].copy_from_slice(&3u32.to_le_bytes());
    bytes[32..36].copy_from_slice(&crc32c(b"abc").to_le_bytes());
    bytes[36..52].fill(7);
    bytes[52..60].copy_from_slice(&published.to_le_bytes());
    let crc = crc32c(&bytes[..60]);
    bytes[60..64].copy_from_slice(&crc.to_le_bytes());
    bytes.extend_from_slice(b"abc");
    bytes
}
fn refused(error: ObjectError) {
    assert!(
        matches!(error,ObjectError::PreconditionFailed(ref s) if s=="publication-aware log protocol is required"),
        "{error:?}"
    );
}
#[test]
fn old_tail_read_and_identity_requests_never_expose_publication_aware_files() {
    for published in [0, 3] {
        let d = Dir::new();
        let mut store = LogChunkStore::open(&d.0).unwrap();
        let path = store.path(3, 11);
        let bytes = publication_record(published);
        std::fs::write(&path, &bytes).unwrap();
        for request in [
            LogOpRequest::tail(3, 11, 7),
            LogOpRequest::read(3, 11, 7, 0, 3),
            LogOpRequest::identity(3, 11, 7, 0),
        ] {
            refused(store.apply(&request).unwrap_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        assert!(!store.has_dirty());
        assert_eq!(store.commits(), 0);
    }
}
#[test]
fn old_synchronous_and_deferred_mutations_leave_new_format_unchanged() {
    let d = Dir::new();
    let mut store = LogChunkStore::open(&d.0).unwrap();
    let path = store.path(3, 11);
    let bytes = publication_record(3);
    std::fs::write(&path, &bytes).unwrap();
    for request in [
        LogOpRequest::append(3, 11, 7, 2, 3, b"x".to_vec()),
        LogOpRequest::append_identified(3, 11, 7, 2, 3, b"x".to_vec(), [8; 16]),
        LogOpRequest::truncate(3, 11, 7, 2, 0),
        LogOpRequest::seal(3, 11, 7, 2, false),
    ] {
        refused(store.apply(&request).unwrap_err());
        refused(store.apply_deferred(&request).unwrap_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(!store.has_dirty());
        assert_eq!(store.commits(), 0);
    }
}
#[test]
fn a_truncated_empty_file_retains_its_new_protocol_floor() {
    let d = Dir::new();
    let mut store = LogChunkStore::open(&d.0).unwrap();
    let path = store.path(3, 11);
    let mut bytes = publication_record(3);
    let mut cut = [0u8; 40];
    cut[..4].copy_from_slice(b"MOL1");
    cut[4] = 3;
    cut[8..12].copy_from_slice(&7u32.to_le_bytes());
    cut[12..20].copy_from_slice(&2u64.to_le_bytes());
    let crc = crc32c(&cut[..36]);
    cut[36..40].copy_from_slice(&crc.to_le_bytes());
    bytes.extend_from_slice(&cut);
    std::fs::write(&path, &bytes).unwrap();
    refused(store.apply(&LogOpRequest::tail(3, 11, 7)).unwrap_err());
    refused(
        store
            .apply(&LogOpRequest::append(3, 11, 7, 3, 0, b"x".to_vec()))
            .unwrap_err(),
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    // Refusing a new-format file does not disable other legacy chunks in this store.
    let r = store
        .apply(&LogOpRequest::append(3, 12, 7, 3, 0, b"old".to_vec()))
        .unwrap();
    assert_eq!(r.durable_length, 3);
    assert_eq!(
        store
            .apply(&LogOpRequest::read(3, 12, 7, 0, 3))
            .unwrap()
            .data,
        b"old"
    );
}
