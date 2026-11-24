// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "native-authority-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn owner() -> StorageOwner {
    StorageOwner {
        volume: 3,
        chunk: 11,
        generation: 1,
        epoch: 9,
        initialization: vec![7; 16],
        state: PENDING,
    }
}
#[test]
fn reservation_and_ready_identity_survive_reopen() {
    let d = Directory::new();
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    let root = a.incarnation().unwrap();
    a.begin(owner(), false).unwrap();
    assert!(a.authorize(3, 11, 1, [7; 16]).is_err());
    drop(a);
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    assert_eq!(a.incarnation().unwrap(), root);
    a.begin(owner(), false).unwrap();
    a.complete(3, 11).unwrap();
    a.authorize(3, 11, 1, [7; 16]).unwrap();
    drop(a);
    let a = NativeStorageAuthority::open(&d.0).unwrap();
    a.authorize(3, 11, 1, [7; 16]).unwrap();
    assert!(a.authorize(3, 11, 2, [7; 16]).is_err());
    assert!(a.authorize(3, 11, 1, [8; 16]).is_err());
}
#[test]
fn ready_and_retired_namespaces_cannot_be_recreated_or_reassigned() {
    let d = Directory::new();
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    a.begin(owner(), false).unwrap();
    a.complete(3, 11).unwrap();
    assert!(a.begin(owner(), false).is_err());
    a.begin(owner(), true).unwrap();
    let mut wrong = owner();
    wrong.generation = 2;
    assert!(a.begin(wrong, true).is_err());
    a.retire(3, 11).unwrap();
    assert!(a.authorize(3, 11, 1, [7; 16]).is_err());
    assert!(a.begin(owner(), false).is_err());
    drop(a);
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    assert!(a.begin(owner(), false).is_err());
}
#[test]
fn unowned_existing_files_cannot_be_adopted_as_native_members() {
    let d = Directory::new();
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    assert!(a.begin(owner(), true).is_err());
    assert!(!a.contains(3, 11));
}
#[test]
fn new_physical_storage_refuses_an_old_incarnation() {
    let first = Directory::new();
    let old = NativeStorageAuthority::open(&first.0)
        .unwrap()
        .incarnation()
        .unwrap();
    let second = Directory::new();
    let fresh = NativeStorageAuthority::open(&second.0).unwrap();
    assert_ne!(fresh.incarnation().unwrap(), old);
    assert!(fresh.verify_incarnation(old).is_err());
}
#[test]
fn committed_registry_truncation_and_missing_files_fail_closed() {
    for fault in 0..4 {
        let d = Directory::new();
        let mut a = NativeStorageAuthority::open(&d.0).unwrap();
        a.begin(owner(), false).unwrap();
        let pending = a.anchor.length;
        a.complete(3, 11).unwrap();
        drop(a);
        let directory = NativeStorageAuthority::path(&d.0);
        match fault {
            0 => OpenOptions::new()
                .write(true)
                .open(directory.join("owners"))
                .unwrap()
                .set_len(pending)
                .unwrap(),
            1 => std::fs::remove_file(directory.join("owners")).unwrap(),
            2 => std::fs::remove_file(directory.join("anchor")).unwrap(),
            _ => {
                let mut b = std::fs::read(directory.join("owners")).unwrap();
                let n = b.len();
                b[n - 1] ^= 1;
                std::fs::write(directory.join("owners"), b).unwrap();
            }
        }
        assert!(
            NativeStorageAuthority::open(&d.0).is_err(),
            "accepted fault {fault}"
        );
    }
}
#[test]
fn unanchored_suffix_is_removed_without_authorizing_a_member() {
    let d = Directory::new();
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    a.begin(owner(), false).unwrap();
    let length = a.anchor.length;
    drop(a);
    let path = NativeStorageAuthority::path(&d.0).join("owners");
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"unfinished")
        .unwrap();
    let a = NativeStorageAuthority::open(&d.0).unwrap();
    assert_eq!(std::fs::metadata(path).unwrap().len(), length);
    assert!(a.authorize(3, 11, 1, [7; 16]).is_err());
}
#[test]
fn failed_anchor_commit_poisoning_is_recovered_from_the_committed_prefix() {
    let d = Directory::new();
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    a.begin(owner(), false).unwrap();
    let pending = NativeStorageAuthority::path(&d.0).join("anchor.pending");
    std::fs::create_dir(&pending).unwrap();
    assert!(a.complete(3, 11).is_err());
    assert!(a.incarnation().is_err());
    assert!(a.authorize(3, 11, 1, [7; 16]).is_err());
    drop(a);
    let mut a = NativeStorageAuthority::open(&d.0).unwrap();
    assert!(a.authorize(3, 11, 1, [7; 16]).is_err());
    std::fs::remove_dir(pending).unwrap();
    a.complete(3, 11).unwrap();
    a.authorize(3, 11, 1, [7; 16]).unwrap();
}
#[test]
fn anchor_unknown_version_and_unknown_fields_are_rejected() {
    for unknown in [false, true] {
        let d = Directory::new();
        let a = NativeStorageAuthority::open(&d.0).unwrap();
        let mut anchor = a.anchor.clone();
        let directory = a.directory.clone();
        drop(a);
        if unknown {
            let mut b = ANCHOR_MAGIC.to_vec();
            anchor.encode(&mut b).unwrap();
            b.extend_from_slice(&[0x78, 1]);
            let hash = digest(&b);
            b.extend_from_slice(&hash);
            std::fs::write(directory.join("anchor"), b).unwrap();
        } else {
            anchor.version = 2;
            NativeStorageAuthority::persist_anchor(&directory, &anchor).unwrap();
        }
        assert!(NativeStorageAuthority::open(&d.0).is_err());
    }
}

fn store(path: &Path) -> super::super::log_chunk_store::LogChunkStore {
    let mut s = super::super::log_chunk_store::LogChunkStore::open(path).unwrap();
    s.set_publication_writes(true);
    s
}
fn probe(s: &mut super::super::log_chunk_store::LogChunkStore) -> [u8; 16] {
    s.apply(&super::super::log_op::LogOpRequest::storage_probe())
        .unwrap()
        .storage_incarnation()
        .unwrap()
}
fn initialize(root: [u8; 16]) -> super::super::log_op::LogOpRequest {
    super::super::log_op::LogOpRequest::initialize_publication(3, 11, 1, 9, [7; 16])
        .for_storage(root, [7; 16])
}
#[test]
fn native_wire_identity_and_owned_payload_decoders_are_canonical() {
    use super::super::log_op::{LogOpReply, LogOpRequest};
    let request = initialize([8; 16]);
    request.validate().unwrap();
    let encoded = request.encode_to_vec();
    assert_eq!(LogOpRequest::from_payload(&encoded).unwrap(), request);
    assert_eq!(LogOpRequest::from_payload_owned(encoded).unwrap(), request);
    let mut wrong = request.clone();
    wrong.version = 4;
    assert!(wrong.validate().is_err());
    let probe = LogOpRequest::storage_probe();
    assert_eq!(
        LogOpRequest::from_payload(&probe.encode_to_vec()).unwrap(),
        probe
    );
    let d = Directory::new();
    let mut s = store(&d.0);
    let root = probe_storage(&mut s);
    let reply = s.apply(&initialize(root)).unwrap();
    let encoded = reply.encode_to_vec();
    assert_eq!(LogOpReply::from_payload(&encoded).unwrap(), reply);
    assert_eq!(LogOpReply::from_payload_owned(encoded).unwrap(), reply);
}
fn probe_storage(s: &mut super::super::log_chunk_store::LogChunkStore) -> [u8; 16] {
    probe(s)
}
#[test]
fn native_wire_prevents_legacy_access_and_preserves_identity_after_restart() {
    use super::super::log_op::LogOpRequest;
    let d = Directory::new();
    let mut s = store(&d.0);
    let root = probe(&mut s);
    s.apply(&initialize(root)).unwrap();
    s.apply(
        &LogOpRequest::append_published(3, 11, 1, 9, 0, b"abcd".to_vec(), [3; 16], 4, [7; 16])
            .for_storage(root, [7; 16]),
    )
    .unwrap();
    let registry = NativeStorageAuthority::path(&d.0).join("owners");
    let size = std::fs::metadata(&registry).unwrap().len();
    for i in 1..=20u64 {
        s.apply(
            &LogOpRequest::append_published(
                3,
                11,
                1,
                9,
                i * 4,
                vec![i as u8; 4],
                [i as u8; 16],
                i * 4 + 4,
                [7; 16],
            )
            .for_storage(root, [7; 16]),
        )
        .unwrap();
    }
    assert_eq!(
        std::fs::metadata(&registry).unwrap().len(),
        size,
        "steady writes must not append ownership receipts"
    );
    assert!(s.apply(&LogOpRequest::publication_tail(3, 11, 1)).is_err());
    drop(s);
    let mut s = store(&d.0);
    assert_eq!(probe(&mut s), root);
    let tail = s
        .apply(&LogOpRequest::publication_tail(3, 11, 1).for_storage(root, [7; 16]))
        .unwrap();
    assert_eq!(tail.published_length, 84);
    assert_eq!(tail.storage_incarnation(), Some(root));
    assert!(s
        .apply(&LogOpRequest::initialize_publication(3, 11, 1, 9, [7; 16]))
        .is_err());
}
#[test]
fn lost_published_file_is_never_reinitialized_as_an_empty_native_vote() {
    use super::super::log_op::LogOpRequest;
    for truncate in [false, true] {
        let d = Directory::new();
        let mut s = store(&d.0);
        let root = probe(&mut s);
        s.apply(&initialize(root)).unwrap();
        drop(s);
        let path = d.0.join("0000000000000003-000000000000000b.log");
        if truncate {
            OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(0)
                .unwrap();
        } else {
            std::fs::remove_file(&path).unwrap();
        }
        let mut s = store(&d.0);
        assert_eq!(probe(&mut s), root);
        assert!(s.apply(&initialize(root)).is_err());
        assert!(s
            .apply(&LogOpRequest::publication_tail(3, 11, 1).for_storage(root, [7; 16]))
            .is_err());
        assert_eq!(path.exists(), truncate);
    }
}
#[test]
fn physical_replacement_rejects_stale_initialization_and_unowned_adoption() {
    use super::super::log_op::LogOpRequest;
    let first = Directory::new();
    let mut old = store(&first.0);
    let root = probe(&mut old);
    old.apply(&initialize(root)).unwrap();
    let replacement = Directory::new();
    let mut fresh = store(&replacement.0);
    let next = probe(&mut fresh);
    assert_ne!(root, next);
    assert!(fresh.apply(&initialize(root)).is_err());
    assert!(fresh
        .apply(&LogOpRequest::publication_tail(3, 11, 1).for_storage(root, [7; 16]))
        .is_err());
    fresh
        .apply(&LogOpRequest::initialize_publication(3, 11, 1, 9, [7; 16]))
        .unwrap();
    assert!(fresh.apply(&initialize(next)).is_err());
    assert!(fresh
        .apply(&LogOpRequest::publication_tail(3, 11, 1).for_storage(next, [7; 16]))
        .is_err());
}
