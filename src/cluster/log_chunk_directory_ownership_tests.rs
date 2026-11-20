// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

struct Fixture(PathBuf);
impl Fixture {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("log-directory-lazy-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn construction_does_not_reserve_an_unused_directory() {
    let fixture = Fixture::new("construction");
    let unused = LogChunkStore::open(&fixture.0).unwrap();
    let mut active = LogChunkStore::open(&fixture.0).unwrap();
    active
        .apply(&LogOpRequest::append(1, 1, 1, 7, 0, b"held".to_vec()))
        .unwrap();
    assert_eq!(
        active
            .apply(&LogOpRequest::read(1, 1, 1, 0, 4))
            .unwrap()
            .data,
        b"held"
    );
    drop(unused);
    assert_eq!(
        active
            .apply(&LogOpRequest::tail(1, 1, 1))
            .unwrap()
            .durable_length,
        4
    );
}

#[test]
fn another_store_cannot_replace_an_active_writers_bytes() {
    let fixture = Fixture::new("writer");
    let mut first = LogChunkStore::open(&fixture.0).unwrap();
    let mut second = LogChunkStore::open(&fixture.0).unwrap();
    first
        .apply(&LogOpRequest::append(1, 1, 1, 7, 0, b"held".to_vec()))
        .unwrap();
    let path = fixture.0.join("0000000000000001-0000000000000001.log");
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        second.apply(&LogOpRequest::append(1, 1, 1, 9, 0, b"lost".to_vec())),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(
        first
            .apply(&LogOpRequest::read(1, 1, 1, 0, 4))
            .unwrap()
            .data,
        b"held"
    );
    drop(first);
    assert_eq!(
        second
            .apply(&LogOpRequest::read(1, 1, 1, 0, 4))
            .unwrap()
            .data,
        b"held"
    );
}

#[test]
fn pending_sync_retains_ownership_after_the_store_is_dropped() {
    let fixture = Fixture::new("pending");
    let mut first = LogChunkStore::open(&fixture.0).unwrap();
    first
        .apply_deferred(&LogOpRequest::append(1, 1, 1, 7, 0, b"held".to_vec()))
        .unwrap();
    let pending = first.take_pending_syncs().unwrap();
    assert_eq!(pending.len(), 1);
    drop(first);
    let mut next = LogChunkStore::open(&fixture.0).unwrap();
    assert!(matches!(
        next.apply(&LogOpRequest::tail(1, 1, 1)),
        Err(ObjectError::PreconditionFailed(_))
    ));
    sync_pending(&pending).unwrap();
    assert!(matches!(
        next.apply(&LogOpRequest::tail(1, 1, 1)),
        Err(ObjectError::PreconditionFailed(_))
    ));
    drop(pending);
    assert_eq!(
        next.apply(&LogOpRequest::tail(1, 1, 1))
            .unwrap()
            .durable_length,
        4
    );
}

#[test]
fn path_aliases_share_one_directory_owner() {
    let fixture = Fixture::new("alias");
    let mut first = LogChunkStore::open(&fixture.0).unwrap();
    let mut alias = LogChunkStore::open(&fixture.0.join(".")).unwrap();
    first
        .apply(&LogOpRequest::append(1, 1, 1, 7, 0, b"held".to_vec()))
        .unwrap();
    assert!(matches!(
        alias.apply(&LogOpRequest::read(1, 1, 1, 0, 4)),
        Err(ObjectError::PreconditionFailed(_))
    ));
    drop(first);
    assert_eq!(
        alias
            .apply(&LogOpRequest::read(1, 1, 1, 0, 4))
            .unwrap()
            .data,
        b"held"
    );
}

#[test]
fn independent_directories_hold_independent_data() {
    let fixture = Fixture::new("separate");
    let mut first = LogChunkStore::open(&fixture.0.join("first")).unwrap();
    let mut second = LogChunkStore::open(&fixture.0.join("second")).unwrap();
    first
        .apply(&LogOpRequest::append(1, 1, 1, 7, 0, b"left".to_vec()))
        .unwrap();
    second
        .apply(&LogOpRequest::append(1, 1, 1, 7, 0, b"next".to_vec()))
        .unwrap();
    assert_eq!(
        first
            .apply(&LogOpRequest::read(1, 1, 1, 0, 4))
            .unwrap()
            .data,
        b"left"
    );
    assert_eq!(
        second
            .apply(&LogOpRequest::read(1, 1, 1, 0, 4))
            .unwrap()
            .data,
        b"next"
    );
}
