// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

struct Fixture(PathBuf);
impl Fixture {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("log-generation-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn chunk_path(&self, chunk: u64) -> PathBuf {
        self.0.join(format!("{:016x}-{chunk:016x}.log", 1))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn wrong_operations(chunk: u64, generation: u32) -> Vec<LogOpRequest> {
    vec![
        LogOpRequest::append(1, chunk, generation, 99, 4, b"ef".to_vec()),
        LogOpRequest::seal(1, chunk, generation, 99, false),
        LogOpRequest::truncate(1, chunk, generation, 99, 2),
        LogOpRequest::tail(1, chunk, generation),
        LogOpRequest::read(1, chunk, generation, 0, 4),
    ]
}
#[test]
fn cached_generation_refusal_preserves_data_epoch_and_file_bytes() {
    let fixture = Fixture::new("warm");
    let mut store = LogChunkStore::open(&fixture.0).unwrap();
    for (position, generation) in [0, 2, u32::MAX].into_iter().enumerate() {
        let chunk = position as u64 + 1;
        for (index, operation) in wrong_operations(chunk, generation).into_iter().enumerate() {
            if index == 0 {
                store
                    .apply(&LogOpRequest::append(1, chunk, 1, 7, 0, b"abcd".to_vec()))
                    .unwrap();
            }
            let before = std::fs::read(fixture.chunk_path(chunk)).unwrap();
            assert!(
                matches!(
                    store.apply(&operation),
                    Err(ObjectError::PreconditionFailed(_))
                ),
                "generation {generation}, operation {index}"
            );
            assert_eq!(std::fs::read(fixture.chunk_path(chunk)).unwrap(), before);
            let read = store.apply(&LogOpRequest::read(1, chunk, 1, 0, 4)).unwrap();
            assert_eq!(read.data, b"abcd");
            assert_eq!(read.epoch, 7);
            assert!(!read.sealed);
        }
        let appended = store
            .apply(&LogOpRequest::append(1, chunk, 1, 7, 4, b"ef".to_vec()))
            .unwrap();
        assert_eq!(appended.durable_length, 6);
    }
}
#[test]
fn reopened_generation_refusal_keeps_the_valid_incarnation_readable() {
    let fixture = Fixture::new("cold");
    let mut store = LogChunkStore::open(&fixture.0).unwrap();
    store
        .apply(&LogOpRequest::append(1, 1, 1, 7, 0, b"abcd".to_vec()))
        .unwrap();
    drop(store);
    let before = std::fs::read(fixture.chunk_path(1)).unwrap();
    let mut store = LogChunkStore::open(&fixture.0).unwrap();
    for operation in wrong_operations(1, 2) {
        assert!(store.apply(&operation).is_err());
        assert_eq!(std::fs::read(fixture.chunk_path(1)).unwrap(), before);
    }
    let read = store.apply(&LogOpRequest::read(1, 1, 1, 0, 4)).unwrap();
    assert_eq!(read.data, b"abcd");
    assert_eq!(read.epoch, 7);
    assert!(matches!(
        store.apply(&LogOpRequest::tail(1, 1, 2)),
        Err(ObjectError::PreconditionFailed(_))
    ));
}
