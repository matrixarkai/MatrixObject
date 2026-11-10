// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use prost::Message;
use std::{
    io::Read,
    path::Path,
    time::{Duration, Instant},
};
#[derive(Clone, PartialEq, Message)]
struct WalRecord {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "5")]
    hard_state: Vec<u8>,
    #[prost(bytes = "vec", tag = "7")]
    snapshot: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
struct HardStatePosition {
    #[prost(uint64, tag = "3")]
    commit: u64,
}
#[derive(Clone, PartialEq, Message)]
struct SnapshotPosition {
    #[prost(message, optional, tag = "2")]
    metadata: Option<SnapshotIndex>,
}
#[derive(Clone, PartialEq, Message)]
struct SnapshotIndex {
    #[prost(uint64, tag = "2")]
    index: u64,
}
fn record(file: &mut std::fs::File) -> Option<WalRecord> {
    let mut header = [0; 24];
    file.read_exact(&mut header).ok()?;
    if &header[..4] != b"MRPW" {
        return None;
    }
    let length = u32::from_be_bytes(header[4..8].try_into().ok()?) as usize;
    if length > 8 * 1024 * 1024 {
        return None;
    }
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes).ok()?;
    WalRecord::decode(bytes.as_slice()).ok()
}
#[allow(dead_code)]
pub fn committed_index(path: &Path) -> u64 {
    let mut file = std::fs::File::open(path).unwrap();
    let mut commit = 0;
    while let Some(record) = record(&mut file) {
        commit = commit.max(
            HardStatePosition::decode(record.hard_state.as_slice())
                .unwrap()
                .commit,
        );
    }
    commit
}
#[allow(dead_code)]
pub fn wait_for_checkpoint_index(path: &Path, minimum_index: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let index = (|| -> Option<u64> {
            let mut file = std::fs::File::open(path).ok()?;
            let record = record(&mut file)?;
            if record.version != 4 {
                return None;
            }
            Some(
                SnapshotPosition::decode(record.snapshot.as_slice())
                    .ok()?
                    .metadata?
                    .index,
            )
        })();
        if index.is_some_and(|index| index >= minimum_index) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "checkpoint did not include committed index {minimum_index}: {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
pub fn wait_for_checkpoint(path: &Path, minimum_version: u32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = (|| -> Option<u32> {
            let mut file = std::fs::File::open(path).ok()?;
            Some(record(&mut file)?.version)
        })();
        if result.is_some_and(|version| version >= minimum_version) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "checkpoint was not published: {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
