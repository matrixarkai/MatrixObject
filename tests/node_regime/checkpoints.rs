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
}
pub fn wait_for_checkpoint(path: &Path, minimum_version: u32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = (|| -> Option<u32> {
            let mut file = std::fs::File::open(path).ok()?;
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
            Some(WalRecord::decode(bytes.as_slice()).ok()?.version)
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
