// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "matrix-raft"))]
//! The largest zero-fill chunk, 512 MiB, is also the peer WAL's own size limit. The fill must not
//! trip that limit: the limit is on the log, and the file holds the log plus its fill.

use matrixraft::peer_runtime::{DurablePeer, PeerOptions, PeerTuning};

#[test]
fn a_fill_as_large_as_the_wal_limit_opens_writes_and_reopens() {
    let directory = std::env::temp_dir().join(format!("zero-fill-limit-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let options = PeerOptions {
        group_id: 1,
        node_id: 1,
        voters: vec![1],
        wal_path: directory.join("peer.wal"),
    };
    let tuning = PeerTuning {
        wal_zero_fill_bytes: Some(512 * 1024 * 1024),
        ..PeerTuning::default()
    };
    let mut peer = DurablePeer::open_with_tuning(options.clone(), tuning).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    for n in 0..20u8 {
        peer.propose(vec![n; 4096]).unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    let logical = peer.persisted_wal_bytes().unwrap();
    let physical = std::fs::metadata(&options.wal_path).unwrap().len();
    assert!(logical < 1 << 20, "the log is small: {logical}");
    assert_eq!(physical, 512 * 1024 * 1024, "the file holds exactly one fill");
    drop(peer);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    assert_eq!(peer.persisted_wal_bytes().unwrap(), logical);
    let mut delivered = Vec::new();
    peer.campaign().unwrap();
    peer.process_ready(|_, data| {
        if !data.is_empty() {
            delivered.push(data.to_vec());
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(delivered.len(), 20);
    drop(peer);
    std::fs::remove_dir_all(&directory).unwrap();
}
