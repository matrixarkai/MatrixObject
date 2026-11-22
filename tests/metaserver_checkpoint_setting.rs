// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(unix, feature = "meta-server", feature = "metadata-consensus"))]
mod node_regime;
use std::{
    io::Read,
    process::{Command, Stdio},
    time::Duration,
};

#[test]
fn a_raft_checkpoint_interval_of_zero_or_out_of_range_is_refused_before_serving() {
    let directory = std::env::temp_dir().join(format!("metaserver-checkpoint-setting-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    for value in ["0", "1023", "134217729"] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_metaserver"))
            .env("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0")
            .env("MATRIXOBJECT_METASERVER_RAFT_PEERS", "1@127.0.0.1:1")
            .env("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", "1")
            .env("MATRIXOBJECT_METASERVER_RAFT_STATE_DIR", &directory)
            .env("MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES", value)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let status = node_regime::processes::reap_within(&mut child, Duration::from_secs(10));
        if status.is_none() {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("a checkpoint interval of {value} started a metaserver");
        }
        assert_eq!(status, Some(2), "{value}");
        let mut error = String::new();
        child.stderr.take().unwrap().read_to_string(&mut error).unwrap();
        assert!(
            error.contains("MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES") && error.contains("must be from 1024 to 134217728"),
            "{value}: {error}"
        );
    }
    let _ = std::fs::remove_dir_all(&directory);
}
