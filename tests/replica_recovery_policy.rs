// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(unix, feature = "replica-server"))]
mod node_regime;
use std::{
    io::Read,
    process::{Command, Stdio},
    time::Duration,
};

#[test]
fn invalid_replica_persistence_settings_are_refused_before_serving() {
    for (setting, modes, expected_error) in [
        (
            "MATRIXOBJECT_REPLICA_LOCAL_RECOVERY",
            &["", "replica", "PEERS"][..],
            "MATRIXOBJECT_REPLICA_LOCAL_RECOVERY must be xor or peers",
        ),
        (
            "MATRIXOBJECT_REPLICA_GROUP_COMMIT",
            &["", "yes", "1", "TRUE"][..],
            "MATRIXOBJECT_REPLICA_GROUP_COMMIT must be true or false",
        ),
    ] {
        for mode in modes {
            let mut child = Command::new(env!("CARGO_BIN_EXE_chunkserver"))
                .env("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0")
                .env_remove("MATRIXOBJECT_REPLICA_LOCAL_RECOVERY")
                .env_remove("MATRIXOBJECT_REPLICA_GROUP_COMMIT")
                .env(setting, mode)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let status = node_regime::processes::reap_within(&mut child, Duration::from_secs(5));
            if status.is_none() {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("invalid persistence setting started a server");
            }
            assert_eq!(status, Some(2));
            let mut error = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut error)
                .unwrap();
            assert!(error.contains(expected_error), "{setting}: {error}");
        }
    }
}
