// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "meta-server")]
use matrixobjectstore_rs::{
    cluster::{
        MetaClientOptions, MetaNodeOptions, MetaNodeServer, MetaNodeShutdown, RemoteMetaClient,
    },
    ObjectError,
};
use std::{
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

struct Running {
    stop: MetaNodeShutdown,
    task: Option<thread::JoinHandle<std::io::Result<()>>>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}
fn start(root: &Path) -> (RemoteMetaClient, Running) {
    let server =
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), root)
            .unwrap();
    let address = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let task = Some(thread::spawn(move || server.serve()));
    (
        RemoteMetaClient::connect(&address, &MetaClientOptions::default()).unwrap(),
        Running { stop, task },
    )
}

#[test]
fn standalone_expiry_releases_abandoned_slots_preserves_renewals_and_keeps_idle_journal_stable() {
    let root = std::env::temp_dir().join(format!(
        "staged-auto-expiry-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let (mut client, server) = start(&root);
    client.enable_publication_fence().unwrap();
    let permanent = client
        .allocate_staged_object("bucket", "permanent", None, 1, false)
        .unwrap();
    let expired = client
        .allocate_leased_staged_object("bucket", "abandoned", None, 1, false, 1000)
        .unwrap();
    let old = client
        .allocate_leased_staged_object("bucket", "renewed", None, 1, false, 1000)
        .unwrap();
    let renewed = client.renew_staged_allocation(&old, 60_000).unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if matches!(
            client.read_staged_allocation(expired.allocation_id),
            Err(ObjectError::NotFound(_))
        ) {
            break;
        }
        assert!(
            Instant::now() < until,
            "automatic standalone expiry did not retire abandoned ownership"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        client
            .read_staged_allocation(permanent.allocation_id)
            .unwrap(),
        permanent
    );
    assert_eq!(
        client
            .read_staged_allocation(renewed.allocation_id)
            .unwrap(),
        renewed
    );
    let journal = std::fs::read(root.join("metadata.outcomes")).unwrap();
    thread::sleep(Duration::from_millis(2200));
    assert_eq!(
        std::fs::read(root.join("metadata.outcomes")).unwrap(),
        journal
    );
    drop(client);
    drop(server);
    let (mut client, server) = start(&root);
    assert!(matches!(
        client.read_staged_allocation(expired.allocation_id),
        Err(ObjectError::NotFound(_))
    ));
    assert_eq!(
        client
            .read_staged_allocation(permanent.allocation_id)
            .unwrap(),
        permanent
    );
    assert_eq!(
        client
            .read_staged_allocation(renewed.allocation_id)
            .unwrap(),
        renewed
    );
    let next = client
        .allocate_staged_object("bucket", "abandoned", None, 1, false)
        .unwrap();
    assert!(next.allocation_id > expired.allocation_id);
    drop(client);
    drop(server);
}
