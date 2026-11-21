// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    metadata: Group,
    chunks: Vec<NodeProcess>,
    directory: Directory,
    endpoints: Vec<String>,
}
impl Fixture {
    fn start_chunk(root: &std::path::Path, index: usize, endpoint: &str) -> NodeProcess {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", endpoint),
                (
                    "MATRIXOBJECT_REPLICA_JOURNAL",
                    root.join(format!("data-{index}")).to_str().unwrap(),
                ),
                ("MATRIXOBJECT_REPLICA_SEGMENTED_OUTCOMES", "true"),
                ("MATRIXOBJECT_REPLICA_LOCAL_RECOVERY", "peers"),
                ("MATRIXOBJECT_REPLICA_DEFERRED_COMPLETION", "true"),
                ("MATRIXOBJECT_REPLICA_RESIDENT_PAYLOAD_BYTES", "16777216"),
                ("MATRIXOBJECT_REPLICA_RESIDENT_PAGE_ENTRIES", "128"),
            ],
        )
    }
    fn new() -> Self {
        let directory =
            Directory(std::env::temp_dir().join(node_regime::run_token("ordered-appends")));
        std::fs::create_dir(&directory.0).unwrap();
        let chunks: Vec<_> = (0..3)
            .map(|i| Self::start_chunk(&directory.0, i, "127.0.0.1:0"))
            .collect();
        let endpoints: Vec<_> = chunks.iter().map(|node| node.endpoint.clone()).collect();
        let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
            placement_id: "group-a".into(),
            failure_domain: "rack-a".into(),
            identity: Some(ChunkGroupIdentity {
                version: 3,
                group_id: 1,
                node_id: 1,
                capacity_bytes: 1 << 28,
                max_pages: 4096,
                voters: vec![1, 2, 3],
                peer_endpoints: vec![],
                bootstrap_voters: vec![],
                bootstrap_id: 0,
            }),
            api_endpoints: endpoints.clone(),
            member_ids: (1..=3).map(|i| format!("star-{i}")).collect(),
        }])
        .unwrap();
        let metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
        let fixture = Self {
            metadata,
            chunks,
            directory,
            endpoints,
        };
        let mut client = fixture.client();
        for i in 1..=3 {
            client
                .heartbeat_placement_node(&format!("star-{i}"), 0, 1 << 28, false, true, 100)
                .unwrap();
        }
        assert_eq!(
            client
                .read_chunk_route("group-a")
                .unwrap()
                .standalone_star_route()
                .unwrap()
                .unwrap()
                .1,
            3
        );
        client.enable_publication_fence().unwrap();
        fixture
    }
    fn client(&self) -> RemoteMetaClient {
        RemoteMetaClient::connect(
            &self.metadata.nodes[self.metadata.leader()]
                .as_ref()
                .unwrap()
                .endpoint,
            &MetaClientOptions {
                read_timeout: Duration::from_secs(10),
                write_timeout: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn options(&self) -> ObjectWriteOptions {
        ObjectWriteOptions {
            page_bytes: 32768,
            target_groups: 1,
            replica: ReplicaClientOptions {
                read_timeout: Duration::from_secs(10),
                write_timeout: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        }
    }
    fn statistics(&self) -> Vec<(Vec<u8>, u64, u64, u64, u64)> {
        self.endpoints
            .iter()
            .map(|endpoint| {
                let mut node =
                    RemoteReplicaClient::connect(endpoint, &self.options().replica).unwrap();
                let stats = node
                    .journal_write_statistics()
                    .unwrap()
                    .write_statistics
                    .unwrap();
                assert!(!stats.saturated);
                (
                    stats.scope_id,
                    stats.foreground_batches,
                    stats.foreground_decisions,
                    stats.foreground_record_bytes,
                    stats.foreground_payload_bytes,
                )
            })
            .collect()
    }
    fn assert_delta(&self, before: &[(Vec<u8>, u64, u64, u64, u64)], batches: u64, bytes: u64) {
        for (before, after) in before.iter().zip(self.statistics()) {
            assert_eq!(before.0, after.0);
            assert_eq!(
                (
                    after.1 - before.1,
                    after.2 - before.2,
                    after.3 - before.3,
                    after.4 - before.4
                ),
                (batches, 2, bytes, 4279)
            );
        }
    }
    fn cold_restart(&mut self) {
        for i in 0..3 {
            self.metadata.kill(i);
        }
        self.chunks.clear();
        for i in 0..3 {
            self.chunks
                .push(Self::start_chunk(&self.directory.0, i, &self.endpoints[i]));
        }
        for i in 0..3 {
            self.metadata.start(i);
        }
    }
}
#[test]
fn ordered_staged_appends_share_one_durable_record_and_survive_cold_restart() {
    let mut fixture = Fixture::new();
    let options = fixture.options();
    let mut client = fixture.client();
    let first_bytes = vec![0x51; 4154];
    let second_bytes = vec![0x2b; 125];
    let first = client
        .write_object("ordered", "wal", None, b"", &options)
        .unwrap();
    let second = client
        .write_object("ordered", "index", None, b"", &options)
        .unwrap();
    let allocation1 = client
        .allocate_leased_staged_object("ordered", "wal", Some(first.generation), 1, true, 60000)
        .unwrap();
    let allocation2 = client
        .allocate_leased_staged_object("ordered", "index", Some(second.generation), 1, true, 60000)
        .unwrap();
    let before = fixture.statistics();
    let paired = client
        .append_confirmed_staged_pair(
            StagedObjectAppend {
                allocation: &allocation1,
                expected: first,
                data: &first_bytes,
                reserve_next: None,
            },
            StagedObjectAppend {
                allocation: &allocation2,
                expected: second,
                data: &second_bytes,
                reserve_next: None,
            },
            &options,
        )
        .unwrap();
    fixture.assert_delta(&before, 1, 12288);
    let base1 = client
        .write_object("ordered", "separate-wal", None, b"", &options)
        .unwrap();
    let base2 = client
        .write_object("ordered", "separate-index", None, b"", &options)
        .unwrap();
    let allocation3 = client
        .allocate_leased_staged_object(
            "ordered",
            "separate-wal",
            Some(base1.generation),
            1,
            true,
            60000,
        )
        .unwrap();
    let allocation4 = client
        .allocate_leased_staged_object(
            "ordered",
            "separate-index",
            Some(base2.generation),
            1,
            true,
            60000,
        )
        .unwrap();
    let before = fixture.statistics();
    client.confirm_staged_allocation(allocation3.allocation_id);
    let separate1 = client
        .append_staged_object_compact(&allocation3, base1, &first_bytes, &options)
        .unwrap();
    client.confirm_staged_allocation(allocation4.allocation_id);
    let separate2 = client
        .append_staged_object_compact(&allocation4, base2, &second_bytes, &options)
        .unwrap();
    fixture.assert_delta(&before, 2, 20480);
    drop(client);
    fixture.cold_restart();
    let mut client = fixture.client();
    for (manifest, bytes) in [
        (&paired[0].manifest, &first_bytes),
        (&paired[1].manifest, &second_bytes),
        (&separate1, &first_bytes),
        (&separate2, &second_bytes),
    ] {
        assert_eq!(
            client
                .read_object(
                    &manifest.bucket,
                    &manifest.key,
                    bytes.len(),
                    &options.replica
                )
                .unwrap(),
            (manifest.clone(), bytes.clone())
        );
    }
    for endpoint in &fixture.endpoints {
        let mut replica = RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
        for (allocation, bytes) in [
            (&allocation1, &first_bytes),
            (&allocation2, &second_bytes),
            (&allocation3, &first_bytes),
            (&allocation4, &second_bytes),
        ] {
            let page = allocation.allocation().unwrap().page(0).unwrap();
            assert_eq!(
                replica
                    .read_chunk(&page, 0, bytes.len() as u64)
                    .unwrap()
                    .data,
                *bytes
            );
        }
    }
}
#[test]
fn ordered_staged_appends_return_the_committed_prefix_when_second_publication_is_refused() {
    let fixture = Fixture::new();
    let options = fixture.options();
    let mut client = fixture.client();
    let first = client
        .write_object("ordered", "wal", None, b"", &options)
        .unwrap();
    let second = client
        .write_object("ordered", "index", None, b"", &options)
        .unwrap();
    let allocation1 = client
        .allocate_leased_staged_object("ordered", "wal", Some(first.generation), 1, true, 60000)
        .unwrap();
    let allocation2 = client
        .allocate_leased_staged_object("ordered", "index", Some(second.generation), 1, true, 60000)
        .unwrap();
    // An allocation can be aborted after its writer obtained it but before publication.
    client
        .abort_staged_allocation(allocation2.allocation_id)
        .unwrap();
    let failed = client
        .append_confirmed_staged_pair(
            StagedObjectAppend {
                allocation: &allocation1,
                expected: first,
                data: b"durable-wal",
                reserve_next: Some((1, 60000)),
            },
            StagedObjectAppend {
                allocation: &allocation2,
                expected: second.clone(),
                data: b"index",
                reserve_next: None,
            },
            &options,
        )
        .unwrap_err();
    assert_eq!(failed.failed_append, 1);
    // A normal metadata rejection does not carry a non-admission certificate.
    let ObjectWriteFailure::PublicationUnconfirmed { outcome, .. } = &failed.failure else {
        panic!(
            "publication must retain its uncertain candidate: {:?}",
            failed.failure
        );
    };
    assert_eq!(outcome.after.as_ref().unwrap().bucket, "ordered");
    assert_eq!(outcome.after.as_ref().unwrap().key, "index");
    assert_eq!(outcome.after.as_ref().unwrap().length, 5);
    let committed = failed.committed_first.unwrap();
    let next = committed.next_allocation.unwrap();
    assert_eq!(
        client.read_staged_allocation(next.allocation_id).unwrap(),
        next
    );
    assert_eq!(
        client
            .read_object("ordered", "wal", 11, &options.replica)
            .unwrap(),
        (committed.manifest, b"durable-wal".to_vec())
    );
    assert_eq!(client.read_manifest("ordered", "index").unwrap(), second);
}

#[test]
fn ordered_staged_appends_do_not_publish_the_second_when_the_first_is_refused() {
    let fixture = Fixture::new();
    let options = fixture.options();
    let mut client = fixture.client();
    let first = client
        .write_object("ordered", "wal", None, b"", &options)
        .unwrap();
    let second = client
        .write_object("ordered", "index", None, b"", &options)
        .unwrap();
    let allocation1 = client
        .allocate_leased_staged_object("ordered", "wal", Some(first.generation), 1, true, 60000)
        .unwrap();
    let allocation2 = client
        .allocate_leased_staged_object("ordered", "index", Some(second.generation), 1, true, 60000)
        .unwrap();
    client
        .abort_staged_allocation(allocation1.allocation_id)
        .unwrap();
    let failed = client
        .append_confirmed_staged_pair(
            StagedObjectAppend {
                allocation: &allocation1,
                expected: first.clone(),
                data: b"wal",
                reserve_next: None,
            },
            StagedObjectAppend {
                allocation: &allocation2,
                expected: second.clone(),
                data: b"index",
                reserve_next: None,
            },
            &options,
        )
        .unwrap_err();
    assert_eq!(failed.failed_append, 0);
    assert!(failed.committed_first.is_none());
    // The retained candidate is required even when a later read sees the old value.
    let ObjectWriteFailure::PublicationUnconfirmed { outcome, .. } = &failed.failure else {
        panic!(
            "publication must retain its uncertain candidate: {:?}",
            failed.failure
        );
    };
    let candidate = outcome.after.as_ref().unwrap();
    assert_eq!(candidate.bucket, "ordered");
    assert_eq!(candidate.key, "wal");
    assert_eq!(candidate.length, 3);
    assert_eq!(client.read_manifest("ordered", "wal").unwrap(), first);
    assert_eq!(client.read_manifest("ordered", "index").unwrap(), second);
}

fn proxy_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut header = [0; 6];
    stream.read_exact(&mut header)?;
    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    if size > 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "proxy frame limit",
        ));
    }
    let mut frame = header.to_vec();
    frame.resize(6 + size, 0);
    stream.read_exact(&mut frame[6..])?;
    Ok(frame)
}

#[test]
fn ordered_staged_appends_keep_first_publication_uncertain_when_ack_and_readback_are_lost() {
    let fixture = Fixture::new();
    let options = fixture.options();
    let mut direct = fixture.client();
    let first = direct
        .write_object("ordered", "wal", None, b"", &options)
        .unwrap();
    let second = direct
        .write_object("ordered", "index", None, b"", &options)
        .unwrap();
    let allocation1 = direct
        .allocate_leased_staged_object("ordered", "wal", Some(first.generation), 1, true, 60000)
        .unwrap();
    let allocation2 = direct
        .allocate_leased_staged_object("ordered", "index", Some(second.generation), 1, true, 60000)
        .unwrap();
    let real_endpoint = fixture.metadata.nodes[fixture.metadata.leader()]
        .as_ref()
        .unwrap()
        .endpoint
        .clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let publications = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = publications.clone();
    let worker = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut frontend = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                other => panic!("proxy connection missing: {other:?}"),
            }
        };
        frontend
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        frontend
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut backend = TcpStream::connect(real_endpoint).unwrap();
        backend
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        backend
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        loop {
            let frame = proxy_frame(&mut frontend).unwrap();
            let request =
                MetaRequest::decode(MetaRequestCode::from_u8(frame[1]).unwrap(), &frame[6..])
                    .unwrap();
            let publication = matches!(request, MetaRequest::StagedAllocation(ref command)
                if command.manifest_append.is_some());
            backend.write_all(&frame).unwrap();
            let reply = proxy_frame(&mut backend).unwrap();
            if publication {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // The real voter has replied, but the writer cannot observe that reply
                // or read back through this connection. No other request is forwarded.
                let _ = frontend.shutdown(std::net::Shutdown::Both);
                return;
            }
            frontend.write_all(&reply).unwrap();
        }
    });
    let mut client = RemoteMetaClient::connect(
        &endpoint,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .unwrap();
    client.cache_append_plans(Duration::from_secs(3600));
    let first_bytes = b"durable-first";
    let failed = client
        .append_confirmed_staged_pair(
            StagedObjectAppend {
                allocation: &allocation1,
                expected: first.clone(),
                data: first_bytes,
                reserve_next: None,
            },
            StagedObjectAppend {
                allocation: &allocation2,
                expected: second.clone(),
                data: b"index",
                reserve_next: None,
            },
            &options,
        )
        .unwrap_err();
    // The index plan was most recently cached. A failed pair must invalidate it;
    // the closed connection cannot answer a fresh metadata planning request.
    assert!(client.plan_append_routes_compact(&second, 5, 1).is_err());
    drop(client);
    worker.join().unwrap();
    assert_eq!(publications.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(failed.failed_append, 0);
    assert!(failed.committed_first.is_none());
    let ObjectWriteFailure::PublicationUnconfirmed { outcome, .. } = failed.failure else {
        panic!("lost acknowledgement and read-back must stay uncertain");
    };
    let committed = direct.read_manifest("ordered", "wal").unwrap();
    assert_eq!(committed.generation, first.generation + 1);
    assert_eq!(outcome.before.as_ref(), Some(&first));
    assert_eq!(outcome.after.as_ref(), Some(&committed));
    assert_eq!(
        direct
            .read_object("ordered", "wal", first_bytes.len(), &options.replica)
            .unwrap()
            .1,
        first_bytes
    );
    assert_eq!(direct.read_manifest("ordered", "index").unwrap(), second);
}
