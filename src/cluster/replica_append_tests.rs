// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::{ChunkId, PageId};

struct Fixture {
    node: Arc<Mutex<NodeReplica>>,
    directory: std::path::PathBuf,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "append-outcomes-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("replica.journal");
        let mut service = MatrixReplicaService::new();
        service.set_keep_ec_sidecar(false);
        let journal = ReplicaJournal::open(&path, &mut service).unwrap();
        Self {
            node: Arc::new(Mutex::new(NodeReplica {
                service,
                journal: Some(journal),
                used_bytes: 0,
                journal_checkpoint_after_bytes: 0,
                staged: None,
                maintenance: false,
                max_pages: 10,
                failed: false,
                #[cfg(all(feature = "chunk-consensus", feature = "meta-server"))]
                applied_membership: None,
            })),
            directory,
            path,
        }
    }
    fn request(&self, request: ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        apply(&self.node, request, MAX_PAYLOAD_BYTES)
    }
    fn length(&self) -> u64 {
        std::fs::metadata(&self.path).unwrap().len()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn page() -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 2).unwrap(),
        page_index: 0,
    }
}

#[test]
fn journal_append_rpc_variants_keep_conditional_guards_and_accounting() {
    for variant in 0..4 {
        let fixture = Fixture::new();
        fixture
            .request(ReplicaRequest::WriteChunk {
                page_id: page(),
                offset: 0,
                data: b"base".to_vec(),
            })
            .unwrap();
        let checksum = fixture
            .node
            .lock()
            .unwrap()
            .service
            .head_chunk(&page())
            .unwrap()
            .state_checksum;
        let request = match variant {
            0 => ReplicaRequest::WriteChunk {
                page_id: page(),
                offset: 4,
                data: b"tail".to_vec(),
            },
            1 => ReplicaRequest::WriteChunkIfSequence {
                page_id: page(),
                offset: 4,
                data: b"tail".to_vec(),
                expected_sequence: 1,
            },
            2 => ReplicaRequest::WriteChunkIfState {
                page_id: page(),
                offset: 4,
                data: b"tail".to_vec(),
                expected_sequence: 1,
                expected_state_checksum: checksum,
            },
            _ => ReplicaRequest::WriteChunkIfSequenceDigest {
                page_id: page(),
                offset: 4,
                data: b"tail".to_vec(),
                expected_sequence: 1,
            },
        };
        assert!(
            prepare_rpc_append(&fixture.node.lock().unwrap().service, &request)
                .unwrap()
                .is_some()
        );
        fixture.request(request).unwrap();
        let before = fixture.length();
        for refused in [
            ReplicaRequest::WriteChunkIfSequence {
                page_id: page(),
                offset: 8,
                data: vec![1],
                expected_sequence: 1,
            },
            ReplicaRequest::WriteChunkIfState {
                page_id: page(),
                offset: 8,
                data: vec![1],
                expected_sequence: 2,
                expected_state_checksum: "wrong".into(),
            },
        ] {
            assert!(
                prepare_rpc_append(&fixture.node.lock().unwrap().service, &refused)
                    .unwrap()
                    .is_none()
            );
            assert!(matches!(
                fixture.request(refused),
                Err(ObjectError::PreconditionFailed(_))
            ));
            assert_eq!(fixture.length(), before);
        }
        let node = fixture.node.lock().unwrap();
        assert_eq!(node.used_bytes, 8);
        assert_eq!(
            node.service.read_chunk(&page(), 0, 8).unwrap().data,
            b"basetail"
        );
        assert_eq!(node.service.head_chunk(&page()).unwrap().write_sequence, 2);
    }
}

#[test]
fn journal_append_persistence_failure_refuses_ack_and_recovers_only_prior_data() {
    let fixture = Fixture::new();
    fixture
        .request(ReplicaRequest::WriteChunk {
            page_id: page(),
            offset: 0,
            data: b"base".to_vec(),
        })
        .unwrap();
    fixture.node.lock().unwrap().journal = Some(ReplicaJournal::read_only_for_test(&fixture.path));
    assert!(fixture
        .request(ReplicaRequest::WriteChunk {
            page_id: page(),
            offset: 4,
            data: b"tail".to_vec()
        })
        .is_err());
    assert!(fixture.node.lock().unwrap().failed);
    assert!(fixture.request(ReplicaRequest::Ping).is_err());
    assert!(fixture
        .request(ReplicaRequest::HeadChunk { page_id: page() })
        .is_err());
    fixture.node.lock().unwrap().journal = None;
    let mut recovered = MatrixReplicaService::new();
    recovered.set_keep_ec_sidecar(false);
    drop(ReplicaJournal::open(&fixture.path, &mut recovered).unwrap());
    assert_eq!(recovered.read_chunk(&page(), 0, 4).unwrap().data, b"base");
    assert_eq!(recovered.head_chunk(&page()).unwrap().logical_length, 4);
}

#[test]
fn journal_append_recovery_policy_is_applied_after_checkpoint_restore() {
    let fixture = Fixture::new();
    fixture.node.lock().unwrap().journal = None;
    let write = |server: &ReplicaNodeServer, offset, data: &[u8]| {
        apply(
            &server.replica,
            ReplicaRequest::WriteChunk {
                page_id: page(),
                offset,
                data: data.to_vec(),
            },
            MAX_PAYLOAD_BYTES,
        )
        .unwrap();
    };
    let checkpoint = |server: &ReplicaNodeServer| {
        let mut guard = server.replica.lock().unwrap();
        let NodeReplica {
            service, journal, ..
        } = &mut *guard;
        journal.as_mut().unwrap().checkpoint(service).unwrap();
    };
    let original = ReplicaNodeServer::bind_with_journal(
        "127.0.0.1:0",
        ReplicaNodeOptions::default(),
        &fixture.path,
    )
    .unwrap();
    write(&original, 0, b"base");
    assert!(!original
        .replica
        .lock()
        .unwrap()
        .service
        .read_chunk_state(&page())
        .unwrap()
        .fragment_bytes()
        .is_empty());
    checkpoint(&original);
    drop(original);
    let peer = ReplicaNodeServer::bind_with_journal(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            local_recovery_sidecar: false,
            ..Default::default()
        },
        &fixture.path,
    )
    .unwrap();
    {
        let node = peer.replica.lock().unwrap();
        assert!(node
            .service
            .read_chunk_state(&page())
            .unwrap()
            .fragment_bytes()
            .is_empty());
        assert!(node
            .service
            .prepare_journal_append(&page(), 4, b"tail")
            .unwrap()
            .is_some());
    }
    write(&peer, 4, b"tail");
    checkpoint(&peer);
    drop(peer);
    let original = ReplicaNodeServer::bind_with_journal(
        "127.0.0.1:0",
        ReplicaNodeOptions::default(),
        &fixture.path,
    )
    .unwrap();
    let mut node = original.replica.lock().unwrap();
    assert!(!node
        .service
        .read_chunk_state(&page())
        .unwrap()
        .fragment_bytes()
        .is_empty());
    assert!(node
        .service
        .prepare_journal_append(&page(), 8, b"x")
        .unwrap()
        .is_none());
    node.service
        .corrupt_chunk_payload_for_repair(&page())
        .unwrap();
    assert_eq!(
        node.service.read_chunk(&page(), 0, 8).unwrap().data,
        b"basetail"
    );
}

#[test]
fn journal_append_peer_recovery_requires_durable_storage() {
    assert!(ReplicaNodeOptions::default().local_recovery_sidecar);
    assert!(ReplicaNodeServer::bind(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            local_recovery_sidecar: false,
            ..Default::default()
        }
    )
    .is_err());
}
