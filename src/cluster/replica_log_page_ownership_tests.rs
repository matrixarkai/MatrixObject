// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::{
    log_page_catalog::LogPageCatalog, replica_checkpoint_catalog, replica_checkpoint_recovery,
    replica_checkpoint_spans::PageCatalog, replica_segmented_journal::SegmentedReplicaJournal,
};
use crate::durable_payload::{outcome_log::DecidedMutation, outcome_segments::SegmentedOutcomeLog};
use crate::log_page_outcome::{LogPageOutcome, LogPageState, LOG_PAGE_INITIALIZE};
use crate::{ChunkId, PageId, PageState};
use std::path::PathBuf;
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    replica: Arc<Mutex<NodeReplica>>,
    _directory: Directory,
    path: PathBuf,
}
fn page(chunk: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, chunk).unwrap(),
        page_index: 0,
    }
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "log-page-ownership-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("outcomes");
        let log = SegmentedOutcomeLog::open(&path, 32 * 1024 * 1024).unwrap();
        let outcome = LogPageOutcome {
            version: 1,
            volume_id: 1,
            chunk_id: 2,
            kind: LOG_PAGE_INITIALIZE,
            before: None,
            after: Some(LogPageState {
                generation: 1,
                writer_epoch: 7,
                initialization: vec![8; 16],
                storage_incarnation: vec![9; 16],
                length: 0,
                published_length: 0,
                limit: 64 * 1024 * 1024,
                page_bytes: 4096,
                crc32c: 0,
                sealed: false,
                seal_sha256: vec![],
            }),
            append: None,
        };
        let metadata = replica_checkpoint_recovery::encode_scalars(
            replica_checkpoint_recovery::scalar_template(),
        )
        .unwrap();
        let record = log
            .append_decided_batch_with_metadata(&[DecidedMutation::LogPage(&outcome)], &metadata)
            .unwrap();
        let mut logs = LogPageCatalog::default();
        logs.fold(&outcome).unwrap();
        let catalog = PageCatalog {
            logs,
            ..PageCatalog::default()
        };
        replica_checkpoint_catalog::prepare_checked(
            &log,
            &catalog,
            metadata,
            Some(record.position),
            1,
            false,
            &|| Ok(()),
        )
        .unwrap()
        .publish_record(&log)
        .unwrap();
        drop(log);
        let mut service = MatrixReplicaService::new();
        let journal = SegmentedReplicaJournal::open_with_settings(
            &path,
            &mut service,
            65536,
            32 * 1024 * 1024,
            16,
        )
        .unwrap();
        let (used_bytes, health) = super::replica_health::test_parts();
        let replica = Arc::new(Mutex::new(NodeReplica {
            service,
            resident: None,
            journal: Some(Box::new(journal)),
            journal_checkpoint_after_bytes: 0,
            staged: None,
            max_pages: 65536,
            used_bytes,
            health,
            log_chunks: None,
        }));
        Self {
            replica,
            _directory: Directory(directory),
            path,
        }
    }
}
fn refused(result: Result<ReplicaReply, ObjectError>) {
    assert!(
        matches!(result,Err(ObjectError::PreconditionFailed(message)) if message.contains("page-backed log"))
    );
}
#[test]
fn log_ownership_refuses_page_rpc_bypasses_without_poisoning_unrelated_writes() {
    let f = Fixture::new();
    let p = page(2);
    let requests = vec![
        ReplicaRequest::WriteChunk {
            page_id: p.clone(),
            offset: 0,
            data: vec![1],
        },
        ReplicaRequest::WriteChunkIfSequence {
            page_id: p.clone(),
            offset: 0,
            data: vec![1],
            expected_sequence: 0,
        },
        ReplicaRequest::WriteChunkIfState {
            page_id: p.clone(),
            offset: 0,
            data: vec![1],
            expected_sequence: 0,
            expected_state_checksum: String::new(),
        },
        ReplicaRequest::WriteChunkIfSequenceDigest {
            page_id: p.clone(),
            offset: 0,
            data: vec![1],
            expected_sequence: 0,
        },
        ReplicaRequest::DiscardChunk {
            page_id: p.clone(),
            offset: 0,
            length: 1,
        },
        ReplicaRequest::DiscardChunkIfSequence {
            page_id: p.clone(),
            offset: 0,
            length: 1,
            expected_sequence: 0,
        },
        ReplicaRequest::ReadChunk {
            page_id: p.clone(),
            offset: 0,
            length: 1,
        },
        ReplicaRequest::ReadChunkBatch {
            reads: vec![(p.clone(), 0, 1)],
        },
        ReplicaRequest::DigestChunk {
            page_id: p.clone(),
            offset: 0,
            length: 1,
        },
        ReplicaRequest::DigestChunkBatch {
            reads: vec![(p.clone(), 0, 1)],
        },
        ReplicaRequest::HeadChunk { page_id: p.clone() },
        ReplicaRequest::HeadChunkBatch {
            pages: vec![p.clone()],
        },
        ReplicaRequest::ReadChunkStateRange {
            page_id: p.clone(),
            expected_state_checksum: String::new(),
            offset: 0,
            length: 1,
        },
        ReplicaRequest::ReadNamespaceFence {
            namespace: p.chunk_id.clone(),
        },
        ReplicaRequest::FenceNamespace(crate::chunk_namespace_fence::NamespaceFence {
            version: 1,
            volume_id: 1,
            chunk_id: 2,
            generation: 1,
            pages: vec![],
        }),
        ReplicaRequest::LogOp(super::super::log_op::LogOpRequest {
            version: 1,
            op: super::super::log_op::OP_TAIL,
            volume: 1,
            chunk: 2,
            generation: 1,
            ..Default::default()
        }),
    ];
    for request in requests {
        refused(apply(&f.replica, request, MAX_PAYLOAD_BYTES));
    }
    assert!(!f.replica.lock().unwrap().health.failed());
    assert!(check_install_destination(&f.replica.lock().unwrap(), &p, "", 0).is_err());
    apply(
        &f.replica,
        ReplicaRequest::WriteChunk {
            page_id: page(3),
            offset: 0,
            data: b"ordinary".to_vec(),
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap();
    assert_eq!(
        f.replica
            .lock()
            .unwrap()
            .service
            .read_chunk(&page(3), 0, 8)
            .unwrap()
            .data,
        b"ordinary"
    );
    assert!(!f.replica.lock().unwrap().health.failed());
    let path = f.path.clone();
    drop(f.replica);
    let mut service = MatrixReplicaService::new();
    let journal = SegmentedReplicaJournal::open_with_settings(
        &path,
        &mut service,
        65536,
        32 * 1024 * 1024,
        16,
    )
    .unwrap();
    assert!(journal.check_page_namespace(&page(2).chunk_id).is_err());
    assert_eq!(
        service.read_chunk(&page(3), 0, 8).unwrap().data,
        b"ordinary"
    );
}
#[test]
fn log_ownership_batch_preflight_refuses_before_an_unrelated_first_write() {
    let f = Fixture::new();
    for digest in [false, true] {
        let writes = vec![
            super::super::message::ConditionalChunkWrite {
                page_id: page(3),
                offset: 0,
                data: vec![1],
                expected_sequence: 0,
            },
            super::super::message::ConditionalChunkWrite {
                page_id: page(2),
                offset: 0,
                data: vec![1],
                expected_sequence: 0,
            },
        ];
        let result = if digest {
            apply_batch_digests(&f.replica, writes, MAX_PAYLOAD_BYTES)
        } else {
            apply_batch(&f.replica, writes, MAX_PAYLOAD_BYTES)
        };
        refused(result);
        assert!(matches!(
            f.replica.lock().unwrap().service.read_chunk_state(&page(3)),
            Err(ObjectError::NotFound(_))
        ));
        assert!(!f.replica.lock().unwrap().health.failed());
    }
}
#[test]
fn log_ownership_install_guards_run_before_epoch_or_page_changes() {
    let f = Fixture::new();
    let mut node = f.replica.lock().unwrap();
    let state = PageState {
        logical_length: 1,
        write_sequence: 1,
        checksum_fnv1a64: crate::fnv1a64(&[23]),
        data: Arc::new(vec![23]),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: vec![],
    };
    let mut candidate = node.service.clone();
    candidate
        .install_chunk_state(page(2), state.clone())
        .unwrap();
    let journal = node.journal.as_mut().unwrap();
    let before = journal.status().unwrap();
    assert!(journal.checkpoint_for_install(&candidate).is_err());
    assert!(journal
        .checkpoint_page_install(&candidate, &page(2), &state)
        .is_err());
    let after = journal.status().unwrap();
    assert_eq!(before.started_epoch, after.started_epoch);
    assert_eq!(before.published_epoch, after.published_epoch);
    assert_eq!(journal.logical_length(), 0);
    journal.check_health().unwrap();
    assert!(matches!(
        node.service.read_chunk_state(&page(2)),
        Err(ObjectError::NotFound(_))
    ));
    let mut candidate = node.service.clone();
    candidate
        .install_chunk_state(page(3), state.clone())
        .unwrap();
    node.journal
        .as_mut()
        .unwrap()
        .checkpoint_page_install(&candidate, &page(3), &state)
        .unwrap();
    assert!(node
        .journal
        .as_ref()
        .unwrap()
        .check_page_namespace(&page(2).chunk_id)
        .is_err());
}
#[test]
fn log_ownership_survives_checkpoint_worker_catalog_transfer() {
    let f = Fixture::new();
    let mut node = f.replica.lock().unwrap();
    let NodeReplica {
        service, journal, ..
    } = &mut *node;
    let journal = journal.as_mut().unwrap();
    journal.checkpoint_requested(service).unwrap();
    let before = journal.status().unwrap();
    assert!(before.checkpoint_in_flight);
    assert!(journal.checkpoint_for_install(service).is_err());
    assert_eq!(
        journal.status().unwrap().started_epoch,
        before.started_epoch
    );
    assert!(journal.check_page_namespace(&page(2).chunk_id).is_err());
    journal.check_health().unwrap();
}

#[test]
fn log_ownership_also_guards_cold_read_capture_before_any_page_is_resident() {
    let f = Fixture::new();
    {
        let mut node = f.replica.lock().unwrap();
        let backing = node
            .journal
            .as_mut()
            .unwrap()
            .enable_page_backing()
            .unwrap();
        node.resident = Some(ResidentCache::new(backing, 4096, 16).unwrap());
    }
    for request in [
        ReplicaRequest::ReadChunk {
            page_id: page(2),
            offset: 0,
            length: 1,
        },
        ReplicaRequest::ReadChunkBatch {
            reads: vec![(page(2), 0, 1)],
        },
        ReplicaRequest::DigestChunk {
            page_id: page(2),
            offset: 0,
            length: 1,
        },
        ReplicaRequest::DigestChunkBatch {
            reads: vec![(page(2), 0, 1)],
        },
        ReplicaRequest::ReadChunkStateRange {
            page_id: page(2),
            expected_state_checksum: String::new(),
            offset: 0,
            length: 1,
        },
    ] {
        refused(apply(&f.replica, request, MAX_PAYLOAD_BYTES));
    }
    assert!(!f.replica.lock().unwrap().health.failed());
}
