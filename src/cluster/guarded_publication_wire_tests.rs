// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "guarded-wire-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
const INIT: [u8; 16] = [7; 16];
fn init() -> LogOpRequest {
    LogOpRequest::initialize_publication(3, 11, 7, 1, INIT)
}
fn append(epoch: u64, offset: u64, data: &[u8], published: u64) -> LogOpRequest {
    LogOpRequest::append_published(
        3,
        11,
        7,
        epoch,
        offset,
        data.to_vec(),
        [9; 16],
        published,
        INIT,
    )
}
fn store() -> (Dir, LogChunkStore) {
    let d = Dir::new();
    let mut s = LogChunkStore::open(&d.0).unwrap();
    s.set_publication_writes(true);
    (d, s)
}
fn probe(s: &mut LogChunkStore) -> LogOpReply {
    s.apply(&LogOpRequest::publication_probe(3, 11, 7)).unwrap()
}
fn valid_reply() -> LogOpReply {
    LogOpReply {
        publication_protocol: 4,
        publication_writes_enabled: true,
        publication_payload_limit: (crate::cluster::log_op::MAX_OP_DATA + 256) as u64,
        publication_supported: true,
        publication_mode: true,
        chunk_present: true,
        initialization_present: true,
        initialization_hi: u64::from_le_bytes([7; 8]),
        initialization_lo: u64::from_le_bytes([7; 8]),
        length: 3,
        durable_length: 3,
        published_length: 3,
        record_present: true,
        record_identified: true,
        record_length: 3,
        record_publication: true,
        record_published_length: 3,
        ..Default::default()
    }
}
#[test]
fn version4_constructors_roundtrip_canonical_borrowed_and_owned_requests() {
    for r in [
        init(),
        append(1, 0, b"abc", 3),
        LogOpRequest::fence_publication(3, 11, 7, 2, INIT),
        LogOpRequest::seal_publication(3, 11, 7, 1, INIT, true),
        LogOpRequest::publication_probe(3, 11, 7),
        LogOpRequest::read_initialized_publication(3, 11, 7, 0, 3, INIT),
        LogOpRequest::identity_initialized_publication(3, 11, 7, 0, INIT),
    ] {
        r.validate().unwrap();
        let b = r.encode_to_vec();
        assert_eq!(LogOpRequest::from_payload(&b).unwrap(), r);
        assert_eq!(LogOpRequest::from_payload_owned(b).unwrap(), r);
    }
}
#[test]
fn default_guard_reports_read_capability_but_refuses_writes_without_creating_files() {
    let d = Dir::new();
    let mut s = LogChunkStore::open(&d.0).unwrap();
    let r = probe(&mut s);
    assert_eq!(r.publication_protocol, 4);
    assert!(r.has_publication_observation());
    assert!(!r.has_publication_write_protocol());
    assert!(!r.chunk_present);
    assert!(s.apply(&init()).is_err());
    assert!(s.apply_deferred(&init()).is_err());
    assert_eq!(std::fs::read_dir(&d.0).unwrap().count(), 0);
}
#[test]
fn disabling_the_guard_preserves_read_access_and_refuses_existing_member_mutations() {
    let (_d, mut s) = store();
    s.apply(&init()).unwrap();
    s.apply(&append(1, 0, b"abc", 3)).unwrap();
    let before = std::fs::read(s.path(3, 11)).unwrap();
    s.set_publication_writes(false);
    let r = probe(&mut s);
    assert!(!r.has_publication_write_protocol());
    assert_eq!(r.initialized_publication_prefix(INIT).unwrap().length, 3);
    assert!(s.apply(&append(1, 3, b"def", 6)).is_err());
    assert_eq!(std::fs::read(s.path(3, 11)).unwrap(), before);
}
#[test]
fn mutations_and_bound_reads_cannot_create_a_missing_member() {
    let (d, mut s) = store();
    for r in [
        append(1, 0, b"abc", 3),
        LogOpRequest::fence_publication(3, 11, 7, 2, INIT),
        LogOpRequest::seal_publication(3, 11, 7, 1, INIT, false),
        LogOpRequest::read_initialized_publication(3, 11, 7, 0, 3, INIT),
        LogOpRequest::identity_initialized_publication(3, 11, 7, 0, INIT),
    ] {
        assert!(s.apply(&r).is_err());
    }
    assert_eq!(std::fs::read_dir(&d.0).unwrap().count(), 0);
}
#[test]
fn initialization_retries_bind_the_exact_identity_and_do_not_reset_published_data() {
    let (_d, mut s) = store();
    let r = s.apply_deferred(&init()).unwrap();
    assert_eq!(r.publication_initialization(), Some(INIT));
    assert!(!s.has_dirty());
    s.apply(&append(1, 0, b"abc", 3)).unwrap();
    let before = std::fs::read(s.path(3, 11)).unwrap();
    s.apply(&init()).unwrap();
    assert!(s
        .apply(&LogOpRequest::initialize_publication(3, 11, 7, 1, [8; 16]))
        .is_err());
    assert!(s
        .apply(&LogOpRequest::initialize_publication(3, 11, 7, 2, INIT))
        .is_err());
    assert_eq!(std::fs::read(s.path(3, 11)).unwrap(), before);
    assert_eq!(probe(&mut s).published_length, 3);
}
#[test]
fn appends_require_matching_initialization_and_a_previously_fenced_epoch() {
    let (_d, mut s) = store();
    s.apply(&init()).unwrap();
    let before = std::fs::read(s.path(3, 11)).unwrap();
    let mut wrong = append(1, 0, b"abc", 3);
    wrong.initialization_hi ^= 1;
    for r in [wrong, append(2, 0, b"abc", 3), append(0, 0, b"abc", 3)] {
        assert!(s.apply(&r).is_err());
        assert_eq!(std::fs::read(s.path(3, 11)).unwrap(), before);
    }
    s.apply(&append(1, 0, b"abc", 3)).unwrap();
}
#[test]
fn wire_fences_keep_complete_bytes_and_remove_only_the_partial_suffix() {
    let (_d, mut s) = store();
    s.apply(&init()).unwrap();
    s.apply(&append(1, 0, b"abc", 3)).unwrap();
    s.apply(&append(1, 3, b"part", 3)).unwrap();
    let r = s
        .apply(&LogOpRequest::fence_publication(3, 11, 7, 2, INIT))
        .unwrap();
    assert_eq!((r.length, r.published_length, r.epoch), (3, 3, 2));
    assert!(s.apply(&append(1, 3, b"def", 6)).is_err());
    s.apply(&append(2, 3, b"def", 6)).unwrap();
    assert_eq!(
        s.apply(&LogOpRequest::read_initialized_publication(
            3, 11, 7, 0, 6, INIT
        ))
        .unwrap()
        .data,
        b"abcdef"
    );
}
#[test]
fn bound_queries_refuse_another_initialization_identity() {
    let (_d, mut s) = store();
    s.apply(&init()).unwrap();
    s.apply(&append(1, 0, b"abc", 3)).unwrap();
    for r in [
        LogOpRequest::read_initialized_publication(3, 11, 7, 0, 3, [8; 16]),
        LogOpRequest::identity_initialized_publication(3, 11, 7, 0, [8; 16]),
    ] {
        assert!(s.apply(&r).is_err());
    }
    let r = probe(&mut s);
    assert!(r.initialized_publication_prefix([8; 16]).is_none());
    assert_eq!(r.initialized_publication_prefix(INIT).unwrap().length, 3);
}
#[test]
fn deferred_replies_capture_applied_publication_but_queries_wait_for_actual_sync() {
    let (_d, mut s) = store();
    s.apply(&init()).unwrap();
    let r = s.apply_deferred(&append(1, 0, b"abc", 3)).unwrap();
    assert_eq!((r.length, r.durable_length, r.published_length), (3, 3, 3));
    assert_eq!(probe(&mut s).published_length, 0);
    let syncs = s.take_pending_syncs().unwrap();
    sync_pending(&syncs).unwrap();
    s.note_synced(&syncs);
    assert_eq!(probe(&mut s).published_length, 3);
}
#[test]
fn version4_refuses_client_selected_truncation_and_malformed_phases() {
    let mut truncate = LogOpRequest::truncate(3, 11, 7, 1, 0);
    truncate.version = 4;
    let mut fence = LogOpRequest::fence_publication(3, 11, 7, 2, INIT);
    fence.offset = 1;
    let mut initialize = init();
    initialize.published_length = 1;
    let mut old = LogOpRequest::append(3, 11, 7, 1, 0, b"abc".to_vec());
    old.published_length = 3;
    for r in [
        truncate,
        fence,
        initialize,
        old,
        append(1, 0, b"abc", 2),
        append(1, u64::MAX, b"abc", 0),
    ] {
        assert!(r.validate().is_err());
        assert!(LogOpRequest::from_payload(&r.encode_to_vec()).is_err());
    }
}
#[test]
fn replies_refuse_forged_protocol_and_initialization_flags() {
    let r = valid_reply();
    let invalid = [
        LogOpReply {
            publication_protocol: 0,
            ..r.clone()
        },
        LogOpReply {
            publication_protocol: 5,
            ..r.clone()
        },
        LogOpReply {
            initialization_present: false,
            ..r.clone()
        },
        LogOpReply {
            chunk_present: false,
            ..r.clone()
        },
        LogOpReply {
            publication_mode: false,
            ..r.clone()
        },
        LogOpReply {
            publication_supported: false,
            ..r
        },
    ];
    for bad in invalid {
        let bytes = bad.encode_to_vec();
        assert!(LogOpReply::from_payload(&bytes).is_err());
        assert!(LogOpReply::from_payload_owned(bytes).is_err());
    }
}
#[test]
fn version4_owned_decoding_reuses_the_body_and_checks_new_field_encodings() {
    let mut r = valid_reply();
    r.data = b"abc".to_vec();
    let mut bytes = r.encode_to_vec();
    bytes.reserve(128);
    let p = bytes.as_ptr();
    let c = bytes.capacity();
    let got = LogOpReply::from_payload_owned(bytes).unwrap();
    assert_eq!(got, r);
    assert_eq!(got.data.as_ptr(), p);
    assert_eq!(got.data.capacity(), c);
    for suffix in [vec![0xc0, 1, 1], vec![0xc0, 1, 2], vec![0xc8, 1, 1]] {
        let mut b = r.encode_to_vec();
        b.extend(suffix);
        assert!(LogOpReply::from_payload(&b).is_err());
        assert!(LogOpReply::from_payload_owned(b).is_err());
    }
}
#[test]
fn older_queries_omit_all_new_fields_and_legacy_history_is_not_initialized() {
    let (_d, mut s) = store();
    s.apply(&init()).unwrap();
    s.apply(&append(1, 0, b"abc", 3)).unwrap();
    let old = s.apply(&LogOpRequest::publication_tail(3, 11, 7)).unwrap();
    assert_eq!(old.publication_protocol, 0);
    assert!(!old.initialization_present);
    assert_eq!((old.initialization_hi, old.initialization_lo), (0, 0));
    assert!(!old.publication_writes_enabled);
    LogOpReply::from_payload(&old.encode_to_vec()).unwrap();
    let mut legacy = LogOpRequest::append(3, 12, 7, 1, 0, b"old".to_vec());
    s.apply(&legacy).unwrap();
    let r = s.apply(&LogOpRequest::publication_probe(3, 12, 7)).unwrap();
    assert!(r.has_publication_write_protocol());
    assert!(r.publication_initialization().is_none());
    assert!(r.initialized_publication_prefix(INIT).is_none());
    legacy.version = 4;
    assert!(s.apply(&legacy).is_err());
}
use crate::cluster::{
    RemoteReplicaClient, ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer,
    ReplicaNodeShutdown,
};
struct Node {
    dir: PathBuf,
    addr: String,
    stop: ReplicaNodeShutdown,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
}
impl Node {
    fn new(dir: PathBuf, options: ReplicaNodeOptions) -> Self {
        std::fs::create_dir_all(&dir).unwrap();
        let server =
            ReplicaNodeServer::bind_with_journal("127.0.0.1:0", options, &dir.join("journal"))
                .unwrap();
        let addr = server.local_addr().unwrap().to_string();
        let stop = server.shutdown_handle();
        let worker = Some(std::thread::spawn(move || server.serve()));
        Self {
            dir,
            addr,
            stop,
            worker,
        }
    }
    fn call(&self, r: LogOpRequest) -> Result<LogOpReply, crate::cluster::ChunkLogError> {
        let mut c = RemoteReplicaClient::connect(&self.addr, &ReplicaClientOptions::default())
            .map_err(|e| crate::cluster::ChunkLogError::Refused(e))?;
        c.log_call(r).map_err(Into::into)
    }
    fn halt(&mut self) {
        self.stop.stop();
        if let Some(w) = self.worker.take() {
            w.join().unwrap().unwrap();
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.halt();
    }
}
#[test]
fn replica_default_reports_version4_without_enabling_mutation() {
    let d = Dir::new();
    let n = Node::new(d.0.join("node"), ReplicaNodeOptions::default());
    let r = n.call(LogOpRequest::publication_probe(3, 11, 7)).unwrap();
    assert_eq!(r.publication_protocol, 4);
    assert!(!r.has_publication_write_protocol());
    assert!(n.call(init()).is_err());
    assert!(!n
        .dir
        .join("logchunks/0000000000000003-000000000000000b.log")
        .exists());
}
#[test]
fn opted_in_replica_runs_init_append_fence_seal_and_bound_reads() {
    let d = Dir::new();
    let n = Node::new(
        d.0.join("node"),
        ReplicaNodeOptions {
            publication_writes: true,
            ..Default::default()
        },
    );
    assert!(n
        .call(LogOpRequest::publication_probe(3, 11, 7))
        .unwrap()
        .has_publication_write_protocol());
    n.call(init()).unwrap();
    n.call(append(1, 0, b"old", 0)).unwrap();
    assert_eq!(
        n.call(LogOpRequest::publication_probe(3, 11, 7))
            .unwrap()
            .published_length,
        0
    );
    n.call(LogOpRequest::fence_publication(3, 11, 7, 2, INIT))
        .unwrap();
    n.call(append(2, 0, b"new", 3)).unwrap();
    let sealed = n
        .call(LogOpRequest::seal_publication(3, 11, 7, 2, INIT, true))
        .unwrap();
    assert!(sealed.sealed);
    assert_eq!(sealed.sha256.len(), 32);
    assert_eq!(
        n.call(LogOpRequest::read_initialized_publication(
            3, 11, 7, 0, 3, INIT
        ))
        .unwrap()
        .data,
        b"new"
    );
}
#[test]
fn maintenance_mode_never_advertises_writable_publication() {
    let d = Dir::new();
    let n = Node::new(
        d.0.join("node"),
        ReplicaNodeOptions {
            publication_writes: true,
            maintenance: true,
            ..Default::default()
        },
    );
    assert!(!n
        .call(LogOpRequest::publication_probe(3, 11, 7))
        .unwrap()
        .has_publication_write_protocol());
    assert!(n.call(init()).is_err());
}
#[test]
fn in_memory_nodes_cannot_opt_into_publication_writes() {
    assert!(ReplicaNodeServer::bind(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            publication_writes: true,
            ..Default::default()
        }
    )
    .is_err());
}
#[test]
fn three_replica_wire_fences_preserve_completed_prefixes_before_repair() {
    let d = Dir::new();
    let mut nodes: Vec<_> = (0..3)
        .map(|i| {
            Node::new(
                d.0.join(i.to_string()),
                ReplicaNodeOptions {
                    publication_writes: true,
                    ..Default::default()
                },
            )
        })
        .collect();
    for n in &nodes {
        n.call(init()).unwrap();
    }
    for n in &nodes[..2] {
        n.call(append(1, 0, b"abc", 3)).unwrap();
    }
    let kept: Vec<_> = nodes
        .iter()
        .map(|n| {
            n.call(LogOpRequest::fence_publication(3, 11, 7, 2, INIT))
                .unwrap()
                .published_length
        })
        .collect();
    assert_eq!(kept, vec![3, 3, 0]);
    nodes[2].call(append(2, 0, b"abc", 3)).unwrap();
    nodes[0].halt();
    for n in &nodes[1..] {
        assert_eq!(
            n.call(LogOpRequest::publication_probe(3, 11, 7))
                .unwrap()
                .initialized_publication_prefix(INIT)
                .unwrap()
                .length,
            3
        );
        assert_eq!(
            n.call(LogOpRequest::read_initialized_publication(
                3, 11, 7, 0, 3, INIT
            ))
            .unwrap()
            .data,
            b"abc"
        );
    }
}
#[test]
fn restart_recovers_initialization_epoch_and_complete_prefix() {
    let d = Dir::new();
    let dir = d.0.join("node");
    let mut n = Node::new(
        dir.clone(),
        ReplicaNodeOptions {
            publication_writes: true,
            ..Default::default()
        },
    );
    n.call(init()).unwrap();
    n.call(append(1, 0, b"abc", 3)).unwrap();
    n.call(LogOpRequest::fence_publication(3, 11, 7, 2, INIT))
        .unwrap();
    n.halt();
    let n = Node::new(
        dir,
        ReplicaNodeOptions {
            publication_writes: true,
            ..Default::default()
        },
    );
    let r = n.call(LogOpRequest::publication_probe(3, 11, 7)).unwrap();
    assert_eq!(r.epoch, 2);
    assert_eq!(r.publication_initialization(), Some(INIT));
    assert_eq!(r.initialized_publication_prefix(INIT).unwrap().length, 3);
}
#[test]
fn a_failed_group_sync_cannot_make_a_publication_visible() {
    fn fail(_: &File) -> std::io::Result<()> {
        Err(std::io::Error::from_raw_os_error(5))
    }
    let (_d, mut s) = store();
    s.apply(&init()).unwrap();
    s.set_syncer(fail);
    let staged = s.apply_deferred(&append(1, 0, b"abc", 3)).unwrap();
    assert_eq!(staged.published_length, 3);
    let pending = s.take_pending_syncs().unwrap();
    assert!(sync_pending(&pending).is_err());
    assert_eq!(probe(&mut s).published_length, 0);
    assert_eq!(s.commits(), 1);
}

#[test]
fn writable_nodes_require_safe_reply_capacity_and_advertise_record_limits() {
    let d = Dir::new();
    for max_payload_bytes in [24, 511] {
        assert!(ReplicaNodeServer::bind_with_journal(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                publication_writes: true,
                max_payload_bytes,
                ..Default::default()
            },
            &d.0.join("journal")
        )
        .is_err());
        assert!(!d.0.join("journal").exists());
    }
    let n = Node::new(
        d.0.join("node"),
        ReplicaNodeOptions {
            publication_writes: true,
            max_payload_bytes: 512,
            ..Default::default()
        },
    );
    let r = n.call(LogOpRequest::publication_probe(3, 11, 7)).unwrap();
    assert_eq!(r.publication_payload_limit, 512);
    assert_eq!(r.publication_record_capacity(), Some(256));
    let mut maximum = valid_reply();
    maximum.length = LOG_CHUNK_LIMIT;
    maximum.durable_length = LOG_CHUNK_LIMIT;
    maximum.published_length = LOG_CHUNK_LIMIT;
    maximum.epoch = u64::MAX;
    maximum.crc = u32::MAX;
    maximum.sealed = true;
    maximum.sha256 = vec![255; 32];
    maximum.record_present = false;
    maximum.record_identified = false;
    maximum.record_length = 0;
    maximum.record_publication = false;
    maximum.record_published_length = 0;
    assert!(maximum.encoded_len() <= 256);
    LogOpReply::from_payload(&maximum.encode_to_vec()).unwrap();
}
