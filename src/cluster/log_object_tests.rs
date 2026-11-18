// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The log object against real replica nodes (three nodes with journals, over TCP) and a metadata
//! plane that follows the rules the real one follows and counts every command it is sent. The real
//! metadata group is exercised in `tests/network_metadata_consensus.rs`.
use super::*;
use crate::cluster::{ReplicaNodeOptions, ReplicaNodeServer, StatusCode};
use crate::log_chunk_outcome::WriterLease;
use crate::object_manifest::{EXTENT_FLAG_FOOTER_SHA256, EXTENT_FLAG_TAIL};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

static NEXT: AtomicU64 = AtomicU64::new(0);
const CHUNK: u64 = 300_000;
const NOT_ENOUGH: &str = "the object holds the most entries it may; end it";

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("log-object-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Node {
    stop: crate::cluster::ReplicaNodeShutdown,
    worker: Option<thread::JoinHandle<std::io::Result<()>>>,
    addr: String,
    dir: PathBuf,
}
impl Node {
    fn start(dir: &Path, addr: &str) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let server = ReplicaNodeServer::bind_with_journal(addr, ReplicaNodeOptions::default(), &dir.join("journal")).unwrap();
        let addr = server.local_addr().unwrap().to_string();
        let stop = server.shutdown_handle();
        let worker = thread::spawn(move || server.serve());
        Self { stop, worker: Some(worker), addr, dir: dir.to_path_buf() }
    }
    fn halt(&mut self) {
        self.stop.stop();
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap().unwrap();
        }
    }
    fn restart(&mut self) {
        self.halt();
        let addr = self.addr.clone();
        *self = Self::start(&self.dir.clone(), &addr);
    }
    fn tail(&self, volume: u64, chunk: u64) -> super::super::log_op::LogOpReply {
        RemoteReplicaClient::connect(&self.addr, &ReplicaClientOptions::default()).unwrap().log_tail(volume, chunk, 1).unwrap()
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.halt();
    }
}

fn cluster() -> (Directory, Vec<Node>) {
    let dir = Directory::new();
    let nodes = (0..3).map(|i| Node::start(&dir.0.join(format!("n{i}")), "127.0.0.1:0")).collect();
    (dir, nodes)
}
fn endpoints(nodes: &[Node]) -> Vec<String> {
    nodes.iter().map(|n| n.addr.clone()).collect()
}
fn client_options() -> ReplicaClientOptions {
    ReplicaClientOptions { read_timeout: Duration::from_secs(10), connect_timeout: Duration::from_secs(2), ..Default::default() }
}
fn options(holder: &str) -> LogObjectOptions {
    let mut options = LogObjectOptions::new(holder, "group-a");
    options.chunk_bytes = CHUNK;
    options.replica = client_options();
    options
}
fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i * 7) as u8).wrapping_add((i >> 8) as u8)).collect()
}

/// A metadata plane in memory. It follows the rules of the real commands (a lease with an epoch
/// that rises on every grant, one open chunk per object, a switch that needs the live lease, the
/// entry cap) and counts what it is sent.
struct FakeMeta {
    endpoints: Vec<String>,
    index: u64,
    objects: BTreeMap<(String, String), Object>,
    cap: usize,
    grants: usize,
    switches: usize,
    renews: usize,
    manifests: usize,
    group_reads: usize,
    fail_next_switch: bool,
}
#[derive(Default)]
struct Object {
    lease: Option<(String, u64, Instant)>,
    extents: Vec<ObjectExtent>,
}
impl FakeMeta {
    fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            index: 100,
            objects: BTreeMap::new(),
            cap: MAX_OBJECT_ENTRIES,
            grants: 0,
            switches: 0,
            renews: 0,
            manifests: 0,
            group_reads: 0,
            fail_next_switch: false,
        }
    }
    fn commands(&self) -> usize {
        self.grants + self.switches + self.renews
    }
    /// The lease runs out on the metadata plane while its holder has not noticed.
    fn expire_lease(&mut self, bucket: &str, key: &str) {
        let object = self.objects.get_mut(&(bucket.to_string(), key.to_string())).unwrap();
        if let Some(lease) = object.lease.as_mut() {
            lease.2 = Instant::now().checked_sub(Duration::from_secs(1)).unwrap_or_else(Instant::now);
        }
    }
    fn answer(&self, bucket: &str, key: &str) -> LogChunkReply {
        let object = &self.objects[&(bucket.to_string(), key.to_string())];
        LogChunkReply {
            version: 1,
            lease: object.lease.as_ref().map(|(holder, epoch, _)| WriterLease { holder: holder.clone(), epoch: *epoch, expires_at_ms: 1 }),
            open: object.extents.last().filter(|extent| extent.flags & EXTENT_FLAG_TAIL != 0).cloned(),
            generation: object.extents.len() as u64,
            length: object.extents.iter().filter(|e| e.flags & EXTENT_FLAG_SEALED != 0).map(|e| e.length).sum(),
            ..Default::default()
        }
    }
    fn open_tail(&mut self, bucket: &str, key: &str, group: &str) {
        self.index += 1;
        let index = self.index;
        let object = self.objects.entry((bucket.to_string(), key.to_string())).or_default();
        let offset = object.extents.last().map_or(0, |last| last.object_offset + last.length);
        let (volume_id, chunk_id) = super::super::log_chunk_commands::chunk_identity(index);
        object.extents.push(ObjectExtent {
            group: group.into(),
            volume_id,
            chunk_id,
            object_offset: offset,
            kind: EXTENT_KIND_LOG_CHUNK,
            flags: EXTENT_FLAG_TAIL,
            ..Default::default()
        });
    }
}
fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
impl LogMeta for FakeMeta {
    fn grant(&mut self, bucket: &str, key: &str, holder: &str, ttl_ms: u64, group: &str) -> Result<LogChunkReply, ObjectError> {
        self.grants += 1;
        let now = Instant::now();
        let id = (bucket.to_string(), key.to_string());
        if let Some(object) = self.objects.get(&id) {
            if let Some((current, _, until)) = &object.lease {
                if *until > now && current != holder {
                    return Err(refused("the object is leased to another writer"));
                }
            }
        }
        self.index += 1;
        let epoch = self.index;
        let has_open = self.objects.get(&id).and_then(|o| o.extents.last()).is_some_and(|e| e.flags & EXTENT_FLAG_TAIL != 0);
        if !has_open {
            self.open_tail(bucket, key, group);
        }
        self.objects.get_mut(&id).unwrap().lease = Some((holder.to_string(), epoch, now + Duration::from_millis(ttl_ms)));
        Ok(self.answer(bucket, key))
    }
    fn switch(
        &mut self,
        bucket: &str,
        key: &str,
        holder: &str,
        epoch: u64,
        group: &str,
        sealed_length: u64,
        footer_crc: u32,
        footer_sha256: Vec<u8>,
    ) -> Result<LogChunkReply, ObjectError> {
        self.switches += 1;
        if self.fail_next_switch {
            self.fail_next_switch = false;
            return Err(refused("injected metadata failure"));
        }
        let now = Instant::now();
        let id = (bucket.to_string(), key.to_string());
        let cap = self.cap;
        let object = self.objects.get_mut(&id).ok_or_else(|| refused("the writer does not hold the live lease of this object"))?;
        match &object.lease {
            Some((current, held, until)) if current == holder && *held == epoch && *until > now => {}
            _ => return Err(refused("the writer does not hold the live lease of this object")),
        }
        if object.extents.len() + 1 > cap {
            return Err(refused(NOT_ENOUGH));
        }
        let last = object.extents.last_mut().filter(|e| e.flags & EXTENT_FLAG_TAIL != 0).ok_or_else(|| refused("the object has no chunk being written"))?;
        last.length = sealed_length;
        last.flags = EXTENT_FLAG_SEALED | if footer_sha256.is_empty() { 0 } else { EXTENT_FLAG_FOOTER_SHA256 };
        last.footer_crc = footer_crc;
        last.footer_sha256 = footer_sha256;
        if let Some(lease) = object.lease.as_mut() {
            lease.2 = lease.2.max(now + Duration::from_millis(LogChunkRequest::SWITCH_TERM_MS));
        }
        self.open_tail(bucket, key, group);
        Ok(self.answer(bucket, key))
    }
    fn renew(&mut self, holder: &str, ttl_ms: u64, renewals: Vec<LeaseRef>) -> Result<LogChunkReply, ObjectError> {
        self.renews += 1;
        let now = Instant::now();
        let mut reply = LogChunkReply { version: 1, ..Default::default() };
        for renewal in renewals {
            let object = self.objects.get_mut(&(renewal.bucket.clone(), renewal.key.clone()));
            match object.and_then(|o| o.lease.as_mut()) {
                Some((current, held, until)) if current == holder && *held == renewal.epoch && *until > now => {
                    *until = (*until).max(now + Duration::from_millis(ttl_ms));
                    reply.renewed += 1;
                }
                _ => reply.lost.push(renewal),
            }
        }
        Ok(reply)
    }
    fn manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        self.manifests += 1;
        let object = self.objects.get(&(bucket.to_string(), key.to_string())).ok_or_else(|| ObjectError::NotFound("no such object".into()))?;
        Ok(ObjectManifest {
            format_version: 3,
            bucket: bucket.into(),
            key: key.into(),
            generation: object.extents.len() as u64,
            length: object.extents.iter().filter(|e| e.flags & EXTENT_FLAG_SEALED != 0).map(|e| e.length).sum(),
            extents: object.extents.clone(),
            deleted: false,
        })
    }
    fn group_endpoints(&mut self, _group: &str) -> Result<Vec<String>, ObjectError> {
        self.group_reads += 1;
        Ok(self.endpoints.clone())
    }
}

fn id(key: &str) -> (String, String) {
    ("bucket".to_string(), key.to_string())
}

fn reader() -> LogObjectReader {
    LogObjectReader::new("bucket", "log", client_options())
}

#[test]
fn appends_cross_chunks_and_read_back_byte_identical() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut object = LogObject::new("bucket", "log", options("gw-a"));
    assert_eq!(object.open_for_append(&mut meta).unwrap(), 0);
    let mut expected = Vec::new();
    for i in 0..5u8 {
        let piece = bytes(i, 170_000);
        expected.extend_from_slice(&piece);
        assert_eq!(object.append(&mut meta, &piece).unwrap(), expected.len() as u64);
    }
    // 850,000 bytes at 300,000 per chunk: two switches, three entries, the first two sealed
    assert_eq!((meta.grants, meta.switches, meta.renews), (1, 2, 0));
    let manifest = meta.manifest("bucket", "log").unwrap();
    let lengths: Vec<(u64, bool)> = manifest.extents.iter().map(|e| (e.object_offset, e.flags & EXTENT_FLAG_SEALED != 0)).collect();
    assert_eq!(lengths, vec![(0, true), (300_000, true), (600_000, false)]);
    assert_eq!(manifest.extents[0].length, 300_000);
    assert_eq!(manifest.extents[1].length, 300_000);
    // a sealed chunk is sealed on the nodes, not only in the manifest
    for extent in &manifest.extents[..2] {
        for node in &nodes {
            let tail = node.tail(extent.volume_id, extent.chunk_id);
            assert!(tail.sealed && tail.length == 300_000, "a sealed chunk is sealed on every node");
            assert_eq!(tail.crc, extent.footer_crc);
        }
    }
    let open = &manifest.extents[2];
    assert!(nodes.iter().all(|n| !n.tail(open.volume_id, open.chunk_id).sealed));
    let mut reader = reader();
    assert_eq!(reader.length(&mut meta).unwrap(), 850_000);
    assert_eq!(reader.read_range(&mut meta, 0, 2_000_000).unwrap(), expected);
    // ranges inside a chunk, across one boundary, across both, and at the very end
    for (offset, length) in [(10u64, 1_000usize), (299_000, 2_000), (250_000, 400_000), (849_000, 5_000), (600_000, 250_000)] {
        let end = (offset as usize + length).min(expected.len());
        assert_eq!(reader.read_range(&mut meta, offset, length).unwrap(), expected[offset as usize..end], "range {offset}+{length}");
    }
    assert!(reader.read_range(&mut meta, 850_000, 10).unwrap().is_empty());
}

#[test]
fn no_metadata_command_is_sent_between_chunk_switches() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut object = LogObject::new("bucket", "log", options("gw-a"));
    object.open_for_append(&mut meta).unwrap();
    object.append(&mut meta, &bytes(1, 10)).unwrap();
    let (commands, manifests, groups) = (meta.commands(), meta.manifests, meta.group_reads);
    for i in 0..20u8 {
        object.append(&mut meta, &bytes(i, 5_000)).unwrap();
    }
    // 100,010 bytes: still inside the first chunk. Not a command, not a manifest read, not a lookup.
    assert_eq!((meta.commands(), meta.manifests, meta.group_reads), (commands, manifests, groups));
    // crossing the boundary costs exactly one command: the switch
    object.append(&mut meta, &bytes(9, 250_000)).unwrap();
    assert_eq!((meta.commands(), meta.switches), (commands + 1, 1));
}

#[test]
fn a_reader_sees_exactly_the_acknowledged_bytes_while_the_writer_appends() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut object = LogObject::new("bucket", "log", options("gw-a"));
    object.open_for_append(&mut meta).unwrap();
    let mut reader = reader();
    let mut expected = Vec::new();
    for i in 0..7u8 {
        let piece = bytes(i, 130_000);
        expected.extend_from_slice(&piece);
        object.append(&mut meta, &piece).unwrap();
        // the live tail, and the chunks it has moved through since the reader last looked
        assert_eq!(reader.read_range(&mut meta, 0, 5_000_000).unwrap(), expected, "after append {i}");
        assert_eq!(reader.length(&mut meta).unwrap(), expected.len() as u64);
    }
}

#[test]
fn a_new_writer_takes_over_at_the_tail_the_nodes_hold_and_the_old_one_is_fenced() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut a = LogObject::new("bucket", "log", options("gw-a"));
    a.open_for_append(&mut meta).unwrap();
    let mut expected = bytes(1, 500_000);
    a.append(&mut meta, &expected).unwrap();
    // the lease runs out on the metadata plane; A has not noticed; B takes the object
    meta.expire_lease("bucket", "log");
    let mut b = LogObject::new("bucket", "log", options("gw-b"));
    assert_eq!(b.open_for_append(&mut meta).unwrap(), 500_000, "the tail is what the nodes hold");
    // A's late append is refused by the nodes, which B fenced when it opened the chunk
    assert_eq!(a.append(&mut meta, &bytes(7, 1_000)).unwrap_err(), LogObjectError::LostLease);
    let more = bytes(2, 150_000);
    b.append(&mut meta, &more).unwrap();
    expected.extend_from_slice(&more);
    let mut reader = reader();
    assert_eq!(reader.read_range(&mut meta, 0, 2_000_000).unwrap(), expected);
}

#[test]
fn a_node_that_was_down_is_caught_up_and_a_new_writer_starts_at_the_acknowledged_length() {
    let (_dir, mut nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut a = LogObject::new("bucket", "log", options("gw-a"));
    a.open_for_append(&mut meta).unwrap();
    let mut expected = bytes(3, 100_000);
    a.append(&mut meta, &expected).unwrap();
    nodes[2].halt();
    // two of three nodes hold each append: it is acknowledged
    let more = bytes(4, 150_000);
    a.append(&mut meta, &more).unwrap();
    expected.extend_from_slice(&more);
    nodes[2].restart();
    meta.expire_lease("bucket", "log");
    let mut b = LogObject::new("bucket", "log", options("gw-b"));
    assert_eq!(b.open_for_append(&mut meta).unwrap(), 250_000, "no acknowledged byte is lost and none is added");
    let tail = bytes(5, 200_000);
    b.append(&mut meta, &tail).unwrap();
    expected.extend_from_slice(&tail);
    let mut reader = reader();
    assert_eq!(reader.read_range(&mut meta, 0, 2_000_000).unwrap(), expected);
}

fn unknown_verb_node() -> (String, thread::JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut count = 0;
        loop {
            let mut header = [0u8; 6];
            if peer.read_exact(&mut header).is_err() {
                break;
            }
            let mut payload = vec![0; u32::from_be_bytes(header[2..].try_into().unwrap()) as usize];
            if peer.read_exact(&mut payload).is_err() {
                break;
            }
            count += 1;
            peer.write_all(&crate::cluster::message::encode_failure(StatusCode::UnknownRequest, "no log chunks here")).unwrap();
        }
        count
    });
    (addr, worker)
}

#[test]
fn a_node_that_does_not_speak_log_chunks_stops_the_writer_before_anything_is_granted() {
    let (_dir, nodes) = cluster();
    let (old, worker) = unknown_verb_node();
    let mut eps = endpoints(&nodes);
    eps[1] = old;
    let mut meta = FakeMeta::new(eps);
    let mut object = LogObject::new("bucket", "log", options("gw-a"));
    assert_eq!(object.open_for_append(&mut meta).unwrap_err(), LogObjectError::NotSpoken);
    // the grant is a metadata commit that cannot be taken back: none was sent, and none of the
    // good nodes was asked to hold a chunk
    assert_eq!(meta.grants, 0);
    assert_eq!(object.append(&mut meta, b"x").unwrap_err(), LogObjectError::NotOpen);
    drop(object);
    assert_eq!(worker.join().unwrap(), 1, "the old node was asked once");
}

#[test]
fn a_lease_that_has_run_out_is_refused_before_a_byte_is_written() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut settings = options("gw-a");
    settings.ttl_ms = 300;
    let mut object = LogObject::new("bucket", "log", settings);
    object.open_for_append(&mut meta).unwrap();
    object.append(&mut meta, &bytes(1, 1_000)).unwrap();
    let extent = meta.objects[&id("log")].extents[0].clone();
    let before = nodes.iter().map(|n| n.tail(extent.volume_id, extent.chunk_id).length).collect::<Vec<_>>();
    thread::sleep(Duration::from_millis(400));
    assert_eq!(object.append(&mut meta, &bytes(2, 1_000)).unwrap_err(), LogObjectError::LostLease);
    let after = nodes.iter().map(|n| n.tail(extent.volume_id, extent.chunk_id).length).collect::<Vec<_>>();
    assert_eq!(before, after);
    // the handle is closed: it must be opened again to continue
    assert_eq!(object.append(&mut meta, b"x").unwrap_err(), LogObjectError::NotOpen);
}

#[test]
fn a_lease_is_renewed_at_half_its_term_by_one_command_and_a_batch_is_one_command_for_all() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut settings = options("gw-a");
    settings.ttl_ms = 1_000;
    let mut objects: Vec<LogObject> = ["a", "b", "c"].iter().map(|key| LogObject::new("bucket", key, settings.clone())).collect();
    for object in objects.iter_mut() {
        object.open_for_append(&mut meta).unwrap();
        object.append(&mut meta, &bytes(1, 1_000)).unwrap();
    }
    assert_eq!(meta.renews, 0, "a young lease is not renewed");
    thread::sleep(Duration::from_millis(550));
    assert!(objects.iter().all(|o| o.renew_due()));
    // one writer holding three objects renews them all with ONE command
    let mut refs: Vec<&mut LogObject> = objects.iter_mut().collect();
    let lost = renew_leases(&mut meta, "gw-a", 1_000, &mut refs).unwrap();
    assert!(lost.is_empty());
    assert_eq!(meta.renews, 1);
    assert!(objects.iter().all(|o| !o.renew_due()));
    // appends after that send nothing
    for object in objects.iter_mut() {
        object.append(&mut meta, &bytes(2, 1_000)).unwrap();
    }
    assert_eq!(meta.renews, 1);
    // an object appended to past half of its term renews itself, with one command
    thread::sleep(Duration::from_millis(550));
    objects[0].append(&mut meta, &bytes(3, 1_000)).unwrap();
    assert_eq!(meta.renews, 2);
    // the metadata plane no longer holds a lease for this writer: the batch says which, and closes it
    meta.objects.get_mut(&id("b")).unwrap().lease = Some(("gw-z".into(), 1, Instant::now() + Duration::from_secs(60)));
    let mut refs: Vec<&mut LogObject> = objects.iter_mut().collect();
    let lost = renew_leases(&mut meta, "gw-a", 1_000, &mut refs).unwrap();
    assert_eq!(lost.len(), 1);
    assert_eq!(lost[0].key, "b");
    assert_eq!(objects[1].append(&mut meta, b"x").unwrap_err(), LogObjectError::NotOpen);
    assert!(objects[0].append(&mut meta, b"y").is_ok());
}

#[test]
fn an_object_at_its_cap_fills_its_open_chunk_and_refuses_the_append_that_does_not_fit() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    meta.cap = 2;
    let mut settings = options("gw-a");
    settings.max_entries = 2;
    let mut object = LogObject::new("bucket", "log", settings);
    object.open_for_append(&mut meta).unwrap();
    let data = bytes(1, 700_000);
    // chunk one (300,000), a switch (two entries), chunk two (300,000), then no third entry
    assert_eq!(object.append(&mut meta, &data).unwrap_err(), LogObjectError::EntryCap);
    assert_eq!(object.length(), Some(600_000));
    assert_eq!(meta.switches, 1, "the cap is checked before a second switch is sent");
    // the open chunk was NOT sealed: it is full, and still readable and appendable up to its size
    let manifest = meta.manifest("bucket", "log").unwrap();
    let open = manifest.extents.last().unwrap();
    assert!(nodes.iter().all(|n| !n.tail(open.volume_id, open.chunk_id).sealed));
    let mut reader = reader();
    assert_eq!(reader.read_range(&mut meta, 0, 2_000_000).unwrap(), data[..600_000]);
}

#[test]
fn a_switch_that_never_reached_the_metadata_plane_is_finished_by_the_next_writer() {
    let (_dir, nodes) = cluster();
    let mut meta = FakeMeta::new(endpoints(&nodes));
    let mut a = LogObject::new("bucket", "log", options("gw-a"));
    a.open_for_append(&mut meta).unwrap();
    let first = bytes(1, 250_000);
    a.append(&mut meta, &first).unwrap();
    // the chunk is sealed on the nodes, then the switch is lost on its way to the metadata plane
    meta.fail_next_switch = true;
    let error = a.append(&mut meta, &bytes(2, 100_000)).unwrap_err();
    assert!(matches!(error, LogObjectError::Meta(_)), "{error:?}");
    assert_eq!(a.append(&mut meta, b"x").unwrap_err(), LogObjectError::NotOpen);
    let manifest = meta.manifest("bucket", "log").unwrap();
    assert_eq!(manifest.extents.len(), 1, "the manifest still names the sealed chunk as the open one");
    // the same writer opens again: the nodes' footer is recorded, and the object goes on
    meta.expire_lease("bucket", "log");
    let mut b = LogObject::new("bucket", "log", options("gw-a"));
    let length = b.open_for_append(&mut meta).unwrap();
    assert_eq!(length, 300_000, "the sealed chunk ended where the nodes say");
    let manifest = meta.manifest("bucket", "log").unwrap();
    assert_eq!(manifest.extents.len(), 2);
    assert!(manifest.extents[0].flags & EXTENT_FLAG_SEALED != 0);
    let more = bytes(3, 50_000);
    b.append(&mut meta, &more).unwrap();
    let mut reader = reader();
    let whole = reader.read_range(&mut meta, 0, 2_000_000).unwrap();
    assert_eq!(whole.len(), 350_000);
    assert_eq!(whole[..250_000], first[..]);
    assert_eq!(whole[300_000..], more[..]);
}

#[test]
fn the_error_mapping_names_what_the_metadata_plane_and_the_nodes_say() {
    assert_eq!(from_meta(refused("the object is leased to another writer")), LogObjectError::LeasedElsewhere);
    assert_eq!(from_meta(refused("the writer does not hold the live lease of this object")), LogObjectError::LostLease);
    assert_eq!(from_meta(refused(NOT_ENOUGH)), LogObjectError::EntryCap);
    assert!(matches!(from_meta(refused("chunk-granular manifests are not enabled for this cluster")), LogObjectError::Meta(_)));
    assert_eq!(from_chunk(ChunkLogError::StaleEpoch { current: 9 }), LogObjectError::LostLease);
    assert_eq!(from_chunk(ChunkLogError::NotSpoken), LogObjectError::NotSpoken);
    assert!(matches!(from_chunk(ChunkLogError::Sealed), LogObjectError::Chunk(ChunkLogError::Sealed)));
}
