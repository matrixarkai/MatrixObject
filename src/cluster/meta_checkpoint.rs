// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Metadata checkpoints stream the existing service codec through bounded buffers.
use super::consensus_checkpoint::{byte, storage, varint, Image};
use crate::{MetaSnapshot, ObjectError};
use matrixraft::{
    peer_runtime::{MAX_CHECKPOINT_BYTES, MAX_FILE_CHECKPOINT_BYTES},
    LogId, RaftError,
};
use prost::Message;
use std::{
    io::{self, Cursor, Read},
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle},
};
const BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, PartialEq, Message)]
struct Header {
    #[prost(uint32, tag = "1")]
    kind: u32,
    #[prost(uint64, tag = "2")]
    index: u64,
    #[prost(uint64, tag = "3")]
    term: u64,
    #[prost(bytes = "vec", tag = "4")]
    catalog: Vec<u8>,
    #[prost(uint64, tag = "5")]
    roster_shape: u64,
}
struct Producer {
    receiver: Option<Receiver<Vec<u8>>>,
    worker: Option<JoinHandle<Result<(), ObjectError>>>,
    current: Cursor<Vec<u8>>,
    ended: bool,
}
impl Producer {
    fn new(snapshot: MetaSnapshot) -> Result<Self, RaftError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("metadata-checkpoint".into())
            .spawn(move || {
                let mut buffer = Vec::with_capacity(BUFFER_BYTES);
                snapshot.visit_encoded(&mut |mut bytes: &[u8]| {
                    while !bytes.is_empty() {
                        let count = bytes.len().min(BUFFER_BYTES - buffer.len());
                        buffer.extend_from_slice(&bytes[..count]);
                        bytes = &bytes[count..];
                        if buffer.len() == BUFFER_BYTES {
                            let full =
                                std::mem::replace(&mut buffer, Vec::with_capacity(BUFFER_BYTES));
                            sender.send(full).map_err(|_| {
                                ObjectError::Corruption("metadata capture reader closed".into())
                            })?;
                        }
                    }
                    Ok(())
                })?;
                if !buffer.is_empty() {
                    sender.send(buffer).map_err(|_| {
                        ObjectError::Corruption("metadata capture reader closed".into())
                    })?;
                }
                Ok(())
            })
            .map_err(storage)?;
        Ok(Self {
            receiver: Some(receiver),
            worker: Some(worker),
            current: Cursor::new(Vec::new()),
            ended: false,
        })
    }
}
impl Read for Producer {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() || self.ended {
            return Ok(0);
        }
        loop {
            let count = self.current.read(out)?;
            if count != 0 {
                return Ok(count);
            }
            match self.receiver.as_ref().unwrap().recv() {
                Ok(bytes) => self.current = Cursor::new(bytes),
                Err(_) => {
                    self.ended = true;
                    drop(self.receiver.take());
                    return match self.worker.take().unwrap().join() {
                        Ok(Ok(())) => Ok(0),
                        Ok(Err(error)) => Err(io::Error::other(error.to_string())),
                        Err(_) => Err(io::Error::other("metadata checkpoint encoder failed")),
                    };
                }
            }
        }
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        // Unblock a producer waiting on a full channel before joining it.
        drop(self.receiver.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
pub(super) struct Stream {
    reader: io::Chain<Cursor<Vec<u8>>, Producer>,
    pub length: u64,
}
impl Read for Stream {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.reader.read(out)
    }
}
pub(super) fn capture(
    snapshot: MetaSnapshot,
    catalog: Vec<u8>,
    id: LogId,
) -> Result<Option<Stream>, RaftError> {
    snapshot.validate().map_err(storage)?;
    if id.index == 0 || id.term == 0 {
        return Err(storage("metadata checkpoint position is invalid"));
    }
    let header = Header {
        kind: 1,
        index: id.index,
        term: id.term,
        catalog,
        roster_shape: MetaSnapshot::STREAM_ROSTER_SHAPE,
    };
    if header.encoded_len() > MAX_CHECKPOINT_BYTES {
        return Ok(None);
    }
    let mut length = 0u64;
    // Count without retaining encoded records. The immutable snapshot is then
    // moved into a producer which emits those same bytes through one queued buffer.
    let counted = snapshot.visit_encoded(&mut |bytes: &[u8]| {
        length = length.saturating_add(bytes.len() as u64);
        if length > MAX_FILE_CHECKPOINT_BYTES {
            Err(())
        } else {
            Ok(())
        }
    });
    if counted.is_err() {
        return Ok(None);
    }
    // Exercise the complete decoder before compacting history. Its collection and
    // string bounds also apply to otherwise valid application state.
    let mut validation_reader = Producer::new(snapshot)?;
    let snapshot = match MetaSnapshot::read_from(&mut validation_reader, length) {
        Ok(snapshot) => snapshot,
        Err(_) => return Ok(None),
    };
    drop(validation_reader);
    let mut prefix = vec![8, 2, 18];
    prost::encoding::encode_varint(header.encoded_len() as u64, &mut prefix);
    header.encode(&mut prefix).map_err(storage)?;
    prefix.push(26);
    prost::encoding::encode_varint(length, &mut prefix);
    let length = length.saturating_add(prefix.len() as u64);
    if length > MAX_FILE_CHECKPOINT_BYTES {
        return Ok(None);
    }
    let reader = Cursor::new(prefix).chain(Producer::new(snapshot)?);
    Ok(Some(Stream { reader, length }))
}
// Exact-byte restoration remains available for fixed-catalog format checks.
#[cfg(test)]
pub(super) fn restore(
    reader: &mut dyn Read,
    size: u64,
    id: LogId,
    catalog: &[u8],
) -> Result<MetaSnapshot, RaftError> {
    let (snapshot, encoded_catalog) = restore_image(reader, size, id)?;
    if encoded_catalog != catalog {
        return Err(storage("metadata checkpoint group catalog mismatch"));
    }
    Ok(snapshot)
}
/// Recover live membership without allowing a checkpoint to replace the inventory.
pub(super) fn restore_live(
    reader: &mut dyn Read,
    size: u64,
    id: LogId,
    inventory: &super::ChunkGroupCatalog,
) -> Result<(MetaSnapshot, super::ChunkGroupCatalog), RaftError> {
    let (snapshot, encoded_catalog) = restore_image(reader, size, id)?;
    let catalog = super::ChunkGroupCatalog::from_bytes(&encoded_catalog).map_err(storage)?;
    if !catalog.same_inventory(inventory) {
        return Err(storage("metadata checkpoint group inventory mismatch"));
    }
    Ok((snapshot, catalog))
}
fn restore_image(
    reader: &mut dyn Read,
    size: u64,
    id: LogId,
) -> Result<(MetaSnapshot, Vec<u8>), RaftError> {
    if !(2..=MAX_FILE_CHECKPOINT_BYTES).contains(&size) {
        return Err(storage("metadata checkpoint size exceeds limits"));
    }
    let mut prefix = [0; 2];
    reader.read_exact(&mut prefix).map_err(storage)?;
    if prefix == [8, 1] {
        let image = Image::read_at(&mut Cursor::new(prefix).chain(reader), size, 1, id)?;
        return Ok((
            MetaSnapshot::from_bytes(&image.state).map_err(storage)?,
            image.catalog,
        ));
    }
    if prefix != [8, 2] {
        return Err(storage("metadata checkpoint version mismatch"));
    }
    let mut reader = reader.take(size - 2);
    if byte(&mut reader)? != 18 {
        return Err(storage("metadata checkpoint header is missing"));
    }
    let length = varint(&mut reader)?;
    if length > MAX_CHECKPOINT_BYTES as u64 || length > reader.limit() {
        return Err(storage("metadata checkpoint header exceeds limits"));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length as usize).map_err(storage)?;
    bytes.resize(length as usize, 0);
    reader.read_exact(&mut bytes).map_err(storage)?;
    let header = Header::decode(bytes.as_slice()).map_err(storage)?;
    if header.encode_to_vec() != bytes
        || header.kind != 1
        || header.index != id.index
        || header.term != id.term
        || id.index == 0
        || id.term == 0
        || header.roster_shape != MetaSnapshot::STREAM_ROSTER_SHAPE
    {
        return Err(storage(
            "metadata checkpoint identity, catalog or encoding mismatch",
        ));
    }
    if byte(&mut reader)? != 26 {
        return Err(storage("metadata checkpoint state is missing"));
    }
    let length = varint(&mut reader)?;
    if length == 0 || length != reader.limit() {
        return Err(storage("metadata checkpoint state length mismatch"));
    }
    Ok((
        MetaSnapshot::read_from(&mut reader, length).map_err(storage)?,
        header.catalog,
    ))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MatrixMetaService, MetadataRecord, RecordValue, SharedText};
    fn id() -> LogId {
        LogId { index: 29, term: 4 }
    }
    fn fixture(count: usize, bytes: usize) -> MetaSnapshot {
        let mut snapshot = MatrixMetaService::new().export_snapshot();
        let text: RecordValue = "q".repeat(bytes).into();
        for i in 0..count {
            let key: SharedText = format!("record-{i:06}").into();
            snapshot.records.insert(
                key.clone(),
                MetadataRecord::new(key, text.clone(), 1, 10, false),
            );
        }
        let manifest = crate::object_manifest::ObjectManifest {
            format_version: 1,
            bucket: "bucket".into(),
            key: "checkpoint-object".into(),
            generation: 1,
            length: 0,
            extents: vec![],
            deleted: false,
        };
        snapshot
            .object_manifests
            .apply(&crate::object_manifest::ManifestOutcome::decide(None, manifest).unwrap())
            .unwrap();
        snapshot.checksum = snapshot.checksum_material_hash();
        snapshot
    }
    #[test]
    fn metadata_checkpoint_stream_preserves_state_and_old_envelopes() {
        let snapshot = fixture(10, 1024);
        let mut stream = capture(snapshot.clone(), vec![1, 2], id())
            .unwrap()
            .unwrap();
        let length = stream.length;
        assert_eq!(
            restore(&mut stream, length, id(), &[1, 2]).unwrap(),
            snapshot
        );
        let legacy = Image::new(1, id(), snapshot.to_bytes().unwrap(), vec![1, 2]).encode_to_vec();
        assert_eq!(
            restore(
                &mut Cursor::new(&legacy),
                legacy.len() as u64,
                id(),
                &[1, 2]
            )
            .unwrap(),
            snapshot
        );
    }
    #[test]
    fn metadata_checkpoint_stream_refuses_invalid_headers_and_damage() {
        // A valid service snapshot can exceed a decoder string bound. Capture
        // must keep WAL history rather than publish an unreadable payload.
        assert!(capture(fixture(1, 64 * 1024 * 1024 + 1), vec![1, 2], id())
            .unwrap()
            .is_none());
        let mut bytes = Vec::new();
        capture(fixture(2, 1024), vec![1, 2], id())
            .unwrap()
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        for cut in [0, 1, 2, bytes.len() / 2, bytes.len() - 1] {
            assert!(restore(&mut Cursor::new(&bytes[..cut]), cut as u64, id(), &[1, 2]).is_err());
        }
        assert!(restore(&mut Cursor::new(&bytes), bytes.len() as u64, id(), &[3]).is_err());
        assert!(restore(
            &mut Cursor::new(&bytes),
            bytes.len() as u64,
            LogId { index: 30, term: 4 },
            &[1, 2]
        )
        .is_err());
        let offset = bytes.windows(20).position(|w| w == [b'q'; 20]).unwrap();
        bytes[offset] ^= 1;
        assert!(restore(&mut Cursor::new(&bytes), bytes.len() as u64, id(), &[1, 2]).is_err());
    }
    #[test]
    fn dropping_metadata_capture_unblocks_its_producer() {
        let mut producer = Producer::new(fixture(5, BUFFER_BYTES)).unwrap();
        let worker = producer.worker.take().unwrap();
        drop(producer);
        assert!(worker.join().unwrap().is_err());
    }
    #[test]
    #[ignore = "272 MiB metadata checkpoint recovery; run explicitly in release mode"]
    fn metadata_stream_recovers_more_than_256_mib() {
        use matrixraft::peer_runtime::{DurablePeer, PeerOptions};
        let root = std::env::temp_dir().join(format!(
            "matrixobject-meta-stream-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let cleanup = Cleanup(root.clone());
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.join("metadata.peer.wal"),
        };
        let mut peer = DurablePeer::open(options.clone()).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert!(peer.is_leader());
        let position = LogId {
            index: peer.applied_index(),
            term: peer.term(),
        };
        let snapshot = fixture(272, 1024 * 1024);
        let checksum = snapshot.checksum.clone();
        let mut stream = capture(snapshot, vec![1, 2], position.clone())
            .unwrap()
            .unwrap();
        let length = stream.length;
        assert!(length > MAX_CHECKPOINT_BYTES as u64);
        peer.checkpoint_from_reader(&mut stream, length).unwrap();
        drop(stream);
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        let mut restored = None;
        peer.process_ready_with_snapshot_reader(
            |_, _| Ok(()),
            |id, reader, length| {
                assert_eq!(id, position);
                restored = Some(restore(reader, length, id, &[1, 2])?);
                Ok(())
            },
        )
        .unwrap();
        let restored = restored.unwrap();
        assert_eq!(restored.checksum, checksum);
        assert_eq!(restored.records.len(), 272);
        for record in restored.records.values() {
            assert_eq!(record.value.as_text().len(), 1024 * 1024);
        }
        println!("streamed metadata checkpoint: {length} bytes, 272 records recovered after peer restart");
        drop(peer);
        drop(cleanup);
    }
    // Test commands construct deterministic metadata fixtures. Peer packets,
    // checkpoint capture, restore, durable storage and replay are production paths.
    fn exercise_metadata_group(record_count: usize, manifest_count: usize) {
        use matrixraft::peer_runtime::{DurablePeer, PeerOptions};
        use std::{cell::RefCell, collections::BTreeMap};
        let root = std::env::temp_dir().join(format!(
            "metadata-group-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let options = |node_id| PeerOptions {
            group_id: 72,
            node_id,
            voters: vec![1, 2, 3],
            wal_path: root.join(format!("{node_id}.wal")),
        };
        let mut peers: BTreeMap<_, _> = (1..=3)
            .map(|id| (id, DurablePeer::open(options(id)).unwrap()))
            .collect();
        let (large_snapshot, later_command) = if manifest_count > 0 {
            use crate::object_manifest::{ManifestOutcome, ObjectManifest};
            use crate::placement_outcome::PlacementOutcome;
            let mut service = crate::MatrixMetaService::new();
            for index in 0..manifest_count {
                let value = ObjectManifest {
                    format_version: 1,
                    bucket: "capacity".into(),
                    key: format!("object-{index:06}"),
                    generation: 1,
                    length: 0,
                    extents: vec![],
                    deleted: false,
                };
                PlacementOutcome::publish_manifest(
                    &service,
                    ManifestOutcome::decide(None, value).unwrap(),
                    0,
                )
                .unwrap()
                .apply(&mut service)
                .unwrap();
            }
            let snapshot = service.export_snapshot();
            let before = service
                .object_manifests
                .get(&crate::ObjectId::new("capacity", "object-000000"))
                .unwrap()
                .clone();
            let mut after = before.clone();
            after.generation = 2;
            after.deleted = true;
            let command = PlacementOutcome::publish_manifest(
                &service,
                ManifestOutcome::decide(Some(before), after).unwrap(),
                1,
            )
            .unwrap()
            .to_payload()
            .unwrap();
            (snapshot, command)
        } else {
            (fixture(record_count, 1024 * 1024), vec![2])
        };
        let expected_manifests = large_snapshot.object_manifests.len();
        let initial_snapshot = if manifest_count > 0 {
            crate::MatrixMetaService::new().export_snapshot()
        } else {
            fixture(0, 0)
        };
        let updated = |state: &MetaSnapshot| {
            if manifest_count > 0 {
                state
                    .object_manifests
                    .get(&crate::ObjectId::new("capacity", "object-000000"))
                    .is_some_and(|m| m.generation == 2 && m.deleted)
            } else {
                state.records.contains_key("after-checkpoint")
            }
        };
        let mut states: BTreeMap<u64, MetaSnapshot> = BTreeMap::new();
        let mut dropped = false;
        let mut restores = 0usize;
        let mut drive = |peers: &mut BTreeMap<u64, DurablePeer>,
                         states: &mut BTreeMap<u64, MetaSnapshot>,
                         isolate: Option<u64>| {
            let mut waiting_since = None;
            let mut exchanges = 0;
            while exchanges < 4000 {
                let mut packets = Vec::new();
                for (node, peer) in peers.iter_mut() {
                    let state = RefCell::new(
                        states
                            .remove(node)
                            .unwrap_or_else(|| initial_snapshot.clone()),
                    );
                    packets.extend(
                        peer.process_ready_with_snapshot_reader(
                            |_, bytes| {
                                if bytes == [1] {
                                    *state.borrow_mut() = large_snapshot.clone();
                                } else if manifest_count > 0 && !bytes.is_empty() {
                                    let mut service = crate::MatrixMetaService::new();
                                    service.install_placement_roster(&state.borrow()).unwrap();
                                    service.install_snapshot(state.borrow().clone()).unwrap();
                                    crate::placement_outcome::PlacementOutcome::from_payload(bytes)
                                        .unwrap()
                                        .apply(&mut service)
                                        .unwrap();
                                    *state.borrow_mut() = service.export_snapshot();
                                } else if bytes == [2] {
                                    let mut s = state.borrow_mut();
                                    assert_eq!(s.records.len(), record_count);
                                    s.records.insert(
                                        "after-checkpoint".into(),
                                        MetadataRecord::new(
                                            "after-checkpoint".into(),
                                            "committed-after-transfer".into(),
                                            1,
                                            20,
                                            false,
                                        ),
                                    );
                                    s.checksum = s.checksum_material_hash();
                                } else {
                                    assert!(bytes.is_empty());
                                }
                                Ok(())
                            },
                            |id, reader, size| {
                                *state.borrow_mut() = restore(reader, size, id, &[1, 2])?;
                                restores += 1;
                                Ok(())
                            },
                        )
                        .unwrap()
                        .into_iter()
                        .filter(|p| Some(*node) != isolate && Some(p.to) != isolate),
                    );
                    states.insert(*node, state.into_inner());
                }
                if packets.is_empty() {
                    if peers.values().any(DurablePeer::checkpoint_receive_pending) {
                        let began = waiting_since.get_or_insert_with(std::time::Instant::now);
                        assert!(
                            began.elapsed() < std::time::Duration::from_secs(30),
                            "metadata checkpoint finalization did not finish"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    return;
                }
                waiting_since = None;
                exchanges += 1;
                for packet in packets {
                    assert!(packet.bytes.len() < 2 * 1024 * 1024);
                    // Canonical version-3 packets carry file payload fragments.
                    let file_packet = packet.bytes.starts_with(&[8, 3]);
                    if file_packet && !dropped {
                        dropped = true;
                        continue;
                    }
                    if file_packet {
                        peers
                            .get_mut(&packet.to)
                            .unwrap()
                            .step(&packet.bytes)
                            .unwrap();
                    }
                    peers
                        .get_mut(&packet.to)
                        .unwrap()
                        .step(&packet.bytes)
                        .unwrap();
                }
            }
            panic!("metadata checkpoint transfer did not settle");
        };
        peers.get_mut(&1).unwrap().campaign().unwrap();
        drive(&mut peers, &mut states, None);
        peers.get_mut(&1).unwrap().propose(vec![1]).unwrap();
        drive(&mut peers, &mut states, Some(3));
        assert_eq!(states[&1].records.len(), record_count);
        assert_eq!(states[&1].object_manifests.len(), expected_manifests);
        assert!(states[&3].records.is_empty());
        assert_eq!(states[&3].checksum, initial_snapshot.checksum);
        let peer = peers.get_mut(&1).unwrap();
        let id = LogId {
            index: peer.applied_index(),
            term: peer.term(),
        };
        let mut stream = capture(states[&1].clone(), vec![1, 2], id)
            .unwrap()
            .unwrap();
        let length = stream.length;
        assert!(length > record_count as u64 * 1024 * 1024);
        if manifest_count > 0 {
            assert!(length > 8 * 1024 * 1024);
        }
        peer.checkpoint_from_reader(&mut stream, length).unwrap();
        drop(stream);
        peers.get_mut(&1).unwrap().propose(later_command).unwrap();
        drive(&mut peers, &mut states, None);
        for _ in 0..80 {
            if updated(&states[&3]) {
                break;
            }
            for peer in peers.values_mut() {
                peer.tick().unwrap();
            }
            drive(&mut peers, &mut states, None);
        }
        let expected = states[&1].checksum.clone();
        for state in states.values() {
            assert_eq!(
                state.records.len(),
                record_count + usize::from(manifest_count == 0)
            );
            assert_eq!(state.object_manifests.len(), expected_manifests);
            assert!(updated(state));
            assert_eq!(state.checksum, expected);
            state.validate().unwrap();
        }
        drop(peers);
        states.clear();
        let mut peers = (1..=3)
            .map(|id| (id, DurablePeer::open(options(id)).unwrap()))
            .collect();
        drive(&mut peers, &mut states, None);
        for state in states.values() {
            assert_eq!(state.checksum, expected);
            assert_eq!(state.object_manifests.len(), expected_manifests);
            assert!(updated(state));
            state.validate().unwrap();
        }
        assert!(dropped, "file transfer was not exercised");
        assert!(
            restores >= 3,
            "expected receiver install and restart recovery"
        );
        println!("metadata group recovered {length} checkpoint bytes and a later mutation; restores={restores}");
    }
    #[test]
    fn metadata_group_stream_retries_transfer_and_recovers_later_mutations() {
        exercise_metadata_group(9, 0);
    }
    #[test]
    fn full_manifest_catalog_transfers_between_raft_voters_and_recovers_later_publication() {
        exercise_metadata_group(0, 100_000);
    }
    #[test]
    #[ignore = "272 MiB three-peer metadata checkpoint transfer and restart"]
    fn metadata_group_stream_transfers_more_than_256_mib() {
        exercise_metadata_group(272, 0);
    }
}
