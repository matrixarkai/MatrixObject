// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Raft positions and legacy decoding around the shared replica image stream.
use super::consensus_checkpoint::{storage, Image};
pub(super) use super::replica_image::Stream;
use crate::ReplicaServiceSnapshot;
use matrixraft::{LogId, RaftError};
use std::io::{Cursor, Read};

pub(super) fn capture(
    snapshot: ReplicaServiceSnapshot,
    id: LogId,
) -> Result<Option<Stream>, RaftError> {
    super::replica_image::capture(snapshot, (id.index, id.term)).map_err(storage)
}
pub(super) fn restore(
    reader: &mut dyn Read,
    size: u64,
    id: LogId,
    capacity: u64,
    max_pages: usize,
) -> Result<ReplicaServiceSnapshot, RaftError> {
    if !(2..=super::replica_image::MAX_FILE_CHECKPOINT_BYTES).contains(&size) {
        return Err(storage("chunk checkpoint size exceeds limits"));
    }
    let mut prefix = [0; 2];
    reader.read_exact(&mut prefix).map_err(storage)?;
    let mut reader = Cursor::new(prefix).chain(reader);
    if prefix == [8, 1] {
        let image = Image::read_at(&mut reader, size, 2, id)?;
        if !image.catalog.is_empty() {
            return Err(storage("chunk checkpoint contains a metadata catalog"));
        }
        return ReplicaServiceSnapshot::from_bytes(&image.state).map_err(storage);
    }
    super::replica_image::restore(&mut reader, size, (id.index, id.term), capacity, max_pages)
        .map_err(storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChunkId, MatrixReplicaService, PageId, PageState};
    use matrixraft::peer_runtime::MAX_CHECKPOINT_BYTES;
    use prost::Message;
    use std::io;
    use std::sync::Arc;
    fn id() -> LogId {
        LogId { index: 19, term: 3 }
    }
    fn page(index: u64) -> PageId {
        PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: index,
        }
    }
    fn fixture() -> ReplicaServiceSnapshot {
        let mut service = MatrixReplicaService::new();
        service.set_capacity_bytes(16384).unwrap();
        service
            .write_chunk_idempotent(page(0), 0, &[3; 4096], "write-key")
            .unwrap();
        service.write_chunk(page(1), 16, &[7; 128]).unwrap();
        service.discard_chunk(&page(0), 20, 10).unwrap();
        service.export_snapshot()
    }
    #[test]
    fn deferred_chunk_sealing_preserves_captured_payload_and_statistics() {
        let mut service = MatrixReplicaService::new();
        service.write_chunk(page(0), 0, &[3; 4096]).unwrap();
        let expected = service.export_snapshot();
        let mut captured = service.export_snapshot_unsealed();
        assert!(captured.checksum.is_empty());
        service.write_chunk(page(0), 0, &[9; 4096]).unwrap();
        service.discard_chunk(&page(0), 20, 10).unwrap();
        let sealed = std::thread::spawn(move || {
            captured.seal_for_checkpoint();
            captured
        })
        .join()
        .unwrap();
        assert_eq!(sealed, expected);
        assert_eq!(sealed.to_bytes().unwrap(), expected.to_bytes().unwrap());
        assert_ne!(sealed, service.export_snapshot());
        sealed.validate().unwrap();
    }
    #[test]
    fn streamed_checkpoint_preserves_complete_state_and_legacy_bytes() {
        let original = fixture();
        let bytes = original.to_bytes().unwrap();
        // The historic material is the v22 body between its magic and two
        // trailing checksum strings. This independently checks the resumed hash.
        assert_eq!(
            original.checksum,
            crate::etag(&bytes[8 + "MOSRSCHUNK22".len()..bytes.len() - 64])
        );
        let mut stream = capture(original.clone(), id()).unwrap().unwrap();
        for state in original.pages.values() {
            assert!(stream
                .segments
                .iter()
                .any(|s| Arc::ptr_eq(&s.bytes, &state.data)));
        }
        let len = stream.length;
        let restored = restore(&mut stream, len, id(), 16384, 2).unwrap();
        assert_eq!(restored, original);
        assert_eq!(restored.to_bytes().unwrap(), bytes);
        let old = Image::new(2, id(), bytes, vec![]).encode_to_vec();
        assert_eq!(
            restore(&mut Cursor::new(&old), old.len() as u64, id(), 16384, 2).unwrap(),
            original
        );
    }
    #[test]
    fn streamed_checkpoint_refuses_damage_truncation_identity_and_limits() {
        let original = fixture();
        let mut encoded = Vec::new();
        capture(original, id())
            .unwrap()
            .unwrap()
            .read_to_end(&mut encoded)
            .unwrap();
        for cut in [0, 1, 2, 10, encoded.len() - 1] {
            assert!(restore(
                &mut Cursor::new(&encoded[..cut]),
                cut as u64,
                id(),
                16384,
                2
            )
            .is_err());
        }
        for (capacity, pages, position) in [
            (8192, 2, id()),
            (16384, 1, id()),
            (16384, 2, LogId { index: 20, term: 3 }),
        ] {
            assert!(restore(
                &mut Cursor::new(&encoded),
                encoded.len() as u64,
                position,
                capacity,
                pages
            )
            .is_err());
        }
        let mut bad = encoded.clone();
        let offset = bad.windows(100).position(|w| w == [3; 100]).unwrap();
        bad[offset] ^= 1;
        assert!(restore(&mut Cursor::new(&bad), bad.len() as u64, id(), 16384, 2).is_err());
        bad = encoded.clone();
        bad.extend([0]);
        assert!(restore(&mut Cursor::new(&bad), bad.len() as u64, id(), 16384, 2).is_err());
        bad = encoded;
        bad[1] = 3;
        assert!(restore(&mut Cursor::new(&bad), bad.len() as u64, id(), 16384, 2).is_err());
    }
    #[test]
    #[ignore = "272 MiB file-backed application recovery; run explicitly in release mode"]
    fn streamed_checkpoint_recovers_more_than_256_mib() {
        use std::io::{Seek, SeekFrom};
        let mut service = MatrixReplicaService::new();
        service.set_capacity_bytes(300 * 1024 * 1024).unwrap();
        let mut snapshot = service.export_snapshot_unsealed();
        let data = Arc::new(vec![37; 1024 * 1024]);
        let state = Arc::new(PageState {
            logical_length: data.len() as u64,
            write_sequence: 1,
            checksum_fnv1a64: crate::fnv1a64(&data),
            data: data.clone(),
            ec_parity_fragment: None,
            ec_fragment_digests: None,
            discarded_ranges: vec![],
        });
        for index in 0..272 {
            snapshot.pages.insert(page(index), state.clone());
        }
        snapshot.seal_checksum();
        let expected_checksum = snapshot.checksum.clone();
        let mut stream = capture(snapshot, id()).unwrap().unwrap();
        assert!(stream.length > MAX_CHECKPOINT_BYTES as u64);
        assert_eq!(
            stream
                .segments
                .iter()
                .filter(|s| Arc::ptr_eq(&s.bytes, &data))
                .count(),
            272
        );
        assert!(stream.segments.iter().all(|s| s.bytes.len() <= 1024 * 1024));
        let path = std::env::temp_dir().join(format!(
            "matrixobject-stream-{}.checkpoint",
            std::process::id()
        ));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let cleanup = Cleanup(path.clone());
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let length = stream.length;
        assert_eq!(io::copy(&mut stream, &mut file).unwrap(), length);
        file.sync_all().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let restored = restore(&mut file, length, id(), 300 * 1024 * 1024, 272).unwrap();
        assert_eq!(restored.checksum, expected_checksum);
        assert_eq!(restored.pages.len(), 272);
        for state in restored.pages.values() {
            assert_eq!(state.data.as_slice(), data.as_slice());
        }
        println!("streamed application checkpoint: {length} bytes, 272 pages recovered");
        drop(file);
        drop(cleanup);
    }
}
