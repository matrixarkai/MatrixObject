// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Chunk checkpoint v2: bounded metadata records followed by borrowed page bytes.
use super::consensus_checkpoint::{storage, Image};
use crate::{ChunkId, DataRange, PageId, PageState, ReplicaServiceSnapshot};
use matrixraft::{
    peer_runtime::{MAX_CHECKPOINT_BYTES, MAX_FILE_CHECKPOINT_BYTES},
    LogId, RaftError,
};
use prost::Message;
use std::{
    collections::VecDeque,
    io::{self, Cursor, Read},
    sync::Arc,
};

#[derive(Clone, PartialEq, Message)]
struct Header {
    #[prost(uint32, tag = "1")]
    kind: u32,
    #[prost(uint64, tag = "2")]
    index: u64,
    #[prost(uint64, tag = "3")]
    term: u64,
    #[prost(bytes = "vec", tag = "4")]
    metadata: Vec<u8>,
    #[prost(string, tag = "5")]
    checksum: String,
    #[prost(uint64, tag = "6")]
    compressed_count: u64,
    #[prost(uint64, tag = "7")]
    compression_saved: u64,
    #[prost(uint64, tag = "8")]
    pages: u64,
}
#[derive(Clone, PartialEq, Message)]
struct PageHeader {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    chunk: u64,
    #[prost(uint64, tag = "3")]
    page: u64,
    #[prost(uint64, tag = "4")]
    logical_length: u64,
    #[prost(uint64, tag = "5")]
    sequence: u64,
    #[prost(fixed64, tag = "6")]
    checksum: u64,
    #[prost(message, repeated, tag = "7")]
    discarded: Vec<Range>,
}
#[derive(Clone, PartialEq, Message)]
struct Range {
    #[prost(uint64, tag = "1")]
    offset: u64,
    #[prost(uint64, tag = "2")]
    length: u64,
}
struct Segment {
    bytes: Arc<Vec<u8>>,
    offset: usize,
}
pub(super) struct Stream {
    segments: VecDeque<Segment>,
    pub length: u64,
}
impl Stream {
    fn push(&mut self, bytes: Arc<Vec<u8>>) {
        self.length += bytes.len() as u64;
        self.segments.push_back(Segment { bytes, offset: 0 });
    }
    fn owned(&mut self, bytes: Vec<u8>) {
        self.push(Arc::new(bytes));
    }
}
impl Read for Stream {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        while let Some(segment) = self.segments.front_mut() {
            let count = out.len().min(segment.bytes.len() - segment.offset);
            if count == 0 {
                self.segments.pop_front();
                continue;
            }
            out[..count].copy_from_slice(&segment.bytes[segment.offset..segment.offset + count]);
            segment.offset += count;
            return Ok(count);
        }
        Ok(0)
    }
}
fn field(tag: u8, length: u64) -> Vec<u8> {
    let mut bytes = vec![tag];
    prost::encoding::encode_varint(length, &mut bytes);
    bytes
}
fn framed_len(length: usize) -> u64 {
    1 + prost::encoding::encoded_len_varint(length as u64) as u64 + length as u64
}

pub(super) fn capture(
    mut snapshot: ReplicaServiceSnapshot,
    id: LogId,
) -> Result<Option<Stream>, RaftError> {
    snapshot.validate().map_err(storage)?;
    let pages = std::mem::take(&mut snapshot.pages);
    let checksum = snapshot.checksum.clone();
    let compressed_count = std::mem::take(&mut snapshot.compressed_chunk_count);
    let compression_saved = std::mem::take(&mut snapshot.compression_saved_bytes);
    snapshot.seal_checksum();
    let metadata = snapshot.to_bytes().map_err(storage)?;
    let header = Header {
        kind: 2,
        index: id.index,
        term: id.term,
        metadata,
        checksum,
        compressed_count,
        compression_saved,
        pages: pages.len() as u64,
    };
    if id.index == 0 || id.term == 0 {
        return Err(storage("chunk checkpoint position is invalid"));
    }
    if header.encoded_len() > MAX_CHECKPOINT_BYTES {
        return Ok(None);
    }
    let header = header.encode_to_vec();
    let mut stream = Stream {
        segments: VecDeque::new(),
        length: 0,
    };
    stream.owned(vec![8, 2]);
    stream.owned(field(18, header.len() as u64));
    stream.owned(header);
    for (id, state) in pages {
        let header = PageHeader {
            volume: id.chunk_id.volume_id(),
            chunk: id.chunk_id.id(),
            page: id.page_index,
            logical_length: state.logical_length,
            sequence: state.write_sequence,
            checksum: state.checksum_fnv1a64,
            discarded: state
                .discarded_ranges
                .iter()
                .map(|r| Range {
                    offset: r.offset,
                    length: r.length,
                })
                .collect(),
        };
        if header.encoded_len() > MAX_CHECKPOINT_BYTES || state.data.len() > MAX_CHECKPOINT_BYTES {
            return Ok(None);
        }
        let header = header.encode_to_vec();
        let length = framed_len(header.len())
            + framed_len(state.data.len())
            + framed_len(state.fragment_bytes().len());
        stream.owned(field(26, length));
        stream.owned(field(10, header.len() as u64));
        stream.owned(header);
        stream.owned(field(18, state.data.len() as u64));
        stream.push(state.data.clone());
        stream.owned(field(26, state.fragment_bytes().len() as u64));
        if let Some(bytes) = &state.ec_parity_fragment {
            stream.push(bytes.clone());
        }
    }
    if stream.length > MAX_FILE_CHECKPOINT_BYTES {
        return Ok(None);
    }
    Ok(Some(stream))
}
fn byte(reader: &mut dyn Read) -> Result<u8, RaftError> {
    super::consensus_checkpoint::byte(reader)
}
fn varint(reader: &mut dyn Read) -> Result<u64, RaftError> {
    super::consensus_checkpoint::varint(reader)
}
fn length<R: Read>(reader: &mut io::Take<R>, tag: u8, maximum: u64) -> Result<u64, RaftError> {
    if byte(reader)? != tag {
        return Err(storage("chunk checkpoint field order mismatch"));
    }
    let len = varint(reader)?;
    if len > maximum || len > reader.limit() {
        return Err(storage("chunk checkpoint field exceeds limits"));
    }
    Ok(len)
}
fn blob<R: Read>(reader: &mut io::Take<R>, tag: u8, maximum: u64) -> Result<Vec<u8>, RaftError> {
    let len = length(reader, tag, maximum)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len as usize).map_err(storage)?;
    bytes.resize(len as usize, 0);
    reader.read_exact(&mut bytes).map_err(storage)?;
    Ok(bytes)
}
fn canonical<M: Message + Default>(bytes: &[u8]) -> Result<M, RaftError> {
    let message = M::decode(bytes).map_err(storage)?;
    if message.encode_to_vec() != bytes {
        return Err(storage("chunk checkpoint message is not canonical"));
    }
    Ok(message)
}
pub(super) fn restore(
    reader: &mut dyn Read,
    size: u64,
    id: LogId,
    capacity: u64,
    max_pages: usize,
) -> Result<ReplicaServiceSnapshot, RaftError> {
    if !(2..=MAX_FILE_CHECKPOINT_BYTES).contains(&size) {
        return Err(storage("chunk checkpoint size exceeds limits"));
    }
    let mut prefix = [0; 2];
    reader.read_exact(&mut prefix).map_err(storage)?;
    if prefix == [8, 1] {
        let image = Image::read_at(&mut Cursor::new(prefix).chain(reader), size, 2, id)?;
        if !image.catalog.is_empty() {
            return Err(storage("chunk checkpoint contains a metadata catalog"));
        }
        return ReplicaServiceSnapshot::from_bytes(&image.state).map_err(storage);
    }
    if prefix != [8, 2] {
        return Err(storage("chunk checkpoint version mismatch"));
    }
    let mut reader = reader.take(size - 2);
    let header: Header = canonical(&blob(&mut reader, 18, MAX_CHECKPOINT_BYTES as u64)?)?;
    if header.kind != 2
        || header.index != id.index
        || header.term != id.term
        || id.index == 0
        || id.term == 0
        || header.pages > max_pages as u64
        || header.pages > reader.limit()
    {
        return Err(storage("chunk checkpoint identity or page count mismatch"));
    }
    let mut snapshot = ReplicaServiceSnapshot::from_bytes(&header.metadata).map_err(storage)?;
    if !snapshot.pages.is_empty()
        || snapshot.capacity_bytes != capacity
        || snapshot.compressed_chunk_count != 0
        || snapshot.compression_saved_bytes != 0
    {
        return Err(storage("chunk checkpoint metadata or capacity mismatch"));
    }
    let mut used = 0;
    let mut previous = None;
    for _ in 0..header.pages {
        let len = length(&mut reader, 26, MAX_FILE_CHECKPOINT_BYTES)?;
        let mut page = (&mut reader).take(len);
        let meta: PageHeader = canonical(&blob(&mut page, 10, MAX_CHECKPOINT_BYTES as u64)?)?;
        let page_id = PageId {
            chunk_id: ChunkId::new(meta.volume, meta.chunk).map_err(storage)?,
            page_index: meta.page,
        };
        if previous.as_ref().is_some_and(|old| old >= &page_id) {
            return Err(storage("chunk checkpoint page order mismatch"));
        }
        let data = blob(
            &mut page,
            18,
            capacity
                .saturating_sub(used)
                .min(MAX_CHECKPOINT_BYTES as u64),
        )?;
        used += data.len() as u64;
        let sidecar = blob(&mut page, 26, data.len() as u64)?;
        if page.limit() != 0 {
            return Err(storage("chunk checkpoint page has trailing fields"));
        }
        let digests = if sidecar.is_empty() {
            None
        } else {
            Some(Arc::new(crate::chunk_ec_fragment_digests_from_data(&data)))
        };
        let state = PageState {
            logical_length: meta.logical_length,
            write_sequence: meta.sequence,
            checksum_fnv1a64: meta.checksum,
            data: Arc::new(data),
            ec_parity_fragment: if sidecar.is_empty() {
                None
            } else {
                Some(Arc::new(sidecar))
            },
            ec_fragment_digests: digests,
            discarded_ranges: meta
                .discarded
                .into_iter()
                .map(|r| DataRange {
                    offset: r.offset,
                    length: r.length,
                })
                .collect(),
        };
        previous = Some(page_id.clone());
        snapshot.pages.insert(page_id, Arc::new(state));
    }
    if reader.limit() != 0 {
        return Err(storage("chunk checkpoint has trailing fields"));
    }
    snapshot.compressed_chunk_count = header.compressed_count;
    snapshot.compression_saved_bytes = header.compression_saved;
    snapshot.checksum = header.checksum;
    snapshot.validate().map_err(storage)?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MatrixReplicaService;
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
