// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Chunk checkpoint v2: bounded metadata records followed by borrowed page bytes.
use crate::ObjectError;
use crate::{ChunkId, DataRange, PageId, PageState, ReplicaServiceSnapshot};
const MAX_CHECKPOINT_BYTES: usize = 256 * 1024 * 1024;
pub(super) const MAX_FILE_CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024 * 1024;
fn storage(error: impl ToString) -> ObjectError {
    ObjectError::Corruption(error.to_string())
}
use prost::Message;
use std::{
    collections::VecDeque,
    io::{self, Read},
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
pub(super) struct Segment {
    pub(super) bytes: Arc<Vec<u8>>,
    offset: usize,
}
pub(super) struct Stream {
    pub(super) segments: VecDeque<Segment>,
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
    id: (u64, u64),
) -> Result<Option<Stream>, ObjectError> {
    snapshot.validate().map_err(storage)?;
    let pages = std::mem::take(&mut snapshot.pages);
    let checksum = snapshot.checksum.clone();
    let compressed_count = std::mem::take(&mut snapshot.compressed_chunk_count);
    let compression_saved = std::mem::take(&mut snapshot.compression_saved_bytes);
    snapshot.seal_checksum();
    let metadata = snapshot.to_bytes().map_err(storage)?;
    let header = Header {
        kind: 2,
        index: id.0,
        term: id.1,
        metadata,
        checksum,
        compressed_count,
        compression_saved,
        pages: pages.len() as u64,
    };
    if id.0 == 0 || id.1 == 0 {
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
fn byte(reader: &mut dyn Read) -> Result<u8, ObjectError> {
    let mut value = [0];
    reader.read_exact(&mut value).map_err(storage)?;
    Ok(value[0])
}
fn varint(reader: &mut dyn Read) -> Result<u64, ObjectError> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let next = byte(reader)?;
        if shift == 63 && next > 1 {
            return Err(storage("application checkpoint integer overflow"));
        }
        value |= u64::from(next & 127) << shift;
        if next < 128 {
            if shift != 0 && next == 0 {
                return Err(storage("application checkpoint integer is not canonical"));
            }
            return Ok(value);
        }
    }
    Err(storage("application checkpoint integer overflow"))
}

fn length<R: Read>(reader: &mut io::Take<R>, tag: u8, maximum: u64) -> Result<u64, ObjectError> {
    if byte(reader)? != tag {
        return Err(storage("chunk checkpoint field order mismatch"));
    }
    let len = varint(reader)?;
    if len > maximum || len > reader.limit() {
        return Err(storage("chunk checkpoint field exceeds limits"));
    }
    Ok(len)
}
fn blob<R: Read>(reader: &mut io::Take<R>, tag: u8, maximum: u64) -> Result<Vec<u8>, ObjectError> {
    let len = length(reader, tag, maximum)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len as usize).map_err(storage)?;
    bytes.resize(len as usize, 0);
    reader.read_exact(&mut bytes).map_err(storage)?;
    Ok(bytes)
}
fn canonical<M: Message + Default>(bytes: &[u8]) -> Result<M, ObjectError> {
    let message = M::decode(bytes).map_err(storage)?;
    if message.encode_to_vec() != bytes {
        return Err(storage("chunk checkpoint message is not canonical"));
    }
    Ok(message)
}
pub(super) fn restore(
    reader: &mut dyn Read,
    size: u64,
    id: (u64, u64),
    capacity: u64,
    max_pages: usize,
) -> Result<ReplicaServiceSnapshot, ObjectError> {
    if !(2..=MAX_FILE_CHECKPOINT_BYTES).contains(&size) {
        return Err(storage("chunk checkpoint size exceeds limits"));
    }
    let mut prefix = [0; 2];
    reader.read_exact(&mut prefix).map_err(storage)?;
    if prefix != [8, 2] {
        return Err(storage("chunk checkpoint version mismatch"));
    }
    let mut reader = reader.take(size - 2);
    let header: Header = canonical(&blob(&mut reader, 18, MAX_CHECKPOINT_BYTES as u64)?)?;
    if header.kind != 2
        || header.index != id.0
        || header.term != id.1
        || id.0 == 0
        || id.1 == 0
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
