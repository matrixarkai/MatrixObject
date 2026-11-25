// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Binary outcome batches and immutable payloads committed with one file sync.
use super::{
    argument, invalid, padded, read_at, PayloadExtent, PayloadExtentStore, MAX_BATCH_BYTES,
    MAX_BATCH_EXTENTS, MAX_EXTENT_BYTES, READY, SECTOR_BYTES,
};
use crate::{
    chunk_namespace_fence::NamespaceFenceOutcome,
    log_page_outcome::LogPageOutcome,
    outcome::{BytePatch, PageOutcome},
    MatrixReplicaService, ObjectError,
};
use prost::Message;
use ring::digest;
use std::{
    borrow::Cow,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    path::Path,
    sync::atomic::Ordering,
};
const RECORD_MAGIC: &[u8; 8] = b"MOEXREC1";
const ENVELOPE: usize = 128;
const COMMIT_OFFSET: u64 = 127;
const MAX_HEADER: usize = 16 * 1024 * 1024;
pub const MAX_REPLICA_METADATA_BYTES: usize = 64 * 1024;
/// A log transition can accompany every page of a full 64-page chunk.
/// Earlier batch versions retain their original limit.
pub const MAX_LOG_BATCH_DECISIONS: usize = 128;
fn decision_limit(version: u32) -> usize {
    if version == 4 {
        MAX_LOG_BATCH_DECISIONS
    } else {
        MAX_BATCH_EXTENTS
    }
}
#[derive(Debug)]
pub(super) struct CapacityExhausted;
impl std::fmt::Display for CapacityExhausted {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("stored outcome file capacity exhausted")
    }
}
impl std::error::Error for CapacityExhausted {}

pub use crate::checkpoint_fragment::CheckpointFragment;

/// An already decided mutation. Persistence does not re-execute a client request.
#[derive(Clone, Copy)]
pub enum DecidedMutation<'a> {
    Page(&'a PageOutcome),
    LogPage(&'a LogPageOutcome),
    NamespaceFence(&'a NamespaceFenceOutcome),
    CheckpointFragment(&'a CheckpointFragment),
}
#[derive(Debug, PartialEq)]
pub enum RecoveredMutation {
    Page(PageOutcome),
    LogPage(LogPageOutcome),
    NamespaceFence(NamespaceFenceOutcome),
    CheckpointFragment(CheckpointFragment),
}
impl RecoveredMutation {
    pub fn apply(
        &self,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> Result<(), ObjectError> {
        match self {
            Self::Page(value) => value.apply(service, max_pages),
            Self::LogPage(_) => Err(ObjectError::Corruption(
                "log page outcomes require log-aware recovery".into(),
            )),
            Self::NamespaceFence(value) => value.apply(service),
            Self::CheckpointFragment(_) => Err(ObjectError::Corruption(
                "checkpoint storage records require checkpoint-aware recovery".into(),
            )),
        }
    }
    pub fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        match self {
            Self::Page(value) => value.to_payload(),
            Self::LogPage(value) => value.to_payload(),
            Self::NamespaceFence(value) => value.to_payload(),
            Self::CheckpointFragment(value) => value.to_payload(),
        }
    }
}
/// Node-local physical location, suitable for a binary checkpoint catalog.
#[derive(Clone, PartialEq, Message)]
pub struct StoredOutcomePosition {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub store_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(uint64, tag = "4")]
    pub length: u64,
}
#[derive(Clone, PartialEq, Message)]
pub struct StoredDecision {
    /// Patch positions remain here; their data is stored in the payload stream.
    #[prost(message, optional, tag = "1")]
    pub page: Option<PageOutcome>,
    #[prost(uint32, repeated, packed = "true", tag = "2")]
    pub patch_lengths: Vec<u32>,
    #[prost(uint64, tag = "3")]
    pub data_offset: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub original_sha256: Vec<u8>,
    #[prost(uint32, tag = "5")]
    pub original_length: u32,
    #[prost(message, optional, tag = "6")]
    pub namespace_fence: Option<NamespaceFenceOutcome>,
    #[prost(message, optional, boxed, tag = "7")]
    pub checkpoint_fragment: Option<Box<CheckpointFragment>>,
    #[prost(message, optional, tag = "8")]
    pub log_page: Option<LogPageOutcome>,
}
#[derive(Clone, PartialEq, Message)]
pub struct StoredOutcomeBatch {
    /// Version 1 carries state transitions; version 2 carries only checkpoint fragments.
    /// Version 4 includes log transitions and permits up to 128 decisions.
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub decisions: Vec<StoredDecision>,
    #[prost(message, repeated, tag = "3")]
    pub payloads: Vec<PayloadExtent>,
    /// Version 3 binds bounded replica metadata to the same data synchronization.
    #[prost(bytes = "vec", tag = "4")]
    pub replica_metadata: Vec<u8>,
}
#[derive(Debug)]
pub struct StoredRecordMetadata {
    pub position: StoredOutcomePosition,
    pub batch: StoredOutcomeBatch,
}

/// An append-only outcome log. Its file profile is distinct from raw payload files.
/// Metadata readers do not load payloads. Hydration/replay verifies payloads and
/// the original logical outcome bytes. The caller validates the decided state;
/// an I/O failure returns no committed location and fences the handle.
pub struct DurableOutcomeLog {
    store: PayloadExtentStore,
}

enum Frame {
    Complete(StoredRecordMetadata, bool),
    Partial,
}
fn object(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
fn page_shape(page: &PageOutcome, lengths: &[u32], external: bool) -> io::Result<u64> {
    if page.version != 1
        || page.data_length > 256 * 1024 * 1024
        || page.physical_deltas.len() != 4
        || page.patches.len() != lengths.len()
    {
        return Err(invalid("invalid stored page outcome shape"));
    }
    page.page_id().map_err(object)?;
    let mut end = 0u64;
    let mut bytes = 0u64;
    for (patch, &length) in page.patches.iter().zip(lengths) {
        let next = patch
            .offset
            .checked_add(u64::from(length))
            .ok_or_else(|| invalid("stored patch offset overflow"))?;
        if length == 0
            || patch.offset < end
            || next > page.data_length
            || (external && !patch.data.is_empty())
            || (!external && patch.data.len() != length as usize)
        {
            return Err(invalid("invalid stored page patch"));
        }
        end = next;
        bytes = bytes
            .checked_add(u64::from(length))
            .ok_or_else(|| invalid("stored patch length overflow"))?;
    }
    Ok(bytes)
}
fn stripped(page: &PageOutcome) -> PageOutcome {
    PageOutcome {
        version: page.version,
        volume_id: page.volume_id,
        chunk_id: page.chunk_id,
        page_index: page.page_index,
        before_checksum: page.before_checksum.clone(),
        logical_length: page.logical_length,
        write_sequence: page.write_sequence,
        data_checksum: page.data_checksum,
        patches: page
            .patches
            .iter()
            .map(|p| BytePatch {
                offset: p.offset,
                data: Vec::new(),
            })
            .collect(),
        discarded_ranges: page.discarded_ranges.clone(),
        after_checksum: page.after_checksum.clone(),
        data_length: page.data_length,
        physical_deltas: page.physical_deltas.clone(),
    }
}
// Version 4 binds an append identity to exactly its contiguous page patches.
fn validate_log_pages(batch: &StoredOutcomeBatch) -> io::Result<()> {
    let mut chunks = std::collections::BTreeSet::new();
    for log in batch
        .decisions
        .iter()
        .filter_map(|decision| decision.log_page.as_ref())
    {
        log.validate().map_err(object)?;
        if !chunks.insert((log.volume_id, log.chunk_id)) {
            return Err(invalid("stored batch repeats a log chunk transition"));
        }
        let Some(append) = &log.append else { continue };
        let page_bytes = u64::from(log.after.as_ref().unwrap().page_bytes);
        let mut patches = Vec::new();
        for decision in &batch.decisions {
            let Some(page) = &decision.page else { continue };
            if (page.volume_id, page.chunk_id) != (log.volume_id, log.chunk_id) {
                continue;
            }
            if page.data_length > page_bytes
                || page.logical_length != page.data_length
                || !page.discarded_ranges.is_empty()
            {
                return Err(invalid("log append carries invalid page state"));
            }
            for (patch, length) in page.patches.iter().zip(&decision.patch_lengths) {
                let offset = page
                    .page_index
                    .checked_mul(page_bytes)
                    .and_then(|start| start.checked_add(patch.offset))
                    .ok_or_else(|| invalid("log append page address overflow"))?;
                patches.push((offset, u64::from(*length)));
            }
        }
        patches.sort_unstable();
        let mut next = append.offset;
        for (offset, length) in patches {
            if offset != next {
                return Err(invalid("log append page coverage is not contiguous"));
            }
            next = next
                .checked_add(length)
                .ok_or_else(|| invalid("log append coverage overflow"))?;
        }
        if next != log.after.as_ref().unwrap().length {
            return Err(invalid("log append page coverage differs from identity"));
        }
    }
    Ok(())
}

fn validate_log_data<'a>(
    logs: impl Iterator<Item = &'a LogPageOutcome>,
    pages: &[&PageOutcome],
) -> io::Result<()> {
    for log in logs.filter(|log| log.append.is_some()) {
        let page_bytes = u64::from(log.after.as_ref().unwrap().page_bytes);
        let mut parts = Vec::new();
        for page in pages {
            if (page.volume_id, page.chunk_id) == (log.volume_id, log.chunk_id) {
                for patch in &page.patches {
                    let offset = page
                        .page_index
                        .checked_mul(page_bytes)
                        .and_then(|start| start.checked_add(patch.offset))
                        .ok_or_else(|| invalid("log append page address overflow"))?;
                    parts.push((offset, patch.data.as_slice()));
                }
            }
        }
        parts.sort_unstable_by_key(|(offset, _)| *offset);
        log.check_append_parts(parts.into_iter().map(|(_, data)| data))
            .map_err(object)?;
    }
    Ok(())
}

fn descriptors(id: &[u8], data: &[u8]) -> Vec<PayloadExtent> {
    data.chunks(MAX_EXTENT_BYTES)
        .map(|bytes| {
            let mut hashes = Vec::with_capacity(bytes.len().div_ceil(SECTOR_BYTES) * 32);
            for sector in bytes.chunks(SECTOR_BYTES) {
                if sector.len() == SECTOR_BYTES {
                    hashes.extend_from_slice(digest::digest(&digest::SHA256, sector).as_ref());
                } else {
                    let mut block = [0; SECTOR_BYTES];
                    block[..sector.len()].copy_from_slice(sector);
                    hashes.extend_from_slice(digest::digest(&digest::SHA256, &block).as_ref());
                }
            }
            let checksum = if bytes.len() == SECTOR_BYTES {
                hashes[..32].to_vec()
            } else {
                digest::digest(&digest::SHA256, bytes).as_ref().to_vec()
            };
            PayloadExtent {
                version: 1,
                store_id: id.to_vec(),
                offset: 0,
                length: bytes.len() as u64,
                sha256: checksum,
                sector_sha256: hashes,
            }
        })
        .collect()
}
fn header_digest(bytes: &[u8]) -> digest::Digest {
    let mut hash = digest::Context::new(&digest::SHA256);
    hash.update(&bytes[..56]);
    hash.update(&bytes[88..112]);
    hash.finish()
}
fn envelope(body: &[u8], header: usize, span: u64, start: u64, id: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0; header];
    bytes[..8].copy_from_slice(RECORD_MAGIC);
    bytes[8..12].copy_from_slice(&(header as u32).to_be_bytes());
    bytes[12..16].copy_from_slice(&(body.len() as u32).to_be_bytes());
    bytes[16..24].copy_from_slice(&span.to_be_bytes());
    bytes[24..56].copy_from_slice(digest::digest(&digest::SHA256, body).as_ref());
    bytes[88..96].copy_from_slice(&start.to_be_bytes());
    bytes[96..112].copy_from_slice(id);
    let checksum = header_digest(&bytes);
    bytes[56..88].copy_from_slice(checksum.as_ref());
    bytes[ENVELOPE..ENVELOPE + body.len()].copy_from_slice(body);
    bytes
}
// Bound repeated top-level messages before protobuf allocation. Canonical
// encoding and nested semantic limits are checked after decoding.
fn preflight(mut bytes: &[u8]) -> io::Result<()> {
    let mut decisions = 0;
    let mut payloads = 0;
    let mut version = 0;
    while !bytes.is_empty() {
        let key = prost::encoding::decode_varint(&mut bytes).map_err(object)?;
        match (key >> 3, key & 7) {
            (1, 0) => {
                version = prost::encoding::decode_varint(&mut bytes).map_err(object)?;
            }
            (field @ (2 | 3 | 4), 2) => {
                let length =
                    usize::try_from(prost::encoding::decode_varint(&mut bytes).map_err(object)?)
                        .map_err(object)?;
                if length > bytes.len() {
                    return Err(invalid("truncated stored outcome protobuf"));
                }
                if field == 2 {
                    decisions += 1;
                    if decisions > MAX_LOG_BATCH_DECISIONS {
                        return Err(invalid("too many stored decisions"));
                    }
                } else if field == 4 {
                    if length > MAX_REPLICA_METADATA_BYTES {
                        return Err(invalid("replica metadata exceeds bound"));
                    }
                } else {
                    payloads += 1;
                    if payloads > MAX_BATCH_BYTES / MAX_EXTENT_BYTES || length > 16384 {
                        return Err(invalid("too many stored payload descriptors"));
                    }
                }
                bytes = &bytes[length..];
            }
            _ => return Err(invalid("unsupported stored outcome field")),
        }
    }
    // Protobuf fields may arrive in any order. Bound allocation while scanning,
    // then enforce the final version's narrower limit before decoding.
    if decisions > decision_limit(if version == 4 { 4 } else { 0 }) {
        return Err(invalid("too many stored decisions"));
    }
    Ok(())
}
impl DurableOutcomeLog {
    pub fn open(path: impl AsRef<Path>, capacity: u64) -> io::Result<Self> {
        let log = Self {
            store: PayloadExtentStore::open_inner(path.as_ref(), capacity, true, true, 2)?,
        };
        log.recover_tail()?;
        Ok(log)
    }

    pub(super) fn open_existing(
        path: &Path,
        capacity: u64,
        id: &[u8],
        sealed: Option<u64>,
    ) -> io::Result<Self> {
        let log = Self {
            store: PayloadExtentStore::open_existing_bound(
                path,
                capacity,
                id,
                2,
                sealed.is_some(),
                sealed.is_none(),
            )?,
        };
        if let Some(length) = sealed {
            if log.length() != length {
                return Err(invalid("sealed outcome segment length changed"));
            }
        } else {
            log.recover_tail()?;
        }
        Ok(log)
    }
    pub(super) fn seal(&self) -> io::Result<()> {
        self.store.healthy()?;
        fs2::FileExt::try_lock_shared(&self.store.file)?;
        fs2::FileExt::try_lock_shared(&self.store._lock)
    }
    pub(super) fn healthy(&self) -> io::Result<()> {
        self.store.healthy()
    }
    pub fn length(&self) -> u64 {
        self.store.length()
    }
    pub fn store_id(&self) -> &[u8] {
        self.store.store_id()
    }
    pub fn remaining_bytes(&self) -> u64 {
        self.store.remaining_bytes()
    }
    pub fn append_page(&self, outcome: &PageOutcome) -> io::Result<StoredRecordMetadata> {
        self.append_decided_batch(&[DecidedMutation::Page(outcome)])
    }
    /// One synchronization commits the complete bounded physical batch; logical
    /// mutations remain independent and can be replayed in their original order.
    pub fn append_decided_batch(
        &self,
        mutations: &[DecidedMutation<'_>],
    ) -> io::Result<StoredRecordMetadata> {
        self.append_with_sync(mutations, File::sync_data)
    }
    pub(super) fn append_with_sync(
        &self,
        mutations: &[DecidedMutation<'_>],
        sync: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<StoredRecordMetadata> {
        self.append_with_sync_metadata(mutations, None, sync)
    }
    pub fn append_decided_batch_with_metadata(
        &self,
        mutations: &[DecidedMutation<'_>],
        metadata: &[u8],
    ) -> io::Result<StoredRecordMetadata> {
        self.append_with_sync_metadata(mutations, Some(metadata), File::sync_data)
    }
    pub(super) fn append_with_sync_metadata(
        &self,
        mutations: &[DecidedMutation<'_>],
        metadata: Option<&[u8]>,
        sync: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<StoredRecordMetadata> {
        if metadata
            .is_some_and(|bytes| bytes.is_empty() || bytes.len() > MAX_REPLICA_METADATA_BYTES)
        {
            return Err(argument("invalid replica metadata context length"));
        }
        self.store.healthy()?;
        let log_batch = mutations
            .iter()
            .any(|value| matches!(value, DecidedMutation::LogPage(_)));
        let limit = decision_limit(if log_batch { 4 } else { 1 });
        if mutations.is_empty() || mutations.len() > limit {
            return Err(argument("stored outcome batch exceeds its decision limit"));
        }
        let checkpoint_batch = matches!(mutations[0], DecidedMutation::CheckpointFragment(_));
        if mutations.iter().any(|value| {
            matches!(value, DecidedMutation::CheckpointFragment(_)) != checkpoint_batch
        }) {
            return Err(argument(
                "checkpoint fragments cannot share a batch with state transitions",
            ));
        }
        if checkpoint_batch && metadata.is_some() {
            return Err(argument(
                "checkpoint fragments cannot carry replica metadata context",
            ));
        }

        let mut decisions = Vec::with_capacity(mutations.len());
        let mut logical = 0usize;
        let mut data_length = 0u64;
        let mut patches = Vec::new();
        for mutation in mutations {
            let original = match mutation {
                DecidedMutation::Page(page) => page.to_payload(),
                DecidedMutation::LogPage(log) => log.to_payload(),
                DecidedMutation::NamespaceFence(fence) => fence.to_payload(),
                DecidedMutation::CheckpointFragment(fragment) => fragment.to_payload(),
            }
            .map_err(object)?;
            logical = logical
                .checked_add(original.len())
                .ok_or_else(|| argument("outcome batch length overflow"))?;
            if logical > MAX_BATCH_BYTES {
                return Err(argument("stored outcome batch exceeds byte limit"));
            }
            let mut decision = StoredDecision {
                page: None,
                patch_lengths: Vec::new(),
                data_offset: data_length,
                original_sha256: digest::digest(&digest::SHA256, &original).as_ref().to_vec(),
                original_length: original.len() as u32,
                namespace_fence: None,
                checkpoint_fragment: None,
                log_page: None,
            };
            match mutation {
                DecidedMutation::Page(page) => {
                    decision.patch_lengths =
                        page.patches.iter().map(|p| p.data.len() as u32).collect();
                    data_length += page_shape(page, &decision.patch_lengths, false)?;
                    patches.extend(page.patches.iter().map(|p| p.data.as_slice()));
                    decision.page = Some(stripped(page));
                }
                DecidedMutation::LogPage(log) => {
                    decision.log_page = Some((*log).clone());
                }
                DecidedMutation::NamespaceFence(fence) => {
                    decision.namespace_fence = Some((*fence).clone());
                }
                DecidedMutation::CheckpointFragment(fragment) => {
                    decision.patch_lengths.push(fragment.data.len() as u32);
                    data_length += fragment.data.len() as u64;
                    patches.push(fragment.data.as_slice());
                    decision.checkpoint_fragment = Some(Box::new(fragment.without_data()));
                }
            }
            decisions.push(decision);
        }
        let data: Cow<'_, [u8]> = match patches.as_slice() {
            [] => Cow::Borrowed(&[]),
            [one] => Cow::Borrowed(one),
            _ => {
                let mut packed = Vec::with_capacity(data_length as usize);
                for bytes in patches {
                    packed.extend_from_slice(bytes);
                }
                Cow::Owned(packed)
            }
        };
        if data.len() > MAX_BATCH_BYTES {
            return Err(argument("stored payload stream exceeds byte limit"));
        }
        let mut batch = StoredOutcomeBatch {
            version: if checkpoint_batch {
                2
            } else if log_batch {
                4
            } else if metadata.is_some() {
                3
            } else {
                1
            },
            decisions,
            payloads: descriptors(self.store_id(), &data),
            replica_metadata: metadata.map_or_else(Vec::new, |bytes| bytes.to_vec()),
        };
        if log_batch {
            validate_log_pages(&batch)?;
            let pages: Vec<_> = mutations
                .iter()
                .filter_map(|mutation| {
                    if let DecidedMutation::Page(page) = mutation {
                        Some(*page)
                    } else {
                        None
                    }
                })
                .collect();
            validate_log_data(
                mutations.iter().filter_map(|mutation| {
                    if let DecidedMutation::LogPage(log) = mutation {
                        Some(*log)
                    } else {
                        None
                    }
                }),
                &pages,
            )?;
        }
        let _writer = self.store.writer.lock().map_err(|_| {
            self.store.failed.store(true, Ordering::Release);
            invalid("stored outcome writer is poisoned")
        })?;
        self.store.healthy()?;
        let start = self.length();
        let mut header = SECTOR_BYTES;
        let mut encoded = Vec::new();
        let mut converged = false;
        for _ in 0..8 {
            let mut offset = start + header as u64;
            for extent in &mut batch.payloads {
                extent.offset = offset;
                offset += padded(extent.length)?;
            }
            encoded.clear();
            batch.encode(&mut encoded).map_err(object)?;
            let required = padded((ENVELOPE + encoded.len()) as u64)? as usize;
            if required > MAX_HEADER {
                return Err(argument("stored outcome header exceeds bound"));
            }
            if required == header {
                converged = true;
                break;
            }
            header = required;
        }
        if !converged {
            return Err(invalid("stored outcome geometry did not converge"));
        }
        let span = header as u64 + padded(data.len() as u64)?;
        let end = start
            .checked_add(span)
            .ok_or_else(|| argument("stored outcome position overflow"))?;
        if end > SECTOR_BYTES as u64 + self.store.capacity {
            return Err(io::Error::other(CapacityExhausted));
        }
        let header = envelope(&encoded, header, span, start, self.store_id());
        let result = (|| {
            let mut file = &self.store.file;
            file.seek(SeekFrom::Start(start))?;
            file.write_all(&header)?;
            file.write_all(&data)?;
            let zeros = [0; SECTOR_BYTES];
            file.write_all(&zeros[..padded(data.len() as u64)? as usize - data.len()])?;
            file.seek(SeekFrom::Start(start + COMMIT_OFFSET))?;
            file.write_all(&[READY])?;
            if start == SECTOR_BYTES as u64 {
                file.seek(SeekFrom::Start(SECTOR_BYTES as u64 - 1))?;
                file.write_all(&[READY])?;
            }
            sync(&self.store.file)
        })();
        if let Err(error) = result {
            self.store.failed.store(true, Ordering::Release);
            return Err(error);
        }
        self.store.end.store(end, Ordering::Release);
        Ok(StoredRecordMetadata {
            position: StoredOutcomePosition {
                version: 1,
                store_id: self.store.id.clone(),
                offset: start,
                length: span,
            },
            batch,
        })
    }
    fn validate_batch(
        &self,
        batch: &StoredOutcomeBatch,
        start: u64,
        header: usize,
        end: u64,
    ) -> io::Result<()> {
        if !matches!(batch.version, 1 | 2 | 3 | 4)
            || batch.decisions.is_empty()
            || batch.decisions.len() > decision_limit(batch.version)
            || batch.payloads.len() > 8
        {
            return Err(invalid("invalid stored outcome batch"));
        }
        if batch.replica_metadata.len() > MAX_REPLICA_METADATA_BYTES
            || (batch.version != 4 && (batch.version == 3) != !batch.replica_metadata.is_empty())
            || (batch.version == 4)
                != batch
                    .decisions
                    .iter()
                    .any(|decision| decision.log_page.is_some())
        {
            return Err(invalid("invalid stored replica metadata context"));
        }
        let mut data = 0u64;
        let mut logical = 0usize;
        for decision in &batch.decisions {
            if decision.data_offset != data
                || decision.original_sha256.len() != 32
                || decision.original_length == 0
            {
                return Err(invalid("invalid stored decision binding"));
            }
            logical = logical
                .checked_add(decision.original_length as usize)
                .ok_or_else(|| invalid("stored decision length overflow"))?;
            if logical > MAX_BATCH_BYTES {
                return Err(invalid("stored decisions exceed byte bound"));
            }
            match (
                &decision.page,
                &decision.namespace_fence,
                &decision.checkpoint_fragment,
                &decision.log_page,
            ) {
                (Some(page), None, None, None) if matches!(batch.version, 1 | 3 | 4) => {
                    data = data
                        .checked_add(page_shape(page, &decision.patch_lengths, true)?)
                        .ok_or_else(|| invalid("stored payload length overflow"))?;
                }
                (None, Some(fence), None, None)
                    if matches!(batch.version, 1 | 3 | 4) && decision.patch_lengths.is_empty() =>
                {
                    fence.to_payload().map_err(object)?;
                }
                (None, None, Some(fragment), None)
                    if batch.version == 2 && decision.patch_lengths.len() == 1 =>
                {
                    let length = decision.patch_lengths[0];
                    fragment.validate_external(length).map_err(object)?;
                    data = data
                        .checked_add(u64::from(length))
                        .ok_or_else(|| invalid("stored checkpoint payload length overflow"))?;
                }
                (None, None, None, Some(log))
                    if batch.version == 4 && decision.patch_lengths.is_empty() =>
                {
                    log.to_payload().map_err(object)?;
                }
                _ => return Err(invalid("stored decision must have one supported outcome")),
            }
        }
        if batch.version == 4 {
            validate_log_pages(batch)?;
        }
        if data > MAX_BATCH_BYTES as u64 {
            return Err(invalid("stored payload stream exceeds bound"));
        }
        let mut offset = start + header as u64;
        let mut bytes = 0u64;
        for (index, extent) in batch.payloads.iter().enumerate() {
            self.store.validate_extent(extent)?;
            if extent.offset != offset
                || (index + 1 < batch.payloads.len() && extent.length != MAX_EXTENT_BYTES as u64)
            {
                return Err(invalid("stored payload geometry mismatch"));
            }
            offset += padded(extent.length)?;
            bytes += extent.length;
        }
        if bytes != data || offset != end {
            return Err(invalid("stored payload coverage mismatch"));
        }
        Ok(())
    }
    fn frame(&self, start: u64, limit: u64) -> io::Result<Frame> {
        if start < SECTOR_BYTES as u64 || start % SECTOR_BYTES as u64 != 0 || start >= limit {
            return Err(argument("invalid stored record offset"));
        }
        let remaining = limit - start;
        if remaining < ENVELOPE as u64 {
            let mut bytes = vec![0; remaining as usize];
            read_at(&self.store.file, &mut bytes, start)?;
            let n = bytes.len().min(8);
            if bytes[..n] != RECORD_MAGIC[..n] {
                return Err(invalid("damaged stored record tail"));
            }
            return Ok(Frame::Partial);
        }
        let mut envelope = [0; ENVELOPE];
        read_at(&self.store.file, &mut envelope, start)?;
        if &envelope[..8] != RECORD_MAGIC
            || header_digest(&envelope).as_ref() != &envelope[56..88]
            || u64::from_be_bytes(envelope[88..96].try_into().unwrap()) != start
            || &envelope[96..112] != self.store_id()
            || envelope[112..127].iter().any(|n| *n != 0)
            || ![0, READY].contains(&envelope[127])
        {
            return Err(invalid("stored record envelope damaged"));
        }
        let header = u32::from_be_bytes(envelope[8..12].try_into().unwrap()) as usize;
        let length = u32::from_be_bytes(envelope[12..16].try_into().unwrap()) as usize;
        let span = u64::from_be_bytes(envelope[16..24].try_into().unwrap());
        let ready = envelope[127] == READY;
        if !(SECTOR_BYTES..=MAX_HEADER).contains(&header)
            || header % SECTOR_BYTES != 0
            || length > header - ENVELOPE
            || span < header as u64
            || span % SECTOR_BYTES as u64 != 0
        {
            return Err(invalid("invalid stored record lengths"));
        }
        let end = start
            .checked_add(span)
            .ok_or_else(|| invalid("stored record end overflow"))?;
        if end > SECTOR_BYTES as u64 + self.store.capacity {
            return Err(invalid("stored record exceeds file capacity"));
        }
        if span > remaining {
            if ready {
                return Err(invalid("committed stored record is truncated"));
            }
            return Ok(Frame::Partial);
        }
        let mut body = vec![0; header - ENVELOPE];
        read_at(&self.store.file, &mut body, start + ENVELOPE as u64)?;
        if digest::digest(&digest::SHA256, &body[..length]).as_ref() != &envelope[24..56]
            || body[length..].iter().any(|n| *n != 0)
        {
            if !ready && end == limit {
                return Ok(Frame::Partial);
            }
            return Err(invalid("stored outcome metadata checksum mismatch"));
        }
        let body = &body[..length];
        preflight(body)?;
        let batch = StoredOutcomeBatch::decode(body).map_err(object)?;
        if batch.encode_to_vec() != body {
            return Err(invalid("noncanonical stored outcome metadata"));
        }
        self.validate_batch(&batch, start, header, end)?;
        Ok(Frame::Complete(
            StoredRecordMetadata {
                position: StoredOutcomePosition {
                    version: 1,
                    store_id: self.store.id.clone(),
                    offset: start,
                    length: span,
                },
                batch,
            },
            ready,
        ))
    }
    fn recover_tail(&self) -> io::Result<()> {
        let mut marker = [0];
        read_at(&self.store.file, &mut marker, SECTOR_BYTES as u64 - 1)?;
        let had_records = marker[0] == READY;
        let physical = self.length();
        let mut at = SECTOR_BYTES as u64;
        let mut changed = false;
        while at < physical {
            let frame = self.frame(at, physical)?;
            match frame {
                Frame::Partial => {
                    if at == SECTOR_BYTES as u64 && had_records {
                        return Err(invalid("initialized outcome log lost its first record"));
                    }
                    self.store.file.set_len(at)?;
                    self.store.end.store(at, Ordering::Release);
                    changed = true;
                    break;
                }
                Frame::Complete(record, ready) => {
                    if !ready {
                        if let Err(error) = self.hydrate(&record.batch) {
                            if at + record.position.length != physical
                                || (at == SECTOR_BYTES as u64 && had_records)
                            {
                                return Err(error);
                            }
                            self.store.file.set_len(at)?;
                            self.store.end.store(at, Ordering::Release);
                            changed = true;
                            break;
                        }
                        let mut file = &self.store.file;
                        file.seek(SeekFrom::Start(at + COMMIT_OFFSET))?;
                        file.write_all(&[READY])?;
                        changed = true;
                    }
                    at += record.position.length;
                }
            }
        }
        if at == SECTOR_BYTES as u64 && had_records {
            return Err(invalid("initialized outcome log is empty"));
        }
        if self.length() > SECTOR_BYTES as u64 && !had_records {
            let mut file = &self.store.file;
            file.seek(SeekFrom::Start(SECTOR_BYTES as u64 - 1))?;
            file.write_all(&[READY])?;
            changed = true;
        }
        if changed {
            self.store.file.sync_all()?;
        }
        Ok(())
    }
    /// Read metadata for one known record boundary without reading payload bytes.
    pub fn next_metadata(&self, offset: u64) -> io::Result<Option<StoredRecordMetadata>> {
        self.store.healthy()?;
        if offset == self.length() {
            return Ok(None);
        }
        match self.frame(offset, self.length())? {
            Frame::Complete(record, true) => Ok(Some(record)),
            _ => Err(invalid("uncommitted stored record is not readable")),
        }
    }
    pub fn read_metadata(
        &self,
        position: &StoredOutcomePosition,
    ) -> io::Result<StoredRecordMetadata> {
        if position.version != 1 || position.store_id != self.store.id {
            return Err(argument("stored outcome position identity mismatch"));
        }
        let record = self
            .next_metadata(position.offset)?
            .ok_or_else(|| invalid("stored outcome position is beyond log"))?;
        if record.position != *position {
            return Err(invalid("stored outcome position length mismatch"));
        }
        Ok(record)
    }
    /// Read a catalog-selected immutable payload range without hydrating decisions.
    pub fn read_payload_range(
        &self,
        extent: &PayloadExtent,
        offset: u64,
        length: u64,
    ) -> io::Result<Vec<u8>> {
        self.store.read_range(extent, offset, length)
    }
    /// Append a verified immutable range to a reusable buffer. Errors preserve
    /// its original bytes and length; only its capacity may change.
    pub fn read_payload_range_append(
        &self,
        extent: &PayloadExtent,
        offset: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> io::Result<()> {
        self.store.read_range_append(extent, offset, length, output)
    }
    fn hydrate(&self, batch: &StoredOutcomeBatch) -> io::Result<Vec<RecoveredMutation>> {
        let mut flat = Vec::new();
        for extent in &batch.payloads {
            let bytes = self.store.read(extent)?;
            if batch.payloads.len() == 1 {
                flat = bytes;
            } else {
                if flat.is_empty() {
                    flat.reserve_exact(batch.payloads.iter().map(|p| p.length as usize).sum());
                }
                flat.extend_from_slice(&bytes);
            }
        }
        let one_patch = batch.decisions.len() == 1 && batch.decisions[0].patch_lengths.len() == 1;
        let mut result = Vec::with_capacity(batch.decisions.len());
        for decision in &batch.decisions {
            let value = if let Some(page) = &decision.page {
                let mut page = page.clone();
                let mut offset = decision.data_offset;
                for (patch, &length) in page.patches.iter_mut().zip(&decision.patch_lengths) {
                    let end = (offset as usize)
                        .checked_add(length as usize)
                        .ok_or_else(|| invalid("stored patch data overflow"))?;
                    if end > flat.len() {
                        return Err(invalid("stored patch data coverage mismatch"));
                    }
                    patch.data = if one_patch {
                        std::mem::take(&mut flat)
                    } else {
                        flat[offset as usize..end].to_vec()
                    };
                    offset += u64::from(length);
                }
                RecoveredMutation::Page(page)
            } else if let Some(log) = &decision.log_page {
                RecoveredMutation::LogPage(log.clone())
            } else if let Some(fragment) = &decision.checkpoint_fragment {
                let mut fragment = (**fragment).clone();
                let offset = usize::try_from(decision.data_offset).map_err(object)?;
                let end = offset
                    .checked_add(decision.patch_lengths[0] as usize)
                    .filter(|end| *end <= flat.len())
                    .ok_or_else(|| invalid("stored checkpoint payload coverage mismatch"))?;
                fragment.data = if one_patch {
                    std::mem::take(&mut flat)
                } else {
                    flat[offset..end].to_vec()
                };
                RecoveredMutation::CheckpointFragment(fragment)
            } else {
                RecoveredMutation::NamespaceFence(
                    decision.namespace_fence.as_ref().unwrap().clone(),
                )
            };
            let bytes = value.to_payload().map_err(object)?;
            if bytes.len() != decision.original_length as usize
                || digest::digest(&digest::SHA256, &bytes).as_ref()
                    != decision.original_sha256.as_slice()
            {
                return Err(invalid("stored outcome logical digest mismatch"));
            }
            result.push(value);
        }
        if batch.version == 4 {
            let pages: Vec<_> = result
                .iter()
                .filter_map(|mutation| {
                    if let RecoveredMutation::Page(page) = mutation {
                        Some(page)
                    } else {
                        None
                    }
                })
                .collect();
            validate_log_data(
                result.iter().filter_map(|mutation| {
                    if let RecoveredMutation::LogPage(log) = mutation {
                        Some(log)
                    } else {
                        None
                    }
                }),
                &pages,
            )?;
        }
        Ok(result)
    }
    pub fn read_outcomes(
        &self,
        position: &StoredOutcomePosition,
    ) -> io::Result<Vec<RecoveredMutation>> {
        let record = self.read_metadata(position)?;
        if !record.batch.replica_metadata.is_empty() {
            return Err(invalid(
                "replica metadata context requires context-aware recovery",
            ));
        }
        self.hydrate(&record.batch)
    }
    pub fn read_outcomes_with_metadata(
        &self,
        position: &StoredOutcomePosition,
    ) -> io::Result<(Vec<RecoveredMutation>, Vec<u8>)> {
        let record = self.read_metadata(position)?;
        let outcomes = self.hydrate(&record.batch)?;
        Ok((outcomes, record.batch.replica_metadata))
    }
    /// Replay into startup state; callers must not serve partially replayed state
    /// if a later record fails validation. Returns the applied mutation count.
    pub fn replay_from(
        &self,
        offset: u64,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> io::Result<usize> {
        let mut at = offset;
        let mut count = 0;
        while let Some(record) = self.next_metadata(at)? {
            if record.batch.version == 4 {
                return Err(invalid("log page outcomes require log-aware recovery"));
            }
            if !record.batch.replica_metadata.is_empty() {
                return Err(invalid(
                    "replica metadata context requires context-aware recovery",
                ));
            }
            for outcome in self.hydrate(&record.batch)? {
                outcome.apply(service, max_pages).map_err(object)?;
                count += 1;
            }
            at += record.position.length;
        }
        Ok(count)
    }
}
#[cfg(all(test, unix))]
#[path = "stored_outcome_record_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
#[path = "stored_checkpoint_fragment_tests.rs"]
mod checkpoint_fragment_tests;

#[cfg(all(test, unix))]
#[path = "stored_log_page_outcome_tests.rs"]
mod log_page_tests;
