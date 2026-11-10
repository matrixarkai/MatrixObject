// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Monotonic namespace closure with immutable page identities for repair.
use crate::{ChunkId, MatrixReplicaService, ObjectError, PageId, PageState};
use prost::Message;
use std::{collections::BTreeMap, sync::Arc};

pub const MAX_FENCE_PAGES: usize = 4096;
pub const MAX_FENCE_BYTES: usize = 512 * 1024;
pub const MAX_FENCE_CATALOG_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAGIC: &[u8; 4] = b"MOCF";

#[derive(Clone, PartialEq, Eq, Message)]
pub(crate) struct NamespaceFenceQuery {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub volume_id: u64,
    #[prost(uint64, tag = "3")]
    pub chunk_id: u64,
}
impl NamespaceFenceQuery {
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<ChunkId, ObjectError> {
        if bytes.len() > 64 {
            return Err(invalid("namespace query exceeds limit"));
        }
        let query = Self::decode(bytes).map_err(|_| invalid("invalid namespace fence query"))?;
        if query.version != 1 || query.encode_to_vec() != bytes {
            return Err(invalid("noncanonical namespace fence query"));
        }
        ChunkId::new(query.volume_id, query.chunk_id)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ImmutablePage {
    #[prost(uint64, tag = "1")]
    pub page_index: u64,
    #[prost(uint64, tag = "2")]
    pub length: u64,
    #[prost(uint64, tag = "3")]
    pub write_sequence: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub sha256: Vec<u8>,
}
impl ImmutablePage {
    pub fn from_state(page_index: u64, state: &PageState) -> Result<Self, ObjectError> {
        crate::validate_chunk_state_with_data_etag(state)?;
        if state.logical_length != state.data.len() as u64 || !state.discarded_ranges.is_empty() {
            return Err(refused(
                "namespace fencing requires a complete visible page",
            ));
        }
        let page = Self {
            page_index,
            length: state.logical_length,
            write_sequence: state.write_sequence,
            sha256: ring::digest::digest(&ring::digest::SHA256, &state.data)
                .as_ref()
                .to_vec(),
        };
        page.validate()?;
        Ok(page)
    }
    fn validate(&self) -> Result<(), ObjectError> {
        if self.length > 256 * 1024 * 1024 || self.write_sequence == 0 || self.sha256.len() != 32 {
            return Err(invalid("invalid immutable page identity"));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct NamespaceFence {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub volume_id: u64,
    #[prost(uint64, tag = "3")]
    pub chunk_id: u64,
    /// The first closure is generation 1. Later transitions remove pinned pages.
    #[prost(uint64, tag = "4")]
    pub generation: u64,
    #[prost(message, repeated, tag = "5")]
    pub pages: Vec<ImmutablePage>,
}
impl NamespaceFence {
    pub fn namespace(&self) -> Result<ChunkId, ObjectError> {
        ChunkId::new(self.volume_id, self.chunk_id)
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        self.namespace()?;
        if self.version != 1
            || self.generation == 0
            || self.generation.saturating_add(self.pages.len() as u64) > MAX_FENCE_PAGES as u64 + 1
            || self.pages.len() > MAX_FENCE_PAGES
            || self.encoded_len() > MAX_FENCE_BYTES
            || self
                .pages
                .windows(2)
                .any(|pair| pair[0].page_index >= pair[1].page_index)
        {
            return Err(invalid("invalid namespace fence bounds or order"));
        }
        for page in &self.pages {
            page.validate()?;
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_FENCE_BYTES {
            return Err(invalid("namespace fence exceeds byte limit"));
        }
        // Bound nested allocation count before prost allocates repeated page rows.
        let mut cursor = bytes;
        let mut pages = 0;
        while !cursor.is_empty() {
            let key = varint(&mut cursor)?;
            match key & 7 {
                0 => {
                    varint(&mut cursor)?;
                }
                1 => skip(&mut cursor, 8)?,
                2 => {
                    let length = usize::try_from(varint(&mut cursor)?)
                        .map_err(|_| invalid("fence length overflow"))?;
                    if key >> 3 == 5 {
                        pages += 1;
                        if pages > MAX_FENCE_PAGES || length > 96 {
                            return Err(invalid("immutable page wire bound exceeded"));
                        }
                    }
                    skip(&mut cursor, length)?;
                }
                5 => skip(&mut cursor, 4)?,
                _ => return Err(invalid("invalid namespace fence wire type")),
            }
        }
        let fence = Self::decode(bytes).map_err(|_| invalid("invalid namespace fence protobuf"))?;
        fence.validate()?;
        if fence.encode_to_vec() != bytes {
            return Err(invalid("noncanonical namespace fence"));
        }
        Ok(fence)
    }
}
fn varint(bytes: &mut &[u8]) -> Result<u64, ObjectError> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let (&byte, tail) = bytes
            .split_first()
            .ok_or_else(|| invalid("truncated fence varint"))?;
        *bytes = tail;
        if shift == 63 && byte > 1 {
            return Err(invalid("fence varint overflow"));
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Ok(value);
        }
    }
    Err(invalid("fence varint overflow"))
}
fn skip(bytes: &mut &[u8], length: usize) -> Result<(), ObjectError> {
    *bytes = bytes
        .get(length..)
        .ok_or_else(|| invalid("truncated fence field"))?;
    Ok(())
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
fn entry_size(fence: &NamespaceFence) -> usize {
    let length = fence.encoded_len();
    1 + prost::encoding::encoded_len_varint(length as u64) + length
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NamespaceFenceCatalog {
    entries: Arc<BTreeMap<ChunkId, Arc<NamespaceFence>>>,
    bytes: usize,
    revision: u64,
}
impl NamespaceFenceCatalog {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }
    pub fn get(&self, namespace: &ChunkId) -> Option<&NamespaceFence> {
        self.entries.get(namespace).map(Arc::as_ref)
    }
    pub(crate) fn encoded_len(&self) -> usize {
        2 + self.bytes
    }
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(&[8, 1]);
        for fence in self.entries.values() {
            prost::encoding::message::encode(2, fence.as_ref(), &mut bytes);
        }
        bytes
    }
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_FENCE_CATALOG_BYTES {
            return Err(invalid("namespace fence catalog exceeds limit"));
        }
        let mut cursor = bytes
            .strip_prefix(&[8, 1])
            .ok_or_else(|| invalid("invalid namespace fence catalog version"))?;
        let mut catalog = Self::default();
        let mut previous = None;
        while !cursor.is_empty() {
            if varint(&mut cursor)? != 18 {
                return Err(invalid("invalid namespace fence catalog tag"));
            }
            let length = usize::try_from(varint(&mut cursor)?)
                .map_err(|_| invalid("fence length overflow"))?;
            let record = cursor
                .get(..length)
                .ok_or_else(|| invalid("truncated namespace catalog"))?;
            let fence = NamespaceFence::from_payload(record)?;
            let id = fence.namespace()?;
            if previous.as_ref().is_some_and(|before| before >= &id)
                || catalog.entries.len() >= 65_536
            {
                return Err(invalid("namespace fence catalog order or count exceeded"));
            }
            previous = Some(id.clone());
            catalog.bytes += entry_size(&fence);
            catalog.revision += fence.generation;
            Arc::make_mut(&mut catalog.entries).insert(id, Arc::new(fence));
            skip(&mut cursor, length)?;
        }
        if catalog.to_bytes() != bytes {
            return Err(invalid("noncanonical namespace fence catalog"));
        }
        Ok(catalog)
    }
    fn check(&self, before: u64, after: &NamespaceFence) -> Result<usize, ObjectError> {
        after.validate()?;
        let id = after.namespace()?;
        let current = self.get(&id);
        if current.map_or(0, |record| record.generation) != before
            || before.checked_add(1) != Some(after.generation)
        {
            return Err(refused("namespace fence generation changed"));
        }
        if let Some(current) = current {
            if after.pages.len() >= current.pages.len()
                || after.pages.iter().any(|page| {
                    current
                        .pages
                        .binary_search_by_key(&page.page_index, |p| p.page_index)
                        .ok()
                        .is_none_or(|index| &current.pages[index] != page)
                })
            {
                return Err(refused("a closed namespace may only retire pinned pages"));
            }
        } else if self.entries.len() >= 65_536 {
            return Err(refused("namespace fence catalog count exceeded"));
        }
        let bytes = self.bytes - current.map_or(0, entry_size) + entry_size(after);
        if bytes + 2 > MAX_FENCE_CATALOG_BYTES {
            return Err(refused("namespace fence catalog byte limit exceeded"));
        }
        Ok(bytes)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct NamespaceFenceOutcome {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub before_generation: u64,
    #[prost(message, optional, tag = "3")]
    pub after: Option<NamespaceFence>,
}
impl NamespaceFenceOutcome {
    pub fn decide(
        service: &MatrixReplicaService,
        after: NamespaceFence,
    ) -> Result<Self, ObjectError> {
        let before_generation = after
            .generation
            .checked_sub(1)
            .ok_or_else(|| invalid("zero fence generation"))?;
        service.namespace_fences.check(before_generation, &after)?;
        Ok(Self {
            version: 1,
            before_generation,
            after: Some(after),
        })
    }
    fn validate(&self) -> Result<&NamespaceFence, ObjectError> {
        let after = self
            .after
            .as_ref()
            .ok_or_else(|| invalid("namespace fence outcome lacks state"))?;
        after.validate()?;
        if self.version != 1 || self.before_generation.checked_add(1) != Some(after.generation) {
            return Err(invalid("invalid namespace fence outcome generation"));
        }
        Ok(after)
    }
    pub fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut bytes = MAGIC.to_vec();
        self.encode(&mut bytes)
            .map_err(|_| invalid("namespace fence outcome encoding failed"))?;
        bytes.extend_from_slice(&crate::fnv1a64(&bytes).to_be_bytes());
        Ok(bytes)
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() < 12 || bytes.len() > MAX_FENCE_BYTES + 64 || !bytes.starts_with(MAGIC) {
            return Err(invalid("invalid namespace fence outcome envelope"));
        }
        let end = bytes.len() - 8;
        if crate::fnv1a64(&bytes[..end]) != u64::from_be_bytes(bytes[end..].try_into().unwrap()) {
            return Err(invalid("namespace fence outcome checksum mismatch"));
        }
        let mut cursor = &bytes[4..end];
        let mut outcome = Self::default();
        while !cursor.is_empty() {
            match varint(&mut cursor)? {
                8 => {
                    outcome.version = u32::try_from(varint(&mut cursor)?)
                        .map_err(|_| invalid("fence version overflow"))?
                }
                16 => outcome.before_generation = varint(&mut cursor)?,
                26 => {
                    let length = usize::try_from(varint(&mut cursor)?)
                        .map_err(|_| invalid("fence length overflow"))?;
                    outcome.after = Some(NamespaceFence::from_payload(
                        cursor
                            .get(..length)
                            .ok_or_else(|| invalid("truncated namespace fence outcome"))?,
                    )?);
                    skip(&mut cursor, length)?;
                }
                _ => return Err(invalid("invalid namespace fence outcome tag")),
            }
        }
        outcome.validate()?;
        if outcome.to_payload()? != bytes {
            return Err(invalid("noncanonical namespace fence outcome"));
        }
        Ok(outcome)
    }
    pub fn apply(&self, service: &mut MatrixReplicaService) -> Result<(), ObjectError> {
        let after = self.validate()?;
        let bytes = service
            .namespace_fences
            .check(self.before_generation, after)?;
        Arc::make_mut(&mut service.namespace_fences.entries)
            .insert(after.namespace()?, Arc::new(after.clone()));
        service.namespace_fences.bytes = bytes;
        service.namespace_fences.revision += 1;
        service.mark_metadata_mutation();
        Ok(())
    }
}

impl MatrixReplicaService {
    pub fn namespace_fence(&self, namespace: &ChunkId) -> Option<&NamespaceFence> {
        self.namespace_fences.get(namespace)
    }
    pub(crate) fn ensure_namespace_open(&self, namespace: &ChunkId) -> Result<(), ObjectError> {
        if self.namespace_fences.get(namespace).is_some() {
            return Err(refused("namespace is closed to ordinary mutations"));
        }
        Ok(())
    }
    pub(crate) fn validate_namespace_install_shape(
        &self,
        page: &PageId,
        logical_length: u64,
        data_length: u64,
        sequence: u64,
        has_discards: bool,
    ) -> Result<Option<&ImmutablePage>, ObjectError> {
        let Some(fence) = self.namespace_fences.get(&page.chunk_id) else {
            return Ok(None);
        };
        let pinned = fence
            .pages
            .binary_search_by_key(&page.page_index, |p| p.page_index)
            .ok()
            .map(|index| &fence.pages[index])
            .ok_or_else(|| refused("page is retired by namespace fence"))?;
        if logical_length != pinned.length
            || data_length != pinned.length
            || sequence != pinned.write_sequence
            || has_discards
        {
            return Err(refused("repair differs from pinned immutable page"));
        }
        Ok(Some(pinned))
    }
    pub(crate) fn validate_namespace_install(
        &self,
        page: &PageId,
        state: &PageState,
    ) -> Result<(), ObjectError> {
        if let Some(pinned) = self.validate_namespace_install_shape(
            page,
            state.logical_length,
            state.data.len() as u64,
            state.write_sequence,
            !state.discarded_ranges.is_empty(),
        )? {
            if ring::digest::digest(&ring::digest::SHA256, &state.data).as_ref()
                != pinned.sha256.as_slice()
            {
                return Err(refused("repair differs from pinned immutable page"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn page(index: u64) -> PageId {
        PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: index,
        }
    }
    fn fence(state: &PageState) -> NamespaceFence {
        NamespaceFence {
            version: 1,
            volume_id: 1,
            chunk_id: 2,
            generation: 1,
            pages: vec![ImmutablePage::from_state(0, state).unwrap()],
        }
    }

    #[test]
    fn namespace_closure_blocks_stale_mutations_and_only_repairs_pinned_state() {
        let mut service = MatrixReplicaService::new();
        service.write_chunk(page(0), 0, b"immutable").unwrap();
        let state = service.read_chunk_state(&page(0)).unwrap();
        let closed = fence(&state);
        NamespaceFenceOutcome::decide(&service, closed.clone())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        assert_eq!(service.namespace_fence(&page(0).chunk_id), Some(&closed));
        let saved = service.export_snapshot();
        assert!(service.write_chunk(page(0), 0, b"changed").is_err());
        assert!(service.write_chunk(page(1), 0, b"late").is_err());
        assert!(service
            .write_chunk_ranges_borrowed(page(0), &[(0, b"changed".as_slice())])
            .is_err());
        assert!(service.discard_chunk(&page(0), 0, 1).is_err());
        assert!(service
            .discard_chunk_ranges(
                &page(0),
                vec![crate::DataRange {
                    offset: 0,
                    length: 1
                }]
            )
            .is_err());
        let mut wrong = MatrixReplicaService::new();
        wrong.write_chunk(page(0), 0, b"different").unwrap();
        assert!(service
            .install_chunk_state(page(0), wrong.read_chunk_state(&page(0)).unwrap())
            .is_err());
        assert_eq!(service.export_snapshot(), saved);
        service.install_chunk_state(page(0), state.clone()).unwrap();
        // An empty replica may recover exactly the page pinned before data arrival.
        let mut missing = MatrixReplicaService::new();
        NamespaceFenceOutcome::decide(&missing, closed.clone())
            .unwrap()
            .apply(&mut missing)
            .unwrap();
        missing.install_chunk_state(page(0), state.clone()).unwrap();
        assert!(missing.install_chunk_state(page(1), state.clone()).is_err());
        let retired = NamespaceFence {
            generation: 2,
            pages: Vec::new(),
            ..closed.clone()
        };
        NamespaceFenceOutcome::decide(&missing, retired.clone())
            .unwrap()
            .apply(&mut missing)
            .unwrap();
        assert!(missing.install_chunk_state(page(0), state).is_err());
        assert!(NamespaceFenceOutcome::decide(&missing, closed.clone()).is_err());
        assert!(NamespaceFenceOutcome::decide(
            &missing,
            NamespaceFence {
                generation: 3,
                ..closed
            }
        )
        .is_err());
        service
            .write_chunk(
                PageId {
                    chunk_id: ChunkId::new(1, 3).unwrap(),
                    page_index: 0,
                },
                0,
                b"other",
            )
            .unwrap();
    }

    #[test]
    fn namespace_fences_preserve_ec_repair_identity_and_refuse_retired_repairs() {
        let mut service = MatrixReplicaService::new();
        service
            .write_chunk(page(0), 0, b"immutable payload")
            .unwrap();
        let original = service.read_chunk_state(&page(0)).unwrap();
        let closed = fence(&original);
        NamespaceFenceOutcome::decide(&service, closed.clone())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        service.corrupt_chunk_payload_for_repair(&page(0)).unwrap();
        assert!(service.repair_chunk_payload_from_ec(&page(0)).unwrap());
        let repaired = service.read_chunk_state(&page(0)).unwrap();
        assert_eq!(repaired.data, original.data);
        assert_eq!(repaired.write_sequence, original.write_sequence);
        service
            .validate_namespace_install(&page(0), &repaired)
            .unwrap();
        assert!(!service.repair_chunk_payload_from_ec(&page(0)).unwrap());
        NamespaceFenceOutcome::decide(
            &service,
            NamespaceFence {
                generation: 2,
                pages: Vec::new(),
                ..closed
            },
        )
        .unwrap()
        .apply(&mut service)
        .unwrap();
        service.corrupt_chunk_payload_for_repair(&page(0)).unwrap();
        let damaged = service.read_chunk_state(&page(0)).unwrap();
        assert!(service.repair_chunk_payload_from_ec(&page(0)).is_err());
        assert_eq!(service.read_chunk_state(&page(0)).unwrap(), damaged);
    }

    #[test]
    fn namespace_fence_snapshot_keeps_captured_control_and_replays_monotonic_outcomes() {
        let mut source = MatrixReplicaService::new();
        source.write_chunk(page(0), 0, b"pinned").unwrap();
        let state = source.read_chunk_state(&page(0)).unwrap();
        let mut service = MatrixReplicaService::new();
        let closed = fence(&state);
        let outcome = NamespaceFenceOutcome::decide(&service, closed.clone()).unwrap();
        let bytes = outcome.to_payload().unwrap();
        NamespaceFenceOutcome::from_payload(&bytes)
            .unwrap()
            .apply(&mut service)
            .unwrap();
        let captured = service.export_snapshot();
        let retired = NamespaceFence {
            generation: 2,
            pages: Vec::new(),
            ..closed.clone()
        };
        NamespaceFenceOutcome::decide(&service, retired.clone())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        let mut recovered = MatrixReplicaService::new();
        recovered
            .install_snapshot(
                crate::ReplicaServiceSnapshot::from_bytes(&captured.to_bytes().unwrap()).unwrap(),
            )
            .unwrap();
        assert_eq!(recovered.namespace_fence(&page(0).chunk_id), Some(&closed));
        recovered
            .install_chunk_state(page(0), state.clone())
            .unwrap();
        let restored = crate::ReplicaServiceSnapshot::from_bytes(
            &service.export_snapshot().to_bytes().unwrap(),
        )
        .unwrap();
        recovered.install_snapshot(restored).unwrap();
        assert_eq!(recovered.namespace_fence(&page(0).chunk_id), Some(&retired));
        assert!(recovered.install_chunk_state(page(0), state).is_err());
        assert!(NamespaceFenceOutcome::from_payload(&bytes)
            .unwrap()
            .apply(&mut recovered)
            .is_err());
        for cut in 0..bytes.len() {
            assert!(NamespaceFenceOutcome::from_payload(&bytes[..cut]).is_err());
        }
    }

    #[test]
    fn namespace_fence_codec_bounds_nested_pages_before_decode_and_rejects_noncanonical_input() {
        let mut service = MatrixReplicaService::new();
        service.write_chunk(page(0), 0, b"data").unwrap();
        let record = fence(&service.read_chunk_state(&page(0)).unwrap());
        let bytes = record.encode_to_vec();
        assert_eq!(NamespaceFence::from_payload(&bytes).unwrap(), record);
        let empty_len = NamespaceFence {
            pages: Vec::new(),
            ..record.clone()
        }
        .encoded_len();
        // Omitting the complete repeated field is a valid empty closure.
        for cut in (0..empty_len).chain(empty_len + 1..bytes.len()) {
            assert!(NamespaceFence::from_payload(&bytes[..cut]).is_err());
        }
        let mut unknown = bytes;
        unknown.extend_from_slice(&[0x30, 1]);
        assert!(NamespaceFence::from_payload(&unknown).is_err());
        let mut crowded = NamespaceFence {
            pages: Vec::new(),
            ..record.clone()
        }
        .encode_to_vec();
        for _ in 0..=MAX_FENCE_PAGES {
            crowded.extend_from_slice(&[42, 0]);
        }
        assert!(NamespaceFence::from_payload(&crowded).is_err());
        let mut large_row = NamespaceFence {
            pages: Vec::new(),
            ..record.clone()
        }
        .encode_to_vec();
        large_row.extend_from_slice(&[42, 97]);
        large_row.resize(large_row.len() + 97, 0);
        assert!(NamespaceFence::from_payload(&large_row).is_err());
        assert!(NamespaceFence {
            generation: 0,
            ..record.clone()
        }
        .validate()
        .is_err());
        assert!(NamespaceFence {
            pages: vec![record.pages[0].clone(), record.pages[0].clone()],
            ..record
        }
        .validate()
        .is_err());
    }
}
