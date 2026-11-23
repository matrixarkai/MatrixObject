// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable ownership of unpublished native chunks and protection of captured generations.
//! Storage durability and metadata lease admission are checked by the command layer before
//! these deterministic transitions are committed. No operation here performs storage IO.
use crate::chunk_catalog::{ChunkCatalog, ChunkOutcome};
use crate::chunk_manifest::{
    ChunkAttributes, ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_FLAG_TAIL,
    CHUNK_KIND_LOG, MAX_CHUNK_EXTENTS,
};
use crate::initialized_log_manifest::{InitializedChunk, CHUNK_MANIFEST_INITIALIZED_FORMAT};
use crate::object_manifest::{frame, unframe};
use crate::{ObjectError, ObjectId};
use imbl::{OrdMap, OrdSet, Vector};
use prost::Message;
use std::sync::Arc;

pub const MAX_NATIVE_STAGES: usize = 128;
pub const MAX_NATIVE_READ_PINS: usize = 4096;
const MAX_IMAGE: usize = 64 * 1024 * 1024;
const IMAGE_HEADER_ALLOWANCE: usize = 4096;
const MAX_CHUNK_LENGTH: u64 = 512 * 1024 * 1024;
pub const STAGE_REPLACE: u32 = 1;
pub const STAGE_ADD_TAIL: u32 = 2;
fn corrupt(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
fn bytes_cost(value: &impl Message) -> usize {
    value.encoded_len().saturating_add(16)
}
fn digest(manifest: &ChunkManifest) -> Result<Vec<u8>, ObjectError> {
    Ok(crate::chunk_integrity::sha256(&manifest.to_bytes()?).to_vec())
}
fn future_deadline(now: u64, deadline: u64) -> bool {
    now != 0
        && deadline
            .checked_sub(now)
            .is_some_and(|ttl| (1000..=3_600_000).contains(&ttl))
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeStageStart {
    #[prost(uint64, tag = "1")]
    pub stage_id: u64,
    #[prost(uint32, tag = "2")]
    pub kind: u32,
    /// A valid empty initialized manifest with the target identity, attributes and generation.
    #[prost(message, optional, tag = "3")]
    pub header: Option<ChunkManifest>,
    #[prost(uint64, tag = "4")]
    pub expected_generation: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub expected_manifest_digest: Vec<u8>,
    #[prost(string, tag = "6")]
    pub holder: String,
    #[prost(uint64, tag = "7")]
    pub writer_epoch: u64,
    #[prost(uint64, tag = "8")]
    pub created_ms: u64,
    #[prost(uint64, tag = "9")]
    pub expires_at_ms: u64,
    #[prost(bytes = "vec", tag = "10")]
    pub request_nonce: Vec<u8>,
    /// Complete object length at publication, not just the newly allocated bytes.
    #[prost(uint64, tag = "11")]
    pub target_length: u64,
    /// A rollover captures the final complete old tail length before allocating a new one.
    #[prost(uint64, tag = "12")]
    pub base_length: u64,
}
impl NativeStageStart {
    pub fn header(&self) -> Result<&ChunkManifest, ObjectError> {
        self.header
            .as_ref()
            .ok_or_else(|| corrupt("native stage lacks its object header"))
    }
    pub fn replacement_incarnation(&self) -> Result<[u8; 16], ObjectError> {
        let header = self.header()?;
        let mut material = b"MONATIVE-OBJECT-BIRTH1".to_vec();
        material.extend_from_slice(&self.stage_id.to_le_bytes());
        material.extend_from_slice(&self.request_nonce);
        for name in [&header.bucket, &header.key] {
            material.extend_from_slice(&(name.len() as u64).to_le_bytes());
            material.extend_from_slice(name.as_bytes());
        }
        Ok(crate::chunk_integrity::sha256(&material)[..16]
            .try_into()
            .unwrap())
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        let header = self.header()?;
        header.validate()?;
        if self.stage_id == 0
            || !matches!(self.kind, STAGE_REPLACE | STAGE_ADD_TAIL)
            || self.writer_epoch == 0
            || self.writer_epoch > self.stage_id
            || self.holder.is_empty()
            || self.holder.len() > 256
            || self.holder.contains('\0')
            || !future_deadline(self.created_ms, self.expires_at_ms)
            || self.request_nonce.len() != 32
            || self.request_nonce.iter().all(|b| *b == 0)
            || header.deleted
            || header.format_version != CHUNK_MANIFEST_INITIALIZED_FORMAT
            || header.kind != CHUNK_KIND_LOG
            || header.length != 0
            || !header.extents.is_empty()
            || header.generation
                != self
                    .expected_generation
                    .checked_add(1)
                    .ok_or_else(|| corrupt("native object generation exhausted"))?
            || (self.expected_generation == 0 && !self.expected_manifest_digest.is_empty())
            || (self.expected_generation != 0 && self.expected_manifest_digest.len() != 32)
            || self.base_length > self.target_length
            || self.target_length > MAX_CHUNK_LENGTH * MAX_CHUNK_EXTENTS as u64
            || (self.kind == STAGE_REPLACE && self.base_length != 0)
            || (self.kind == STAGE_ADD_TAIL && self.expected_generation == 0)
        {
            return Err(corrupt(
                "invalid native stage identity, bounds, owner or deadline",
            ));
        }
        if self.kind == STAGE_REPLACE
            && header.initialized_log.as_ref().unwrap().object_incarnation
                != self.replacement_incarnation()?.to_vec()
        {
            return Err(corrupt(
                "replacement object incarnation is not bound to its stage attempt",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeOwnedChunk {
    /// One committed metadata allocation position. It is never reused after stage cleanup.
    #[prost(uint64, tag = "1")]
    pub allocation_id: u64,
    /// Planned location and length. Flags and integrity fields remain empty until publication.
    #[prost(message, optional, tag = "2")]
    pub extent: Option<ChunkExtent>,
    #[prost(message, optional, tag = "3")]
    pub initialization: Option<InitializedChunk>,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeReadPin {
    #[prost(uint64, tag = "1")]
    pub pin_id: u64,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
    #[prost(uint64, tag = "4")]
    pub generation: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub manifest_digest: Vec<u8>,
    #[prost(uint64, tag = "6")]
    pub expires_at_ms: u64,
}
impl NativeReadPin {
    fn version_key(&self) -> (ObjectId, u64) {
        (ObjectId::new(&self.bucket, &self.key), self.generation)
    }
    fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.pin_id == 0
            || self.generation == 0
            || self.expires_at_ms == 0
            || self.manifest_digest.len() != 32
        {
            return Err(corrupt("invalid native read pin"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Stage {
    start: NativeStageStart,
    base: Option<Arc<ChunkManifest>>,
    chunks: Vector<Arc<NativeOwnedChunk>>,
    nonces: OrdSet<Vec<u8>>,
    revision: u64,
    next_offset: u64,
}
#[derive(Clone, PartialEq, Eq, Message)]
struct StageImage {
    #[prost(message, optional, tag = "1")]
    start: Option<NativeStageStart>,
    #[prost(message, optional, tag = "2")]
    base: Option<ChunkManifest>,
    #[prost(message, repeated, tag = "3")]
    chunks: Vec<NativeOwnedChunk>,
    #[prost(uint64, tag = "4")]
    revision: u64,
}
impl Stage {
    fn new(start: NativeStageStart, base: Option<Arc<ChunkManifest>>) -> Result<Self, ObjectError> {
        start.validate()?;
        match (start.kind, base.as_ref()) {
            (STAGE_REPLACE, None) => {}
            (STAGE_ADD_TAIL, Some(base)) => {
                base.validate()?;
                if base.deleted
                    || base.initialized_log.is_none()
                    || base.object_id() != start.header()?.object_id()
                    || base.generation != start.expected_generation
                    || digest(base)? != start.expected_manifest_digest
                    || ChunkAttributes::of(base) != ChunkAttributes::of(start.header()?)
                    || base.initialized_log.as_ref().unwrap().object_incarnation
                        != start
                            .header()?
                            .initialized_log
                            .as_ref()
                            .unwrap()
                            .object_incarnation
                    || start.base_length < base.length
                    || (!base
                        .extents
                        .last()
                        .is_some_and(|e| e.flags == CHUNK_FLAG_TAIL)
                        && start.base_length != base.length)
                {
                    return Err(corrupt(
                        "native rollover base does not match its captured object",
                    ));
                }
            }
            _ => return Err(corrupt("native stage has an invalid base")),
        }
        if let Some(base) = &base {
            if let Some(last) = base.extents.last().filter(|e| e.flags == CHUNK_FLAG_TAIL) {
                if start
                    .base_length
                    .checked_sub(last.object_offset)
                    .is_none_or(|n| n > MAX_CHUNK_LENGTH)
                {
                    return Err(corrupt("native rollover exceeds the old tail capacity"));
                }
            }
        }
        let needed = (start.target_length - start.base_length).div_ceil(MAX_CHUNK_LENGTH);
        if needed > (MAX_CHUNK_EXTENTS - base.as_ref().map_or(0, |m| m.extents.len())) as u64 {
            return Err(corrupt("native stage cannot fit its requested new chunks"));
        }
        let nonces = base
            .as_ref()
            .into_iter()
            .flat_map(|m| &m.initialized_log.as_ref().unwrap().chunks)
            .flat_map(|c| &c.replicas)
            .map(|r| r.initialization.clone())
            .collect();
        let revision = start.stage_id;
        let next_offset = start.base_length;
        Ok(Self {
            start,
            base,
            chunks: Vector::new(),
            nonces,
            revision,
            next_offset,
        })
    }
    fn base_count(&self) -> usize {
        self.base.as_ref().map_or(0, |m| m.extents.len())
    }
    fn cost(&self) -> usize {
        bytes_cost(&self.start)
            + self.base.as_ref().map_or(0, |m| bytes_cost(m.as_ref()))
            + self
                .chunks
                .iter()
                .map(|c| bytes_cost(c.as_ref()))
                .sum::<usize>()
            + 128
    }
    fn extend(&mut self, owned: NativeOwnedChunk) -> Result<(), ObjectError> {
        let extent = owned
            .extent
            .as_ref()
            .ok_or_else(|| corrupt("native chunk lacks its extent"))?;
        let binding = owned
            .initialization
            .as_ref()
            .ok_or_else(|| corrupt("native chunk lacks its initialization"))?;
        let index = self.base_count() + self.chunks.len();
        let (volume, chunk) = crate::object_manifest::chunk_identity_of_index(owned.allocation_id);
        if owned.allocation_id <= self.revision
            || index >= MAX_CHUNK_EXTENTS
            || extent.volume_id != volume
            || extent.chunk_id != chunk
            || extent.object_offset != self.next_offset
            || extent.length == 0
            || extent.length > MAX_CHUNK_LENGTH
            || extent.flags != 0
            || !extent.digest.is_empty()
            || extent.footer_crc != 0
            || !extent.footer_sha256.is_empty()
            || binding.extent_index as usize != index
            || binding.writer_epoch != self.start.writer_epoch
        {
            return Err(corrupt(
                "native staged chunk does not own its planned range and allocation",
            ));
        }
        let end = self
            .next_offset
            .checked_add(extent.length)
            .ok_or_else(|| corrupt("native staged range overflow"))?;
        if end > self.start.target_length {
            return Err(corrupt("native staged chunk exceeds the requested length"));
        }
        // Reuse the actual manifest validators for all per-chunk identity and extent rules,
        // without pretending that unpublished bytes already have sealed checksums.
        let mut sample = self.start.header()?.clone();
        sample.length = extent.length;
        let mut sample_extent = extent.clone();
        sample_extent.object_offset = 0;
        sample_extent.flags = CHUNK_FLAG_TAIL;
        sample.extents.push(sample_extent);
        let mut sample_binding = binding.clone();
        sample_binding.extent_index = 0;
        sample
            .initialized_log
            .as_mut()
            .unwrap()
            .chunks
            .push(sample_binding);
        sample.validate()?;
        let mut nonces = self.nonces.clone();
        for replica in &binding.replicas {
            if nonces.contains(&replica.initialization) {
                return Err(corrupt("native stage reuses a storage initialization"));
            }
            nonces.insert(replica.initialization.clone());
        }
        self.revision = owned.allocation_id;
        self.next_offset = end;
        self.nonces = nonces;
        self.chunks.push_back(Arc::new(owned));
        Ok(())
    }
    fn image(&self) -> StageImage {
        StageImage {
            start: Some(self.start.clone()),
            base: self.base.as_ref().map(|m| (**m).clone()),
            chunks: self.chunks.iter().map(|c| (**c).clone()).collect(),
            revision: self.revision,
        }
    }
    fn restore(image: StageImage) -> Result<Self, ObjectError> {
        let mut stage = Self::new(
            image
                .start
                .ok_or_else(|| corrupt("native stage image lacks its start"))?,
            image.base.map(Arc::new),
        )?;
        for chunk in image.chunks {
            stage.extend(chunk)?;
        }
        if stage.revision != image.revision {
            return Err(corrupt("native stage revision mismatch"));
        }
        Ok(stage)
    }
    fn check_publication(&self, after: &ChunkManifest) -> Result<(), ObjectError> {
        after.validate()?;
        let header = self.start.header()?;
        if self.next_offset != self.start.target_length
            || after.deleted
            || after.format_version != header.format_version
            || after.object_id() != header.object_id()
            || after.generation != header.generation
            || after.length != self.start.target_length
            || after.kind != CHUNK_KIND_LOG
            || ChunkAttributes::of(after) != ChunkAttributes::of(header)
            || after.initialized_log.as_ref().unwrap().object_incarnation
                != header.initialized_log.as_ref().unwrap().object_incarnation
            || after.extents.len() != self.base_count() + self.chunks.len()
        {
            return Err(refused(
                "native publication does not match its admitted stage",
            ));
        }
        if let Some(base) = &self.base {
            for (index, old) in base.extents.iter().enumerate() {
                let new = &after.extents[index];
                let equal = if index + 1 == base.extents.len() && old.flags == CHUNK_FLAG_TAIL {
                    let mut expected = old.clone();
                    expected.length = self
                        .start
                        .base_length
                        .checked_sub(old.object_offset)
                        .ok_or_else(|| refused("native rollover shortens its old prefix"))?;
                    expected.flags = new.flags;
                    expected.footer_crc = new.footer_crc;
                    expected.footer_sha256 = new.footer_sha256.clone();
                    expected == *new
                        && new.flags & CHUNK_FLAG_SEALED != 0
                        && new.length >= old.length
                } else {
                    old == new
                };
                let mut binding = base.initialized_log.as_ref().unwrap().chunks[index].clone();
                if index + 1 == base.extents.len() && old.flags == CHUNK_FLAG_TAIL {
                    if self.start.writer_epoch < binding.writer_epoch {
                        return Err(refused(
                            "native rollover moves its qualified epoch backward",
                        ));
                    }
                    binding.writer_epoch = self.start.writer_epoch;
                }
                if !equal || binding != after.initialized_log.as_ref().unwrap().chunks[index] {
                    return Err(refused(
                        "native rollover changes a retained chunk identity or prefix",
                    ));
                }
            }
        }
        for (index, owned) in self.chunks.iter().enumerate() {
            let index = self.base_count() + index;
            let planned = owned.extent.as_ref().unwrap();
            let actual = &after.extents[index];
            let mut expected = planned.clone();
            expected.flags = actual.flags;
            expected.footer_crc = actual.footer_crc;
            expected.footer_sha256 = actual.footer_sha256.clone();
            if expected != *actual
                || owned.initialization.as_ref().unwrap()
                    != &after.initialized_log.as_ref().unwrap().chunks[index]
            {
                return Err(refused(
                    "native publication replaces its owned chunk or storage identity",
                ));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Retained {
    manifest: Arc<ChunkManifest>,
    manifest_digest: Vec<u8>,
    readers: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct RetiredStage {
    stage: Arc<Stage>,
    retired_at_ms: u64,
    position: u64,
}
#[derive(Clone, PartialEq, Eq, Message)]
struct RetiredStageImage {
    #[prost(message, optional, tag = "1")]
    stage: Option<StageImage>,
    #[prost(uint64, tag = "2")]
    retired_at_ms: u64,
    #[prost(uint64, tag = "3")]
    position: u64,
}
type VersionKey = (ObjectId, u64);
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NativeObjectState {
    stages: OrdMap<u64, Arc<Stage>>,
    retired_stages: OrdMap<u64, RetiredStage>,
    owners: OrdMap<ObjectId, u64>,
    pins: OrdMap<u64, NativeReadPin>,
    retained: OrdMap<VersionKey, Retained>,
    revision: u64,
    last_position: u64,
    body_bytes: usize,
}
#[derive(Clone, PartialEq, Eq, Message)]
struct StateImage {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(uint64, tag = "3")]
    last_position: u64,
    #[prost(message, repeated, tag = "4")]
    stages: Vec<StageImage>,
    #[prost(message, repeated, tag = "5")]
    pins: Vec<NativeReadPin>,
    #[prost(message, repeated, tag = "6")]
    retained: Vec<ChunkManifest>,
    #[prost(message, repeated, tag = "7")]
    retired_stages: Vec<RetiredStageImage>,
}
impl NativeObjectState {
    pub fn image_size_bound(&self) -> usize {
        self.body_bytes + IMAGE_HEADER_ALLOWANCE
    }
    pub fn is_staged(&self, id: &ObjectId) -> bool {
        self.owners.contains_key(id)
    }
    pub fn staged_objects(&self) -> impl Iterator<Item = &ObjectId> {
        self.owners.keys()
    }
    /// Snapshot-wide validation, performed off the append path. The command layer uses
    /// the global committed allocation position to admit fresh chunks without this scan.
    pub fn validate_snapshot(
        &self,
        catalog: &ChunkCatalog,
        position: u64,
    ) -> Result<(), ObjectError> {
        use std::collections::HashMap;
        type Seen<'a> = HashMap<(&'a str, u64, u64), (Option<(&'a str, &'a str)>, bool)>;
        fn visit<'a>(
            manifest: &'a ChunkManifest,
            position: u64,
            seen: &mut Seen<'a>,
        ) -> Result<(), ObjectError> {
            manifest.validate()?;
            let native = manifest.initialized_log.as_ref();
            for (index, extent) in manifest.extents.iter().enumerate() {
                if let Some(initialized) = native {
                    let allocated = extent
                        .volume_id
                        .checked_shl(32)
                        .and_then(|v| v.checked_add(extent.chunk_id))
                        .ok_or_else(|| corrupt("native chunk identity overflow"))?;
                    if allocated == 0
                        || allocated > position
                        || initialized.chunks[index].writer_epoch > position
                    {
                        return Err(corrupt(
                            "native chunk allocation or qualified epoch is from the future",
                        ));
                    }
                }
                let key = (extent.group.as_str(), extent.volume_id, extent.chunk_id);
                let owner = (manifest.bucket.as_str(), manifest.key.as_str());
                match seen.get_mut(&key) {
                    None => {
                        seen.insert(key, (Some(owner), native.is_some()));
                    }
                    Some((prior, was_native)) => {
                        if *prior != Some(owner) {
                            if *was_native || native.is_some() {
                                return Err(corrupt(
                                    "native chunk storage is shared across objects",
                                ));
                            }
                            *prior = None;
                        }
                        *was_native |= native.is_some();
                    }
                }
            }
            Ok(())
        }
        if self.last_position > position {
            return Err(corrupt(
                "native state position exceeds the metadata commit position",
            ));
        }
        let mut seen = Seen::new();
        for (_, manifest) in catalog.iter() {
            visit(manifest, position, &mut seen)?;
        }
        for retained in self.retained.values() {
            visit(&retained.manifest, position, &mut seen)?;
        }
        for stage in self
            .stages
            .values()
            .map(|s| s.as_ref())
            .chain(self.retired_stages.values().map(|s| s.stage.as_ref()))
        {
            if let Some(base) = &stage.base {
                visit(base, position, &mut seen)?;
            }
            for chunk in &stage.chunks {
                let extent = chunk.extent.as_ref().unwrap();
                let key = (extent.group.as_str(), extent.volume_id, extent.chunk_id);
                if seen
                    .insert(
                        key,
                        (
                            Some((
                                stage.start.header()?.bucket.as_str(),
                                stage.start.header()?.key.as_str(),
                            )),
                            true,
                        ),
                    )
                    .is_some()
                {
                    return Err(corrupt(
                        "native staged or retired storage overlaps a published generation",
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn is_default(&self) -> bool {
        self.last_position == 0
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn last_position(&self) -> u64 {
        self.last_position
    }
    pub fn stage_start(&self, id: u64) -> Option<&NativeStageStart> {
        self.stages.get(&id).map(|s| &s.start)
    }
    pub fn stage_revision(&self, id: u64) -> Option<u64> {
        self.stages.get(&id).map(|s| s.revision)
    }
    pub fn pin(&self, id: u64) -> Option<&NativeReadPin> {
        self.pins.get(&id)
    }
    pub fn owned_chunks(&self, id: u64) -> impl Iterator<Item = &NativeOwnedChunk> {
        self.stages
            .get(&id)
            .into_iter()
            .flat_map(|s| s.chunks.iter())
            .map(|c| c.as_ref())
    }
    pub fn retired_chunks(&self) -> impl Iterator<Item = (u64, &NativeOwnedChunk)> {
        self.retired_stages
            .iter()
            .flat_map(|(id, s)| s.stage.chunks.iter().map(move |c| (*id, c.as_ref())))
    }
    pub fn retired_stage_count(&self) -> usize {
        self.retired_stages.len()
    }
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }
    pub fn pin_count(&self) -> usize {
        self.pins.len()
    }
    pub fn retained_count(&self) -> usize {
        self.retained.len()
    }
    fn next_revision(&self, position: u64) -> Result<u64, ObjectError> {
        if position <= self.last_position {
            return Err(refused("native metadata position is consumed"));
        }
        self.revision
            .checked_add(1)
            .ok_or_else(|| refused("native metadata revision exhausted"))
    }
    fn budget(&self, remove: usize, add: usize) -> Result<usize, ObjectError> {
        let size = self
            .body_bytes
            .checked_sub(remove)
            .and_then(|n| n.checked_add(add))
            .ok_or_else(|| corrupt("native metadata size accounting overflow"))?;
        if size > MAX_IMAGE - IMAGE_HEADER_ALLOWANCE {
            return Err(refused("native metadata image is full"));
        }
        Ok(size)
    }
    fn expected(
        start: &NativeStageStart,
        catalog: &ChunkCatalog,
    ) -> Result<Option<Arc<ChunkManifest>>, ObjectError> {
        let current = catalog.get(&start.header()?.object_id());
        match current {
            None if start.expected_generation == 0 && start.expected_manifest_digest.is_empty() => {
                Ok(None)
            }
            Some(m)
                if m.generation == start.expected_generation
                    && digest(m)? == start.expected_manifest_digest =>
            {
                Ok(Some(m.clone()))
            }
            _ => Err(refused("native stage expected a different object version")),
        }
    }
    pub fn begin(
        &mut self,
        start: NativeStageStart,
        catalog: &ChunkCatalog,
        now: u64,
    ) -> Result<bool, ObjectError> {
        start.validate()?;
        if let Some(stage) = self.stages.get(&start.stage_id) {
            return if stage.start == start {
                Ok(false)
            } else {
                Err(refused("native stage identity is already owned"))
            };
        }
        if now < start.created_ms || now >= start.expires_at_ms {
            return Err(refused("native stage is outside its lease"));
        }
        let id = start.header()?.object_id();
        if self.owners.contains_key(&id)
            || self.stages.len() + self.retired_stages.len() >= MAX_NATIVE_STAGES
        {
            return Err(refused("native object already staged or stage table full"));
        }
        let current = Self::expected(&start, catalog)?;
        if start.kind == STAGE_REPLACE
            && current
                .as_ref()
                .and_then(|m| m.initialized_log.as_ref())
                .is_some_and(|m| {
                    m.object_incarnation
                        == start
                            .header
                            .as_ref()
                            .unwrap()
                            .initialized_log
                            .as_ref()
                            .unwrap()
                            .object_incarnation
                })
        {
            return Err(refused("replacement must have a fresh object incarnation"));
        }
        let base = if start.kind == STAGE_ADD_TAIL {
            current
        } else {
            None
        };
        let stage = Stage::new(start, base)?;
        let position = stage.start.stage_id;
        let revision = self.next_revision(position)?;
        let size = self.budget(0, stage.cost())?;
        self.stages.insert(position, Arc::new(stage));
        self.owners.insert(id, position);
        self.last_position = position;
        self.revision = revision;
        self.body_bytes = size;
        Ok(true)
    }
    fn live_stage(
        &self,
        id: u64,
        expected_revision: u64,
        holder: &str,
        now: u64,
    ) -> Result<&Arc<Stage>, ObjectError> {
        let stage = self
            .stages
            .get(&id)
            .ok_or_else(|| refused("native stage is missing; outcome is unconfirmed"))?;
        if stage.revision != expected_revision
            || stage.start.holder != holder
            || now < stage.start.created_ms
            || now >= stage.start.expires_at_ms
        {
            return Err(refused("native stage owner, revision or deadline changed"));
        }
        Ok(stage)
    }
    pub fn extend(
        &mut self,
        id: u64,
        expected_revision: u64,
        holder: &str,
        now: u64,
        chunk: NativeOwnedChunk,
    ) -> Result<(), ObjectError> {
        let old = self.live_stage(id, expected_revision, holder, now)?;
        let position = chunk.allocation_id;
        let revision = self.next_revision(position)?;
        let mut stage = (**old).clone();
        stage.extend(chunk)?;
        // The persistent vector and nonce set share old prefixes. Admission accounts only
        // for this new record, without scanning or serializing a growing stage.
        let add = bytes_cost(stage.chunks.back().unwrap().as_ref());
        let size = self.budget(0, add)?;
        self.stages.insert(id, Arc::new(stage));
        self.last_position = position;
        self.revision = revision;
        self.body_bytes = size;
        Ok(())
    }
    /// Release the object mutation slot but retain every owned chunk for durable cleanup.
    pub fn abort(
        &mut self,
        id: u64,
        expected_revision: u64,
        holder: &str,
        now: u64,
        position: u64,
    ) -> Result<(), ObjectError> {
        self.live_stage(id, expected_revision, holder, now)?;
        self.retire_stage(id, now, position)
    }
    pub fn expire_stage(
        &mut self,
        id: u64,
        expected_revision: u64,
        expected_expiry: u64,
        now: u64,
        position: u64,
    ) -> Result<(), ObjectError> {
        let stage = self
            .stages
            .get(&id)
            .ok_or_else(|| refused("native stage is missing"))?;
        if stage.revision != expected_revision
            || stage.start.expires_at_ms != expected_expiry
            || now < expected_expiry
        {
            return Err(refused("native stage expiry changed or has not elapsed"));
        }
        self.retire_stage(id, now, position)
    }
    fn retire_stage(&mut self, id: u64, now: u64, position: u64) -> Result<(), ObjectError> {
        let stage = self
            .stages
            .get(&id)
            .ok_or_else(|| refused("native stage is missing"))?
            .clone();
        let object = stage.start.header()?.object_id();
        let revision = self.next_revision(position)?;
        let size = self.budget(0, 64)?;
        self.retired_stages.insert(
            id,
            RetiredStage {
                stage,
                retired_at_ms: now,
                position,
            },
        );
        self.stages.remove(&id);
        self.owners.remove(&object);
        self.last_position = position;
        self.revision = revision;
        self.body_bytes = size;
        Ok(())
    }
    /// The command layer must first verify durable retirement on every storage member,
    /// including a persistent fence against delayed initialization or append requests.
    pub fn forget_retired_stage(
        &mut self,
        id: u64,
        expected_retirement: u64,
        position: u64,
    ) -> Result<(), ObjectError> {
        let retired = self
            .retired_stages
            .get(&id)
            .ok_or_else(|| refused("native retirement is missing"))?;
        if retired.position != expected_retirement {
            return Err(refused("native retirement position changed"));
        }
        let size = self.budget(retired.stage.cost() + 64, 0)?;
        let revision = self.next_revision(position)?;
        self.retired_stages.remove(&id);
        self.last_position = position;
        self.revision = revision;
        self.body_bytes = size;
        Ok(())
    }
    pub fn publish(
        &mut self,
        id: u64,
        expected_revision: u64,
        holder: &str,
        now: u64,
        position: u64,
        after: ChunkManifest,
        catalog: &mut ChunkCatalog,
    ) -> Result<(), ObjectError> {
        let stage = self.live_stage(id, expected_revision, holder, now)?;
        stage.check_publication(&after)?;
        let before = Self::expected(&stage.start, catalog)?;
        let outcome = ChunkOutcome::decide(before.map(|m| (*m).clone()), after)?;
        let object = stage.start.header()?.object_id();
        let cost = stage.cost();
        let revision = self.next_revision(position)?;
        let size = self.budget(cost, 0)?;
        catalog.apply(&outcome)?;
        self.stages.remove(&id);
        self.owners.remove(&object);
        self.last_position = position;
        self.revision = revision;
        self.body_bytes = size;
        Ok(())
    }
    pub fn open_pin(
        &mut self,
        pin: NativeReadPin,
        catalog: &ChunkCatalog,
        now: u64,
    ) -> Result<(), ObjectError> {
        pin.validate()?;
        if self.pins.len() >= MAX_NATIVE_READ_PINS || !future_deadline(now, pin.expires_at_ms) {
            return Err(refused("native read pin deadline or capacity"));
        }
        let revision = self.next_revision(pin.pin_id)?;
        let manifest = catalog
            .get(&ObjectId::new(&pin.bucket, &pin.key))
            .filter(|m| !m.deleted && m.initialized_log.is_some() && m.generation == pin.generation)
            .ok_or_else(|| refused("native read pin does not name the current generation"))?;
        if digest(manifest)? != pin.manifest_digest {
            return Err(refused("native read pin digest changed"));
        }
        let key = pin.version_key();
        let mut retained = self.retained.get(&key).cloned().unwrap_or(Retained {
            manifest: manifest.clone(),
            manifest_digest: pin.manifest_digest.clone(),
            readers: 0,
        });
        if retained.manifest.as_ref() != manifest.as_ref() {
            return Err(corrupt(
                "native generation has conflicting retained manifests",
            ));
        }
        let add = bytes_cost(&pin)
            + if retained.readers == 0 {
                bytes_cost(manifest.as_ref())
            } else {
                0
            };
        let size = self.budget(0, add)?;
        retained.readers += 1;
        self.retained.insert(key, retained);
        self.last_position = pin.pin_id;
        self.pins.insert(pin.pin_id, pin);
        self.revision = revision;
        self.body_bytes = size;
        Ok(())
    }
    pub fn pinned_manifest(
        &self,
        pin_id: u64,
        now: u64,
    ) -> Result<Arc<ChunkManifest>, ObjectError> {
        let pin = self
            .pins
            .get(&pin_id)
            .filter(|p| now != 0 && now < p.expires_at_ms)
            .ok_or_else(|| refused("native read pin is missing or expired"))?;
        Ok(self
            .retained
            .get(&pin.version_key())
            .ok_or_else(|| corrupt("native read pin lost its retained manifest"))?
            .manifest
            .clone())
    }
    pub fn renew_pin(
        &mut self,
        id: u64,
        expected_expiry: u64,
        expires: u64,
        now: u64,
        position: u64,
    ) -> Result<(), ObjectError> {
        let pin = self
            .pins
            .get(&id)
            .ok_or_else(|| refused("native read pin is missing"))?;
        if pin.expires_at_ms != expected_expiry
            || now == 0
            || now >= expected_expiry
            || expires <= expected_expiry
            || !future_deadline(now, expires)
        {
            return Err(refused("native read pin renewal lost its deadline"));
        }
        let mut next = pin.clone();
        next.expires_at_ms = expires;
        let size = self.budget(bytes_cost(pin), bytes_cost(&next))?;
        let revision = self.next_revision(position)?;
        self.pins.insert(id, next);
        self.last_position = position;
        self.revision = revision;
        self.body_bytes = size;
        Ok(())
    }
    pub fn expire_pin(
        &mut self,
        id: u64,
        expected_expiry: u64,
        now: u64,
        position: u64,
    ) -> Result<(), ObjectError> {
        if now < expected_expiry {
            return Err(refused("native read pin has not expired"));
        }
        self.close_pin(id, expected_expiry, position)
    }
    pub fn close_pin(
        &mut self,
        id: u64,
        expected_expiry: u64,
        position: u64,
    ) -> Result<(), ObjectError> {
        let pin = self
            .pins
            .get(&id)
            .ok_or_else(|| refused("native read pin is missing"))?;
        if pin.expires_at_ms != expected_expiry {
            return Err(refused("native read pin deadline changed"));
        }
        let key = pin.version_key();
        let mut retained = self
            .retained
            .get(&key)
            .cloned()
            .ok_or_else(|| corrupt("native pin retention is missing"))?;
        let remove = bytes_cost(pin)
            + if retained.readers == 1 {
                bytes_cost(retained.manifest.as_ref())
            } else {
                0
            };
        let size = self.budget(remove, 0)?;
        let revision = self.next_revision(position)?;
        retained.readers = retained
            .readers
            .checked_sub(1)
            .ok_or_else(|| corrupt("native pin retention count underflow"))?;
        if retained.readers == 0 {
            self.retained.remove(&key);
        } else {
            self.retained.insert(key, retained);
        }
        self.pins.remove(&id);
        self.last_position = position;
        self.revision = revision;
        self.body_bytes = size;
        Ok(())
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        let image = StateImage {
            version: 1,
            revision: self.revision,
            last_position: self.last_position,
            stages: self.stages.values().map(|s| s.image()).collect(),
            pins: self.pins.values().cloned().collect(),
            retained: self
                .retained
                .values()
                .map(|r| (*r.manifest).clone())
                .collect(),
            retired_stages: self
                .retired_stages
                .values()
                .map(|s| RetiredStageImage {
                    stage: Some(s.stage.image()),
                    retired_at_ms: s.retired_at_ms,
                    position: s.position,
                })
                .collect(),
        };
        frame(b"MONS", &image, MAX_IMAGE)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let image: StateImage = unframe(b"MONS", bytes, MAX_IMAGE)?;
        if image.version != 1
            || image
                .stages
                .len()
                .saturating_add(image.retired_stages.len())
                > MAX_NATIVE_STAGES
            || image.pins.len() > MAX_NATIVE_READ_PINS
            || image.retained.len() > MAX_NATIVE_READ_PINS
            || (image.last_position == 0) != (image.revision == 0)
            || image.revision > image.last_position
            || frame(b"MONS", &image, MAX_IMAGE)? != bytes
        {
            return Err(corrupt("invalid or noncanonical native state image"));
        }
        let mut state = Self {
            revision: image.revision,
            last_position: image.last_position,
            ..Self::default()
        };
        let mut identities = OrdSet::new();
        let mut previous = 0;
        for record in image.stages {
            let stage = Stage::restore(record)?;
            let id = stage.start.stage_id;
            if id <= previous
                || stage.revision > state.last_position
                || state
                    .owners
                    .contains_key(&stage.start.header()?.object_id())
            {
                return Err(corrupt(
                    "native stages repeat or exceed their committed position",
                ));
            }
            if identities.contains(&id) {
                return Err(corrupt("native metadata identities overlap"));
            }
            identities.insert(id);
            for chunk in &stage.chunks {
                if identities.contains(&chunk.allocation_id) {
                    return Err(corrupt("native chunk allocations overlap"));
                }
                identities.insert(chunk.allocation_id);
            }
            previous = id;
            state.body_bytes = state.budget(0, stage.cost())?;
            state.owners.insert(stage.start.header()?.object_id(), id);
            state.stages.insert(id, Arc::new(stage));
        }
        let mut previous_retired = 0;
        for retired in image.retired_stages {
            let stage = Stage::restore(
                retired
                    .stage
                    .ok_or_else(|| corrupt("native retirement lacks owned chunks"))?,
            )?;
            let id = stage.start.stage_id;
            if id <= previous_retired
                || retired.retired_at_ms < stage.start.created_ms
                || retired.position <= stage.revision
                || retired.position > state.last_position
                || identities.contains(&id)
                || identities.contains(&retired.position)
            {
                return Err(corrupt("native retirement identities or positions overlap"));
            }
            identities.insert(id);
            identities.insert(retired.position);
            for chunk in &stage.chunks {
                if identities.contains(&chunk.allocation_id) {
                    return Err(corrupt("native retired allocations overlap"));
                }
                identities.insert(chunk.allocation_id);
            }
            previous_retired = id;
            state.body_bytes = state.budget(0, stage.cost() + 64)?;
            state.retired_stages.insert(
                id,
                RetiredStage {
                    stage: Arc::new(stage),
                    retired_at_ms: retired.retired_at_ms,
                    position: retired.position,
                },
            );
        }
        let mut previous = None;
        for manifest in image.retained {
            manifest.validate()?;
            if manifest.deleted || manifest.initialized_log.is_none() {
                return Err(corrupt(
                    "native retained object is not live initialized data",
                ));
            }
            let key = (manifest.object_id(), manifest.generation);
            if previous.as_ref().is_some_and(|p| p >= &key) {
                return Err(corrupt("native retained generations are not ordered"));
            }
            previous = Some(key.clone());
            state.body_bytes = state.budget(0, bytes_cost(&manifest))?;
            state.retained.insert(
                key,
                Retained {
                    manifest_digest: digest(&manifest)?,
                    manifest: Arc::new(manifest),
                    readers: 0,
                },
            );
        }
        let mut previous = 0;
        for pin in image.pins {
            pin.validate()?;
            if pin.pin_id <= previous || pin.pin_id > state.last_position {
                return Err(corrupt(
                    "native read pins repeat or exceed their committed position",
                ));
            }
            if identities.contains(&pin.pin_id) {
                return Err(corrupt("native pin identity overlaps allocated storage"));
            }
            identities.insert(pin.pin_id);
            previous = pin.pin_id;
            let key = pin.version_key();
            let retained = state
                .retained
                .get_mut(&key)
                .ok_or_else(|| corrupt("native pin has no retained generation"))?;
            if retained.manifest_digest != pin.manifest_digest {
                return Err(corrupt(
                    "native pin digest does not bind its retained generation",
                ));
            }
            retained.readers += 1;
            state.body_bytes = state.budget(0, bytes_cost(&pin))?;
            state.pins.insert(pin.pin_id, pin);
        }
        if state.retained.values().any(|r| r.readers == 0) {
            return Err(corrupt("native state retains an unpinned generation"));
        }
        Ok(state)
    }
}
