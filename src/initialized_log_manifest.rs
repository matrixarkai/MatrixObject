// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable object and storage identities for initialized LOG manifests.
//! Publication admission and read-pin ownership belong to metadata commands, not this codec.
use crate::chunk_manifest::{ChunkManifest, CHUNK_KIND_LOG};
use crate::ObjectError;
use prost::Message;
use std::collections::BTreeSet;

/// A manifest whose LOG extents carry explicit initialization and writer identities.
/// Earlier manifest readers reject this version instead of dropping those identities.
pub const CHUNK_MANIFEST_INITIALIZED_FORMAT: u32 = 2;
/// Physical storage qualification is mandatory in this format.
pub const CHUNK_MANIFEST_PHYSICAL_FORMAT: u32 = 3;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct InitializedReplica {
    /// Member identity in the configured chunk group, in its configured voter order.
    #[prost(string, tag = "1")]
    pub member_id: String,
    /// Durable storage initialization for this member. A replacement gets a new value,
    /// and must catch up before metadata publishes that replacement as a ready member.
    #[prost(bytes = "vec", tag = "2")]
    pub initialization: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub storage_incarnation: Vec<u8>,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct InitializedChunk {
    #[prost(uint32, tag = "1")]
    pub extent_index: u32,
    /// Storage generation, distinct from the object's metadata generation.
    #[prost(uint32, tag = "2")]
    pub chunk_generation: u32,
    /// Last qualified writer epoch. Granting a pending lease does not qualify its epoch.
    #[prost(uint64, tag = "3")]
    pub writer_epoch: u64,
    #[prost(message, repeated, tag = "4")]
    pub replicas: Vec<InitializedReplica>,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct InitializedLogManifest {
    /// Stable across append attempts and lease changes; a fresh object write replaces it.
    #[prost(bytes = "vec", tag = "1")]
    pub object_incarnation: Vec<u8>,
    /// Exactly one binding per extent, in extent order.
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<InitializedChunk>,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
fn identity(bytes: &[u8]) -> bool {
    bytes.len() == 16 && bytes.iter().any(|b| *b != 0)
}
fn member_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}
impl InitializedLogManifest {
    pub fn validate_for(&self, manifest: &ChunkManifest) -> Result<(), ObjectError> {
        if !matches!(
            manifest.format_version,
            CHUNK_MANIFEST_INITIALIZED_FORMAT | CHUNK_MANIFEST_PHYSICAL_FORMAT
        ) || !identity(&self.object_incarnation)
            || self.chunks.len() != manifest.extents.len()
            || (!manifest.deleted && manifest.kind != CHUNK_KIND_LOG)
            || (manifest.deleted && !self.chunks.is_empty())
        {
            return Err(invalid(
                "invalid initialized LOG object identity or extent bindings",
            ));
        }
        let mut initializations = BTreeSet::new();
        for (index, chunk) in self.chunks.iter().enumerate() {
            if chunk.extent_index as usize != index
                || chunk.chunk_generation == 0
                || chunk.writer_epoch == 0
                || chunk.replicas.len() != 3
            {
                return Err(invalid(
                    "invalid initialized LOG chunk generation, epoch or replicas",
                ));
            }
            let mut members = BTreeSet::new();
            let mut storage = BTreeSet::new();
            for replica in &chunk.replicas {
                if !member_name(&replica.member_id)
                    || !identity(&replica.initialization)
                    || !members.insert(&replica.member_id)
                    || !initializations.insert(&replica.initialization)
                    || (manifest.format_version == CHUNK_MANIFEST_INITIALIZED_FORMAT
                        && !replica.storage_incarnation.is_empty())
                    || (manifest.format_version == CHUNK_MANIFEST_PHYSICAL_FORMAT
                        && (!identity(&replica.storage_incarnation)
                            || !storage.insert(&replica.storage_incarnation)))
                {
                    return Err(invalid(
                        "initialized LOG chunk repeats or omits a storage identity",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// One extent's immutable metadata-qualified physical member identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalMemberBinding {
    pub writer_epoch: u64,
    pub generation: u32,
    pub volume_id: u64,
    pub chunk_id: u64,
    pub initializations: [[u8; 16]; 3],
    pub storage_incarnations: [[u8; 16]; 3],
    pub member_ids: [String; 3],
}
impl InitializedLogManifest {
    pub fn physical_binding(
        &self,
        manifest: &ChunkManifest,
        index: usize,
    ) -> Result<PhysicalMemberBinding, ObjectError> {
        // This is a lookup in an immutable metadata-validated manifest. Recheck only the
        // selected extent, keeping per-read binding work independent of corpus size.
        if !identity(&self.object_incarnation) || self.chunks.len() != manifest.extents.len() {
            return Err(invalid("physical binding shape differs"));
        }
        if manifest.format_version != CHUNK_MANIFEST_PHYSICAL_FORMAT {
            return Err(invalid("physical member binding requires manifest format3"));
        }
        let chunk = self
            .chunks
            .get(index)
            .ok_or_else(|| invalid("physical binding extent is absent"))?;
        let extent = &manifest.extents[index];
        if chunk.extent_index as usize != index
            || chunk.chunk_generation == 0
            || chunk.writer_epoch == 0
            || chunk.replicas.len() != 3
            || chunk.replicas.iter().enumerate().any(|(i, r)| {
                !member_name(&r.member_id)
                    || !identity(&r.storage_incarnation)
                    || !identity(&r.initialization)
                    || chunk.replicas[..i].iter().any(|old| {
                        old.member_id == r.member_id
                            || old.storage_incarnation == r.storage_incarnation
                            || old.initialization == r.initialization
                    })
            })
        {
            return Err(invalid("selected physical member binding is invalid"));
        }
        Ok(PhysicalMemberBinding {
            writer_epoch: chunk.writer_epoch,
            generation: chunk.chunk_generation,
            volume_id: extent.volume_id,
            chunk_id: extent.chunk_id,
            initializations: std::array::from_fn(|i| {
                chunk.replicas[i]
                    .initialization
                    .as_slice()
                    .try_into()
                    .unwrap()
            }),
            storage_incarnations: std::array::from_fn(|i| {
                chunk.replicas[i]
                    .storage_incarnation
                    .as_slice()
                    .try_into()
                    .unwrap()
            }),
            member_ids: std::array::from_fn(|i| chunk.replicas[i].member_id.clone()),
        })
    }
}
