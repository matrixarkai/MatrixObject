// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The gateway's view of the cluster when objects are stored as chunk manifests.
//!
//! The gateway reasons about an object through its generation (the ETag and every precondition), its
//! length, whether it is deleted, and, for an append that may have landed, the extents an attempt
//! published, which are that attempt's identity. [`ChunkPlane`] answers the gateway's [`ObjectPlane`]
//! with those, carried in the manifest type the gateway already holds, so no handler changes: a
//! chunk extent is shown as an extent of that manifest whose fields name the same pages and carry the
//! chunk extent's digest, so two attempts never share one.
//!
//! An append adds extents after the last one (see `append_chunk_object`); it is refused when the
//! manifest could not name them, and the object is then rewritten. The append resolve of the gateway
//! works unchanged: the attempt's extents are in the committed manifest if and only if it landed, and
//! the fence republishes the manifest one generation later.
//!
//! Bytes move through `RemoteMetaClient::write_chunk_object`, `read_chunk_object_range` and
//! `delete_chunk_object`, which verify every page they read against the manifest's digests.

use crate::chunk_manifest::{ChunkExtent, ChunkManifest};
use crate::chunk_object::DEFAULT_STRIPE_BYTES;
use crate::cluster::{ChunkWriteFailure, ObjectWriteOptions, RemoteMetaClient, ReplicaClientOptions};
use crate::object_manifest::{ManifestListPage, ManifestListRequest, ObjectExtent, ObjectManifest};
use crate::ObjectError;
use std::time::Instant;

use super::cluster_gateway::{ClusterGateway, GatewayOptions, ObjectPlane, PlaneFailure};

/// One connection's worth of cluster operations over chunk manifests.
pub struct ChunkPlane {
    client: RemoteMetaClient,
    options: GatewayOptions,
}

/// A chunk extent as the gateway's extent type: the pages it names, where it sits in the object, and
/// its digest, which covers the bytes of every page of the run. Pages are allocated per write, so the
/// identity of an extent is unique to the write that made it.
pub(crate) fn identity_extent(extent: &ChunkExtent, page_bytes: u64) -> ObjectExtent {
    ObjectExtent {
        group: extent.group.clone(),
        volume_id: extent.volume_id,
        chunk_id: extent.chunk_id,
        page_index: if page_bytes == 0 { 0 } else { extent.chunk_offset / page_bytes },
        object_offset: extent.object_offset,
        page_offset: 0,
        length: extent.length,
        write_sequence: 1,
        data_checksum: 0,
        data_sha256: extent.digest.clone(),
        ..Default::default()
    }
}

/// A chunk manifest as the gateway sees it.
fn head(manifest: &ChunkManifest) -> ObjectManifest {
    ObjectManifest {
        format_version: 2,
        bucket: manifest.bucket.clone(),
        key: manifest.key.clone(),
        generation: manifest.generation,
        length: manifest.length,
        extents: manifest.extents.iter().map(|extent| identity_extent(extent, manifest.page_bytes)).collect(),
        deleted: manifest.deleted,
    }
}

impl ChunkPlane {
    pub fn new(client: RemoteMetaClient, options: GatewayOptions) -> Self {
        Self { client, options }
    }

    fn replica_options(&self) -> ReplicaClientOptions {
        let mut replica = self.options.replica.clone();
        replica.deadline = Some(Instant::now() + self.options.operation_timeout);
        replica
    }

    fn write_options(&self) -> ObjectWriteOptions {
        let mut options = self.options.write.clone();
        options.max_object_bytes = self.options.max_object_bytes;
        options.replica = self.replica_options();
        options
    }

    /// The manifest the catalog holds now, or `None` for a key that was never published.
    fn current(&mut self, bucket: &str, key: &str) -> Result<Option<ChunkManifest>, ObjectError> {
        match self.client.read_chunk_manifest(bucket, key) {
            Ok(manifest) => Ok(Some(manifest)),
            Err(ObjectError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn live(&mut self, bucket: &str, key: &str) -> Result<ChunkManifest, ObjectError> {
        match self.current(bucket, key)? {
            Some(manifest) if !manifest.deleted => Ok(manifest),
            _ => Err(ObjectError::NotFound(format!("{bucket}/{key} does not exist"))),
        }
    }
}

/// `kept` is how many extents the attempt started from: the ones after it are the attempt's own.
fn failure(failure: ChunkWriteFailure, kept: usize) -> PlaneFailure {
    match failure {
        ChunkWriteFailure::BeforePublication(error) => PlaneFailure { error, unconfirmed: false, attempted: Vec::new() },
        ChunkWriteFailure::PublicationUnconfirmed { error, outcome } => {
            let attempted = outcome
                .after()
                .map(|after| after.extents.iter().skip(kept).map(|extent| identity_extent(extent, after.page_bytes)).collect())
                .unwrap_or_default();
            PlaneFailure { error, unconfirmed: true, attempted }
        }
    }
}

impl ObjectPlane for ChunkPlane {
    fn read_manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        self.client.read_chunk_manifest(bucket, key).map(|manifest| head(&manifest))
    }

    fn write(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        let current = self.current(bucket, key)?;
        // The caller decided from the manifest it read. If the catalog holds another generation the
        // object moved since, and nothing is written.
        if expected.as_ref().map(|manifest| manifest.generation) != current.as_ref().map(|manifest| manifest.generation) {
            return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
        }
        let options = self.write_options();
        self.client
            .write_chunk_object(bucket, key, current.as_ref(), data, &options, DEFAULT_STRIPE_BYTES)
            .map(|manifest| head(&manifest))
            .map_err(|error| failure(error, usize::MAX))
    }

    fn append(&mut self, expected: ObjectManifest, data: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
        let current = self.live(&expected.bucket, &expected.key)?;
        // The caller decided from the manifest it read; another generation means the object moved.
        if current.generation != expected.generation || current.length != expected.length {
            return Err(ObjectError::PreconditionFailed("object manifest changed before append".into()).into());
        }
        let options = self.write_options();
        let kept = current.extents.len();
        self.client
            .append_chunk_object(&current, data, &options, DEFAULT_STRIPE_BYTES)
            .map(|manifest| head(&manifest))
            .map_err(|error| failure(error, kept))
    }

    fn publish_unchanged(&mut self, expected: &ObjectManifest) -> Result<ObjectManifest, PlaneFailure> {
        // a tombstone is fenced too: the resolve of an append to a deleted object publishes it again
        let current = self
            .current(&expected.bucket, &expected.key)?
            .ok_or_else(|| ObjectError::NotFound(format!("{}/{} does not exist", expected.bucket, expected.key)))?;
        if current.generation != expected.generation {
            return Err(ObjectError::PreconditionFailed("object manifest changed before the fence".into()).into());
        }
        self.client
            .republish_chunk_manifest(&current)
            .map(|manifest| head(&manifest))
            .map_err(|error| failure(error, current.extents.len()))
    }

    fn read(&mut self, bucket: &str, key: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        let manifest = self.live(bucket, key)?;
        if manifest.length > self.options.max_object_bytes as u64 {
            return Err(ObjectError::InvalidArgument("object exceeds the gateway's size limit".into()));
        }
        let options = self.write_options();
        let bytes = self.client.read_chunk_object_range(&manifest, 0, manifest.length, &options)?;
        Ok((head(&manifest), bytes))
    }

    fn read_range(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        let manifest = self.live(bucket, key)?;
        if length > self.options.max_object_bytes as u64 {
            return Err(ObjectError::InvalidArgument("range exceeds the gateway's size limit".into()));
        }
        let options = self.write_options();
        let bytes = self.client.read_chunk_object_range(&manifest, offset, length, &options)?;
        Ok((head(&manifest), bytes))
    }

    fn delete(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        self.client
            .delete_chunk_object(bucket, key)
            .map(|manifest| head(&manifest))
            .map_err(ChunkWriteFailure::into_error)
    }

    fn list(&mut self, bucket: &str, after: Option<&str>, limit: u32) -> Result<ManifestListPage, ObjectError> {
        self.client.list_chunk_manifests(ManifestListRequest {
            version: 1,
            bucket: bucket.to_string(),
            after: after.unwrap_or_default().to_string(),
            limit,
            expected_revision: Vec::new(),
            after_bucket: String::new(),
        })
    }
}

impl ClusterGateway {
    /// The constructor of the retired page plane, kept so a caller written against it still builds: the
    /// gateway it returns stores objects as chunk manifests, exactly as [`Self::over_chunk_objects`] does.
    #[deprecated(note = "the gateway stores chunk objects only; use over_chunk_objects")]
    pub fn over_sdk<F>(open: F, options: GatewayOptions, bucket_hint: impl Into<String>) -> Self
    where
        F: Fn() -> Result<RemoteMetaClient, ObjectError> + Send + Sync + 'static,
    {
        Self::over_chunk_objects(open, options, bucket_hint)
    }

    /// The constructor of the retired compact page plane. Chunk objects need no publication fence, so this
    /// only checks that a connection can be opened and returns the same gateway as [`Self::over_chunk_objects`].
    #[deprecated(note = "the gateway stores chunk objects only; use over_chunk_objects")]
    pub fn over_sdk_with_publication_fence<F>(
        open: F,
        options: GatewayOptions,
        bucket_hint: impl Into<String>,
    ) -> Result<Self, ObjectError>
    where
        F: Fn() -> Result<RemoteMetaClient, ObjectError> + Send + Sync + 'static,
    {
        open()?;
        Ok(Self::over_chunk_objects(open, options, bucket_hint))
    }
}

impl ClusterGateway {
    /// A gateway whose connections are metadata clients opened with `open`, storing objects as chunk
    /// manifests. The default `copy` of [`ObjectPlane`] reads and writes the bytes, which is the copy
    /// a chunk object has (no extent is shared, so no page is ever owned by two objects).
    pub fn over_chunk_objects<F>(open: F, options: GatewayOptions, bucket_hint: impl Into<String>) -> Self
    where
        F: Fn() -> Result<RemoteMetaClient, ObjectError> + Send + Sync + 'static,
    {
        let plane_options = options.clone();
        Self::new(
            move || Ok(Box::new(ChunkPlane::new(open()?, plane_options.clone())) as Box<dyn ObjectPlane>),
            options,
            bucket_hint,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_manifest::{CHUNK_FLAG_SEALED, CHUNK_KIND_PAGED};
    use crate::chunk_object::build_manifest;

    fn extent(chunk_id: u64, object_offset: u64, length: u64, digest_byte: u8) -> ChunkExtent {
        ChunkExtent {
            group: "group-a".into(),
            volume_id: 0,
            chunk_id,
            kind: CHUNK_KIND_PAGED,
            object_offset,
            chunk_offset: 3 * 4096,
            length,
            flags: CHUNK_FLAG_SEALED,
            digest: vec![digest_byte; 32],
            footer_crc: 0,
            footer_sha256: Vec::new(),
        }
    }

    #[test]
    fn a_chunk_manifest_is_shown_with_one_identity_per_extent_and_two_writes_never_share_one() {
        let manifest = build_manifest("b", "k", 4, 4096, vec![extent(7, 0, 100, 1), extent(8, 100, 50, 2)]).unwrap();
        let shown = head(&manifest);
        assert_eq!((shown.generation, shown.length, shown.deleted, shown.extents.len()), (4, 150, false, 2));
        assert_eq!(shown.extents[0].page_index, 3, "the first page of the run");
        assert_eq!((shown.extents[1].object_offset, shown.extents[1].length), (100, 50));
        // the same extent is the same identity wherever it is read from, which is what the resolve relies on
        assert_eq!(shown.extents[1], identity_extent(&manifest.extents[1], 4096));
        // an append that adds an extent leaves the earlier identities as they were
        let grown = build_manifest("b", "k", 5, 4096, vec![extent(7, 0, 100, 1), extent(8, 100, 50, 2), extent(9, 150, 10, 3)]).unwrap();
        assert_eq!(head(&grown).extents[..2], shown.extents[..]);
        // two writes of the same bytes at the same offset allocate their own pages, so they differ
        let twin = identity_extent(&extent(9, 150, 10, 3), 4096);
        let other = identity_extent(&extent(10, 150, 10, 3), 4096);
        assert_ne!(twin, other);
        // and a deleted object shows none
        let tombstone = ChunkManifest { format_version: 1, bucket: "b".into(), key: "k".into(), generation: 6, deleted: true, ..Default::default() };
        assert!(head(&tombstone).extents.is_empty() && head(&tombstone).deleted);
    }
}
