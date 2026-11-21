// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The gateway's view of the cluster when objects are stored as chunk manifests.
//!
//! The gateway reasons about an object through three facts: its generation (the ETag and every
//! precondition), its length, and whether it is deleted. [`ChunkPlane`] answers the gateway's
//! [`ObjectPlane`] with exactly those, carried in the manifest type the gateway already holds, so no
//! handler changes: a chunk manifest is shown as a manifest with no extents. What an extent list
//! was used for (choosing an append route, bounding a log's extent count) is not asked of this plane:
//! an append is refused here and is served by the log writer once that lands.
//!
//! Bytes move through `RemoteMetaClient::write_chunk_object`, `read_chunk_object_range` and
//! `delete_chunk_object`, which verify every page they read against the manifest's digests.

use crate::chunk_manifest::ChunkManifest;
use crate::chunk_object::DEFAULT_STRIPE_BYTES;
use crate::cluster::{ChunkWriteFailure, ObjectWriteOptions, RemoteMetaClient, ReplicaClientOptions};
use crate::object_manifest::{ManifestListPage, ManifestListRequest, ObjectManifest};
use crate::ObjectError;
use std::time::Instant;

use super::cluster_gateway::{ClusterGateway, GatewayOptions, ObjectPlane, PlaneFailure};

/// One connection's worth of cluster operations over chunk manifests.
pub struct ChunkPlane {
    client: RemoteMetaClient,
    options: GatewayOptions,
}

/// A chunk manifest as the gateway sees it: generation, length and deletion, with no extents.
fn head(manifest: &ChunkManifest) -> ObjectManifest {
    ObjectManifest {
        format_version: 2,
        bucket: manifest.bucket.clone(),
        key: manifest.key.clone(),
        generation: manifest.generation,
        length: manifest.length,
        extents: Vec::new(),
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

fn failure(failure: ChunkWriteFailure) -> PlaneFailure {
    match failure {
        ChunkWriteFailure::BeforePublication(error) => PlaneFailure { error, unconfirmed: false, attempted: Vec::new() },
        ChunkWriteFailure::PublicationUnconfirmed { error, .. } => PlaneFailure { error, unconfirmed: true, attempted: Vec::new() },
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
            .map_err(failure)
    }

    fn append(&mut self, _expected: ObjectManifest, _data: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
        Err(ObjectError::InvalidArgument(
            "an append to a chunk object is served by the log writer, which this plane does not have yet".into(),
        )
        .into())
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
