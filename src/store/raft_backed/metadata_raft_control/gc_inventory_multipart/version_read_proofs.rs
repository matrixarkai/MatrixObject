// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_object_versions_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<ObjectMetadata>>, ObjectError> {
        let (versions, readiness) = self.list_object_versions_linearizable(bucket, key, now_ms)?;
        Ok(proven_metadata_read(versions, &readiness))
    }

    pub fn list_object_versions_page_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectVersionPage, ObjectError> {
        let (page, readiness) = self.list_object_versions_page_linearizable(
            bucket,
            key,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenObjectVersionPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_bucket_object_versions_page_with_read_proof_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenBucketObjectVersionPage, ObjectError> {
        let (page, readiness) = self.list_bucket_object_versions_page_linearizable(
            bucket,
            prefix,
            delimiter,
            key_marker,
            version_marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenBucketObjectVersionPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_object_versions_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<ObjectMetadata>>, ObjectError> {
        let (versions, readiness) = self.list_object_versions_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(versions, &readiness))
    }

    pub fn list_object_versions_page_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectVersionPage, ObjectError> {
        let (page, readiness) = self.list_object_versions_page_from_meta_shard(
            bucket,
            key,
            marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenObjectVersionPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_bucket_object_versions_page_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenBucketObjectVersionPage, ObjectError> {
        let (page, readiness) = self.list_bucket_object_versions_page_from_meta_shard(
            bucket,
            prefix,
            delimiter,
            key_marker,
            version_marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenBucketObjectVersionPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
