// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_multipart_parts_linearizable(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        now_ms: u64,
    ) -> Result<(Vec<MultipartPart>, MetadataRaftReadiness), ObjectError> {
        self.store.list_multipart_parts_with_metadata_read(
            upload_id,
            marker,
            limit,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn list_multipart_parts_from_meta_shard(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(Vec<MultipartPart>, MetadataRaftReadiness), ObjectError> {
        self.store.list_multipart_parts_with_metadata_read(
            upload_id,
            marker,
            limit,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn list_multipart_parts_page_linearizable(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MultipartPartPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_multipart_parts_page_with_metadata_read(
            upload_id,
            marker,
            limit,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn list_multipart_parts_page_from_meta_shard(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MultipartPartPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_multipart_parts_page_with_metadata_read(
            upload_id,
            marker,
            limit,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn list_multipart_parts_with_read_proof_linearizable(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<MultipartPart>>, ObjectError> {
        let (parts, readiness) =
            self.list_multipart_parts_linearizable(upload_id, marker, limit, now_ms)?;
        Ok(proven_metadata_read(parts, &readiness))
    }

    pub fn list_multipart_parts_page_with_read_proof_linearizable(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartPartPage, ObjectError> {
        let (page, readiness) = self.list_multipart_parts_page_linearizable(
            upload_id,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenMultipartPartPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_multipart_parts_with_read_proof_from_meta_shard(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<MultipartPart>>, ObjectError> {
        let (parts, readiness) = self.list_multipart_parts_from_meta_shard(
            upload_id,
            marker,
            limit,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(parts, &readiness))
    }

    pub fn list_multipart_parts_page_with_read_proof_from_meta_shard(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartPartPage, ObjectError> {
        let (page, readiness) = self.list_multipart_parts_page_from_meta_shard(
            upload_id,
            marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenMultipartPartPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
