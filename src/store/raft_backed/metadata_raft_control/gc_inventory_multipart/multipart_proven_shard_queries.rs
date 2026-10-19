// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_multipart_uploads_page_by_key_with_read_proof_from_meta_shard(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartUploadPage, ObjectError> {
        self.list_multipart_uploads_hierarchy_page_by_key_with_read_proof_from_meta_shard(
            bucket,
            prefix,
            None,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    pub fn list_multipart_uploads_hierarchy_page_by_key_with_read_proof_from_meta_shard(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartUploadPage, ObjectError> {
        let (page, readiness) = self.list_multipart_uploads_hierarchy_page_by_key_from_meta_shard(
            bucket,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenMultipartUploadPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_multipart_uploads_with_read_proof_from_meta_shard(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<MultipartUpload>>, ObjectError> {
        let (uploads, readiness) = self.list_multipart_uploads_from_meta_shard(
            bucket,
            marker,
            limit,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(uploads, &readiness))
    }

    pub fn list_multipart_uploads_by_key_with_read_proof_from_meta_shard(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<MultipartUpload>>, ObjectError> {
        let (uploads, readiness) = self.list_multipart_uploads_by_key_from_meta_shard(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            limit,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(uploads, &readiness))
    }

    pub fn list_multipart_uploads_page_with_read_proof_from_meta_shard(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartUploadPage, ObjectError> {
        let (page, readiness) = self.list_multipart_uploads_page_from_meta_shard(
            bucket,
            marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenMultipartUploadPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
