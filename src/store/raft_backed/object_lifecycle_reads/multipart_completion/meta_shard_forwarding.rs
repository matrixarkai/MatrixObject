// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_complete_multipart_upload_from_meta_shard(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart complete from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_complete_multipart_upload_with_checksum_route_report_linearizable_fenced(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_complete_multipart_upload_with_read_proof_from_meta_shard(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartCompleteReport>, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_complete_multipart_upload_with_checksum_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
