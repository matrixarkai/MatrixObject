// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_delete_objects_by_prefix(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_delete_objects_by_prefix_fenced(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            "",
        )
    }

    pub fn propose_delete_objects_by_prefix_fenced(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_delete_objects_by_prefix_fenced_with_delete_proof(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            "",
        )
    }

    pub fn propose_delete_objects_by_prefix_fenced_with_delete_proof(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_delete_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "prefix object delete",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::DeleteObjectsByPrefix {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_delete_batch_proof_checksum: if expected_delete_batch_proof_checksum
                .as_ref()
                .is_empty()
            {
                None
            } else {
                Some(expected_delete_batch_proof_checksum.as_ref().to_string())
            },
        })
    }
}
