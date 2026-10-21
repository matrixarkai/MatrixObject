// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn can_accept_object_delete_discard_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        self.can_accept_object_delete_discard_linearizable_fenced(
            bucket,
            key,
            max_discard_bytes,
            now_ms,
            "",
        )
    }

    pub fn can_accept_object_delete_discard_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "object delete discard admission",
            expected_page_route_control_checksum,
        )?;
        self.store
            .can_accept_object_delete_discard_with_metadata_read(
                bucket,
                key,
                max_discard_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
            .map(|(mut admission, readiness)| {
                admission.expected_route_control_checksum =
                    expected_page_route_control_checksum.to_string();
                stamp_object_range_admission_proof("CanDiscardObject", &mut admission);
                (admission, readiness)
            })
    }

    pub fn can_accept_object_delete_discard_linearizable_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.as_ref();
        let (admission, readiness) = self.can_accept_object_delete_discard_linearizable_fenced(
            bucket,
            key,
            max_discard_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        check_expected_object_storage_admission_proof_checksum(
            "object delete discard admission",
            &admission,
            Some(expected_storage_admission_proof_checksum),
        )?;
        Ok((admission, readiness))
    }

    pub fn can_accept_object_delete_discard_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        self.can_accept_object_delete_discard_from_meta_shard_fenced(
            bucket,
            key,
            max_discard_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            "",
        )
    }

    pub fn can_accept_object_delete_discard_from_meta_shard_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "object delete discard admission from meta shard",
            expected_page_route_control_checksum,
        )?;
        self.store
            .can_accept_object_delete_discard_with_metadata_read(
                bucket,
                key,
                max_discard_bytes,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
            .map(|(mut admission, readiness)| {
                admission.expected_route_control_checksum =
                    expected_page_route_control_checksum.to_string();
                stamp_object_range_admission_proof("CanDiscardObject", &mut admission);
                (admission, readiness)
            })
    }

    pub fn can_accept_object_delete_discard_from_meta_shard_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.as_ref();
        let (admission, readiness) = self.can_accept_object_delete_discard_from_meta_shard_fenced(
            bucket,
            key,
            max_discard_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        check_expected_object_storage_admission_proof_checksum(
            "object delete discard admission from meta shard",
            &admission,
            Some(expected_storage_admission_proof_checksum),
        )?;
        Ok((admission, readiness))
    }

    pub fn can_accept_object_delete_discard_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectRangeAdmission>, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (admission, readiness) = self.can_accept_object_delete_discard_linearizable_fenced(
            bucket,
            key,
            max_discard_bytes,
            now_ms,
            &route_checksum,
        )?;
        Ok(proven_metadata_read(admission, &readiness))
    }

    pub fn can_accept_object_delete_discard_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectRangeAdmission>, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (admission, readiness) = self.can_accept_object_delete_discard_from_meta_shard_fenced(
            bucket,
            key,
            max_discard_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        Ok(proven_metadata_read(admission, &readiness))
    }

}
