// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_objects_by_tag_linearizable(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        self.store.list_objects_by_tag_with_metadata_read(
            bucket,
            tag_key,
            tag_value,
            marker,
            limit,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn list_objects_by_tag_with_read_proof_linearizable(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<ObjectMetadata>>, ObjectError> {
        let (objects, readiness) = self
            .list_objects_by_tag_linearizable(bucket, tag_key, tag_value, marker, limit, now_ms)?;
        Ok(proven_metadata_read(objects, &readiness))
    }

    pub fn list_objects_by_tag_page_linearizable(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectTagPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_objects_by_tag_page_with_metadata_read(
            bucket,
            tag_key,
            tag_value,
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

    pub fn list_objects_by_tag_page_linearizable_list_page_proof_fenced(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectTagPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_objects_by_tag_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                tag_key,
                tag_value,
                marker,
                limit,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
                Some(expected_object_list_page_proof_checksum.as_ref()),
            )
    }

    pub fn list_objects_by_tag_page_with_read_proof_linearizable(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectTagPage, ObjectError> {
        let (page, readiness) = self.list_objects_by_tag_page_linearizable(
            bucket,
            tag_key,
            tag_value,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenObjectTagPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_objects_by_tag_from_meta_shard(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        self.store.list_objects_by_tag_with_metadata_read(
            bucket,
            tag_key,
            tag_value,
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

    pub fn list_objects_by_tag_page_from_meta_shard(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ObjectTagPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_objects_by_tag_page_with_metadata_read(
            bucket,
            tag_key,
            tag_value,
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

    pub fn list_objects_by_tag_page_list_page_proof_fenced_from_meta_shard(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectTagPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_objects_by_tag_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                tag_key,
                tag_value,
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
                Some(expected_object_list_page_proof_checksum.as_ref()),
            )
    }

    pub fn list_objects_by_tag_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<ObjectMetadata>>, ObjectError> {
        let (objects, readiness) = self.list_objects_by_tag_from_meta_shard(
            bucket,
            tag_key,
            tag_value,
            marker,
            limit,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(objects, &readiness))
    }

    pub fn list_objects_by_tag_page_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectTagPage, ObjectError> {
        let (page, readiness) = self.list_objects_by_tag_page_from_meta_shard(
            bucket,
            tag_key,
            tag_value,
            marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenObjectTagPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
