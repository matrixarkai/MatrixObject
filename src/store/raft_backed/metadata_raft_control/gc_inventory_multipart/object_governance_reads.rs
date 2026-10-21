// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_tags_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(BTreeMap<String, String>, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_tags_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn get_object_tags_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BTreeMap<String, String>>, ObjectError> {
        let (tags, readiness) = self.get_object_tags_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(tags, &readiness))
    }

    pub fn get_object_legal_hold_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(bool, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_legal_hold_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn get_object_legal_hold_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<bool>, ObjectError> {
        let (enabled, readiness) = self.get_object_legal_hold_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(enabled, &readiness))
    }

    pub fn get_object_retention_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(u64, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_retention_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn get_object_retention_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        let (retention, readiness) = self.get_object_retention_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(retention, &readiness))
    }
}
