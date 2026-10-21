// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn metadata_record_linearizable(
        &self,
        key: &str,
        now_ms: u64,
    ) -> Result<(MetadataRecord, MetadataRaftReadiness), ObjectError> {
        self.store.metadata_record_with_metadata_read(
            key,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn metadata_record_from_meta_shard(
        &self,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MetadataRecord, MetadataRaftReadiness), ObjectError> {
        self.store.metadata_record_with_metadata_read(
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

    pub fn metadata_records_batch_linearizable(
        &self,
        keys: Vec<String>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MetadataRecordBatchRead, MetadataRaftReadiness), ObjectError> {
        self.store.metadata_records_batch_with_metadata_read(
            keys,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn metadata_records_batch_from_meta_shard(
        &self,
        keys: Vec<String>,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MetadataRecordBatchRead, MetadataRaftReadiness), ObjectError> {
        self.store.metadata_records_batch_with_metadata_read(
            keys,
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

    pub fn scan_metadata_records_linearizable(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MetadataRecordPage, MetadataRaftReadiness), ObjectError> {
        self.store.scan_metadata_records_with_metadata_read(
            prefix,
            after_key,
            max_records,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn scan_metadata_records_from_meta_shard(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MetadataRecordPage, MetadataRaftReadiness), ObjectError> {
        self.store.scan_metadata_records_with_metadata_read(
            prefix,
            after_key,
            max_records,
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

    pub fn check_metadata_read_set_linearizable(
        &self,
        conditions: Vec<MetadataReadCondition>,
        now_ms: u64,
    ) -> Result<(MetadataReadSetReport, MetadataRaftReadiness), ObjectError> {
        self.store.check_metadata_read_set_with_metadata_read(
            conditions,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn check_metadata_read_set_from_meta_shard(
        &self,
        conditions: Vec<MetadataReadCondition>,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MetadataReadSetReport, MetadataRaftReadiness), ObjectError> {
        self.store.check_metadata_read_set_with_metadata_read(
            conditions,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

}
