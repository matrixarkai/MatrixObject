// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_meta_shard(
        &mut self,
        shard: MetadataShard,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureMetaShard { shard })
    }

    pub fn propose_configure_meta_shard_idempotent(
        &mut self,
        shard: MetadataShard,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureMetaShardIdempotent {
            shard,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_split_meta_shard(
        &mut self,
        source_shard_id: &str,
        split_key: &str,
        left_shard_id: &str,
        right_shard_id: &str,
        left_owner_node_id: &str,
        right_owner_node_id: &str,
        epoch: u64,
        left_draining: bool,
        right_draining: bool,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SplitMetaShard {
            source_shard_id: source_shard_id.to_string(),
            split_key: split_key.to_string(),
            left_shard_id: left_shard_id.to_string(),
            right_shard_id: right_shard_id.to_string(),
            left_owner_node_id: left_owner_node_id.to_string(),
            right_owner_node_id: right_owner_node_id.to_string(),
            epoch,
            left_draining,
            right_draining,
            now_ms,
        })
    }

    pub fn propose_split_meta_shard_idempotent(
        &mut self,
        source_shard_id: &str,
        split_key: &str,
        left_shard_id: &str,
        right_shard_id: &str,
        left_owner_node_id: &str,
        right_owner_node_id: &str,
        epoch: u64,
        left_draining: bool,
        right_draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SplitMetaShardIdempotent {
            source_shard_id: source_shard_id.to_string(),
            split_key: split_key.to_string(),
            left_shard_id: left_shard_id.to_string(),
            right_shard_id: right_shard_id.to_string(),
            left_owner_node_id: left_owner_node_id.to_string(),
            right_owner_node_id: right_owner_node_id.to_string(),
            epoch,
            left_draining,
            right_draining,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_merge_meta_shards(
        &mut self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::MergeMetaShards {
            left_shard_id: left_shard_id.to_string(),
            right_shard_id: right_shard_id.to_string(),
            merged_shard_id: merged_shard_id.to_string(),
            owner_node_id: owner_node_id.to_string(),
            epoch,
            draining,
            now_ms,
        })
    }

    pub fn propose_merge_meta_shards_idempotent(
        &mut self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::MergeMetaShardsIdempotent {
            left_shard_id: left_shard_id.to_string(),
            right_shard_id: right_shard_id.to_string(),
            merged_shard_id: merged_shard_id.to_string(),
            owner_node_id: owner_node_id.to_string(),
            epoch,
            draining,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_migrate_meta_shard(
        &mut self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::MigrateMetaShard {
            shard_id: shard_id.to_string(),
            new_owner_node_id: new_owner_node_id.to_string(),
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
        })
    }

    pub fn propose_migrate_meta_shard_idempotent(
        &mut self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::MigrateMetaShardIdempotent {
            shard_id: shard_id.to_string(),
            new_owner_node_id: new_owner_node_id.to_string(),
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_failover_meta_shards(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::FailoverMetaShards {
            failed_owner_node_id: failed_owner_node_id.to_string(),
            replacement_owner_node_id: replacement_owner_node_id.to_string(),
            max_shards,
            now_ms,
            expected_plan_checksum: String::new(),
            expected_shard_topology_epoch: None,
            expected_shard_topology_checksum: String::new(),
            expected_coordinator_session_checksum: String::new(),
        })
    }

    pub fn propose_failover_meta_shards_fenced(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::FailoverMetaShards {
            failed_owner_node_id: failed_owner_node_id.to_string(),
            replacement_owner_node_id: replacement_owner_node_id.to_string(),
            max_shards,
            now_ms,
            expected_plan_checksum: expected_plan_checksum.to_string(),
            expected_shard_topology_epoch,
            expected_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            expected_coordinator_session_checksum: expected_coordinator_session_checksum
                .unwrap_or_default()
                .to_string(),
        })
    }

    pub fn propose_expired_meta_shard_owner_failover(
        &mut self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ExpiredMetaShardOwnerFailover {
            expired_owner_node_id: expired_owner_node_id.to_string(),
            replacement_owner_node_id: replacement_owner_node_id.to_string(),
            max_shards,
            now_ms,
            expected_plan_checksum: String::new(),
            expected_shard_topology_epoch: None,
            expected_shard_topology_checksum: String::new(),
            expected_coordinator_session_checksum: String::new(),
        })
    }

    pub fn propose_expired_meta_shard_owner_failover_fenced(
        &mut self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ExpiredMetaShardOwnerFailover {
            expired_owner_node_id: expired_owner_node_id.to_string(),
            replacement_owner_node_id: replacement_owner_node_id.to_string(),
            max_shards,
            now_ms,
            expected_plan_checksum: expected_plan_checksum.to_string(),
            expected_shard_topology_epoch,
            expected_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            expected_coordinator_session_checksum: expected_coordinator_session_checksum
                .unwrap_or_default()
                .to_string(),
        })
    }

    pub fn propose_rebalance_meta_shards(
        &mut self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RebalanceMetaShards {
            source_owner_node_id: source_owner_node_id.to_string(),
            target_owner_node_id: target_owner_node_id.to_string(),
            max_shards,
            now_ms,
            expected_plan_checksum: String::new(),
            expected_shard_topology_epoch: None,
            expected_shard_topology_checksum: String::new(),
            expected_coordinator_session_checksum: String::new(),
        })
    }

    pub fn propose_rebalance_meta_shards_fenced(
        &mut self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RebalanceMetaShards {
            source_owner_node_id: source_owner_node_id.to_string(),
            target_owner_node_id: target_owner_node_id.to_string(),
            max_shards,
            now_ms,
            expected_plan_checksum: expected_plan_checksum.to_string(),
            expected_shard_topology_epoch,
            expected_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            expected_coordinator_session_checksum: expected_coordinator_session_checksum
                .unwrap_or_default()
                .to_string(),
        })
    }

}
