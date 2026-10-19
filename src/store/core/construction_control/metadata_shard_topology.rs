// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn configure_metadata_shard(&mut self, shard: MetadataShard) -> Result<(), ObjectError> {
        self.meta_service.configure_shard(shard)?;
        self.invalidate_metadata_route_cache();
        Ok(())
    }

    pub fn configure_metadata_shard_idempotent(
        &mut self,
        shard: MetadataShard,
        idempotency_key: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        let receipt = self
            .meta_service
            .configure_shard_idempotent(shard, idempotency_key)?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

    pub fn split_metadata_shard(
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
    ) -> Result<MetadataShardSplit, ObjectError> {
        let split = self.meta_service.split_shard(
            source_shard_id,
            split_key,
            left_shard_id,
            right_shard_id,
            left_owner_node_id,
            right_owner_node_id,
            epoch,
            left_draining,
            right_draining,
            now_ms,
        )?;
        self.invalidate_metadata_route_cache();
        Ok(split)
    }

    pub fn split_metadata_shard_idempotent(
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
    ) -> Result<MetadataShardReceipt, ObjectError> {
        let receipt = self.meta_service.split_shard_idempotent(
            source_shard_id,
            split_key,
            left_shard_id,
            right_shard_id,
            left_owner_node_id,
            right_owner_node_id,
            epoch,
            left_draining,
            right_draining,
            now_ms,
            idempotency_key,
        )?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

    pub fn merge_metadata_shards(
        &mut self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<MetadataShard, ObjectError> {
        let merged = self.meta_service.merge_shards(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
        )?;
        self.invalidate_metadata_route_cache();
        Ok(merged)
    }

    pub fn merge_metadata_shards_idempotent(
        &mut self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        let receipt = self.meta_service.merge_shards_idempotent(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
            idempotency_key,
        )?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

    pub fn migrate_metadata_shard(
        &mut self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<MetadataShard, ObjectError> {
        let shard = self.meta_service.migrate_shard(
            shard_id,
            new_owner_node_id,
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
        )?;
        self.invalidate_metadata_route_cache();
        Ok(shard)
    }

    pub fn migrate_metadata_shard_idempotent(
        &mut self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        let receipt = self.meta_service.migrate_shard_idempotent(
            shard_id,
            new_owner_node_id,
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
            idempotency_key,
        )?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

}
