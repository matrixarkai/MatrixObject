// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_register_local_pageserver_in_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        Ok(self
            .propose_register_local_pageserver_in_catalog_with_receipts(
                node_id,
                endpoint,
                failure_domain,
                allocator_shard_index,
                allocator_shard_count,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                now_ms,
                catalog_source,
            )?
            .registration)
    }

    pub fn propose_register_local_pageserver_in_catalog_with_receipts(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<ProvenCatalogRegistration<CatalogPageRegistration>, ObjectError> {
        let mut checked = self.store.clone();
        let registration = checked.register_local_pageserver_in_catalog(
            node_id,
            endpoint,
            failure_domain,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        )?;
        let receipt = self.propose_register_placement_node(registration.node.clone())?;
        Ok(ProvenCatalogRegistration {
            registration,
            raft_receipts: vec![receipt],
        })
    }

    pub fn propose_register_local_pageserver_at_catalog_with_receipts(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<ProvenCatalogRegistration<CatalogPageRegistration>, ObjectError> {
        let mut proven = self.propose_register_local_pageserver_in_catalog_with_receipts(
            node_id,
            endpoint,
            failure_domain,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        )?;
        proven.registration = proven.registration.with_catalog_route(
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        Ok(proven)
    }

    pub fn propose_register_local_pageserver_at_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        Ok(self
            .propose_register_local_pageserver_at_catalog_with_receipts(
                node_id,
                endpoint,
                failure_domain,
                allocator_shard_index,
                allocator_shard_count,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                now_ms,
                catalog_source,
                catalog_endpoint,
                catalog_endpoint_count,
            )?
            .registration)
    }

    pub fn propose_heartbeat_local_pageserver_in_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        Ok(self
            .propose_heartbeat_local_pageserver_in_catalog_with_receipts(
                node_id,
                allocator_shard_index,
                allocator_shard_count,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                now_ms,
                catalog_source,
                online,
            )?
            .registration)
    }

    pub fn propose_heartbeat_local_pageserver_in_catalog_with_receipts(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
    ) -> Result<ProvenCatalogRegistration<CatalogPageRegistration>, ObjectError> {
        let mut checked = self.store.clone();
        let registration = checked.heartbeat_local_pageserver_in_catalog(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
            online,
        )?;
        let node_receipt = self.propose_heartbeat_placement_node(
            node_id,
            registration.node.used_bytes,
            registration.node.capacity_bytes,
            registration.node.draining,
            registration.node.online,
            now_ms,
        )?;
        let health_receipt = self.propose_heartbeat_placement_page_health(
            node_id,
            registration.node.page_health.clone(),
            now_ms,
        )?;
        Ok(ProvenCatalogRegistration {
            registration,
            raft_receipts: vec![node_receipt, health_receipt],
        })
    }

    pub fn propose_heartbeat_local_pageserver_at_catalog_with_receipts(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<ProvenCatalogRegistration<CatalogPageRegistration>, ObjectError> {
        let mut proven = self.propose_heartbeat_local_pageserver_in_catalog_with_receipts(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
            online,
        )?;
        proven.registration = proven.registration.with_catalog_route(
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        Ok(proven)
    }

    pub fn propose_heartbeat_local_pageserver_at_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        Ok(self
            .propose_heartbeat_local_pageserver_at_catalog_with_receipts(
                node_id,
                allocator_shard_index,
                allocator_shard_count,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                now_ms,
                catalog_source,
                online,
                catalog_endpoint,
                catalog_endpoint_count,
            )?
            .registration)
    }

    pub fn propose_offline_local_pageserver_in_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        Ok(self
            .propose_offline_local_pageserver_in_catalog_with_receipts(
                node_id,
                allocator_shard_index,
                allocator_shard_count,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                now_ms,
                catalog_source,
            )?
            .registration)
    }

    pub fn propose_offline_local_pageserver_in_catalog_with_receipts(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<ProvenCatalogRegistration<CatalogPageRegistration>, ObjectError> {
        let mut checked = self.store.clone();
        let registration = checked.offline_local_pageserver_in_catalog(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        )?;
        let node_receipt = self.propose_heartbeat_placement_node(
            node_id,
            registration.node.used_bytes,
            registration.node.capacity_bytes,
            registration.node.draining,
            registration.node.online,
            now_ms,
        )?;
        let health_receipt = self.propose_heartbeat_placement_page_health(
            node_id,
            registration.node.page_health.clone(),
            now_ms,
        )?;
        Ok(ProvenCatalogRegistration {
            registration,
            raft_receipts: vec![node_receipt, health_receipt],
        })
    }

    pub fn propose_offline_local_pageserver_at_catalog_with_receipts(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<ProvenCatalogRegistration<CatalogPageRegistration>, ObjectError> {
        let mut proven = self.propose_offline_local_pageserver_in_catalog_with_receipts(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        )?;
        proven.registration = proven.registration.with_catalog_route(
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        Ok(proven)
    }

    pub fn propose_offline_local_pageserver_at_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        Ok(self
            .propose_offline_local_pageserver_at_catalog_with_receipts(
                node_id,
                allocator_shard_index,
                allocator_shard_count,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                now_ms,
                catalog_source,
                catalog_endpoint,
                catalog_endpoint_count,
            )?
            .registration)
    }
}
