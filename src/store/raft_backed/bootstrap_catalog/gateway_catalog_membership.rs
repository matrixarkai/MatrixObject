// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        self.store.clone().apply_command(command.clone())?;
        let entry = self.raft.append(command)?;
        self.store.apply_raft_entry_owned(entry)
    }

    pub fn propose_register_root_node(
        &mut self,
        node: RootNode,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RegisterRootNode { node })
    }

    pub fn propose_heartbeat_root_node(
        &mut self,
        node_id: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::HeartbeatRootNode {
            node_id: node_id.to_string(),
            used_bytes,
            capacity_bytes,
            draining,
            online,
            now_ms,
        })
    }

    pub fn propose_heartbeat_root_page_health(
        &mut self,
        node_id: &str,
        health: RootPageHealth,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::HeartbeatRootPageHealth {
            node_id: node_id.to_string(),
            health,
            now_ms,
        })
    }

    pub fn propose_heartbeat_root_chunk_health(
        &mut self,
        node_id: &str,
        health: RootChunkHealth,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::HeartbeatRootChunkHealth {
            node_id: node_id.to_string(),
            health,
            now_ms,
        })
    }

    pub fn propose_register_local_objectstore_gateway_in_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<RootCatalogObjectStoreRegistration, ObjectError> {
        Ok(self
            .propose_register_local_objectstore_gateway_in_catalog_with_receipts(
                node_id,
                endpoint,
                failure_domain,
                capacity_bytes,
                used_bytes,
                draining,
                now_ms,
                catalog_source,
            )?
            .registration)
    }

    pub fn propose_register_local_objectstore_gateway_in_catalog_with_receipts(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<ProvenRootCatalogRegistration<RootCatalogObjectStoreRegistration>, ObjectError>
    {
        let mut checked = self.store.clone();
        let registration = checked.register_local_objectstore_gateway_in_catalog(
            node_id,
            endpoint,
            failure_domain,
            capacity_bytes,
            used_bytes,
            draining,
            now_ms,
            catalog_source,
        )?;
        let receipt = self.propose_register_root_node(registration.node.clone())?;
        Ok(ProvenRootCatalogRegistration {
            registration,
            raft_receipts: vec![receipt],
        })
    }

    pub fn propose_register_local_objectstore_gateway_at_catalog_with_receipts(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<ProvenRootCatalogRegistration<RootCatalogObjectStoreRegistration>, ObjectError>
    {
        let mut proven = self.propose_register_local_objectstore_gateway_in_catalog_with_receipts(
            node_id,
            endpoint,
            failure_domain,
            capacity_bytes,
            used_bytes,
            draining,
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

    pub fn propose_register_local_objectstore_gateway_at_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<RootCatalogObjectStoreRegistration, ObjectError> {
        Ok(self
            .propose_register_local_objectstore_gateway_at_catalog_with_receipts(
                node_id,
                endpoint,
                failure_domain,
                capacity_bytes,
                used_bytes,
                draining,
                now_ms,
                catalog_source,
                catalog_endpoint,
                catalog_endpoint_count,
            )?
            .registration)
    }

    pub fn propose_heartbeat_local_objectstore_gateway_in_catalog(
        &mut self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<RootCatalogObjectStoreRegistration, ObjectError> {
        Ok(self
            .propose_heartbeat_local_objectstore_gateway_in_catalog_with_receipts(
                node_id,
                capacity_bytes,
                used_bytes,
                draining,
                online,
                now_ms,
                catalog_source,
            )?
            .registration)
    }

    pub fn propose_heartbeat_local_objectstore_gateway_in_catalog_with_receipts(
        &mut self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<ProvenRootCatalogRegistration<RootCatalogObjectStoreRegistration>, ObjectError>
    {
        let mut checked = self.store.clone();
        let registration = checked.heartbeat_local_objectstore_gateway_in_catalog(
            node_id,
            capacity_bytes,
            used_bytes,
            draining,
            online,
            now_ms,
            catalog_source,
        )?;
        let receipt = self.propose_heartbeat_root_node(
            node_id,
            registration.node.used_bytes,
            registration.node.capacity_bytes,
            registration.node.draining,
            registration.node.online,
            now_ms,
        )?;
        Ok(ProvenRootCatalogRegistration {
            registration,
            raft_receipts: vec![receipt],
        })
    }

    pub fn propose_heartbeat_local_objectstore_gateway_at_catalog_with_receipts(
        &mut self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<ProvenRootCatalogRegistration<RootCatalogObjectStoreRegistration>, ObjectError>
    {
        let mut proven = self
            .propose_heartbeat_local_objectstore_gateway_in_catalog_with_receipts(
                node_id,
                capacity_bytes,
                used_bytes,
                draining,
                online,
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

    pub fn propose_heartbeat_local_objectstore_gateway_at_catalog(
        &mut self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<RootCatalogObjectStoreRegistration, ObjectError> {
        Ok(self
            .propose_heartbeat_local_objectstore_gateway_at_catalog_with_receipts(
                node_id,
                capacity_bytes,
                used_bytes,
                draining,
                online,
                now_ms,
                catalog_source,
                catalog_endpoint,
                catalog_endpoint_count,
            )?
            .registration)
    }

    pub fn propose_offline_local_objectstore_gateway_in_catalog(
        &mut self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<RootCatalogObjectStoreRegistration, ObjectError> {
        Ok(self
            .propose_offline_local_objectstore_gateway_in_catalog_with_receipts(
                node_id,
                now_ms,
                catalog_source,
            )?
            .registration)
    }

    pub fn propose_offline_local_objectstore_gateway_in_catalog_with_receipts(
        &mut self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<ProvenRootCatalogRegistration<RootCatalogObjectStoreRegistration>, ObjectError>
    {
        let mut checked = self.store.clone();
        let registration = checked.offline_local_objectstore_gateway_in_catalog(
            node_id,
            now_ms,
            catalog_source,
        )?;
        let receipt = self.propose_heartbeat_root_node(
            node_id,
            registration.node.used_bytes,
            registration.node.capacity_bytes,
            registration.node.draining,
            registration.node.online,
            now_ms,
        )?;
        Ok(ProvenRootCatalogRegistration {
            registration,
            raft_receipts: vec![receipt],
        })
    }

    pub fn propose_offline_local_objectstore_gateway_at_catalog_with_receipts(
        &mut self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<ProvenRootCatalogRegistration<RootCatalogObjectStoreRegistration>, ObjectError>
    {
        let mut proven = self.propose_offline_local_objectstore_gateway_in_catalog_with_receipts(
            node_id,
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

    pub fn propose_offline_local_objectstore_gateway_at_catalog(
        &mut self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<RootCatalogObjectStoreRegistration, ObjectError> {
        Ok(self
            .propose_offline_local_objectstore_gateway_at_catalog_with_receipts(
                node_id,
                now_ms,
                catalog_source,
                catalog_endpoint,
                catalog_endpoint_count,
            )?
            .registration)
    }
}
