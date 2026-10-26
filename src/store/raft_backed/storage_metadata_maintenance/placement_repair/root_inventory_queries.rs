// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn placement_nodes_linearizable(&self) -> Result<Vec<PlacementNode>, ObjectError> {
        self.ensure_placement_read_index("list root nodes")?;
        Ok(self.store.placement_nodes())
    }

    pub fn placement_leader_lease_linearizable(&self) -> Result<Option<PlacementLeaderLease>, ObjectError> {
        self.ensure_placement_read_index("root leader lease")?;
        Ok(self.store.root_leader_lease())
    }

    pub fn placement_leader_lease_with_read_proof_linearizable(
        &self,
    ) -> Result<ProvenPlacementRead<Option<PlacementLeaderLease>>, ObjectError> {
        let proof = self.ensure_placement_read_index("root leader lease")?;
        Ok(ProvenPlacementRead {
            value: self.store.root_leader_lease(),
            proof,
        })
    }

    pub fn list_placement_leader_lease_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        leader_node_id: Option<&str>,
    ) -> Result<PlacementLeaderLeaseReceiptPage, ObjectError> {
        self.ensure_placement_read_index("list root leader lease receipts")?;
        self.store
            .list_placement_leader_lease_receipts(marker, limit, operation, leader_node_id)
    }

    pub fn list_placement_leader_lease_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        leader_node_id: Option<&str>,
    ) -> Result<ProvenPlacementRead<PlacementLeaderLeaseReceiptPage>, ObjectError> {
        let proof = self.ensure_placement_read_index("list root leader lease receipts")?;
        let value =
            self.store
                .list_placement_leader_lease_receipts(marker, limit, operation, leader_node_id)?;
        Ok(ProvenPlacementRead { value, proof })
    }

    pub fn list_placement_leader_lease_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        leader_node_id: Option<&str>,
        expected_root_leader_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenPlacementRead<PlacementLeaderLeaseReceiptPage>, ObjectError> {
        let proven = self.list_placement_leader_lease_receipts_with_read_proof_linearizable(
            marker,
            limit,
            operation,
            leader_node_id,
        )?;
        proven.require_placement_leader_lease_receipt_page_proof_checksum(
            expected_root_leader_lease_receipt_page_proof_checksum,
        )
    }

    pub fn placement_nodes_with_read_proof_linearizable(
        &self,
    ) -> Result<ProvenPlacementRead<Vec<PlacementNode>>, ObjectError> {
        let proof = self.ensure_placement_read_index("list root nodes")?;
        Ok(ProvenPlacementRead {
            value: self.store.placement_nodes(),
            proof,
        })
    }

    pub fn list_placement_nodes_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        metadata_eligible_filter: Option<bool>,
        data_eligible_filter: Option<bool>,
        gateway_eligible_filter: Option<bool>,
    ) -> Result<PlacementNodePage, ObjectError> {
        self.ensure_placement_read_index("list root nodes page")?;
        self.store.list_placement_nodes(
            marker,
            limit,
            service_kind_filter,
            draining_filter,
            online_filter,
            metadata_eligible_filter,
            data_eligible_filter,
            gateway_eligible_filter,
        )
    }

    pub fn list_placement_nodes_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        metadata_eligible_filter: Option<bool>,
        data_eligible_filter: Option<bool>,
        gateway_eligible_filter: Option<bool>,
    ) -> Result<ProvenPlacementRead<PlacementNodePage>, ObjectError> {
        let proof = self.ensure_placement_read_index("list root nodes page")?;
        let value = self.store.list_placement_nodes(
            marker,
            limit,
            service_kind_filter,
            draining_filter,
            online_filter,
            metadata_eligible_filter,
            data_eligible_filter,
            gateway_eligible_filter,
        )?;
        Ok(ProvenPlacementRead { value, proof })
    }

    pub fn list_placement_node_usage_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        capacity_pressure_filter: Option<bool>,
        reclaim_pressure_filter: Option<bool>,
        placement_eligible_filter: Option<bool>,
        min_free_bytes: u64,
        min_reclaimable_bytes: u64,
    ) -> Result<PlacementNodeUsagePage, ObjectError> {
        self.ensure_placement_read_index("list root node usage")?;
        self.store.list_placement_node_usage(
            marker,
            limit,
            service_kind_filter,
            draining_filter,
            online_filter,
            capacity_pressure_filter,
            reclaim_pressure_filter,
            placement_eligible_filter,
            min_free_bytes,
            min_reclaimable_bytes,
        )
    }

    pub fn list_placement_node_usage_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        capacity_pressure_filter: Option<bool>,
        reclaim_pressure_filter: Option<bool>,
        placement_eligible_filter: Option<bool>,
        min_free_bytes: u64,
        min_reclaimable_bytes: u64,
    ) -> Result<ProvenPlacementRead<PlacementNodeUsagePage>, ObjectError> {
        let proof = self.ensure_placement_read_index("list root node usage")?;
        let value = self.store.list_placement_node_usage(
            marker,
            limit,
            service_kind_filter,
            draining_filter,
            online_filter,
            capacity_pressure_filter,
            reclaim_pressure_filter,
            placement_eligible_filter,
            min_free_bytes,
            min_reclaimable_bytes,
        )?;
        Ok(ProvenPlacementRead { value, proof })
    }

    pub fn plan_placement_reclaim_nodes_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        only_online: bool,
        include_draining: bool,
        min_reclaimable_bytes: u64,
        min_overcommitted_bytes: u64,
        min_free_bytes: u64,
        min_page_physical_discard_bytes: u64,
        min_page_physical_discard_ranges: u64,
    ) -> Result<PlacementReclaimNodePlanPage, ObjectError> {
        self.ensure_placement_read_index("plan root reclaim nodes")?;
        self.store.plan_placement_reclaim_nodes(
            marker,
            limit,
            service_kind_filter,
            only_online,
            include_draining,
            min_reclaimable_bytes,
            min_overcommitted_bytes,
            min_free_bytes,
            min_page_physical_discard_bytes,
            min_page_physical_discard_ranges,
        )
    }

    pub fn plan_placement_reclaim_nodes_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        only_online: bool,
        include_draining: bool,
        min_reclaimable_bytes: u64,
        min_overcommitted_bytes: u64,
        min_free_bytes: u64,
        min_page_physical_discard_bytes: u64,
        min_page_physical_discard_ranges: u64,
    ) -> Result<ProvenPlacementRead<PlacementReclaimNodePlanPage>, ObjectError> {
        let proof = self.ensure_placement_read_index("plan root reclaim nodes")?;
        let value = self.store.plan_placement_reclaim_nodes(
            marker,
            limit,
            service_kind_filter,
            only_online,
            include_draining,
            min_reclaimable_bytes,
            min_overcommitted_bytes,
            min_free_bytes,
            min_page_physical_discard_bytes,
            min_page_physical_discard_ranges,
        )?;
        Ok(ProvenPlacementRead { value, proof })
    }
}
