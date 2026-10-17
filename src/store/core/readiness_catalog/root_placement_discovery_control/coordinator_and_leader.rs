impl MatrixObjectStore {
    pub fn root_coordinator_config(&self) -> MetadataCoordinatorConfig {
        self.meta_service.placement_coordinator_config()
    }

    pub fn root_coordinator_policy_control_fields(
        &self,
        field_prefix: &str,
    ) -> BTreeMap<String, String> {
        self.root_coordinator_config()
            .to_control_fields(field_prefix)
    }

    pub fn configure_root_coordinator_config(
        &mut self,
        config: MetadataCoordinatorConfig,
    ) -> Result<MetadataCoordinatorConfig, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let config = self.meta_service.configure_placement_coordinator_config(config)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_root_coordinator_config_journal_record(&config) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(config)
    }

    pub fn campaign_root_leader(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<RootLeaderLease, ObjectError> {
        self.meta_service.campaign_leader(node_id, ttl_ms, now_ms)
    }

    pub fn campaign_root_leader_idempotent(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(RootLeaderLease, RootLeaderLeaseReceipt), ObjectError> {
        self.meta_service
            .campaign_leader_idempotent(node_id, ttl_ms, now_ms, idempotency_key)
    }

    pub fn renew_root_leader(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<RootLeaderLease, ObjectError> {
        self.meta_service
            .renew_leader(node_id, epoch, ttl_ms, now_ms)
    }

    pub fn renew_root_leader_idempotent(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(RootLeaderLease, RootLeaderLeaseReceipt), ObjectError> {
        self.meta_service
            .renew_leader_idempotent(node_id, epoch, ttl_ms, now_ms, idempotency_key)
    }

    pub fn root_leader_lease(&self) -> Option<RootLeaderLease> {
        self.meta_service.leader_lease()
    }

    pub fn list_root_leader_lease_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        leader_node_id: Option<&str>,
    ) -> Result<RootLeaderLeaseReceiptPage, ObjectError> {
        self.meta_service
            .list_leader_lease_receipts(marker, limit, operation, leader_node_id)
    }

}
