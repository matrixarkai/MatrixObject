// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_meta_retention_prepared(
        &mut self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_meta_retention_raft_entry(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
        )?;
        self.propose_prepared_meta_retention_config(prepared)
    }

    pub fn propose_configure_meta_retention_prepared_linearizable(
        &mut self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_meta_retention_raft_entry_linearizable(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            now_ms,
        )?;
        self.propose_prepared_meta_retention_config_linearizable(prepared, now_ms)
    }

    pub fn propose_configure_meta_admission_policy_prepared(
        &mut self,
        policy: MetadataAdmissionPolicy,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_meta_admission_policy_raft_entry(policy)?;
        self.propose_prepared_meta_admission_policy(prepared)
    }

    pub fn propose_configure_meta_admission_policy_prepared_linearizable(
        &mut self,
        policy: MetadataAdmissionPolicy,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_configure_meta_admission_policy_raft_entry_linearizable(policy, now_ms)?;
        self.propose_prepared_meta_admission_policy_linearizable(prepared, now_ms)
    }

    pub fn propose_configure_meta_qos_prepared(
        &mut self,
        config: MetadataQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_meta_qos_raft_entry(config)?;
        self.propose_prepared_meta_qos_config(prepared)
    }

    pub fn propose_configure_meta_qos_prepared_linearizable(
        &mut self,
        config: MetadataQosConfig,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_meta_qos_raft_entry_linearizable(config, now_ms)?;
        self.propose_prepared_meta_qos_config_linearizable(prepared, now_ms)
    }

    pub fn propose_configure_meta_route_cache_prepared(
        &mut self,
        config: MetadataRouteCacheConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_meta_route_cache_raft_entry(config)?;
        self.propose_prepared_meta_route_cache_config(prepared)
    }

    pub fn propose_configure_meta_route_cache_prepared_linearizable(
        &mut self,
        config: MetadataRouteCacheConfig,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_configure_meta_route_cache_raft_entry_linearizable(config, now_ms)?;
        self.propose_prepared_meta_route_cache_config_linearizable(prepared, now_ms)
    }

}
