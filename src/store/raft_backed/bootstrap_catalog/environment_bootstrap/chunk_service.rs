impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn bootstrap_replica_service_from_env_report(
        &mut self,
        now_ms: u64,
    ) -> Result<ReplicaServiceBootstrapReport, ObjectError> {
        let replica_index_u64 =
            first_non_empty_env_u64(&MATRIX_REPLICASERVER_REPLICA_INDEX_ENV_NAMES)?.unwrap_or(0);
        let replica_index = usize::try_from(replica_index_u64).map_err(|_| {
            ObjectError::InvalidArgument(format!(
                "chunkserver service bootstrap replica index is too large: {}",
                replica_index_u64
            ))
        })?;
        let before_readiness =
            self.store
                .local_chunk_storage_readiness(replica_index, 0, 0, now_ms)?;
        self.bootstrap_chunk_trim_flow_from_env(replica_index, &before_readiness, now_ms)?;
        let mutation_report = self.bootstrap_chunk_mutation_control_from_env()?;
        let after_readiness =
            self.store
                .local_chunk_storage_readiness(replica_index, 0, 0, now_ms)?;
        let report = ReplicaServiceBootstrapReport {
            replica_index,
            mutation_report,
            before_readiness,
            after_readiness,
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_REPLICASERVER_SERVICE_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected)?;
        }
        Ok(report)
    }
}
