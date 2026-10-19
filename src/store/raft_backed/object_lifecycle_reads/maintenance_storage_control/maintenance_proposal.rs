// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_run_meta_maintenance(
        &mut self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_run_meta_maintenance_prepared(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
        )
    }

    pub fn propose_run_meta_maintenance_with_payload_budget(
        &mut self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_run_meta_maintenance_raft_entry_with_payload_budget(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            max_payload_bytes,
        )?;
        self.propose_prepared_meta_maintenance(prepared)
    }
}
