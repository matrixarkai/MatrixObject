// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_replica_receipt_retention(
        &mut self,
        replica_index: usize,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_receipt_retention_fenced(
            replica_index,
            retention,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_receipt_retention_fenced(
        &mut self,
        replica_index: usize,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReplicaReceiptRetention {
            replica_index,
            retention,
        })
    }

    pub fn propose_configure_chunk_replica_receipt_retention_linearizable_fenced(
        &mut self,
        replica_index: usize,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk replica receipt retention",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_replica_receipt_retention_fenced(
            replica_index,
            retention,
            expected_route_control_checksum,
        )
    }

    pub fn propose_configure_chunk_receipt_retention(
        &mut self,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_receipt_retention_fenced(retention, None)
    }

    pub fn propose_configure_chunk_receipt_retention_fenced(
        &mut self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReceiptRetention { retention })
    }

    pub fn propose_configure_chunk_receipt_retention_linearizable_fenced(
        &mut self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk receipt retention",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_receipt_retention_fenced(
            retention,
            expected_route_control_checksum,
        )
    }
}
