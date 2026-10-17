include!("metadata_coordinator/block_storage.rs");
include!("metadata_coordinator/root_placement.rs");
include!("metadata_coordinator/coordinator.rs");
include!("metadata_coordinator/transactions.rs");

impl MatrixObjectStore {
    fn apply_metadata_coordinator_command(
        &mut self,
        command: ObjectCommand,
        _raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if matches!(
            &command,
            ObjectCommand::ApplyPreparedPageAuditRepair { .. }
                | ObjectCommand::ApplyPreparedPageRangeRepair { .. }
                | ObjectCommand::ApplyPreparedPageChunkRepair { .. }
                | ObjectCommand::ScrubPageReplicas { .. }
                | ObjectCommand::RepairPageRangeBounded { .. }
                | ObjectCommand::RepairPageChunk { .. }
                | ObjectCommand::RepairAuditedPageReplicas { .. }
                | ObjectCommand::ApplyChunkStateHandoffs { .. }
                | ObjectCommand::ReclaimChunkReplicas { .. }
                | ObjectCommand::ReclaimChunkReplicasWithLimits { .. }
                | ObjectCommand::RunStorageMaintenance { .. }
        ) {
            return self.apply_page_storage_metadata_command(command);
        }
        if matches!(
            &command,
            ObjectCommand::RepairDrainingRootPlacements { .. }
                | ObjectCommand::RepairUnhealthyRootPlacements { .. }
                | ObjectCommand::RebalanceRootCapacity { .. }
                | ObjectCommand::ApplyPreparedRootPlacementMigration { .. }
                | ObjectCommand::ApplyPreparedRootPlacementRepair { .. }
                | ObjectCommand::ApplyPreparedRootUnhealthyPlacementRepair { .. }
                | ObjectCommand::ApplyPreparedRootCapacityRebalance { .. }
        ) {
            return self.apply_root_placement_metadata_command(command);
        }
        if matches!(
            &command,
            ObjectCommand::ConfigureMetaCoordinator { .. }
                | ObjectCommand::ApplyPreparedMetaCoordinator { .. }
                | ObjectCommand::RegisterMetaCoordinatorSession { .. }
                | ObjectCommand::RegisterMetaCoordinatorSessionIdempotent { .. }
                | ObjectCommand::RenewMetaCoordinatorSession { .. }
                | ObjectCommand::RenewMetaCoordinatorSessionIdempotent { .. }
                | ObjectCommand::ReleaseMetaCoordinatorSession { .. }
                | ObjectCommand::ReleaseMetaCoordinatorSessionIdempotent { .. }
                | ObjectCommand::ApplyPreparedMetaCoordinatorSession { .. }
                | ObjectCommand::ExpireMetaCoordinatorSessions { .. }
                | ObjectCommand::ApplyPreparedMetaCoordinatorSessionExpiry { .. }
        ) {
            return self.apply_coordinator_metadata_command(command);
        }
        if matches!(
            &command,
            ObjectCommand::BeginMetaTransaction { .. }
                | ObjectCommand::BeginMetaTransactionIdempotent { .. }
                | ObjectCommand::RenewMetaTransaction { .. }
                | ObjectCommand::RenewMetaTransactionIdempotent { .. }
        ) {
            return self.apply_transaction_metadata_command(command);
        }
        unreachable!("command classified outside MetadataCoordinator")
    }
}
