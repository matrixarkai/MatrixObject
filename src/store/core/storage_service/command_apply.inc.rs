#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandApplyFamily {
    RootBucket,
    Multipart,
    MetadataShard,
    MetadataRecordLease,
    ReplicaControl,
    ChunkControl,
    StorageRootMaintenance,
    MetadataCoordinator,
    BucketPolicy,
    ObjectMutation,
    ChunkLifecycle,
}

impl ObjectCommand {
    fn apply_family(&self) -> CommandApplyFamily {
        match self {
            ObjectCommand::CreateBucket { .. }
            | ObjectCommand::CreateBucketIdempotent { .. }
            | ObjectCommand::DeleteBucket { .. }
            | ObjectCommand::DeleteBucketIdempotent { .. }
            | ObjectCommand::RegisterRootNode { .. }
            | ObjectCommand::HeartbeatRootNode { .. }
            | ObjectCommand::HeartbeatRootPageHealth { .. }
            | ObjectCommand::HeartbeatRootChunkHealth { .. }
            | ObjectCommand::ExpireRootNodeHeartbeats { .. }
            | ObjectCommand::RemoveRootNode { .. }
            | ObjectCommand::ConfigureRootCoordinator { .. }
            | ObjectCommand::CampaignRootLeader { .. }
            | ObjectCommand::CampaignRootLeaderIdempotent { .. }
            | ObjectCommand::RenewRootLeader { .. } => CommandApplyFamily::RootBucket,
            ObjectCommand::RenewRootLeaderIdempotent { .. }
            | ObjectCommand::CreateMultipartUpload { .. }
            | ObjectCommand::CreateMultipartUploadIdempotent { .. }
            | ObjectCommand::UploadPart { .. }
            | ObjectCommand::UploadPartIdempotent { .. }
            | ObjectCommand::CompleteMultipartUpload { .. }
            | ObjectCommand::CompleteMultipartUploadIdempotent { .. }
            | ObjectCommand::CompleteMultipartUploadWithRootPlacement { .. }
            | ObjectCommand::AbortMultipartUpload { .. } => CommandApplyFamily::Multipart,
            ObjectCommand::AbortMultipartUploadIdempotent { .. }
            | ObjectCommand::TrimMultipartReceipts { .. }
            | ObjectCommand::ConfigureMetaShard { .. }
            | ObjectCommand::ConfigureMetaShardIdempotent { .. }
            | ObjectCommand::SplitMetaShard { .. }
            | ObjectCommand::SplitMetaShardIdempotent { .. }
            | ObjectCommand::MergeMetaShards { .. }
            | ObjectCommand::MergeMetaShardsIdempotent { .. }
            | ObjectCommand::MigrateMetaShard { .. }
            | ObjectCommand::MigrateMetaShardIdempotent { .. }
            | ObjectCommand::FailoverMetaShards { .. }
            | ObjectCommand::ExpiredMetaShardOwnerFailover { .. }
            | ObjectCommand::RebalanceMetaShards { .. }
            | ObjectCommand::SplitOversizedMetaShards { .. }
            | ObjectCommand::ApplyPreparedMetaShardMigrationPlan { .. } => {
                CommandApplyFamily::MetadataShard
            }
            ObjectCommand::ApplyPreparedMetaShardSplit { .. }
            | ObjectCommand::ApplyPreparedMetaShardMerge { .. }
            | ObjectCommand::ApplyPreparedMetaShardMigration { .. }
            | ObjectCommand::ApplyPreparedMetaShardRecordHandoff { .. }
            | ObjectCommand::CheckMetaReadSet { .. }
            | ObjectCommand::CommitMetaBatchIdempotent { .. }
            | ObjectCommand::ApplyPreparedMetaBatch { .. }
            | ObjectCommand::ApplyPreparedMetaTransaction { .. }
            | ObjectCommand::ApplyPreparedMetaTransactionGc { .. }
            | ObjectCommand::ApplyPreparedMetaChangeCheckpoint { .. }
            | ObjectCommand::ApplyPreparedMetaChangefeedCompaction { .. }
            | ObjectCommand::ApplyPreparedMetaTombstoneCompaction { .. }
            | ObjectCommand::ApplyPreparedMetaPrefixDelete { .. }
            | ObjectCommand::CommitMetaOwnerFencedBatch { .. }
            | ObjectCommand::CommitMetaOwnerFencedBatchIdempotent { .. }
            | ObjectCommand::AcquireMetaLease { .. }
            | ObjectCommand::AcquireMetaLeaseIdempotent { .. }
            | ObjectCommand::RenewMetaLease { .. }
            | ObjectCommand::RenewMetaLeaseIdempotent { .. }
            | ObjectCommand::ReleaseMetaLease { .. }
            | ObjectCommand::ReleaseMetaLeaseIdempotent { .. }
            | ObjectCommand::ApplyPreparedMetaLease { .. }
            | ObjectCommand::ExpireMetaLeases { .. }
            | ObjectCommand::ApplyPreparedMetaLeaseExpiry { .. } => {
                CommandApplyFamily::MetadataRecordLease
            }
            ObjectCommand::RunMetaMaintenance { .. }
            | ObjectCommand::ApplyPreparedMetaMaintenance { .. }
            | ObjectCommand::ApplyIntegrityFencedMetaMaintenance { .. }
            | ObjectCommand::ApplyRaftMembership { .. }
            | ObjectCommand::ApplyPreparedRaftMembership { .. }
            | ObjectCommand::UpdateRaftPeerHealth { .. }
            | ObjectCommand::QuarantinePageReplica { .. }
            | ObjectCommand::FreezePageReplica { .. }
            | ObjectCommand::ConfigurePageChunkEndpoints { .. }
            | ObjectCommand::ConfigureChunkReplicaCapacity { .. }
            | ObjectCommand::ApplyPreparedChunkCapacity { .. }
            | ObjectCommand::ConfigureChunkReplicaServiceMode { .. }
            | ObjectCommand::ApplyPreparedReplicaServiceMode { .. }
            | ObjectCommand::ConfigureChunkTrimFlow { .. } => CommandApplyFamily::ReplicaControl,
            ObjectCommand::ConfigureChunkReplicaTrimFlow { .. }
            | ObjectCommand::ApplyPreparedChunkTrimFlow { .. }
            | ObjectCommand::ConfigureChunkMutationQos { .. }
            | ObjectCommand::ConfigureChunkReplicaMutationQos { .. }
            | ObjectCommand::ApplyPreparedChunkMutationQos { .. }
            | ObjectCommand::ConfigureChunkReadCache { .. }
            | ObjectCommand::ConfigureChunkReplicaReadCache { .. }
            | ObjectCommand::ApplyPreparedChunkReadCache { .. }
            | ObjectCommand::ConfigureChunkReplicaReceiptRetention { .. }
            | ObjectCommand::ConfigureChunkReceiptRetention { .. }
            | ObjectCommand::ApplyPreparedChunkReceiptRetention { .. }
            | ObjectCommand::ApplyPreparedChunkBatchWrite { .. }
            | ObjectCommand::ApplyPreparedChunkBatchDiscard { .. }
            | ObjectCommand::ApplyPreparedChunkReclaim { .. }
            | ObjectCommand::ConfigurePageQuorum { .. }
            | ObjectCommand::ApplyPreparedPageQuorum { .. }
            | ObjectCommand::ConfigurePageReceiptRetention { .. }
            | ObjectCommand::ApplyPreparedPageReceiptRetention { .. }
            | ObjectCommand::ConfigurePageRouteQos { .. }
            | ObjectCommand::ApplyPreparedPageRouteQos { .. }
            | ObjectCommand::SealPageChunk { .. }
            | ObjectCommand::ApplyPreparedPageAllocationReservation { .. }
            | ObjectCommand::ApplyPreparedPageBatchAllocationReservation { .. }
            | ObjectCommand::ReleasePageAllocationIdempotent { .. }
            | ObjectCommand::AllocatePageRangesIdempotent { .. } => {
                CommandApplyFamily::ChunkControl
            }
            ObjectCommand::ApplyPreparedPageAllocationRelease { .. }
            | ObjectCommand::WritePageRangeIdempotent { .. }
            | ObjectCommand::WritePageRangesIdempotent { .. }
            | ObjectCommand::ApplyPreparedPageBatchWrite { .. }
            | ObjectCommand::DiscardPageRangeIdempotent { .. }
            | ObjectCommand::DiscardPageRangesIdempotent { .. }
            | ObjectCommand::ApplyPreparedPageBatchDiscard { .. }
            | ObjectCommand::ApplyPreparedPageReclaim { .. } => {
                CommandApplyFamily::StorageRootMaintenance
            }
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
            | ObjectCommand::RepairDrainingRootPlacements { .. }
            | ObjectCommand::RepairUnhealthyRootPlacements { .. }
            | ObjectCommand::RebalanceRootCapacity { .. }
            | ObjectCommand::ApplyPreparedRootPlacementMigration { .. }
            | ObjectCommand::ApplyPreparedRootPlacementRepair { .. }
            | ObjectCommand::ApplyPreparedRootUnhealthyPlacementRepair { .. }
            | ObjectCommand::ApplyPreparedRootCapacityRebalance { .. }
            | ObjectCommand::ConfigureMetaCoordinator { .. }
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
            | ObjectCommand::BeginMetaTransaction { .. }
            | ObjectCommand::BeginMetaTransactionIdempotent { .. }
            | ObjectCommand::RenewMetaTransaction { .. }
            | ObjectCommand::RenewMetaTransactionIdempotent { .. } => {
                CommandApplyFamily::MetadataCoordinator
            }
            ObjectCommand::CommitMetaTransaction { .. }
            | ObjectCommand::CommitMetaTransactionIdempotent { .. }
            | ObjectCommand::AbortMetaTransaction { .. }
            | ObjectCommand::AbortMetaTransactionIdempotent { .. }
            | ObjectCommand::ExpireMetaTransactions { .. }
            | ObjectCommand::GcMetaTransactions { .. }
            | ObjectCommand::CommitMetaChangeCheckpoint { .. }
            | ObjectCommand::CompactMetaChangefeed { .. }
            | ObjectCommand::CompactMetaSnapshot { .. }
            | ObjectCommand::ApplyPreparedMetaSnapshotCompaction { .. }
            | ObjectCommand::ConfigureMetaRetention { .. }
            | ObjectCommand::ConfigureMetaAdmissionPolicy { .. }
            | ObjectCommand::ApplyPreparedMetaAdmissionPolicy { .. }
            | ObjectCommand::ConfigureMetaQos { .. }
            | ObjectCommand::ApplyPreparedMetaQos { .. }
            | ObjectCommand::ConfigureMetaRouteCache { .. } => CommandApplyFamily::BucketPolicy,
            ObjectCommand::ApplyPreparedMetaRouteCacheConfig { .. }
            | ObjectCommand::ApplyPreparedMetaRetentionConfig { .. }
            | ObjectCommand::CompactMetaTombstones { .. }
            | ObjectCommand::DeleteMetaByPrefix { .. }
            | ObjectCommand::CommitObjectChangeCursor { .. }
            | ObjectCommand::CompactObjectChangefeed { .. }
            | ObjectCommand::ConfigureBucketRetention { .. }
            | ObjectCommand::ConfigureBucketRetentionIdempotent { .. }
            | ObjectCommand::ConfigureBucketQuota { .. }
            | ObjectCommand::ConfigureBucketQuotaIdempotent { .. }
            | ObjectCommand::ConfigureBucketLifecycle { .. }
            | ObjectCommand::ConfigureBucketLifecycleIdempotent { .. }
            | ObjectCommand::ConfigureBucketPolicy { .. }
            | ObjectCommand::ConfigureBucketPolicyIdempotent { .. }
            | ObjectCommand::ConfigureBucketTagging { .. }
            | ObjectCommand::ConfigureBucketTaggingIdempotent { .. }
            | ObjectCommand::ConfigureBucketCors { .. }
            | ObjectCommand::ConfigureBucketCorsIdempotent { .. }
            | ObjectCommand::ConfigureBucketVersioningIdempotent { .. }
            | ObjectCommand::ApplyBucketLifecyclePolicies { .. }
            | ObjectCommand::ApplyBucketLifecycleTransitions { .. }
            | ObjectCommand::ApplyLegacyBucketLifecycle { .. } => {
                CommandApplyFamily::ObjectMutation
            }
            ObjectCommand::PutObject { .. }
            | ObjectCommand::AppendObject { .. }
            | ObjectCommand::AppendObjectWithRootPlacement { .. }
            | ObjectCommand::BatchPutObjects { .. }
            | ObjectCommand::BatchPutObjectsWithRootPlacement { .. }
            | ObjectCommand::CopyObject { .. }
            | ObjectCommand::CopyObjectWithRootPlacement { .. }
            | ObjectCommand::BatchCopyObjects { .. }
            | ObjectCommand::BatchCopyObjectsWithRootPlacement { .. }
            | ObjectCommand::SetObjectTags { .. }
            | ObjectCommand::SetObjectLegalHold { .. }
            | ObjectCommand::SetObjectRetention { .. }
            | ObjectCommand::TransitionObject { .. }
            | ObjectCommand::TransitionObjectWithRootPlacement { .. }
            | ObjectCommand::RestoreObject { .. }
            | ObjectCommand::RestoreObjectVersion { .. }
            | ObjectCommand::RestoreObjectVersionWithRootPlacement { .. }
            | ObjectCommand::PutObjectWithRootPlacement { .. }
            | ObjectCommand::DeleteObject { .. }
            | ObjectCommand::DeleteObjectWithRootRelease { .. }
            | ObjectCommand::BatchDeleteObjects { .. }
            | ObjectCommand::DeleteObjectVersion { .. }
            | ObjectCommand::DeleteObjectsByPrefix { .. }
            | ObjectCommand::RefreshObjectRootPlacement { .. }
            | ObjectCommand::RefreshAllObjectRootPlacements { .. }
            | ObjectCommand::CommitRootPlacementMigration { .. }
            | ObjectCommand::SetBucketVersioning { .. }
            | ObjectCommand::ApplyDiscard { .. }
            | ObjectCommand::ApplyPreparedChunkDiscard { .. }
            | ObjectCommand::CompactChunk { .. }
            | ObjectCommand::CompactAll { .. }
            | ObjectCommand::ApplyPreparedChunkCompaction { .. } => {
                CommandApplyFamily::ChunkLifecycle
            }
        }
    }
}

impl MatrixObjectStore {
    pub fn apply_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        self.apply_command_with_raft_index(command, None)
    }

    fn apply_command_with_raft_index(
        &mut self,
        command: ObjectCommand,
        raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command.apply_family() {
            CommandApplyFamily::RootBucket => {
                self.apply_root_bucket_command(command, raft_entry_index)
            }
            CommandApplyFamily::Multipart => {
                self.apply_multipart_command(command, raft_entry_index)
            }
            CommandApplyFamily::MetadataShard => {
                self.apply_metadata_shard_command(command, raft_entry_index)
            }
            CommandApplyFamily::MetadataRecordLease => {
                self.apply_metadata_record_lease_command(command, raft_entry_index)
            }
            CommandApplyFamily::ReplicaControl => {
                self.apply_chunk_control_command(command, raft_entry_index)
            }
            CommandApplyFamily::ChunkControl => {
                self.apply_page_control_command(command, raft_entry_index)
            }
            CommandApplyFamily::StorageRootMaintenance => {
                self.apply_storage_root_maintenance_command(command, raft_entry_index)
            }
            CommandApplyFamily::MetadataCoordinator => {
                self.apply_metadata_coordinator_command(command, raft_entry_index)
            }
            CommandApplyFamily::BucketPolicy => {
                self.apply_bucket_policy_command(command, raft_entry_index)
            }
            CommandApplyFamily::ObjectMutation => {
                self.apply_object_mutation_command(command, raft_entry_index)
            }
            CommandApplyFamily::ChunkLifecycle => {
                self.apply_chunk_lifecycle_command(command, raft_entry_index)
            }
        }
    }
}

include!("command_apply/root_bucket.rs");
include!("command_apply/multipart.rs");
include!("command_apply/metadata_shard.rs");
include!("command_apply/metadata_record_lease.rs");
include!("command_apply/chunk_control.rs");
include!("command_apply/page_control.rs");
include!("command_apply/storage_root_maintenance.rs");
include!("command_apply/metadata_coordinator.rs");
include!("command_apply/bucket_policy.rs");
include!("command_apply/object_mutation.rs");
include!("command_apply/slab_lifecycle.rs");
