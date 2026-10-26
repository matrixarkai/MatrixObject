// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

/// The marker every object command payload starts with.
///
/// Four bytes, like the chunk write command's, so a payload produced by something else is
/// refused at the first field rather than mis-parsed into a command this store would apply.
pub const OBJECT_COMMAND_MAGIC: [u8; 4] = *b"MOCM";

/// The payload encoding's version. A decoder refuses a version it does not know.
pub const OBJECT_COMMAND_VERSION: u8 = 1;

/// The verb tag is TWO bytes, not one.
///
/// There are 212 variants today. A single byte would hold them with 43 to spare, and a format
/// that runs out of tag space needs a version bump to add a verb -- which is a migration to
/// buy a number. Two bytes costs one byte per entry and removes that entirely.
pub const OBJECT_COMMAND_TAG_BYTES: usize = 2;

const HEADER: usize = 4 + 1 + OBJECT_COMMAND_TAG_BYTES;

fn put_tag(out: &mut Vec<u8>, tag: u16) {
    out.extend_from_slice(&tag.to_be_bytes());
}

fn put_text(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(&(text.len() as u32).to_be_bytes());
    out.extend_from_slice(text.as_bytes());
}

fn need(at: usize, payload: &[u8], want: usize, verb: &str, field: &str) -> Result<(), ObjectError> {
    if payload.len() < at + want {
        return Err(ObjectError::InvalidArgument(format!(
            "object command {verb} is truncated: field {field} wants {want} bytes at offset \
             {at} and the payload is {} bytes",
            payload.len()
        )));
    }
    Ok(())
}

fn read_u64(at: &mut usize, payload: &[u8], verb: &str, field: &str) -> Result<u64, ObjectError> {
    need(*at, payload, 8, verb, field)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[*at..*at + 8]);
    *at += 8;
    Ok(u64::from_be_bytes(bytes))
}

fn read_bool(at: &mut usize, payload: &[u8], verb: &str, field: &str) -> Result<bool, ObjectError> {
    need(*at, payload, 1, verb, field)?;
    let byte = payload[*at];
    *at += 1;
    match byte {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(ObjectError::InvalidArgument(format!(
            "object command {verb} field {field} is {other}, and a boolean on this wire is 0 \
             or 1 -- anything else is a different encoding, not a true value"
        ))),
    }
}

fn read_text(
    at: &mut usize,
    payload: &[u8],
    verb: &str,
    field: &str,
) -> Result<String, ObjectError> {
    need(*at, payload, 4, verb, field)?;
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&payload[*at..*at + 4]);
    *at += 4;
    let len = u32::from_be_bytes(len_bytes) as usize;
    need(*at, payload, len, verb, field)?;
    let text = std::str::from_utf8(&payload[*at..*at + len])
        .map_err(|err| {
            ObjectError::InvalidArgument(format!(
                "object command {verb} field {field} is not utf-8: {err}"
            ))
        })?
        .to_string();
    *at += len;
    Ok(text)
}

impl ObjectCommand {
    /// This verb's name, for a message a person has to act on.
    ///
    /// # WHY THIS IS EXHAUSTIVE AND HAS NO WILDCARD ARM
    ///
    /// Every other match over this enum in the tree ends in `_ =>`, so adding a variant
    /// requires visiting nothing. This one does not, which makes a new verb a decision: it
    /// will not compile until someone has said what it is called, and `wire_tag` below will
    /// not compile until someone has said whether the codec carries it.
    ///
    /// The name never reaches the wire. The tag does. So renaming a variant is free, and this
    /// is a diagnostic rather than a format -- which is the reason the codec could be built
    /// before the remaining vocabulary work rather than after it.
    pub fn verb_name(&self) -> &'static str {
        match self {
            ObjectCommand::CreateBucket { .. } => "CreateBucket",
            ObjectCommand::CreateBucketIdempotent { .. } => "CreateBucketIdempotent",
            ObjectCommand::DeleteBucket { .. } => "DeleteBucket",
            ObjectCommand::DeleteBucketIdempotent { .. } => "DeleteBucketIdempotent",
            ObjectCommand::RegisterRootNode { .. } => "RegisterRootNode",
            ObjectCommand::HeartbeatRootNode { .. } => "HeartbeatRootNode",
            ObjectCommand::HeartbeatRootPageHealth { .. } => "HeartbeatRootPageHealth",
            ObjectCommand::HeartbeatRootChunkHealth { .. } => "HeartbeatRootChunkHealth",
            ObjectCommand::ExpireRootNodeHeartbeats { .. } => "ExpireRootNodeHeartbeats",
            ObjectCommand::RemoveRootNode { .. } => "RemoveRootNode",
            ObjectCommand::ConfigureRootCoordinator { .. } => "ConfigureRootCoordinator",
            ObjectCommand::CampaignRootLeader { .. } => "CampaignRootLeader",
            ObjectCommand::CampaignRootLeaderIdempotent { .. } => "CampaignRootLeaderIdempotent",
            ObjectCommand::RenewRootLeader { .. } => "RenewRootLeader",
            ObjectCommand::RenewRootLeaderIdempotent { .. } => "RenewRootLeaderIdempotent",
            ObjectCommand::CreateMultipartUpload { .. } => "CreateMultipartUpload",
            ObjectCommand::CreateMultipartUploadIdempotent { .. } => "CreateMultipartUploadIdempotent",
            ObjectCommand::UploadPart { .. } => "UploadPart",
            ObjectCommand::UploadPartIdempotent { .. } => "UploadPartIdempotent",
            ObjectCommand::CompleteMultipartUpload { .. } => "CompleteMultipartUpload",
            ObjectCommand::CompleteMultipartUploadIdempotent { .. } => "CompleteMultipartUploadIdempotent",
            ObjectCommand::CompleteMultipartUploadWithRootPlacement { .. } => "CompleteMultipartUploadWithRootPlacement",
            ObjectCommand::AbortMultipartUpload { .. } => "AbortMultipartUpload",
            ObjectCommand::AbortMultipartUploadIdempotent { .. } => "AbortMultipartUploadIdempotent",
            ObjectCommand::TrimMultipartReceipts { .. } => "TrimMultipartReceipts",
            ObjectCommand::ConfigureMetaShard { .. } => "ConfigureMetaShard",
            ObjectCommand::ConfigureMetaShardIdempotent { .. } => "ConfigureMetaShardIdempotent",
            ObjectCommand::SplitMetaShard { .. } => "SplitMetaShard",
            ObjectCommand::SplitMetaShardIdempotent { .. } => "SplitMetaShardIdempotent",
            ObjectCommand::MergeMetaShards { .. } => "MergeMetaShards",
            ObjectCommand::MergeMetaShardsIdempotent { .. } => "MergeMetaShardsIdempotent",
            ObjectCommand::MigrateMetaShard { .. } => "MigrateMetaShard",
            ObjectCommand::MigrateMetaShardIdempotent { .. } => "MigrateMetaShardIdempotent",
            ObjectCommand::FailoverMetaShards { .. } => "FailoverMetaShards",
            ObjectCommand::ExpiredMetaShardOwnerFailover { .. } => "ExpiredMetaShardOwnerFailover",
            ObjectCommand::RebalanceMetaShards { .. } => "RebalanceMetaShards",
            ObjectCommand::SplitOversizedMetaShards { .. } => "SplitOversizedMetaShards",
            ObjectCommand::CheckMetaReadSet { .. } => "CheckMetaReadSet",
            ObjectCommand::CommitMetaBatchIdempotent { .. } => "CommitMetaBatchIdempotent",
            ObjectCommand::ApplyPreparedMetaBatch { .. } => "ApplyPreparedMetaBatch",
            ObjectCommand::ApplyPreparedMetaTransaction { .. } => "ApplyPreparedMetaTransaction",
            ObjectCommand::ApplyPreparedMetaTransactionGc { .. } => "ApplyPreparedMetaTransactionGc",
            ObjectCommand::ApplyPreparedMetaChangeCheckpoint { .. } => "ApplyPreparedMetaChangeCheckpoint",
            ObjectCommand::ApplyPreparedMetaChangefeedCompaction { .. } => "ApplyPreparedMetaChangefeedCompaction",
            ObjectCommand::ApplyPreparedMetaTombstoneCompaction { .. } => "ApplyPreparedMetaTombstoneCompaction",
            ObjectCommand::ApplyPreparedMetaPrefixDelete { .. } => "ApplyPreparedMetaPrefixDelete",
            ObjectCommand::ApplyPreparedMetaSnapshotCompaction { .. } => "ApplyPreparedMetaSnapshotCompaction",
            ObjectCommand::ApplyPreparedMetaShardMigrationPlan { .. } => "ApplyPreparedMetaShardMigrationPlan",
            ObjectCommand::ApplyPreparedMetaShardSplit { .. } => "ApplyPreparedMetaShardSplit",
            ObjectCommand::ApplyPreparedMetaShardMerge { .. } => "ApplyPreparedMetaShardMerge",
            ObjectCommand::ApplyPreparedMetaShardMigration { .. } => "ApplyPreparedMetaShardMigration",
            ObjectCommand::ApplyPreparedMetaShardRecordHandoff { .. } => "ApplyPreparedMetaShardRecordHandoff",
            ObjectCommand::ConfigureMetaRetention { .. } => "ConfigureMetaRetention",
            ObjectCommand::ConfigureMetaAdmissionPolicy { .. } => "ConfigureMetaAdmissionPolicy",
            ObjectCommand::ApplyPreparedMetaAdmissionPolicy { .. } => "ApplyPreparedMetaAdmissionPolicy",
            ObjectCommand::ConfigureMetaQos { .. } => "ConfigureMetaQos",
            ObjectCommand::ApplyPreparedMetaQos { .. } => "ApplyPreparedMetaQos",
            ObjectCommand::ConfigureMetaRouteCache { .. } => "ConfigureMetaRouteCache",
            ObjectCommand::ApplyPreparedMetaRouteCacheConfig { .. } => "ApplyPreparedMetaRouteCacheConfig",
            ObjectCommand::ApplyPreparedMetaRetentionConfig { .. } => "ApplyPreparedMetaRetentionConfig",
            ObjectCommand::CommitMetaOwnerFencedBatch { .. } => "CommitMetaOwnerFencedBatch",
            ObjectCommand::CommitMetaOwnerFencedBatchIdempotent { .. } => "CommitMetaOwnerFencedBatchIdempotent",
            ObjectCommand::AcquireMetaLease { .. } => "AcquireMetaLease",
            ObjectCommand::AcquireMetaLeaseIdempotent { .. } => "AcquireMetaLeaseIdempotent",
            ObjectCommand::RenewMetaLease { .. } => "RenewMetaLease",
            ObjectCommand::RenewMetaLeaseIdempotent { .. } => "RenewMetaLeaseIdempotent",
            ObjectCommand::ReleaseMetaLease { .. } => "ReleaseMetaLease",
            ObjectCommand::ReleaseMetaLeaseIdempotent { .. } => "ReleaseMetaLeaseIdempotent",
            ObjectCommand::ApplyPreparedMetaLease { .. } => "ApplyPreparedMetaLease",
            ObjectCommand::ExpireMetaLeases { .. } => "ExpireMetaLeases",
            ObjectCommand::ApplyPreparedMetaLeaseExpiry { .. } => "ApplyPreparedMetaLeaseExpiry",
            ObjectCommand::RunMetaMaintenance { .. } => "RunMetaMaintenance",
            ObjectCommand::ApplyPreparedMetaMaintenance { .. } => "ApplyPreparedMetaMaintenance",
            ObjectCommand::ApplyIntegrityFencedMetaMaintenance { .. } => "ApplyIntegrityFencedMetaMaintenance",
            ObjectCommand::ApplyRaftMembership { .. } => "ApplyRaftMembership",
            ObjectCommand::ApplyPreparedRaftMembership { .. } => "ApplyPreparedRaftMembership",
            ObjectCommand::UpdateRaftPeerHealth { .. } => "UpdateRaftPeerHealth",
            ObjectCommand::QuarantinePageReplica { .. } => "QuarantinePageReplica",
            ObjectCommand::FreezePageReplica { .. } => "FreezePageReplica",
            ObjectCommand::ConfigurePageChunkEndpoints { .. } => "ConfigurePageChunkEndpoints",
            ObjectCommand::ConfigureChunkReplicaCapacity { .. } => "ConfigureChunkReplicaCapacity",
            ObjectCommand::ApplyPreparedChunkCapacity { .. } => "ApplyPreparedChunkCapacity",
            ObjectCommand::ConfigureChunkReplicaServiceMode { .. } => "ConfigureChunkReplicaServiceMode",
            ObjectCommand::ApplyPreparedReplicaServiceMode { .. } => "ApplyPreparedReplicaServiceMode",
            ObjectCommand::ConfigureChunkTrimFlow { .. } => "ConfigureChunkTrimFlow",
            ObjectCommand::ConfigureChunkReplicaTrimFlow { .. } => "ConfigureChunkReplicaTrimFlow",
            ObjectCommand::ApplyPreparedChunkTrimFlow { .. } => "ApplyPreparedChunkTrimFlow",
            ObjectCommand::ConfigureChunkMutationQos { .. } => "ConfigureChunkMutationQos",
            ObjectCommand::ConfigureChunkReplicaMutationQos { .. } => "ConfigureChunkReplicaMutationQos",
            ObjectCommand::ApplyPreparedChunkMutationQos { .. } => "ApplyPreparedChunkMutationQos",
            ObjectCommand::ConfigureChunkReadCache { .. } => "ConfigureChunkReadCache",
            ObjectCommand::ConfigureChunkReplicaReadCache { .. } => "ConfigureChunkReplicaReadCache",
            ObjectCommand::ApplyPreparedChunkReadCache { .. } => "ApplyPreparedChunkReadCache",
            ObjectCommand::ConfigureChunkReplicaReceiptRetention { .. } => "ConfigureChunkReplicaReceiptRetention",
            ObjectCommand::ConfigureChunkReceiptRetention { .. } => "ConfigureChunkReceiptRetention",
            ObjectCommand::ApplyPreparedChunkReceiptRetention { .. } => "ApplyPreparedChunkReceiptRetention",
            ObjectCommand::ApplyPreparedChunkBatchWrite { .. } => "ApplyPreparedChunkBatchWrite",
            ObjectCommand::ApplyPreparedChunkBatchDiscard { .. } => "ApplyPreparedChunkBatchDiscard",
            ObjectCommand::ApplyPreparedChunkReclaim { .. } => "ApplyPreparedChunkReclaim",
            ObjectCommand::ConfigurePageQuorum { .. } => "ConfigurePageQuorum",
            ObjectCommand::ApplyPreparedPageQuorum { .. } => "ApplyPreparedPageQuorum",
            ObjectCommand::ConfigurePageReceiptRetention { .. } => "ConfigurePageReceiptRetention",
            ObjectCommand::ApplyPreparedPageReceiptRetention { .. } => "ApplyPreparedPageReceiptRetention",
            ObjectCommand::ConfigurePageRouteQos { .. } => "ConfigurePageRouteQos",
            ObjectCommand::ApplyPreparedPageRouteQos { .. } => "ApplyPreparedPageRouteQos",
            ObjectCommand::SealPageChunk { .. } => "SealPageChunk",
            ObjectCommand::ApplyPreparedPageAllocationReservation { .. } => "ApplyPreparedPageAllocationReservation",
            ObjectCommand::ApplyPreparedPageBatchAllocationReservation { .. } => "ApplyPreparedPageBatchAllocationReservation",
            ObjectCommand::ReleasePageAllocationIdempotent { .. } => "ReleasePageAllocationIdempotent",
            ObjectCommand::AllocatePageRangesIdempotent { .. } => "AllocatePageRangesIdempotent",
            ObjectCommand::ApplyPreparedPageAllocationRelease { .. } => "ApplyPreparedPageAllocationRelease",
            ObjectCommand::WritePageRangeIdempotent { .. } => "WritePageRangeIdempotent",
            ObjectCommand::WritePageRangesIdempotent { .. } => "WritePageRangesIdempotent",
            ObjectCommand::ApplyPreparedPageBatchWrite { .. } => "ApplyPreparedPageBatchWrite",
            ObjectCommand::DiscardPageRangeIdempotent { .. } => "DiscardPageRangeIdempotent",
            ObjectCommand::DiscardPageRangesIdempotent { .. } => "DiscardPageRangesIdempotent",
            ObjectCommand::ApplyPreparedPageBatchDiscard { .. } => "ApplyPreparedPageBatchDiscard",
            ObjectCommand::ApplyPreparedPageReclaim { .. } => "ApplyPreparedPageReclaim",
            ObjectCommand::ApplyPreparedPageAuditRepair { .. } => "ApplyPreparedPageAuditRepair",
            ObjectCommand::ApplyPreparedPageRangeRepair { .. } => "ApplyPreparedPageRangeRepair",
            ObjectCommand::ApplyPreparedPageChunkRepair { .. } => "ApplyPreparedPageChunkRepair",
            ObjectCommand::ScrubPageReplicas { .. } => "ScrubPageReplicas",
            ObjectCommand::RepairPageRangeBounded { .. } => "RepairPageRangeBounded",
            ObjectCommand::RepairPageChunk { .. } => "RepairPageChunk",
            ObjectCommand::RepairAuditedPageReplicas { .. } => "RepairAuditedPageReplicas",
            ObjectCommand::ApplyChunkStateHandoffs { .. } => "ApplyChunkStateHandoffs",
            ObjectCommand::ReclaimChunkReplicas { .. } => "ReclaimChunkReplicas",
            ObjectCommand::ReclaimChunkReplicasWithLimits { .. } => "ReclaimChunkReplicasWithLimits",
            ObjectCommand::RunStorageMaintenance { .. } => "RunStorageMaintenance",
            ObjectCommand::RepairDrainingRootPlacements { .. } => "RepairDrainingRootPlacements",
            ObjectCommand::RepairUnhealthyRootPlacements { .. } => "RepairUnhealthyRootPlacements",
            ObjectCommand::RebalanceRootCapacity { .. } => "RebalanceRootCapacity",
            ObjectCommand::ApplyPreparedRootPlacementMigration { .. } => "ApplyPreparedRootPlacementMigration",
            ObjectCommand::ApplyPreparedRootPlacementRepair { .. } => "ApplyPreparedRootPlacementRepair",
            ObjectCommand::ApplyPreparedRootUnhealthyPlacementRepair { .. } => "ApplyPreparedRootUnhealthyPlacementRepair",
            ObjectCommand::ApplyPreparedRootCapacityRebalance { .. } => "ApplyPreparedRootCapacityRebalance",
            ObjectCommand::ConfigureMetaCoordinator { .. } => "ConfigureMetaCoordinator",
            ObjectCommand::ApplyPreparedMetaCoordinator { .. } => "ApplyPreparedMetaCoordinator",
            ObjectCommand::RegisterMetaCoordinatorSession { .. } => "RegisterMetaCoordinatorSession",
            ObjectCommand::RegisterMetaCoordinatorSessionIdempotent { .. } => "RegisterMetaCoordinatorSessionIdempotent",
            ObjectCommand::RenewMetaCoordinatorSession { .. } => "RenewMetaCoordinatorSession",
            ObjectCommand::RenewMetaCoordinatorSessionIdempotent { .. } => "RenewMetaCoordinatorSessionIdempotent",
            ObjectCommand::ReleaseMetaCoordinatorSession { .. } => "ReleaseMetaCoordinatorSession",
            ObjectCommand::ReleaseMetaCoordinatorSessionIdempotent { .. } => "ReleaseMetaCoordinatorSessionIdempotent",
            ObjectCommand::ApplyPreparedMetaCoordinatorSession { .. } => "ApplyPreparedMetaCoordinatorSession",
            ObjectCommand::ExpireMetaCoordinatorSessions { .. } => "ExpireMetaCoordinatorSessions",
            ObjectCommand::ApplyPreparedMetaCoordinatorSessionExpiry { .. } => "ApplyPreparedMetaCoordinatorSessionExpiry",
            ObjectCommand::BeginMetaTransaction { .. } => "BeginMetaTransaction",
            ObjectCommand::BeginMetaTransactionIdempotent { .. } => "BeginMetaTransactionIdempotent",
            ObjectCommand::CommitMetaTransaction { .. } => "CommitMetaTransaction",
            ObjectCommand::RenewMetaTransaction { .. } => "RenewMetaTransaction",
            ObjectCommand::RenewMetaTransactionIdempotent { .. } => "RenewMetaTransactionIdempotent",
            ObjectCommand::CommitMetaTransactionIdempotent { .. } => "CommitMetaTransactionIdempotent",
            ObjectCommand::AbortMetaTransaction { .. } => "AbortMetaTransaction",
            ObjectCommand::AbortMetaTransactionIdempotent { .. } => "AbortMetaTransactionIdempotent",
            ObjectCommand::ExpireMetaTransactions { .. } => "ExpireMetaTransactions",
            ObjectCommand::GcMetaTransactions { .. } => "GcMetaTransactions",
            ObjectCommand::CommitMetaChangeCheckpoint { .. } => "CommitMetaChangeCheckpoint",
            ObjectCommand::CompactMetaChangefeed { .. } => "CompactMetaChangefeed",
            ObjectCommand::CompactMetaSnapshot { .. } => "CompactMetaSnapshot",
            ObjectCommand::CompactMetaTombstones { .. } => "CompactMetaTombstones",
            ObjectCommand::DeleteMetaByPrefix { .. } => "DeleteMetaByPrefix",
            ObjectCommand::CommitObjectChangeCursor { .. } => "CommitObjectChangeCursor",
            ObjectCommand::CompactObjectChangefeed { .. } => "CompactObjectChangefeed",
            ObjectCommand::ConfigureBucketRetention { .. } => "ConfigureBucketRetention",
            ObjectCommand::ConfigureBucketRetentionIdempotent { .. } => "ConfigureBucketRetentionIdempotent",
            ObjectCommand::ConfigureBucketQuota { .. } => "ConfigureBucketQuota",
            ObjectCommand::ConfigureBucketQuotaIdempotent { .. } => "ConfigureBucketQuotaIdempotent",
            ObjectCommand::ConfigureBucketLifecycle { .. } => "ConfigureBucketLifecycle",
            ObjectCommand::ConfigureBucketLifecycleIdempotent { .. } => "ConfigureBucketLifecycleIdempotent",
            ObjectCommand::ConfigureBucketPolicy { .. } => "ConfigureBucketPolicy",
            ObjectCommand::ConfigureBucketPolicyIdempotent { .. } => "ConfigureBucketPolicyIdempotent",
            ObjectCommand::ConfigureBucketTagging { .. } => "ConfigureBucketTagging",
            ObjectCommand::ConfigureBucketTaggingIdempotent { .. } => "ConfigureBucketTaggingIdempotent",
            ObjectCommand::ConfigureBucketVersioningIdempotent { .. } => "ConfigureBucketVersioningIdempotent",
            ObjectCommand::ConfigureBucketCors { .. } => "ConfigureBucketCors",
            ObjectCommand::ConfigureBucketCorsIdempotent { .. } => "ConfigureBucketCorsIdempotent",
            ObjectCommand::ApplyBucketLifecyclePolicies { .. } => "ApplyBucketLifecyclePolicies",
            ObjectCommand::ApplyBucketLifecycleTransitions { .. } => "ApplyBucketLifecycleTransitions",
            ObjectCommand::ApplyLegacyBucketLifecycle { .. } => "ApplyLegacyBucketLifecycle",
            ObjectCommand::PutObject { .. } => "PutObject",
            ObjectCommand::AppendObject { .. } => "AppendObject",
            ObjectCommand::AppendObjectWithRootPlacement { .. } => "AppendObjectWithRootPlacement",
            ObjectCommand::BatchPutObjects { .. } => "BatchPutObjects",
            ObjectCommand::BatchPutObjectsWithRootPlacement { .. } => "BatchPutObjectsWithRootPlacement",
            ObjectCommand::CopyObject { .. } => "CopyObject",
            ObjectCommand::CopyObjectWithRootPlacement { .. } => "CopyObjectWithRootPlacement",
            ObjectCommand::BatchCopyObjects { .. } => "BatchCopyObjects",
            ObjectCommand::BatchCopyObjectsWithRootPlacement { .. } => "BatchCopyObjectsWithRootPlacement",
            ObjectCommand::SetObjectTags { .. } => "SetObjectTags",
            ObjectCommand::SetObjectLegalHold { .. } => "SetObjectLegalHold",
            ObjectCommand::SetObjectRetention { .. } => "SetObjectRetention",
            ObjectCommand::TransitionObject { .. } => "TransitionObject",
            ObjectCommand::TransitionObjectWithRootPlacement { .. } => "TransitionObjectWithRootPlacement",
            ObjectCommand::RestoreObject { .. } => "RestoreObject",
            ObjectCommand::RestoreObjectVersion { .. } => "RestoreObjectVersion",
            ObjectCommand::RestoreObjectVersionWithRootPlacement { .. } => "RestoreObjectVersionWithRootPlacement",
            ObjectCommand::PutObjectWithRootPlacement { .. } => "PutObjectWithRootPlacement",
            ObjectCommand::DeleteObject { .. } => "DeleteObject",
            ObjectCommand::DeleteObjectWithRootRelease { .. } => "DeleteObjectWithRootRelease",
            ObjectCommand::BatchDeleteObjects { .. } => "BatchDeleteObjects",
            ObjectCommand::DeleteObjectVersion { .. } => "DeleteObjectVersion",
            ObjectCommand::DeleteObjectsByPrefix { .. } => "DeleteObjectsByPrefix",
            ObjectCommand::RefreshObjectRootPlacement { .. } => "RefreshObjectRootPlacement",
            ObjectCommand::RefreshAllObjectRootPlacements { .. } => "RefreshAllObjectRootPlacements",
            ObjectCommand::CommitRootPlacementMigration { .. } => "CommitRootPlacementMigration",
            ObjectCommand::SetBucketVersioning { .. } => "SetBucketVersioning",
            ObjectCommand::ApplyDiscard { .. } => "ApplyDiscard",
            ObjectCommand::ApplyPreparedChunkDiscard { .. } => "ApplyPreparedChunkDiscard",
            ObjectCommand::CompactChunk { .. } => "CompactChunk",
            ObjectCommand::CompactAll { .. } => "CompactAll",
            ObjectCommand::ApplyPreparedChunkCompaction { .. } => "ApplyPreparedChunkCompaction",
        }
    }

    /// The tag this verb travels under, or `None` if the codec does not carry it yet.
    ///
    /// Exhaustive on purpose, same reason as `verb_name`. A verb that is not carried is
    /// refused BY NAME rather than encoded into something a replica cannot replay.
    pub fn wire_tag(&self) -> Option<u16> {
        match self {
            ObjectCommand::CreateBucket { .. } => None,
            ObjectCommand::CreateBucketIdempotent { .. } => None,
            ObjectCommand::DeleteBucket { .. } => None,
            ObjectCommand::DeleteBucketIdempotent { .. } => None,
            ObjectCommand::RegisterRootNode { .. } => None,
            ObjectCommand::HeartbeatRootNode { .. } => None,
            ObjectCommand::HeartbeatRootPageHealth { .. } => None,
            ObjectCommand::HeartbeatRootChunkHealth { .. } => None,
            ObjectCommand::ExpireRootNodeHeartbeats { .. } => None,
            ObjectCommand::RemoveRootNode { .. } => None,
            ObjectCommand::ConfigureRootCoordinator { .. } => None,
            ObjectCommand::CampaignRootLeader { .. } => None,
            ObjectCommand::CampaignRootLeaderIdempotent { .. } => None,
            ObjectCommand::RenewRootLeader { .. } => None,
            ObjectCommand::RenewRootLeaderIdempotent { .. } => None,
            ObjectCommand::CreateMultipartUpload { .. } => None,
            ObjectCommand::CreateMultipartUploadIdempotent { .. } => None,
            ObjectCommand::UploadPart { .. } => None,
            ObjectCommand::UploadPartIdempotent { .. } => None,
            ObjectCommand::CompleteMultipartUpload { .. } => None,
            ObjectCommand::CompleteMultipartUploadIdempotent { .. } => None,
            ObjectCommand::CompleteMultipartUploadWithRootPlacement { .. } => None,
            ObjectCommand::AbortMultipartUpload { .. } => None,
            ObjectCommand::AbortMultipartUploadIdempotent { .. } => None,
            ObjectCommand::TrimMultipartReceipts { .. } => None,
            ObjectCommand::ConfigureMetaShard { .. } => None,
            ObjectCommand::ConfigureMetaShardIdempotent { .. } => None,
            ObjectCommand::SplitMetaShard { .. } => None,
            ObjectCommand::SplitMetaShardIdempotent { .. } => None,
            ObjectCommand::MergeMetaShards { .. } => None,
            ObjectCommand::MergeMetaShardsIdempotent { .. } => None,
            ObjectCommand::MigrateMetaShard { .. } => Some(7),
            ObjectCommand::MigrateMetaShardIdempotent { .. } => Some(8),
            ObjectCommand::FailoverMetaShards { .. } => None,
            ObjectCommand::ExpiredMetaShardOwnerFailover { .. } => None,
            ObjectCommand::RebalanceMetaShards { .. } => None,
            ObjectCommand::SplitOversizedMetaShards { .. } => None,
            ObjectCommand::CheckMetaReadSet { .. } => None,
            ObjectCommand::CommitMetaBatchIdempotent { .. } => None,
            ObjectCommand::ApplyPreparedMetaBatch { .. } => None,
            ObjectCommand::ApplyPreparedMetaTransaction { .. } => None,
            ObjectCommand::ApplyPreparedMetaTransactionGc { .. } => None,
            ObjectCommand::ApplyPreparedMetaChangeCheckpoint { .. } => None,
            ObjectCommand::ApplyPreparedMetaChangefeedCompaction { .. } => None,
            ObjectCommand::ApplyPreparedMetaTombstoneCompaction { .. } => None,
            ObjectCommand::ApplyPreparedMetaPrefixDelete { .. } => None,
            ObjectCommand::ApplyPreparedMetaSnapshotCompaction { .. } => None,
            ObjectCommand::ApplyPreparedMetaShardMigrationPlan { .. } => None,
            ObjectCommand::ApplyPreparedMetaShardSplit { .. } => None,
            ObjectCommand::ApplyPreparedMetaShardMerge { .. } => None,
            ObjectCommand::ApplyPreparedMetaShardMigration { .. } => None,
            ObjectCommand::ApplyPreparedMetaShardRecordHandoff { .. } => None,
            ObjectCommand::ConfigureMetaRetention { .. } => None,
            ObjectCommand::ConfigureMetaAdmissionPolicy { .. } => None,
            ObjectCommand::ApplyPreparedMetaAdmissionPolicy { .. } => None,
            ObjectCommand::ConfigureMetaQos { .. } => None,
            ObjectCommand::ApplyPreparedMetaQos { .. } => None,
            ObjectCommand::ConfigureMetaRouteCache { .. } => None,
            ObjectCommand::ApplyPreparedMetaRouteCacheConfig { .. } => None,
            ObjectCommand::ApplyPreparedMetaRetentionConfig { .. } => None,
            ObjectCommand::CommitMetaOwnerFencedBatch { .. } => None,
            ObjectCommand::CommitMetaOwnerFencedBatchIdempotent { .. } => None,
            ObjectCommand::AcquireMetaLease { .. } => Some(1),
            ObjectCommand::AcquireMetaLeaseIdempotent { .. } => Some(2),
            ObjectCommand::RenewMetaLease { .. } => Some(3),
            ObjectCommand::RenewMetaLeaseIdempotent { .. } => Some(4),
            ObjectCommand::ReleaseMetaLease { .. } => Some(5),
            ObjectCommand::ReleaseMetaLeaseIdempotent { .. } => Some(6),
            ObjectCommand::ApplyPreparedMetaLease { .. } => None,
            ObjectCommand::ExpireMetaLeases { .. } => None,
            ObjectCommand::ApplyPreparedMetaLeaseExpiry { .. } => None,
            ObjectCommand::RunMetaMaintenance { .. } => None,
            ObjectCommand::ApplyPreparedMetaMaintenance { .. } => None,
            ObjectCommand::ApplyIntegrityFencedMetaMaintenance { .. } => None,
            ObjectCommand::ApplyRaftMembership { .. } => None,
            ObjectCommand::ApplyPreparedRaftMembership { .. } => None,
            ObjectCommand::UpdateRaftPeerHealth { .. } => None,
            ObjectCommand::QuarantinePageReplica { .. } => None,
            ObjectCommand::FreezePageReplica { .. } => None,
            ObjectCommand::ConfigurePageChunkEndpoints { .. } => None,
            ObjectCommand::ConfigureChunkReplicaCapacity { .. } => None,
            ObjectCommand::ApplyPreparedChunkCapacity { .. } => None,
            ObjectCommand::ConfigureChunkReplicaServiceMode { .. } => None,
            ObjectCommand::ApplyPreparedReplicaServiceMode { .. } => None,
            ObjectCommand::ConfigureChunkTrimFlow { .. } => None,
            ObjectCommand::ConfigureChunkReplicaTrimFlow { .. } => None,
            ObjectCommand::ApplyPreparedChunkTrimFlow { .. } => None,
            ObjectCommand::ConfigureChunkMutationQos { .. } => None,
            ObjectCommand::ConfigureChunkReplicaMutationQos { .. } => None,
            ObjectCommand::ApplyPreparedChunkMutationQos { .. } => None,
            ObjectCommand::ConfigureChunkReadCache { .. } => None,
            ObjectCommand::ConfigureChunkReplicaReadCache { .. } => None,
            ObjectCommand::ApplyPreparedChunkReadCache { .. } => None,
            ObjectCommand::ConfigureChunkReplicaReceiptRetention { .. } => None,
            ObjectCommand::ConfigureChunkReceiptRetention { .. } => None,
            ObjectCommand::ApplyPreparedChunkReceiptRetention { .. } => None,
            ObjectCommand::ApplyPreparedChunkBatchWrite { .. } => None,
            ObjectCommand::ApplyPreparedChunkBatchDiscard { .. } => None,
            ObjectCommand::ApplyPreparedChunkReclaim { .. } => None,
            ObjectCommand::ConfigurePageQuorum { .. } => None,
            ObjectCommand::ApplyPreparedPageQuorum { .. } => None,
            ObjectCommand::ConfigurePageReceiptRetention { .. } => None,
            ObjectCommand::ApplyPreparedPageReceiptRetention { .. } => None,
            ObjectCommand::ConfigurePageRouteQos { .. } => None,
            ObjectCommand::ApplyPreparedPageRouteQos { .. } => None,
            ObjectCommand::SealPageChunk { .. } => None,
            ObjectCommand::ApplyPreparedPageAllocationReservation { .. } => None,
            ObjectCommand::ApplyPreparedPageBatchAllocationReservation { .. } => None,
            ObjectCommand::ReleasePageAllocationIdempotent { .. } => None,
            ObjectCommand::AllocatePageRangesIdempotent { .. } => None,
            ObjectCommand::ApplyPreparedPageAllocationRelease { .. } => None,
            ObjectCommand::WritePageRangeIdempotent { .. } => None,
            ObjectCommand::WritePageRangesIdempotent { .. } => None,
            ObjectCommand::ApplyPreparedPageBatchWrite { .. } => None,
            ObjectCommand::DiscardPageRangeIdempotent { .. } => None,
            ObjectCommand::DiscardPageRangesIdempotent { .. } => None,
            ObjectCommand::ApplyPreparedPageBatchDiscard { .. } => None,
            ObjectCommand::ApplyPreparedPageReclaim { .. } => None,
            ObjectCommand::ApplyPreparedPageAuditRepair { .. } => None,
            ObjectCommand::ApplyPreparedPageRangeRepair { .. } => None,
            ObjectCommand::ApplyPreparedPageChunkRepair { .. } => None,
            ObjectCommand::ScrubPageReplicas { .. } => None,
            ObjectCommand::RepairPageRangeBounded { .. } => None,
            ObjectCommand::RepairPageChunk { .. } => None,
            ObjectCommand::RepairAuditedPageReplicas { .. } => None,
            ObjectCommand::ApplyChunkStateHandoffs { .. } => None,
            ObjectCommand::ReclaimChunkReplicas { .. } => None,
            ObjectCommand::ReclaimChunkReplicasWithLimits { .. } => None,
            ObjectCommand::RunStorageMaintenance { .. } => None,
            ObjectCommand::RepairDrainingRootPlacements { .. } => None,
            ObjectCommand::RepairUnhealthyRootPlacements { .. } => None,
            ObjectCommand::RebalanceRootCapacity { .. } => None,
            ObjectCommand::ApplyPreparedRootPlacementMigration { .. } => None,
            ObjectCommand::ApplyPreparedRootPlacementRepair { .. } => None,
            ObjectCommand::ApplyPreparedRootUnhealthyPlacementRepair { .. } => None,
            ObjectCommand::ApplyPreparedRootCapacityRebalance { .. } => None,
            ObjectCommand::ConfigureMetaCoordinator { .. } => None,
            ObjectCommand::ApplyPreparedMetaCoordinator { .. } => None,
            ObjectCommand::RegisterMetaCoordinatorSession { .. } => None,
            ObjectCommand::RegisterMetaCoordinatorSessionIdempotent { .. } => None,
            ObjectCommand::RenewMetaCoordinatorSession { .. } => None,
            ObjectCommand::RenewMetaCoordinatorSessionIdempotent { .. } => None,
            ObjectCommand::ReleaseMetaCoordinatorSession { .. } => None,
            ObjectCommand::ReleaseMetaCoordinatorSessionIdempotent { .. } => None,
            ObjectCommand::ApplyPreparedMetaCoordinatorSession { .. } => None,
            ObjectCommand::ExpireMetaCoordinatorSessions { .. } => None,
            ObjectCommand::ApplyPreparedMetaCoordinatorSessionExpiry { .. } => None,
            ObjectCommand::BeginMetaTransaction { .. } => None,
            ObjectCommand::BeginMetaTransactionIdempotent { .. } => None,
            ObjectCommand::CommitMetaTransaction { .. } => None,
            ObjectCommand::RenewMetaTransaction { .. } => None,
            ObjectCommand::RenewMetaTransactionIdempotent { .. } => None,
            ObjectCommand::CommitMetaTransactionIdempotent { .. } => None,
            ObjectCommand::AbortMetaTransaction { .. } => None,
            ObjectCommand::AbortMetaTransactionIdempotent { .. } => None,
            ObjectCommand::ExpireMetaTransactions { .. } => None,
            ObjectCommand::GcMetaTransactions { .. } => None,
            ObjectCommand::CommitMetaChangeCheckpoint { .. } => None,
            ObjectCommand::CompactMetaChangefeed { .. } => None,
            ObjectCommand::CompactMetaSnapshot { .. } => None,
            ObjectCommand::CompactMetaTombstones { .. } => None,
            ObjectCommand::DeleteMetaByPrefix { .. } => None,
            ObjectCommand::CommitObjectChangeCursor { .. } => None,
            ObjectCommand::CompactObjectChangefeed { .. } => None,
            ObjectCommand::ConfigureBucketRetention { .. } => None,
            ObjectCommand::ConfigureBucketRetentionIdempotent { .. } => None,
            ObjectCommand::ConfigureBucketQuota { .. } => None,
            ObjectCommand::ConfigureBucketQuotaIdempotent { .. } => None,
            ObjectCommand::ConfigureBucketLifecycle { .. } => None,
            ObjectCommand::ConfigureBucketLifecycleIdempotent { .. } => None,
            ObjectCommand::ConfigureBucketPolicy { .. } => None,
            ObjectCommand::ConfigureBucketPolicyIdempotent { .. } => None,
            ObjectCommand::ConfigureBucketTagging { .. } => None,
            ObjectCommand::ConfigureBucketTaggingIdempotent { .. } => None,
            ObjectCommand::ConfigureBucketVersioningIdempotent { .. } => None,
            ObjectCommand::ConfigureBucketCors { .. } => None,
            ObjectCommand::ConfigureBucketCorsIdempotent { .. } => None,
            ObjectCommand::ApplyBucketLifecyclePolicies { .. } => None,
            ObjectCommand::ApplyBucketLifecycleTransitions { .. } => None,
            ObjectCommand::ApplyLegacyBucketLifecycle { .. } => None,
            ObjectCommand::PutObject { .. } => None,
            ObjectCommand::AppendObject { .. } => None,
            ObjectCommand::AppendObjectWithRootPlacement { .. } => None,
            ObjectCommand::BatchPutObjects { .. } => None,
            ObjectCommand::BatchPutObjectsWithRootPlacement { .. } => None,
            ObjectCommand::CopyObject { .. } => None,
            ObjectCommand::CopyObjectWithRootPlacement { .. } => None,
            ObjectCommand::BatchCopyObjects { .. } => None,
            ObjectCommand::BatchCopyObjectsWithRootPlacement { .. } => None,
            ObjectCommand::SetObjectTags { .. } => None,
            ObjectCommand::SetObjectLegalHold { .. } => None,
            ObjectCommand::SetObjectRetention { .. } => None,
            ObjectCommand::TransitionObject { .. } => None,
            ObjectCommand::TransitionObjectWithRootPlacement { .. } => None,
            ObjectCommand::RestoreObject { .. } => None,
            ObjectCommand::RestoreObjectVersion { .. } => None,
            ObjectCommand::RestoreObjectVersionWithRootPlacement { .. } => None,
            ObjectCommand::PutObjectWithRootPlacement { .. } => None,
            ObjectCommand::DeleteObject { .. } => None,
            ObjectCommand::DeleteObjectWithRootRelease { .. } => None,
            ObjectCommand::BatchDeleteObjects { .. } => None,
            ObjectCommand::DeleteObjectVersion { .. } => None,
            ObjectCommand::DeleteObjectsByPrefix { .. } => None,
            ObjectCommand::RefreshObjectRootPlacement { .. } => None,
            ObjectCommand::RefreshAllObjectRootPlacements { .. } => None,
            ObjectCommand::CommitRootPlacementMigration { .. } => None,
            ObjectCommand::SetBucketVersioning { .. } => None,
            ObjectCommand::ApplyDiscard { .. } => None,
            ObjectCommand::ApplyPreparedChunkDiscard { .. } => None,
            ObjectCommand::CompactChunk { .. } => None,
            ObjectCommand::CompactAll { .. } => None,
            ObjectCommand::ApplyPreparedChunkCompaction { .. } => None,
        }
    }

    /// The bytes that travel in a raft entry.
    ///
    /// Big-endian throughout, so a hexdump reads in the order the fields are written -- the
    /// same choice the chunk write command made, and for the same reason.
    pub fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        if self.wire_tag().is_none() {
            return Err(ObjectError::InvalidArgument(format!(
                "object command {} is not carried by this codec yet, so it cannot be put in a \
                 raft entry; it is refused here rather than encoded into something a replica \
                 could not replay",
                self.verb_name()
            )));
        }
        let mut out = Vec::new();
        out.extend_from_slice(&OBJECT_COMMAND_MAGIC);
        out.push(OBJECT_COMMAND_VERSION);
        match self {
            ObjectCommand::AcquireMetaLease { namespace, owner, ttl_ms, now_ms } => {
                put_tag(&mut out, 1);
                put_text(&mut out, namespace);
                put_text(&mut out, owner);
                out.extend_from_slice(&ttl_ms.to_be_bytes());
                out.extend_from_slice(&now_ms.to_be_bytes());
            }
            ObjectCommand::AcquireMetaLeaseIdempotent { namespace, owner, ttl_ms, now_ms, idempotency_key } => {
                put_tag(&mut out, 2);
                put_text(&mut out, namespace);
                put_text(&mut out, owner);
                out.extend_from_slice(&ttl_ms.to_be_bytes());
                out.extend_from_slice(&now_ms.to_be_bytes());
                put_text(&mut out, idempotency_key);
            }
            ObjectCommand::RenewMetaLease { namespace, owner, epoch, ttl_ms, now_ms } => {
                put_tag(&mut out, 3);
                put_text(&mut out, namespace);
                put_text(&mut out, owner);
                out.extend_from_slice(&epoch.to_be_bytes());
                out.extend_from_slice(&ttl_ms.to_be_bytes());
                out.extend_from_slice(&now_ms.to_be_bytes());
            }
            ObjectCommand::RenewMetaLeaseIdempotent { namespace, owner, epoch, ttl_ms, now_ms, idempotency_key } => {
                put_tag(&mut out, 4);
                put_text(&mut out, namespace);
                put_text(&mut out, owner);
                out.extend_from_slice(&epoch.to_be_bytes());
                out.extend_from_slice(&ttl_ms.to_be_bytes());
                out.extend_from_slice(&now_ms.to_be_bytes());
                put_text(&mut out, idempotency_key);
            }
            ObjectCommand::ReleaseMetaLease { namespace, owner, epoch } => {
                put_tag(&mut out, 5);
                put_text(&mut out, namespace);
                put_text(&mut out, owner);
                out.extend_from_slice(&epoch.to_be_bytes());
            }
            ObjectCommand::ReleaseMetaLeaseIdempotent { namespace, owner, epoch, idempotency_key } => {
                put_tag(&mut out, 6);
                put_text(&mut out, namespace);
                put_text(&mut out, owner);
                out.extend_from_slice(&epoch.to_be_bytes());
                put_text(&mut out, idempotency_key);
            }
            ObjectCommand::MigrateMetaShard { shard_id, new_owner_node_id, expected_epoch, new_epoch, draining, now_ms } => {
                put_tag(&mut out, 7);
                put_text(&mut out, shard_id);
                put_text(&mut out, new_owner_node_id);
                out.extend_from_slice(&expected_epoch.to_be_bytes());
                out.extend_from_slice(&new_epoch.to_be_bytes());
                out.push(u8::from(*draining));
                out.extend_from_slice(&now_ms.to_be_bytes());
            }
            ObjectCommand::MigrateMetaShardIdempotent { shard_id, new_owner_node_id, expected_epoch, new_epoch, draining, now_ms, idempotency_key } => {
                put_tag(&mut out, 8);
                put_text(&mut out, shard_id);
                put_text(&mut out, new_owner_node_id);
                out.extend_from_slice(&expected_epoch.to_be_bytes());
                out.extend_from_slice(&new_epoch.to_be_bytes());
                out.push(u8::from(*draining));
                out.extend_from_slice(&now_ms.to_be_bytes());
                put_text(&mut out, idempotency_key);
            }
            other => {
                return Err(ObjectError::InvalidArgument(format!(
                    "object command {} has a tag but no encoding, which is a codec that \
                     disagrees with itself",
                    other.verb_name()
                )))
            }
        }
        Ok(out)
    }

    /// Read one back.
    ///
    /// Every refusal names the verb and the field. A raft entry that cannot be decoded stalls
    /// the apply loop on every replica at once, so the message is the only thing an operator
    /// has to go on.
    pub fn from_payload(payload: &[u8]) -> Result<Self, ObjectError> {
        if payload.len() < HEADER {
            return Err(ObjectError::InvalidArgument(format!(
                "object command payload is {} bytes, shorter than its {HEADER}-byte header",
                payload.len()
            )));
        }
        if payload[..4] != OBJECT_COMMAND_MAGIC {
            return Err(ObjectError::InvalidArgument(
                "object command payload does not carry this crate's command marker, so it was \
                 produced by something else"
                    .to_string(),
            ));
        }
        let version = payload[4];
        if version != OBJECT_COMMAND_VERSION {
            return Err(ObjectError::InvalidArgument(format!(
                "object command payload is version {version} and this node understands version \
                 {OBJECT_COMMAND_VERSION}"
            )));
        }
        let tag = u16::from_be_bytes([payload[5], payload[6]]);
        let mut at = HEADER;
        let command = match tag {
            1 => {
                let namespace = read_text(&mut at, payload, "AcquireMetaLease", "namespace")?;
                let owner = read_text(&mut at, payload, "AcquireMetaLease", "owner")?;
                let ttl_ms = read_u64(&mut at, payload, "AcquireMetaLease", "ttl_ms")?;
                let now_ms = read_u64(&mut at, payload, "AcquireMetaLease", "now_ms")?;
                ObjectCommand::AcquireMetaLease {
                    namespace,
                    owner,
                    ttl_ms,
                    now_ms,
                }
            }
            2 => {
                let namespace = read_text(&mut at, payload, "AcquireMetaLeaseIdempotent", "namespace")?;
                let owner = read_text(&mut at, payload, "AcquireMetaLeaseIdempotent", "owner")?;
                let ttl_ms = read_u64(&mut at, payload, "AcquireMetaLeaseIdempotent", "ttl_ms")?;
                let now_ms = read_u64(&mut at, payload, "AcquireMetaLeaseIdempotent", "now_ms")?;
                let idempotency_key = read_text(&mut at, payload, "AcquireMetaLeaseIdempotent", "idempotency_key")?;
                ObjectCommand::AcquireMetaLeaseIdempotent {
                    namespace,
                    owner,
                    ttl_ms,
                    now_ms,
                    idempotency_key,
                }
            }
            3 => {
                let namespace = read_text(&mut at, payload, "RenewMetaLease", "namespace")?;
                let owner = read_text(&mut at, payload, "RenewMetaLease", "owner")?;
                let epoch = read_u64(&mut at, payload, "RenewMetaLease", "epoch")?;
                let ttl_ms = read_u64(&mut at, payload, "RenewMetaLease", "ttl_ms")?;
                let now_ms = read_u64(&mut at, payload, "RenewMetaLease", "now_ms")?;
                ObjectCommand::RenewMetaLease {
                    namespace,
                    owner,
                    epoch,
                    ttl_ms,
                    now_ms,
                }
            }
            4 => {
                let namespace = read_text(&mut at, payload, "RenewMetaLeaseIdempotent", "namespace")?;
                let owner = read_text(&mut at, payload, "RenewMetaLeaseIdempotent", "owner")?;
                let epoch = read_u64(&mut at, payload, "RenewMetaLeaseIdempotent", "epoch")?;
                let ttl_ms = read_u64(&mut at, payload, "RenewMetaLeaseIdempotent", "ttl_ms")?;
                let now_ms = read_u64(&mut at, payload, "RenewMetaLeaseIdempotent", "now_ms")?;
                let idempotency_key = read_text(&mut at, payload, "RenewMetaLeaseIdempotent", "idempotency_key")?;
                ObjectCommand::RenewMetaLeaseIdempotent {
                    namespace,
                    owner,
                    epoch,
                    ttl_ms,
                    now_ms,
                    idempotency_key,
                }
            }
            5 => {
                let namespace = read_text(&mut at, payload, "ReleaseMetaLease", "namespace")?;
                let owner = read_text(&mut at, payload, "ReleaseMetaLease", "owner")?;
                let epoch = read_u64(&mut at, payload, "ReleaseMetaLease", "epoch")?;
                ObjectCommand::ReleaseMetaLease {
                    namespace,
                    owner,
                    epoch,
                }
            }
            6 => {
                let namespace = read_text(&mut at, payload, "ReleaseMetaLeaseIdempotent", "namespace")?;
                let owner = read_text(&mut at, payload, "ReleaseMetaLeaseIdempotent", "owner")?;
                let epoch = read_u64(&mut at, payload, "ReleaseMetaLeaseIdempotent", "epoch")?;
                let idempotency_key = read_text(&mut at, payload, "ReleaseMetaLeaseIdempotent", "idempotency_key")?;
                ObjectCommand::ReleaseMetaLeaseIdempotent {
                    namespace,
                    owner,
                    epoch,
                    idempotency_key,
                }
            }
            7 => {
                let shard_id = read_text(&mut at, payload, "MigrateMetaShard", "shard_id")?;
                let new_owner_node_id = read_text(&mut at, payload, "MigrateMetaShard", "new_owner_node_id")?;
                let expected_epoch = read_u64(&mut at, payload, "MigrateMetaShard", "expected_epoch")?;
                let new_epoch = read_u64(&mut at, payload, "MigrateMetaShard", "new_epoch")?;
                let draining = read_bool(&mut at, payload, "MigrateMetaShard", "draining")?;
                let now_ms = read_u64(&mut at, payload, "MigrateMetaShard", "now_ms")?;
                ObjectCommand::MigrateMetaShard {
                    shard_id,
                    new_owner_node_id,
                    expected_epoch,
                    new_epoch,
                    draining,
                    now_ms,
                }
            }
            8 => {
                let shard_id = read_text(&mut at, payload, "MigrateMetaShardIdempotent", "shard_id")?;
                let new_owner_node_id = read_text(&mut at, payload, "MigrateMetaShardIdempotent", "new_owner_node_id")?;
                let expected_epoch = read_u64(&mut at, payload, "MigrateMetaShardIdempotent", "expected_epoch")?;
                let new_epoch = read_u64(&mut at, payload, "MigrateMetaShardIdempotent", "new_epoch")?;
                let draining = read_bool(&mut at, payload, "MigrateMetaShardIdempotent", "draining")?;
                let now_ms = read_u64(&mut at, payload, "MigrateMetaShardIdempotent", "now_ms")?;
                let idempotency_key = read_text(&mut at, payload, "MigrateMetaShardIdempotent", "idempotency_key")?;
                ObjectCommand::MigrateMetaShardIdempotent {
                    shard_id,
                    new_owner_node_id,
                    expected_epoch,
                    new_epoch,
                    draining,
                    now_ms,
                    idempotency_key,
                }
            }
            other => {
                return Err(ObjectError::InvalidArgument(format!(
                    "object command tag {other} is not one this codec carries; a tag it does \
                     not know is refused rather than skipped, because skipping one would \
                     apply an entry with a verb missing"
                )))
            }
        };
        if at != payload.len() {
            return Err(ObjectError::InvalidArgument(format!(
                "object command {} left {} trailing bytes, so the payload holds more than the \
                 verb accounts for",
                command.verb_name(),
                payload.len() - at
            )));
        }
        Ok(command)
    }
}
