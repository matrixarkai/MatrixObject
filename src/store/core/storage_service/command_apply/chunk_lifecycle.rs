// SPDX-License-Identifier: Apache-2.0
include!("chunk_lifecycle/object_writes.rs");
include!("chunk_lifecycle/object_lifecycle.rs");
include!("chunk_lifecycle/chunk_maintenance.rs");

impl MatrixObjectStore {
    fn apply_chunk_lifecycle_command(
        &mut self,
        command: ObjectCommand,
        _raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if matches!(
            &command,
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
        ) {
            return self.apply_object_write_command(command);
        }
        if matches!(
            &command,
            ObjectCommand::DeleteObject { .. }
                | ObjectCommand::DeleteObjectWithRootRelease { .. }
                | ObjectCommand::BatchDeleteObjects { .. }
                | ObjectCommand::DeleteObjectVersion { .. }
                | ObjectCommand::DeleteObjectsByPrefix { .. }
                | ObjectCommand::RefreshObjectRootPlacement { .. }
                | ObjectCommand::RefreshAllObjectRootPlacements { .. }
                | ObjectCommand::CommitRootPlacementMigration { .. }
                | ObjectCommand::SetBucketVersioning { .. }
        ) {
            return self.apply_object_lifecycle_command(command);
        }
        if matches!(
            &command,
            ObjectCommand::ApplyDiscard { .. }
                | ObjectCommand::ApplyPreparedChunkDiscard { .. }
                | ObjectCommand::CompactChunk { .. }
                | ObjectCommand::CompactAll { .. }
                | ObjectCommand::ApplyPreparedChunkCompaction { .. }
        ) {
            return self.apply_chunk_maintenance_command(command);
        }
        unreachable!("command classified outside SegmentLifecycle")
    }
}
