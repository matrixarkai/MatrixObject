// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_storage_root_maintenance_command(
        &mut self,
        command: ObjectCommand,
        _raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ApplyPreparedPageAllocationRelease { prepared } => {
                let receipt = self.apply_prepared_page_allocation_release(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockalloc",
                    &receipt.allocation.allocation_id,
                    receipt.allocation.length,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "blockserver/prepared-allocation-release-deduped"
                    } else {
                        "blockserver/prepared-allocation-release"
                    },
                    receipt.allocation.create_time_ms,
                ))
            }
            ObjectCommand::WritePageRangeIdempotent {
                chunk_id,
                offset,
                data,
                idempotency_key,
            } => {
                let receipt = self.write_page_range_idempotent(
                    &chunk_id,
                    offset,
                    &data,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_blockwrite",
                    &format!("{}:{}:{}", chunk_id.volume_id, chunk_id.id, offset),
                    receipt.deduped as u64,
                    receipt.length,
                    "blockserver/idempotent-write",
                    offset,
                ))
            }
            ObjectCommand::WritePageRangesIdempotent {
                writes,
                idempotency_key,
            } => {
                let (stats, receipt) =
                    self.write_page_ranges_idempotent(writes, &idempotency_key)?;
                Ok(metadata_control_metadata(
                    "_blockwrite",
                    "batch",
                    receipt.deduped as u64,
                    stats.payload_bytes,
                    "blockserver/idempotent-batch-write",
                    stats.coalesced_chunk_ranges,
                ))
            }
            ObjectCommand::ApplyPreparedPageBatchWrite { prepared } => {
                let receipt = self.apply_prepared_page_batch_write(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockwrite",
                    "prepared-batch",
                    receipt.deduped as u64,
                    receipt.stats.payload_bytes,
                    if receipt.deduped {
                        "blockserver/prepared-batch-write-deduped"
                    } else {
                        "blockserver/prepared-batch-write"
                    },
                    receipt.stats.coalesced_chunk_ranges,
                ))
            }
            ObjectCommand::DiscardPageRangeIdempotent {
                chunk_id,
                offset,
                length,
                idempotency_key,
            } => {
                let receipt = self.discard_page_range_idempotent(
                    &chunk_id,
                    offset,
                    length,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_blockdiscard",
                    &format!("{}:{}:{}", chunk_id.volume_id, chunk_id.id, offset),
                    receipt.deduped as u64,
                    receipt.length,
                    "blockserver/idempotent-discard",
                    offset,
                ))
            }
            ObjectCommand::DiscardPageRangesIdempotent {
                requests,
                idempotency_key,
            } => {
                let (stats, receipt) =
                    self.discard_page_ranges_idempotent(requests, &idempotency_key)?;
                Ok(metadata_control_metadata(
                    "_blockdiscard",
                    "batch",
                    receipt.deduped as u64,
                    stats.physical_chunk_discard_bytes,
                    "blockserver/idempotent-batch-discard",
                    stats.coalesced_chunk_ranges,
                ))
            }
            ObjectCommand::ApplyPreparedPageBatchDiscard { prepared } => {
                let receipt = self.apply_prepared_page_batch_discard(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockdiscard",
                    "prepared-batch",
                    receipt.deduped as u64,
                    receipt.stats.physical_chunk_discard_bytes,
                    if receipt.deduped {
                        "blockserver/prepared-batch-discard-deduped"
                    } else {
                        "blockserver/prepared-batch-discard"
                    },
                    receipt.stats.coalesced_chunk_ranges,
                ))
            }
            ObjectCommand::ApplyPreparedPageReclaim { prepared } => {
                let report = self.apply_prepared_page_reclaim(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockreclaim",
                    "prepared-aggregate",
                    report.reclaimed_chunks,
                    report.reclaimed_discard_bytes,
                    if report.trim_flow_limited {
                        "blockserver/prepared-reclaim-throttled"
                    } else if report.budget_limited {
                        "blockserver/prepared-reclaim-more"
                    } else {
                        "blockserver/prepared-reclaim"
                    },
                    report.rewritten_bytes,
                ))
            }
            _ => unreachable!("command classified outside StorageRootMaintenance"),
        }
    }
}
