impl MatrixObjectStore {
    fn apply_page_control_command(
        &mut self,
        command: ObjectCommand,
        raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ConfigureChunkReplicaTrimFlow {
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                now_ms,
            } => {
                self.configure_chunk_replica_trim_flow(
                    replica_index,
                    rate_bytes_per_sec,
                    burst_bytes,
                    now_ms,
                )?;
                Ok(self.chunk_trim_flow_control_metadata(
                    rate_bytes_per_sec,
                    Some(replica_index),
                    "chunkserver/replica-trim-flow",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyPreparedChunkTrimFlow { prepared } => {
                self.apply_prepared_chunk_trim_flow(prepared.clone())?;
                Ok(self.prepared_chunk_trim_flow_control_metadata(
                    &prepared,
                    "chunkserver/prepared-trim-flow",
                ))
            }
            ObjectCommand::ConfigureChunkMutationQos { config } => {
                let config = self.configure_chunk_mutation_qos(config)?;
                Ok(self.chunk_mutation_qos_control_metadata(
                    &config,
                    None,
                    "chunkserver/mutation-qos-config",
                ))
            }
            ObjectCommand::ConfigureChunkReplicaMutationQos {
                replica_index,
                config,
            } => {
                let config =
                    self.configure_chunk_replica_mutation_qos(replica_index, config.clone())?;
                Ok(self.chunk_mutation_qos_control_metadata(
                    &config,
                    Some(replica_index),
                    "chunkserver/replica-mutation-qos-config",
                ))
            }
            ObjectCommand::ApplyPreparedChunkMutationQos { prepared } => {
                self.apply_prepared_chunk_mutation_qos(prepared.clone())?;
                Ok(self.prepared_chunk_mutation_qos_control_metadata(
                    &prepared,
                    "chunkserver/prepared-mutation-qos-config",
                ))
            }
            ObjectCommand::ConfigureChunkReadCache {
                max_bytes_per_replica,
                max_cacheable_read_bytes_per_replica,
            } => {
                self.configure_chunk_read_cache_with_stream_bypass(
                    max_bytes_per_replica,
                    max_cacheable_read_bytes_per_replica,
                )?;
                Ok(self.chunk_read_cache_control_metadata(
                    None,
                    max_bytes_per_replica,
                    max_cacheable_read_bytes_per_replica,
                    None,
                    "chunkserver/read-cache-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigureChunkReplicaReadCache {
                replica_index,
                max_bytes_per_replica,
                max_cacheable_read_bytes_per_replica,
            } => {
                self.configure_chunk_replica_read_cache_with_stream_bypass(
                    replica_index,
                    max_bytes_per_replica,
                    max_cacheable_read_bytes_per_replica,
                )?;
                Ok(self.chunk_read_cache_control_metadata(
                    Some(replica_index),
                    max_bytes_per_replica,
                    max_cacheable_read_bytes_per_replica,
                    Some(1),
                    "chunkserver/replica-read-cache-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedChunkReadCache { prepared } => {
                self.apply_prepared_chunk_read_cache(prepared.clone())?;
                Ok(self.chunk_read_cache_control_metadata(
                    prepared.target_replica_index,
                    prepared.max_bytes_per_replica,
                    prepared.max_cacheable_read_bytes_per_replica,
                    Some(prepared.chunk_prepared_replica_count),
                    "chunkserver/prepared-read-cache-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigureChunkReplicaReceiptRetention {
                replica_index,
                retention,
            } => {
                let trimmed =
                    self.configure_chunk_replica_receipt_retention(replica_index, retention)?;
                Ok(metadata_control_metadata(
                    "_chunkretention",
                    &replica_index.to_string(),
                    retention as u64,
                    trimmed,
                    "chunkserver/receipt-retention-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigureChunkReceiptRetention { retention } => {
                let (trimmed, _) = self.configure_chunk_receipt_retention(retention)?;
                Ok(metadata_control_metadata(
                    "_chunkretention",
                    "all_replicas",
                    retention as u64,
                    trimmed,
                    "chunkserver/all-replica-receipt-retention-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedChunkReceiptRetention { prepared } => {
                let trimmed = self.apply_prepared_chunk_receipt_retention(prepared.clone())?;
                Ok(metadata_control_metadata(
                    "_chunkretention",
                    prepared
                        .target_replica_index
                        .map_or("prepared-all-replicas".to_string(), |index| {
                            format!("prepared-replica-{index}")
                        })
                        .as_str(),
                    prepared.retention as u64,
                    trimmed,
                    "chunkserver/prepared-receipt-retention-config",
                    prepared.chunk_prepared_replica_count,
                ))
            }
            ObjectCommand::ApplyPreparedChunkBatchWrite { prepared } => {
                let receipt = self.apply_prepared_chunk_batch_write(prepared)?;
                Ok(metadata_control_metadata(
                    "_chunkwrite",
                    &format!(
                        "{}:{}",
                        receipt.chunk_id.slab_id.id, receipt.chunk_id.page_index
                    ),
                    receipt.stats.physical_write_bytes,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "chunkserver/prepared-batch-write-deduped"
                    } else {
                        "chunkserver/prepared-batch-write"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedChunkBatchDiscard { prepared } => {
                let receipt = self.apply_prepared_chunk_batch_discard(prepared)?;
                Ok(metadata_control_metadata(
                    "_chunkdiscard",
                    &format!(
                        "{}:{}",
                        receipt.chunk_id.slab_id.id, receipt.chunk_id.page_index
                    ),
                    receipt.stats.physical_discard_bytes,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "chunkserver/prepared-batch-discard-deduped"
                    } else {
                        "chunkserver/prepared-batch-discard"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedChunkReclaim {
                replica_index,
                prepared,
            } => {
                let report = self.apply_prepared_chunk_reclaim(replica_index, prepared)?;
                Ok(metadata_control_metadata(
                    "_chunkreclaim",
                    &format!("prepared-replica-{replica_index}"),
                    report.reclaimed_chunks,
                    report.reclaimed_discard_bytes,
                    if report.trim_flow_limited {
                        "chunkserver/prepared-reclaim-throttled"
                    } else if report.budget_limited {
                        "chunkserver/prepared-reclaim-more"
                    } else {
                        "chunkserver/prepared-reclaim"
                    },
                    report.trimmed_bytes,
                ))
            }
            ObjectCommand::ConfigurePageQuorum {
                write_quorum,
                read_quorum,
            } => {
                self.configure_page_quorum(write_quorum, read_quorum)?;
                Ok(metadata_control_metadata(
                    "_blockquorum",
                    "quorum",
                    write_quorum as u64,
                    read_quorum as u64,
                    "blockserver/quorum-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedPageQuorum { prepared } => {
                self.apply_prepared_page_quorum(prepared.clone())?;
                Ok(metadata_control_metadata(
                    "_blockquorum",
                    "prepared-configure",
                    prepared.write_quorum as u64,
                    prepared.read_quorum as u64,
                    "blockserver/prepared-quorum-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigurePageReceiptRetention { retention } => {
                let trimmed = self.configure_page_receipt_retention(retention)?;
                Ok(metadata_control_metadata(
                    "_blockretention",
                    "allocation",
                    retention as u64,
                    trimmed,
                    "blockserver/receipt-retention-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedPageReceiptRetention { prepared } => {
                let trimmed = self.apply_prepared_page_receipt_retention(prepared.clone())?;
                Ok(metadata_control_metadata(
                    "_blockretention",
                    "prepared-allocation",
                    prepared.retention as u64,
                    trimmed,
                    "blockserver/prepared-receipt-retention-config",
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigurePageRouteQos { config } => {
                let config = if let Some(raft_entry_index) = raft_entry_index {
                    self.chunk_service
                        .configure_route_qos_at_commit(config, raft_entry_index)?
                } else {
                    self.configure_page_route_qos(config)?
                };
                let proof = self.chunk_service.route_control_proof();
                Ok(metadata_control_metadata(
                    "_blockqos",
                    &proof.state_format,
                    config.window_ms,
                    config
                        .max_write_ops_per_window
                        .saturating_add(config.max_discard_ops_per_window),
                    "blockserver/route-qos-config",
                    config
                        .max_write_bytes_per_window
                        .saturating_add(config.max_discard_bytes_per_window),
                ))
            }
            ObjectCommand::ApplyPreparedPageRouteQos { prepared } => {
                let config = self.apply_prepared_page_route_qos(prepared)?;
                let proof = self.chunk_service.route_control_proof();
                Ok(metadata_control_metadata(
                    "_blockqos",
                    &proof.state_format,
                    config.window_ms,
                    config
                        .max_write_ops_per_window
                        .saturating_add(config.max_discard_ops_per_window),
                    "blockserver/prepared-route-qos-config",
                    config
                        .max_write_bytes_per_window
                        .saturating_add(config.max_discard_bytes_per_window),
                ))
            }
            ObjectCommand::SealPageSlab {
                slab_id,
                expected_route_control_checksum,
            } => {
                self.chunk_service.check_expected_route_control_checksum(
                    (!expected_route_control_checksum.is_empty())
                        .then_some(expected_route_control_checksum.as_str()),
                )?;
                let inserted = self.chunk_service.seal_slab(slab_id.clone())?;
                let proof = self.chunk_service.route_control_proof();
                Ok(metadata_control_metadata(
                    "_blockseal",
                    &format!("{}:{}", slab_id.volume_id, slab_id.id),
                    self.chunk_service.sealed_slab_count(),
                    inserted as u64,
                    "blockserver/seal-segment",
                    proof.chunk_route_control_commit_index,
                ))
            }
            ObjectCommand::ApplyPreparedPageAllocationReservation { prepared } => {
                let receipt = self.apply_prepared_page_allocation_reservation(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockalloc",
                    &receipt.allocation.allocation_id,
                    receipt.allocation.length,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "blockserver/prepared-allocation-reservation-deduped"
                    } else {
                        "blockserver/prepared-allocation-reservation"
                    },
                    receipt.allocation.create_time_ms,
                ))
            }
            ObjectCommand::ApplyPreparedPageBatchAllocationReservation { prepared } => {
                let receipt = self.apply_prepared_page_batch_allocation_reservation(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockalloc",
                    &receipt.idempotency_key,
                    receipt.allocations.iter().map(|item| item.length).sum(),
                    receipt.allocations.len() as u64,
                    "blockserver/prepared-batch-allocation-reservation",
                    receipt
                        .allocations
                        .first()
                        .map(|item| item.create_time_ms)
                        .unwrap_or_default(),
                ))
            }
            ObjectCommand::ReleasePageAllocationIdempotent {
                allocation_id,
                idempotency_key,
            } => {
                let receipt =
                    self.release_page_allocation_idempotent(&allocation_id, &idempotency_key)?;
                Ok(metadata_control_metadata(
                    "_blockalloc",
                    &receipt.allocation.allocation_id,
                    receipt.allocation.length,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "blockserver/allocation-release-idempotent-deduped"
                    } else {
                        "blockserver/allocation-release-idempotent"
                    },
                    receipt.allocation.create_time_ms,
                ))
            }
            ObjectCommand::AllocatePageRangesIdempotent {
                lengths,
                now_ms,
                idempotency_key,
            } => {
                let receipt =
                    self.allocate_page_ranges_idempotent(lengths, now_ms, &idempotency_key)?;
                Ok(metadata_control_metadata(
                    "_blockalloc",
                    &receipt.idempotency_key,
                    receipt.allocations.iter().map(|item| item.length).sum(),
                    receipt.allocations.len() as u64,
                    if receipt.deduped {
                        "blockserver/batch-allocation-idempotent-deduped"
                    } else {
                        "blockserver/batch-allocation-idempotent"
                    },
                    receipt
                        .allocations
                        .first()
                        .map(|item| item.create_time_ms)
                        .unwrap_or_default(),
                ))
            }
            _ => unreachable!("command classified outside BlockControl"),
        }
    }
}
