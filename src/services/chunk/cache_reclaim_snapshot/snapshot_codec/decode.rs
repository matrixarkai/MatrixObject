impl ChunkServiceSnapshot {
    pub fn from_bytes(data: &[u8]) -> Result<Self, ObjectError> {
        let mut cursor = SnapshotByteCursor::new(data);
        let magic = cursor.read_string()?;
        let version = magic
            .strip_prefix("MOSRSCHUNK")
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|version| (1..=22).contains(version))
            .ok_or_else(|| {
                ObjectError::Corruption("chunk service snapshot byte magic mismatch".to_string())
            })?;
        if magic != format!("MOSRSCHUNK{version}") {
            return Err(ObjectError::Corruption(
                "chunk service snapshot byte magic mismatch".to_string(),
            ));
        }
        let has_service_mode = version >= 4;
        let (accepting_writes, draining) = if has_service_mode {
            (cursor.read_u64()? != 0, cursor.read_u64()? != 0)
        } else {
            (true, false)
        };
        let frozen = if version >= 2 {
            cursor.read_u64()? != 0
        } else {
            false
        };
        let (capacity_bytes, capacity_rejected_writes) = if version >= 3 {
            (cursor.read_u64()?, cursor.read_u64()?)
        } else {
            (0, 0)
        };
        let service_mode_rejected_mutations = if has_service_mode {
            cursor.read_u64()?
        } else {
            0
        };
        let (
            metadata_dirty,
            metadata_dirty_mutations,
            metadata_checkpoint_interval,
            defer_metadata_checkpoints,
            deferred_metadata_checkpoint_pending,
            deferred_metadata_checkpoint_requests,
            deferred_metadata_checkpoint_runs,
        ) = if version >= 19 {
            (
                cursor.read_u64()? != 0,
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_u64()? != 0,
                cursor.read_u64()? != 0,
                cursor.read_u64()?,
                cursor.read_u64()?,
            )
        } else {
            (false, 0, 1, false, false, 0, 0)
        };
        let (
            trim_flow_rate_bytes_per_sec,
            trim_flow_burst_bytes,
            trim_flow_available_bytes,
            trim_flow_rate_ops_per_sec,
            trim_flow_burst_ops,
            trim_flow_available_ops,
            trim_flow_last_refill_ms,
            trim_flow_throttled_reclaims,
            trim_flow_throttled_ops,
            trim_flow_throttled_bytes,
            partial_reclaim_failures,
            last_partial_reclaim_failure_status,
        ) = if version >= 5 {
            let trim_flow_rate_bytes_per_sec = cursor.read_u64()?;
            let trim_flow_burst_bytes = cursor.read_u64()?;
            let trim_flow_available_bytes = cursor.read_u64()?;
            let (trim_flow_rate_ops_per_sec, trim_flow_burst_ops, trim_flow_available_ops) =
                if version >= 16 {
                    (cursor.read_u64()?, cursor.read_u64()?, cursor.read_u64()?)
                } else {
                    (0, 0, 0)
                };
            let trim_flow_last_refill_ms = cursor.read_u64()?;
            let trim_flow_throttled_reclaims = cursor.read_u64()?;
            let trim_flow_throttled_ops = if version >= 17 { cursor.read_u64()? } else { 0 };
            let trim_flow_throttled_bytes = cursor.read_u64()?;
            let partial_reclaim_failures = if version >= 20 { cursor.read_u64()? } else { 0 };
            let last_partial_reclaim_failure_status = if version >= 21 {
                cursor.read_string()?
            } else {
                "ok".to_string()
            };
            (
                trim_flow_rate_bytes_per_sec,
                trim_flow_burst_bytes,
                trim_flow_available_bytes,
                trim_flow_rate_ops_per_sec,
                trim_flow_burst_ops,
                trim_flow_available_ops,
                trim_flow_last_refill_ms,
                trim_flow_throttled_reclaims,
                trim_flow_throttled_ops,
                trim_flow_throttled_bytes,
                partial_reclaim_failures,
                last_partial_reclaim_failure_status,
            )
        } else {
            (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "ok".to_string())
        };
        let (
            physical_write_count,
            physical_write_bytes,
            physical_discard_ranges,
            physical_discard_bytes,
        ) = if version >= 9 {
            (
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_u64()?,
            )
        } else {
            (0, 0, 0, 0)
        };
        let (compressed_chunk_count, compression_saved_bytes) = if version >= 13 {
            (cursor.read_u64()?, cursor.read_u64()?)
        } else {
            (0, 0)
        };
        let (ec_reconstructed_reads, ec_reconstruction_failures) = if version >= 12 {
            (cursor.read_u64()?, cursor.read_u64()?)
        } else {
            (0, 0)
        };
        let (chunk_mutation_control_commit_index, chunk_mutation_control_next_log_index) =
            if version >= 14 {
                (cursor.read_u64()?, cursor.read_u64()?)
            } else {
                (0, 1)
            };
        let has_mutation_qos = version >= 11;
        let (mutation_qos_config, mutation_qos_state) = if has_mutation_qos {
            (
                read_chunk_mutation_qos_config(&mut cursor)?,
                read_chunk_mutation_qos_state(&mut cursor)?,
            )
        } else {
            (
                ChunkMutationQosConfig::default(),
                ChunkMutationQosState::default(),
            )
        };
        let receipt_retention = if version >= 10 {
            cursor.read_count("chunk service snapshot receipt retention")?
        } else {
            DEFAULT_STORAGE_RECEIPT_RETENTION
        };
        let mut chunks = BTreeMap::new();
        for _ in 0..cursor.read_count("chunk service snapshot chunk count")? {
            let slab_id = SlabId {
                volume_id: cursor.read_u64()?,
                id: cursor.read_u64()?,
            };
            let chunk_id = ChunkId {
                slab_id,
                chunk_index: cursor.read_u64()?,
            };
            let logical_length = cursor.read_u64()?;
            let write_sequence = cursor.read_u64()?;
            let checksum = cursor.read_string()?;
            let data_len = cursor.read_count("chunk service snapshot payload length")?;
            let data = std::sync::Arc::new(cursor.read_bytes(data_len)?.to_vec());
            let ec_parity_fragment = if version >= 22 {
                let parity_len = cursor.read_count("chunk service snapshot EC parity length")?;
                std::sync::Arc::new(cursor.read_bytes(parity_len)?.to_vec())
            } else if version >= 6 {
                // v6..21 carried a verbatim mirror of the payload ahead of the
                // parity. Skip the mirror; the parity is all that is kept now.
                let data_shard_count =
                    cursor.read_count("chunk service snapshot EC data shard count")?;
                for _ in 0..data_shard_count {
                    let shard_len =
                        cursor.read_count("chunk service snapshot EC data shard length")?;
                    let _ = cursor.read_bytes(shard_len)?;
                }
                let parity_len = cursor.read_count("chunk service snapshot EC parity length")?;
                std::sync::Arc::new(cursor.read_bytes(parity_len)?.to_vec())
            } else {
                std::sync::Arc::new(chunk_ec_parity_from_data(&data))
            };
            // The shard count is derived from the chunk size, so a snapshot written
            // under a different shard-size floor stores a sidecar of a different
            // length. Validation recomputes the sidecar from the payload, which the
            // snapshot carries in full, so re-derive a mismatched one instead of
            // letting it fail that check and make the snapshot unreadable.
            let ec_parity_fragment = if ec_parity_fragment.is_empty() && !data.is_empty() {
                // Deliberately absent, from a replica that keeps none. Deriving one here
                // would put back exactly what that replica declined to store.
                ec_parity_fragment
            } else if ec_parity_fragment.len() == chunk_ec_fragment_len(data.len()) {
                ec_parity_fragment
            } else {
                std::sync::Arc::new(chunk_ec_parity_from_data(&data))
            };
            let discard_range_count =
                cursor.read_count("chunk service snapshot discard range count")?;
            let mut discarded_ranges = Vec::with_capacity(discard_range_count);
            for _ in 0..discard_range_count {
                discarded_ranges.push(DataRange {
                    offset: cursor.read_u64()?,
                    length: cursor.read_u64()?,
                });
            }
            chunks.insert(
                chunk_id,
                std::sync::Arc::new(ChunkState {
                    logical_length,
                    write_sequence,
                    checksum,
                    data,
                    ec_parity_fragment,
                    discarded_ranges,
                }),
            );
        }
        let mut write_request_receipts = BTreeMap::new();
        let mut discard_request_receipts = BTreeMap::new();
        if version >= 8 {
            for _ in 0..cursor.read_count("chunk service snapshot write receipt count")? {
                let idempotency_key = cursor.read_string()?;
                write_request_receipts
                    .insert(idempotency_key, read_chunk_write_receipt(&mut cursor)?);
            }
            for _ in 0..cursor.read_count("chunk service snapshot discard receipt count")? {
                let idempotency_key = cursor.read_string()?;
                discard_request_receipts
                    .insert(idempotency_key, read_chunk_discard_receipt(&mut cursor)?);
            }
        }
        let mut batch_write_request_receipts = BTreeMap::new();
        let mut batch_discard_request_receipts = BTreeMap::new();
        if version >= 7 {
            for _ in 0..cursor.read_count("chunk service snapshot batch write receipt count")? {
                let idempotency_key = cursor.read_string()?;
                batch_write_request_receipts.insert(
                    idempotency_key,
                    read_chunk_batch_write_receipt(&mut cursor)?,
                );
            }
            for _ in 0..cursor.read_count("chunk service snapshot batch discard receipt count")? {
                let idempotency_key = cursor.read_string()?;
                batch_discard_request_receipts.insert(
                    idempotency_key,
                    read_chunk_batch_discard_receipt(&mut cursor)?,
                );
            }
        }
        let mut state_install_request_receipts = BTreeMap::new();
        let mut reclaim_request_receipts = BTreeMap::new();
        let mut repair_request_receipts = BTreeMap::new();
        if version >= 15 {
            for _ in 0..cursor.read_count("chunk service snapshot state install receipt count")? {
                let idempotency_key = cursor.read_string()?;
                state_install_request_receipts.insert(
                    idempotency_key,
                    read_chunk_state_install_receipt(&mut cursor)?,
                );
            }
            for _ in 0..cursor.read_count("chunk service snapshot reclaim receipt count")? {
                let idempotency_key = cursor.read_string()?;
                reclaim_request_receipts.insert(
                    idempotency_key,
                    read_chunk_reclaim_receipt(&mut cursor, version)?,
                );
            }
        }
        if version >= 18 {
            for _ in 0..cursor.read_count("chunk service snapshot repair receipt count")? {
                let idempotency_key = cursor.read_string()?;
                repair_request_receipts
                    .insert(idempotency_key, read_chunk_repair_receipt(&mut cursor)?);
            }
        }
        let checksum = cursor.read_string()?;
        let checksum_end = cursor.position();
        let envelope_checksum = cursor.read_string()?;
        if cursor.position() != data.len() {
            return Err(ObjectError::Corruption(
                "chunk service snapshot byte payload has trailing data".to_string(),
            ));
        }
        if !etag_matches(&data[..checksum_end], &envelope_checksum) {
            return Err(ObjectError::Corruption(
                "chunk service snapshot byte envelope checksum mismatch".to_string(),
            ));
        }
        let mut snapshot = Self {
            chunks,
            receipt_retention,
            write_request_receipts,
            discard_request_receipts,
            batch_write_request_receipts,
            batch_discard_request_receipts,
            state_install_request_receipts,
            reclaim_request_receipts,
            repair_request_receipts,
            accepting_writes,
            draining,
            frozen,
            capacity_bytes,
            capacity_rejected_writes,
            service_mode_rejected_mutations,
            metadata_dirty,
            metadata_dirty_mutations,
            metadata_checkpoint_interval,
            defer_metadata_checkpoints,
            deferred_metadata_checkpoint_pending,
            deferred_metadata_checkpoint_requests,
            deferred_metadata_checkpoint_runs,
            trim_flow_rate_bytes_per_sec,
            trim_flow_burst_bytes,
            trim_flow_available_bytes,
            trim_flow_rate_ops_per_sec,
            trim_flow_burst_ops,
            trim_flow_available_ops,
            trim_flow_last_refill_ms,
            trim_flow_throttled_reclaims,
            trim_flow_throttled_ops,
            trim_flow_throttled_bytes,
            partial_reclaim_failures,
            last_partial_reclaim_failure_status,
            physical_write_count,
            physical_write_bytes,
            physical_discard_ranges,
            physical_discard_bytes,
            compressed_chunk_count,
            compression_saved_bytes,
            ec_reconstructed_reads,
            ec_reconstruction_failures,
            chunk_mutation_control_commit_index,
            chunk_mutation_control_next_log_index,
            mutation_qos_config,
            mutation_qos_state,
            checksum,
        };
        if version < 15 || version > 21 {
            snapshot.checksum = snapshot.checksum_material_hash();
        }
        snapshot.validate()?;
        Ok(snapshot)
    }

}
