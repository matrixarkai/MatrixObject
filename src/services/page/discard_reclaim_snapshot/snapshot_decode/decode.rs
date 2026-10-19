impl ChunkServiceSnapshot {
    pub fn from_bytes(data: &[u8]) -> Result<Self, ObjectError> {
        let mut cursor = SnapshotByteCursor::new(data);
        let magic = cursor.read_string()?;
        let version = magic
            .strip_prefix("MOSRSBLOCK")
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|version| (1..=23).contains(version))
            .ok_or_else(|| {
                ObjectError::Corruption("block service snapshot byte magic mismatch".to_string())
            })?;
        if magic != format!("MOSRSBLOCK{version}") {
            return Err(ObjectError::Corruption(
                "block service snapshot byte magic mismatch".to_string(),
            ));
        }
        let has_quarantine = version >= 2;
        let has_allocation_receipts = version >= 3;
        let has_write_receipts = version >= 4;
        let has_discard_receipts = version >= 5;
        let has_batch_discard_receipts = version >= 6;
        let has_batch_write_receipts = version >= 7;
        let has_metadata_head_stats = version >= 8;
        let has_repair_counters = version >= 9;
        let has_release_allocation_receipts = version >= 11;
        let has_endpoint_topology = version >= 12;
        let has_route_qos = version >= 13;
        let has_route_qos_read_limits = version >= 14;
        let has_read_heal_counters = version >= 15;
        let has_degraded_route_counters = version >= 16;
        let has_route_control_watermarks = version >= 17;
        let has_read_efficiency_counters = version >= 18;
        let has_batch_allocation_receipts = version >= 19;
        let has_repair_receipts = version >= 20;
        let has_range_repair_receipts = version >= 21;
        let has_audit_repair_receipts = version >= 22;
        let has_sealed_chunks = version >= 23;
        let page_size = cursor.read_u64()?;
        let write_quorum = cursor.read_count("block service snapshot write quorum")?;
        let read_quorum = cursor.read_count("block service snapshot read quorum")?;
        let allocator_chunk_size = cursor.read_u64()?;
        let allocator_next_chunk_id = cursor.read_u64()?;
        let allocator_next_offset = cursor.read_u64()?;
        let receipt_retention = if version >= 10 {
            cursor.read_count("block service snapshot receipt retention")?
        } else {
            DEFAULT_STORAGE_RECEIPT_RETENTION
        };
        let released_allocation_count = cursor.read_u64()?;
        let mut pending_allocations = BTreeMap::new();
        for _ in 0..cursor.read_count("block service snapshot pending allocation count")? {
            let map_allocation_id = cursor.read_string()?;
            let allocation_id = cursor.read_string()?;
            let chunk_id = ChunkId {
                volume_id: cursor.read_u64()?,
                id: cursor.read_u64()?,
            };
            let allocation = ChunkAllocation {
                allocation_id,
                chunk_id,
                offset: cursor.read_u64()?,
                length: cursor.read_u64()?,
                create_time_ms: cursor.read_u64()?,
            };
            pending_allocations.insert(map_allocation_id, allocation);
        }
        let mut allocation_request_receipts = BTreeMap::new();
        if has_allocation_receipts {
            for _ in 0..cursor.read_count("block service snapshot allocation receipt count")? {
                let idempotency_key = cursor.read_string()?;
                let allocation_id = cursor.read_string()?;
                let chunk_id = ChunkId {
                    volume_id: cursor.read_u64()?,
                    id: cursor.read_u64()?,
                };
                let allocation = ChunkAllocation {
                    allocation_id,
                    chunk_id,
                    offset: cursor.read_u64()?,
                    length: cursor.read_u64()?,
                    create_time_ms: cursor.read_u64()?,
                };
                allocation_request_receipts.insert(idempotency_key, allocation);
            }
        }
        let mut batch_allocation_request_receipts = BTreeMap::new();
        if has_batch_allocation_receipts {
            for _ in
                0..cursor.read_count("block service snapshot batch allocation receipt count")?
            {
                let idempotency_key = cursor.read_string()?;
                let allocation_count = cursor
                    .read_count("block service snapshot batch allocation receipt range count")?;
                let mut allocations = Vec::with_capacity(allocation_count);
                for _ in 0..allocation_count {
                    let allocation_id = cursor.read_string()?;
                    let chunk_id = ChunkId {
                        volume_id: cursor.read_u64()?,
                        id: cursor.read_u64()?,
                    };
                    allocations.push(ChunkAllocation {
                        allocation_id,
                        chunk_id,
                        offset: cursor.read_u64()?,
                        length: cursor.read_u64()?,
                        create_time_ms: cursor.read_u64()?,
                    });
                }
                batch_allocation_request_receipts.insert(idempotency_key, allocations);
            }
        }
        let mut release_allocation_request_receipts = BTreeMap::new();
        if has_release_allocation_receipts {
            for _ in
                0..cursor.read_count("block service snapshot release allocation receipt count")?
            {
                let idempotency_key = cursor.read_string()?;
                let allocation_id = cursor.read_string()?;
                let chunk_id = ChunkId {
                    volume_id: cursor.read_u64()?,
                    id: cursor.read_u64()?,
                };
                let allocation = ChunkAllocation {
                    allocation_id,
                    chunk_id,
                    offset: cursor.read_u64()?,
                    length: cursor.read_u64()?,
                    create_time_ms: cursor.read_u64()?,
                };
                release_allocation_request_receipts.insert(idempotency_key, allocation);
            }
        }
        let mut write_request_receipts = BTreeMap::new();
        if has_write_receipts {
            for _ in 0..cursor.read_count("block service snapshot write receipt count")? {
                let idempotency_key = cursor.read_string()?;
                write_request_receipts
                    .insert(idempotency_key, read_page_write_receipt(&mut cursor)?);
            }
        }
        let mut batch_write_request_receipts = BTreeMap::new();
        if has_batch_write_receipts {
            for _ in 0..cursor.read_count("block service snapshot batch write receipt count")? {
                let idempotency_key = cursor.read_string()?;
                batch_write_request_receipts.insert(
                    idempotency_key,
                    read_page_batch_write_receipt(&mut cursor, has_metadata_head_stats)?,
                );
            }
        }
        let mut discard_request_receipts = BTreeMap::new();
        if has_discard_receipts {
            for _ in 0..cursor.read_count("block service snapshot discard receipt count")? {
                let idempotency_key = cursor.read_string()?;
                discard_request_receipts
                    .insert(idempotency_key, read_page_discard_receipt(&mut cursor)?);
            }
        }
        let mut batch_discard_request_receipts = BTreeMap::new();
        if has_batch_discard_receipts {
            for _ in 0..cursor.read_count("block service snapshot batch discard receipt count")? {
                let idempotency_key = cursor.read_string()?;
                batch_discard_request_receipts.insert(
                    idempotency_key,
                    read_page_batch_discard_receipt(&mut cursor, has_metadata_head_stats)?,
                );
            }
        }
        let mut quarantined_replicas = BTreeSet::new();
        if has_quarantine {
            for _ in 0..cursor.read_count("block service snapshot quarantine count")? {
                quarantined_replicas
                    .insert(cursor.read_count("block service snapshot quarantine index")?);
            }
        }
        let (chunk_endpoints, disabled_chunk_endpoints) = if has_endpoint_topology {
            let endpoint_count =
                cursor.read_count("block service snapshot chunk endpoint count")?;
            let mut endpoints = Vec::with_capacity(endpoint_count);
            for _ in 0..endpoint_count {
                endpoints.push(cursor.read_string()?);
            }
            let mut disabled = BTreeSet::new();
            for _ in 0..cursor.read_count("block service snapshot disabled endpoint count")? {
                disabled
                    .insert(cursor.read_count("block service snapshot disabled endpoint index")?);
            }
            (endpoints, disabled)
        } else {
            (Vec::new(), BTreeSet::new())
        };
        let (chunk_route_control_commit_index, chunk_route_control_next_log_index) =
            if has_route_control_watermarks {
                (cursor.read_u64()?, cursor.read_u64()?)
            } else {
                (0, 1)
            };
        let (route_qos_config, route_qos_state) = if has_route_qos {
            (
                read_page_route_qos_config(&mut cursor, has_route_qos_read_limits)?,
                read_page_route_qos_state(&mut cursor, has_route_qos_read_limits)?,
            )
        } else {
            (
                ChunkRouteQosConfig::default(),
                ChunkRouteQosState::default(),
            )
        };
        let mut sealed_chunks = BTreeSet::new();
        if has_sealed_chunks {
            for _ in 0..cursor.read_count("block service snapshot sealed segment count")? {
                sealed_chunks.insert(ChunkId {
                    volume_id: cursor.read_u64()?,
                    id: cursor.read_u64()?,
                });
            }
        }
        let (
            cumulative_quorum_write_degraded_chunks,
            cumulative_quorum_read_degraded_chunks,
            cumulative_quorum_discard_degraded_chunks,
            cumulative_replica_fallback_reads,
        ) = if has_degraded_route_counters {
            (
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_u64()?,
            )
        } else {
            (0, 0, 0, 0)
        };
        let (cumulative_physical_chunk_read_count, cumulative_coalesced_chunk_read_count) =
            if has_read_efficiency_counters {
                (cursor.read_u64()?, cursor.read_u64()?)
            } else {
                (0, 0)
            };
        let (
            cumulative_repair_checked_chunks,
            cumulative_repair_degraded_chunks,
            cumulative_repaired_replicas,
            cumulative_failed_repairs,
        ) = if has_repair_counters {
            (
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_u64()?,
            )
        } else {
            (0, 0, 0, 0)
        };
        let (cumulative_replica_read_repairs, cumulative_replica_read_repair_failures) =
            if has_read_heal_counters {
                (cursor.read_u64()?, cursor.read_u64()?)
            } else {
                (0, 0)
            };
        let replica_count = cursor.read_count("block service snapshot replica count")?;
        let mut replicas = Vec::with_capacity(replica_count);
        for _ in 0..replica_count {
            let replica_len = cursor.read_count("block service snapshot replica length")?;
            replicas.push(ReplicaServiceSnapshot::from_bytes(
                cursor.read_bytes(replica_len)?,
            )?);
        }
        let mut repair_request_receipts = BTreeMap::new();
        if has_repair_receipts {
            for _ in 0..cursor.read_count("block service snapshot repair receipt count")? {
                let idempotency_key = cursor.read_string()?;
                repair_request_receipts
                    .insert(idempotency_key, read_chunk_repair_receipt(&mut cursor)?);
            }
        }
        let mut range_repair_request_receipts = BTreeMap::new();
        if has_range_repair_receipts {
            for _ in 0..cursor.read_count("block service snapshot range repair receipt count")? {
                let idempotency_key = cursor.read_string()?;
                range_repair_request_receipts.insert(
                    idempotency_key,
                    read_page_range_repair_receipt(&mut cursor)?,
                );
            }
        }
        let mut audit_repair_request_receipts = BTreeMap::new();
        if has_audit_repair_receipts {
            for _ in 0..cursor.read_count("block service snapshot audit repair receipt count")? {
                let idempotency_key = cursor.read_string()?;
                audit_repair_request_receipts.insert(
                    idempotency_key,
                    read_page_audit_repair_receipt(&mut cursor)?,
                );
            }
        }
        let checksum = cursor.read_string()?;
        let checksum_end = cursor.position();
        let envelope_checksum = cursor.read_string()?;
        if cursor.position() != data.len() {
            return Err(ObjectError::Corruption(
                "block service snapshot byte payload has trailing data".to_string(),
            ));
        }
        if !etag_matches(&data[..checksum_end], &envelope_checksum) {
            return Err(ObjectError::Corruption(
                "block service snapshot byte envelope checksum mismatch".to_string(),
            ));
        }
        let mut snapshot = Self {
            page_size,
            write_quorum,
            read_quorum,
            allocator_chunk_size,
            allocator_next_chunk_id,
            allocator_next_offset,
            receipt_retention,
            pending_allocations,
            allocation_request_receipts,
            batch_allocation_request_receipts,
            release_allocation_request_receipts,
            write_request_receipts,
            batch_write_request_receipts,
            discard_request_receipts,
            batch_discard_request_receipts,
            repair_request_receipts,
            range_repair_request_receipts,
            audit_repair_request_receipts,
            released_allocation_count,
            quarantined_replicas,
            chunk_endpoints,
            disabled_chunk_endpoints,
            chunk_route_control_commit_index,
            chunk_route_control_next_log_index,
            route_qos_config,
            route_qos_state,
            sealed_chunks,
            cumulative_quorum_write_degraded_chunks,
            cumulative_quorum_read_degraded_chunks,
            cumulative_quorum_discard_degraded_chunks,
            cumulative_replica_fallback_reads,
            cumulative_physical_chunk_read_count,
            cumulative_coalesced_chunk_read_count,
            cumulative_repair_checked_chunks,
            cumulative_repair_degraded_chunks,
            cumulative_repaired_replicas,
            cumulative_failed_repairs,
            cumulative_replica_read_repairs,
            cumulative_replica_read_repair_failures,
            replicas,
            checksum,
        };
        if version != 19 && version != 20 && version != 21 && version != 22 {
            if snapshot.chunk_endpoints.is_empty() {
                snapshot.chunk_endpoints = default_chunk_endpoints(snapshot.replicas.len());
            }
            snapshot.checksum = snapshot.checksum_material_hash();
        }
        snapshot.validate()?;
        Ok(snapshot)
    }
}
