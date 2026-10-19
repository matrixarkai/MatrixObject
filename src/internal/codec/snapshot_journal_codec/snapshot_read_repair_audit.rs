pub(crate) fn read_page_route_control_proof(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkRouteControlProof, ObjectError> {
    Ok(ChunkRouteControlProof {
        control_receipt: cursor.read_u64()? != 0,
        state_format: cursor.read_string()?,
        state_bytes: cursor.read_u64()?,
        state_checksum: cursor.read_string()?,
        chunk_route_control_commit_index: cursor.read_u64()?,
        chunk_route_control_next_log_index: cursor.read_u64()?,
        chunk_endpoint_count: cursor.read_u64()?,
        disabled_chunk_endpoint_count: cursor.read_u64()?,
        write_quorum: cursor.read_u64()?,
        configured_write_quorum: cursor.read_u64()?,
        read_quorum: cursor.read_u64()?,
        configured_read_quorum: cursor.read_u64()?,
        disabled_replica_count: cursor.read_u64()?,
        qos_window_ms: cursor.read_u64()?,
        qos_max_read_ops_per_window: cursor.read_u64()?,
        qos_max_read_bytes_per_window: cursor.read_u64()?,
        qos_max_write_ops_per_window: cursor.read_u64()?,
        qos_max_write_bytes_per_window: cursor.read_u64()?,
        qos_max_discard_ops_per_window: cursor.read_u64()?,
        qos_max_discard_bytes_per_window: cursor.read_u64()?,
        qos_throttled_ops: cursor.read_u64()?,
        qos_throttled_bytes: cursor.read_u64()?,
        receipt_retention: cursor.read_u64()?,
        primary_batch_read_repair_probe_limit: cursor.read_u64()?,
        cumulative_quorum_write_degraded_chunks: cursor.read_u64()?,
        cumulative_quorum_read_degraded_chunks: cursor.read_u64()?,
        cumulative_quorum_discard_degraded_chunks: cursor.read_u64()?,
        cumulative_replica_fallback_reads: cursor.read_u64()?,
        cumulative_physical_chunk_read_count: cursor.read_u64()?,
        cumulative_coalesced_chunk_read_count: cursor.read_u64()?,
        cumulative_repair_checked_chunks: cursor.read_u64()?,
        cumulative_repair_degraded_chunks: cursor.read_u64()?,
        cumulative_repaired_replicas: cursor.read_u64()?,
        cumulative_failed_repairs: cursor.read_u64()?,
        cumulative_replica_read_repairs: cursor.read_u64()?,
        cumulative_replica_read_repair_failures: cursor.read_u64()?,
    })
}

pub(crate) fn read_chunk_repair_report(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaRepairReport, ObjectError> {
    let has_chunk_id = cursor.read_u64()? != 0;
    let chunk_id = if has_chunk_id {
        Some(read_chunk_id(cursor)?)
    } else {
        None
    };
    Ok(ReplicaRepairReport {
        chunk_id,
        checked_chunks: cursor.read_u64()?,
        healthy_chunks: cursor.read_u64()?,
        degraded_chunks: cursor.read_u64()?,
        repaired_chunks: cursor.read_u64()?,
        repaired_replicas: cursor.read_u64()?,
        failed_repairs: cursor.read_u64()?,
        missing_replicas: cursor.read_u64()?,
        corrupt_replicas: cursor.read_u64()?,
        divergent_replicas: cursor.read_u64()?,
        frozen_replicas: cursor.read_u64()?,
        quarantined_replicas: cursor.read_u64()?,
        repairable_replicas: cursor.read_u64()?,
        unrepaired_chunks: cursor.read_u64()?,
        committed: cursor.read_u64()? != 0,
        first_error: if cursor.read_u64()? != 0 {
            Some(cursor.read_string()?)
        } else {
            None
        },
        route_control_proof: read_page_route_control_proof(cursor)?,
        expected_route_control_checksum: cursor.read_string()?,
    })
}

pub(crate) fn read_chunk_repair_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaRepairReceipt, ObjectError> {
    Ok(ReplicaRepairReceipt {
        chunk_id: read_chunk_id(cursor)?,
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        report: read_chunk_repair_report(cursor)?,
        deduped: false,
    })
}

pub(crate) fn read_repair_stats(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<RepairStats, ObjectError> {
    Ok(RepairStats {
        checked_chunks: cursor.read_u64()?,
        repaired_replicas: cursor.read_u64()?,
        failed_repairs: cursor.read_u64()?,
        degraded_chunks: cursor.read_u64()?,
    })
}

pub(crate) fn read_bounded_repair_report(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<BoundedRepairReport, ObjectError> {
    Ok(BoundedRepairReport {
        total_chunk_count: cursor.read_u64()?,
        selected_chunk_count: cursor.read_u64()?,
        skipped_chunk_count: cursor.read_u64()?,
        candidate_repair_bytes: cursor.read_u64()?,
        selected_repair_bytes: cursor.read_u64()?,
        skipped_repair_bytes: cursor.read_u64()?,
        start_chunk_index: cursor.read_u64()?,
        next_chunk_index: cursor.read_u64()?,
        has_more: cursor.read_u64()? != 0,
        stats: read_repair_stats(cursor)?,
        route_control_proof: read_page_route_control_proof(cursor)?,
        expected_route_control_checksum: cursor.read_string()?,
    })
}

pub(crate) fn read_page_range_repair_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkRangeRepairReceipt, ObjectError> {
    Ok(ChunkRangeRepairReceipt {
        slab_id: read_slab_id(cursor)?,
        offset: cursor.read_u64()?,
        length: cursor.read_u64()?,
        max_repair_chunks: cursor.read_u64()?,
        start_chunk_index: cursor.read_u64()?,
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        report: read_bounded_repair_report(cursor)?,
        deduped: false,
    })
}

pub(crate) fn read_page_replica_audit_item(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkReplicaAuditItem, ObjectError> {
    Ok(ChunkReplicaAuditItem {
        chunk_id: read_chunk_id(cursor)?,
        replica_index: cursor.read_count("block replica audit item replica index")?,
        status: cursor.read_string()?,
        logical_length: cursor.read_u64()?,
        write_sequence: cursor.read_u64()?,
        checksum: cursor.read_string()?,
        discarded_range_count: cursor.read_u64()?,
        frozen: cursor.read_u64()? != 0,
        quarantined: cursor.read_u64()? != 0,
    })
}

pub(crate) fn read_page_replica_audit_page(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkReplicaAuditChunk, ObjectError> {
    let item_count = cursor.read_count("block replica audit page item count")?;
    let mut items = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        items.push(read_page_replica_audit_item(cursor)?);
    }
    let marker = if cursor.read_u64()? != 0 {
        Some(read_chunk_id(cursor)?)
    } else {
        None
    };
    let next_marker = if cursor.read_u64()? != 0 {
        Some(read_chunk_id(cursor)?)
    } else {
        None
    };
    Ok(ChunkReplicaAuditChunk {
        items,
        marker,
        next_marker,
        scanned_chunks: cursor.read_u64()?,
        healthy_chunks: cursor.read_u64()?,
        degraded_chunks: cursor.read_u64()?,
        missing_replicas: cursor.read_u64()?,
        corrupt_replicas: cursor.read_u64()?,
        divergent_replicas: cursor.read_u64()?,
        quarantined_replicas: cursor.read_u64()?,
        frozen_replicas: cursor.read_u64()?,
        payload_bytes: cursor.read_u64()?,
        budget_limited: cursor.read_u64()? != 0,
        has_more: cursor.read_u64()? != 0,
    })
}

pub(crate) fn read_page_replica_audit_repair_report(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkReplicaAuditRepairReport, ObjectError> {
    Ok(ChunkReplicaAuditRepairReport {
        audit: read_page_replica_audit_page(cursor)?,
        candidate_chunks: cursor.read_u64()?,
        repaired_chunks: cursor.read_u64()?,
        unrepaired_chunks: cursor.read_u64()?,
        stats: read_repair_stats(cursor)?,
        repair_needed: cursor.read_u64()? != 0,
        repair_can_apply: cursor.read_u64()? != 0,
        repair_reason: cursor.read_string()?,
        route_control_proof: read_page_route_control_proof(cursor)?,
        expected_route_control_checksum: cursor.read_string()?,
    })
}

pub(crate) fn read_page_audit_repair_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkAuditRepairReceipt, ObjectError> {
    let after_chunk_id = if cursor.read_u64()? != 0 {
        Some(read_chunk_id(cursor)?)
    } else {
        None
    };
    Ok(ChunkAuditRepairReceipt {
        after_chunk_id,
        max_chunks: cursor.read_u64()?,
        max_payload_bytes: cursor.read_u64()?,
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        report: read_page_replica_audit_repair_report(cursor)?,
        deduped: false,
    })
}

