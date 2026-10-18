pub(crate) fn slab_compaction_request_checksum(
    slab_id: &SlabId,
    min_stale_bytes: u64,
    update_time_ms: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-SEGMENT-COMPACTION-REQUEST1");
    append_slab_id(&mut material, slab_id);
    append_u64(&mut material, min_stale_bytes);
    append_u64(&mut material, update_time_ms);
    etag(&material)
}

pub(crate) fn prepared_slab_compaction_checksum(prepared: &PreparedSlabCompaction) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-SEGMENT-COMPACTION3");
    append_slab_id(&mut material, &prepared.slab_id);
    append_u64(&mut material, prepared.min_stale_bytes);
    append_u64(&mut material, prepared.update_time_ms);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.after_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.live_objects.len() as u64);
    for object_id in &prepared.live_objects {
        append_object_id(&mut material, object_id);
    }
    append_u64(&mut material, prepared.source_stale_ranges.len() as u64);
    for range in &prepared.source_stale_ranges {
        append_range(&mut material, range);
    }
    append_u64(&mut material, prepared.source_discarded_ranges.len() as u64);
    for range in &prepared.source_discarded_ranges {
        append_range(&mut material, range);
    }
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    append_string(
        &mut material,
        &prepared.expected_object_gc_report_proof_checksum,
    );
    etag(&material)
}

pub(crate) fn append_prepared_slab_compaction_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedSlabCompactionEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_slab_id(out, &envelope.slab_id);
    append_u64(out, envelope.min_stale_bytes);
    append_u64(out, envelope.update_time_ms);
    append_u64(out, envelope.live_object_count);
    append_u64(out, envelope.source_stale_range_count);
    append_u64(out, envelope.source_discarded_range_count);
    append_string(out, &envelope.first_live_object_bucket);
    append_string(out, &envelope.first_live_object_key);
    append_string(out, &envelope.before_state_fingerprint);
    append_string(out, &envelope.after_state_fingerprint);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.route_control_proof_format);
    append_string(out, &envelope.route_control_proof_checksum);
    append_u64(out, envelope.route_control_proof_bytes);
    append_u64(out, envelope.route_control_commit_index);
    append_u64(out, envelope.route_control_next_log_index);
    append_string(out, &envelope.expected_route_control_checksum);
    append_string(out, &envelope.expected_object_gc_report_proof_checksum);
    append_string(out, &envelope.raft_entry_checksum);
}

pub(crate) fn prepared_slab_compaction_envelope_checksum(
    envelope: &PreparedSlabCompactionEnvelope,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-SEGMENT-COMPACTION-ENVELOPE2");
    append_prepared_slab_compaction_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_slab_compaction_envelope(
    prepared: &PreparedSlabCompaction,
) -> PreparedSlabCompactionEnvelope {
    let first_live_object = prepared.live_objects.first();
    let mut envelope = PreparedSlabCompactionEnvelope {
        envelope_format: "MXOSSEGMENT-COMPACTION-ENVELOPE2".to_string(),
        slab_id: prepared.slab_id.clone(),
        min_stale_bytes: prepared.min_stale_bytes,
        update_time_ms: prepared.update_time_ms,
        live_object_count: prepared.live_objects.len() as u64,
        source_stale_range_count: prepared.source_stale_ranges.len() as u64,
        source_discarded_range_count: prepared.source_discarded_ranges.len() as u64,
        first_live_object_bucket: first_live_object
            .map(|object_id| object_id.bucket.clone())
            .unwrap_or_default().to_string(),
        first_live_object_key: first_live_object
            .map(|object_id| object_id.key.clone())
            .unwrap_or_default().to_string(),
        before_state_fingerprint: prepared.before_state_fingerprint.clone(),
        after_state_fingerprint: prepared.after_state_fingerprint.clone(),
        request_checksum: prepared.request_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        route_control_proof_format: prepared.route_control_proof.state_format.clone(),
        route_control_proof_checksum: prepared.route_control_proof.state_checksum.clone(),
        route_control_proof_bytes: prepared.route_control_proof.state_bytes,
        route_control_commit_index: prepared
            .route_control_proof
            .page_route_control_commit_index,
        route_control_next_log_index: prepared
            .route_control_proof
            .page_route_control_next_log_index,
        expected_route_control_checksum: prepared.expected_route_control_checksum.clone(),
        expected_object_gc_report_proof_checksum: prepared
            .expected_object_gc_report_proof_checksum
            .clone(),
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_slab_compaction_envelope_checksum(&envelope);
    envelope
}
