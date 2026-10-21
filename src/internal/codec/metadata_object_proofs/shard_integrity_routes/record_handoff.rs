// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn metadata_shard_record_handoff_batch_checksum(
    source_node_id: &str,
    shard: &MetadataShard,
    marker: Option<&str>,
    next_marker: Option<&str>,
    limit: usize,
    max_payload_bytes: u64,
    include_deleted: bool,
    records: &[MetadataRecord],
    scanned_records: u64,
    payload_bytes: u64,
    truncated: bool,
    source_commit_index: u64,
    source_read_index: u64,
    source_shard_topology_epoch: u64,
    source_shard_topology_checksum: &str,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-SHARD-RECORD-HANDOFF-BATCH1");
    append_string(&mut material, source_node_id);
    append_metadata_shard_image(&mut material, shard);
    append_string(&mut material, marker.unwrap_or(""));
    append_string(&mut material, next_marker.unwrap_or(""));
    append_u64(&mut material, limit as u64);
    append_u64(&mut material, max_payload_bytes);
    append_u64(&mut material, include_deleted as u64);
    append_u64(&mut material, scanned_records);
    append_u64(&mut material, payload_bytes);
    append_u64(&mut material, truncated as u64);
    append_u64(&mut material, source_commit_index);
    append_u64(&mut material, source_read_index);
    append_u64(&mut material, source_shard_topology_epoch);
    append_string(&mut material, source_shard_topology_checksum);
    append_u64(&mut material, records.len() as u64);
    for record in records {
        append_metadata_record(&mut material, record);
        append_string(&mut material, &etag(record.value.as_text().as_bytes()));
    }
    etag(&material)
}

pub(crate) fn metadata_shard_record_handoff_request_checksum(
    prepared: &PreparedMetadataShardRecordHandoff,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-SHARD-RECORD-HANDOFF-REQUEST1");
    append_string(&mut material, &prepared.source_node_id);
    append_metadata_shard_image(&mut material, &prepared.shard);
    append_string(&mut material, prepared.marker.as_deref().unwrap_or(""));
    append_string(&mut material, prepared.next_marker.as_deref().unwrap_or(""));
    append_u64(&mut material, prepared.limit as u64);
    append_u64(&mut material, prepared.max_payload_bytes);
    append_u64(&mut material, prepared.include_deleted as u64);
    append_u64(&mut material, prepared.scanned_records);
    append_u64(&mut material, prepared.payload_bytes);
    append_u64(&mut material, prepared.truncated as u64);
    append_u64(&mut material, prepared.source_commit_index);
    append_u64(&mut material, prepared.source_read_index);
    append_u64(&mut material, prepared.source_shard_topology_epoch);
    append_string(&mut material, &prepared.source_shard_topology_checksum);
    append_string(&mut material, &prepared.handoff_token);
    append_string(&mut material, &prepared.handoff_batch_checksum);
    append_string(&mut material, &prepared.apply_mutation_id);
    append_u64(&mut material, prepared.records.len() as u64);
    for record in &prepared.records {
        append_metadata_record(&mut material, record);
    }
    etag(&material)
}

pub(crate) fn prepared_metadata_shard_record_handoff_checksum(
    prepared: &PreparedMetadataShardRecordHandoff,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-META-SHARD-RECORD-HANDOFF1",
    );
    append_string(
        &mut material,
        &metadata_shard_record_handoff_request_checksum(prepared),
    );
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}

