// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_metadata_snapshot_compaction_report(
    out: &mut Vec<u8>,
    report: &MetadataSnapshotCompactionReport,
) {
    append_u64(out, report.raft_snapshot_floor_commit_index);
    append_u64(out, report.compacted_change_records);
    append_u64(out, report.compacted_batch_receipts);
    append_u64(out, report.compacted_lease_receipts);
    append_u64(out, report.compacted_coordinator_config_receipts);
    append_u64(out, report.compacted_coordinator_session_receipts);
    append_u64(out, report.compacted_shard_receipts);
    append_u64(out, report.compacted_transaction_receipts);
    append_u64(out, report.raft_snapshot_compacted_change_count);
    append_u64(out, report.raft_snapshot_compacted_batch_receipt_count);
    append_u64(out, report.remaining_changes);
    append_u64(out, report.remaining_batch_receipts);
    append_u64(out, report.remaining_lease_receipts);
    append_u64(out, report.remaining_coordinator_config_receipts);
    append_u64(out, report.remaining_coordinator_session_receipts);
    append_u64(out, report.remaining_shard_receipts);
    append_u64(out, report.remaining_transaction_receipts);
}

pub(crate) fn metadata_snapshot_compaction_request_checksum(
    snapshot_commit_index: u64,
    max_change_records: usize,
    max_batch_receipts: usize,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-SNAPSHOT-COMPACTION-REQUEST1");
    append_u64(&mut material, snapshot_commit_index);
    append_u64(&mut material, max_change_records as u64);
    append_u64(&mut material, max_batch_receipts as u64);
    etag(&material)
}

pub(crate) fn prepared_metadata_snapshot_compaction_checksum(
    prepared: &PreparedMetadataSnapshotCompaction,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-SNAPSHOT-COMPACTION1");
    append_string(&mut material, &prepared.mutation_id);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.snapshot_commit_index);
    append_u64(&mut material, prepared.max_change_records as u64);
    append_u64(&mut material, prepared.max_batch_receipts as u64);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_metadata_snapshot_compaction_report(&mut material, &prepared.report);
    append_string(&mut material, &prepared.snapshot.checksum);
    append_u64(&mut material, prepared.snapshot.next_commit_index);
    append_u64(
        &mut material,
        prepared.snapshot.raft_snapshot_floor_commit_index,
    );
    append_u64(
        &mut material,
        prepared.snapshot.raft_snapshot_compacted_change_count,
    );
    append_u64(
        &mut material,
        prepared
            .snapshot
            .raft_snapshot_compacted_batch_receipt_count,
    );
    etag(&material)
}

pub(crate) fn metadata_snapshot_compaction_mutation_id(
    expected_previous_commit_index: u64,
    prepared_commit_index: u64,
    request_checksum: &str,
) -> String {
    let checksum_suffix = request_checksum
        .rsplit(':')
        .next()
        .unwrap_or(request_checksum)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(32)
        .collect::<String>();
    format!(
        "meta-snapshot-compaction-{expected_previous_commit_index}-{prepared_commit_index}-{checksum_suffix}"
    )
}

