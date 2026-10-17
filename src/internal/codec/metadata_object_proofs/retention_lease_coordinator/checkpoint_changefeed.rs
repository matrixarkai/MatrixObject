pub(crate) fn metadata_change_checkpoint_request_checksum(
    checkpoint: &MetadataChangeCheckpoint,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-CHANGE-CHECKPOINT-REQUEST1");
    append_metadata_change_checkpoint(&mut material, checkpoint);
    etag(&material)
}

pub(crate) fn prepared_metadata_change_checkpoint_checksum(
    prepared: &PreparedMetadataChangeCheckpoint,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-CHANGE-CHECKPOINT3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.idempotency_key);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_metadata_change_checkpoint(&mut material, &prepared.checkpoint);
    etag(&material)
}

pub(crate) fn append_metadata_change(out: &mut Vec<u8>, change: &MetadataChange) {
    append_u64(out, change.sequence);
    append_u64(out, change.commit_index);
    append_string(out, &change.key);
    append_u64(out, change.version);
    append_u64(out, change.deleted as u64);
    append_u64(out, change.update_time_ms);
}

pub(crate) fn append_metadata_changefeed_compaction_report(
    out: &mut Vec<u8>,
    report: &MetadataChangefeedCompactionReport,
) {
    append_u64(out, report.safe_sequence);
    append_u64(out, report.scanned_changes);
    append_u64(out, report.compacted_changes);
    append_u64(out, report.retained_changes);
    append_u64(out, report.remaining_changes);
}

pub(crate) fn metadata_changefeed_compaction_request_checksum(
    prefix: &str,
    max_changes: usize,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-META-CHANGEFEED-COMPACTION-REQUEST1",
    );
    append_string(&mut material, prefix);
    append_u64(&mut material, max_changes as u64);
    etag(&material)
}

pub(crate) fn prepared_metadata_changefeed_compaction_checksum(
    prepared: &PreparedMetadataChangefeedCompaction,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-META-CHANGEFEED-COMPACTION1",
    );
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.prefix);
    append_u64(&mut material, prepared.max_changes as u64);
    append_string(&mut material, &prepared.request_checksum);
    append_metadata_changefeed_compaction_report(&mut material, &prepared.report);
    append_u64(&mut material, prepared.compacted.len() as u64);
    for change in &prepared.compacted {
        append_metadata_change(&mut material, change);
    }
    etag(&material)
}

