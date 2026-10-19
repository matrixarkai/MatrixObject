// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_metadata_tombstone_compaction_report(
    out: &mut Vec<u8>,
    report: &MetadataTombstoneCompactionReport,
) {
    append_u64(out, report.safe_sequence);
    append_u64(out, report.scanned_records);
    append_u64(out, report.compactable_records);
    append_u64(out, report.compacted_records);
    append_u64(out, report.skipped_records);
    append_u64(out, report.compacted_bytes);
    append_u64(out, report.budget_limited as u64);
    append_u64(out, report.truncated as u64);
    append_string(out, report.next_key_marker.as_deref().unwrap_or_default());
    append_u64(out, report.compacted_tombstone_count);
    append_u64(out, report.last_tombstone_compaction_commit_index);
}

pub(crate) fn metadata_tombstone_compaction_request_checksum(
    prefix: &str,
    after_key: Option<&str>,
    max_records: usize,
    max_compact_bytes: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-TOMBSTONE-COMPACTION-REQUEST1");
    append_string(&mut material, prefix);
    append_string(&mut material, after_key.unwrap_or_default());
    append_u64(&mut material, max_records as u64);
    append_u64(&mut material, max_compact_bytes);
    etag(&material)
}

pub(crate) fn prepared_metadata_tombstone_compaction_checksum(
    prepared: &PreparedMetadataTombstoneCompaction,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-META-TOMBSTONE-COMPACTION1",
    );
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.prefix);
    append_string(
        &mut material,
        prepared.after_key.as_deref().unwrap_or_default(),
    );
    append_u64(&mut material, prepared.max_records as u64);
    append_u64(&mut material, prepared.max_compact_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_metadata_tombstone_compaction_report(&mut material, &prepared.report);
    append_u64(&mut material, prepared.compacted.len() as u64);
    for record in &prepared.compacted {
        append_metadata_record(&mut material, record);
    }
    etag(&material)
}

