// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn page_receipt_page_common_control_fields(
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    limit: u64,
    truncated: bool,
    next_marker: &Option<String>,
    chunk_id: Option<&ChunkId>,
    skipped_chunk_count: u64,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert(
        format!("{field_prefix}receipt_count"),
        receipt_count.to_string(),
    );
    fields.insert(
        format!("{field_prefix}total_receipt_count"),
        total_receipt_count.to_string(),
    );
    fields.insert(
        format!("{field_prefix}scanned_count"),
        scanned_count.to_string(),
    );
    fields.insert(format!("{field_prefix}limit"), limit.to_string());
    fields.insert(format!("{field_prefix}truncated"), truncated.to_string());
    fields.insert(
        format!("{field_prefix}next_marker"),
        next_marker.clone().unwrap_or_default(),
    );
    fields.insert(
        format!("{field_prefix}segment_filter_present"),
        chunk_id.is_some().to_string(),
    );
    if let Some(chunk_id) = chunk_id {
        fields.extend(chunk_id.to_control_fields(&format!("{field_prefix}segment_filter.")));
    }
    fields.insert(
        format!("{field_prefix}skipped_segment_count"),
        skipped_chunk_count.to_string(),
    );
    fields
}

fn insert_page_chunk_filter_control_fields(
    fields: &mut BTreeMap<String, String>,
    field_prefix: &str,
    page_index: Option<u64>,
    page_size: Option<u64>,
    skipped_page_count: u64,
) {
    if let Some(page_index) = page_index {
        fields.insert(
            format!("{field_prefix}chunk_filter_present"),
            "true".to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_index"),
            page_index.to_string(),
        );
        if let Some(page_size) = page_size {
            fields.insert(format!("{field_prefix}chunk_size"), page_size.to_string());
        }
    } else {
        fields.insert(
            format!("{field_prefix}chunk_filter_present"),
            "false".to_string(),
        );
    }
    fields.insert(
        format!("{field_prefix}skipped_chunk_count"),
        skipped_page_count.to_string(),
    );
}

fn page_audit_repair_receipt_page_common_control_fields(
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    limit: u64,
    truncated: bool,
    next_marker: &Option<String>,
    after_chunk_id: &Option<PageId>,
    skipped_after_chunk_count: u64,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert(
        format!("{field_prefix}receipt_count"),
        receipt_count.to_string(),
    );
    fields.insert(
        format!("{field_prefix}total_receipt_count"),
        total_receipt_count.to_string(),
    );
    fields.insert(
        format!("{field_prefix}scanned_count"),
        scanned_count.to_string(),
    );
    fields.insert(format!("{field_prefix}limit"), limit.to_string());
    fields.insert(format!("{field_prefix}truncated"), truncated.to_string());
    fields.insert(
        format!("{field_prefix}next_marker"),
        next_marker.clone().unwrap_or_default(),
    );
    fields.insert(
        format!("{field_prefix}after_chunk_filter_present"),
        after_chunk_id.is_some().to_string(),
    );
    if let Some(after_chunk_id) = after_chunk_id {
        fields.extend(
            after_chunk_id.to_control_fields(&format!("{field_prefix}after_chunk_filter.")),
        );
    }
    fields.insert(
        format!("{field_prefix}skipped_after_chunk_count"),
        skipped_after_chunk_count.to_string(),
    );
    fields
}
