// SPDX-License-Identifier: Apache-2.0
pub(crate) fn metadata_transaction_receipt_proof_checksum(
    receipt: &MetadataTransactionReceipt,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-TRANSACTION-RECEIPT-PROOF1");
    append_metadata_transaction_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn metadata_transaction_receipt_page_proof_checksum(
    page: &MetadataTransactionReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-TRANSACTION-RECEIPT-PAGE-PROOF1");
    append_u64(&mut material, page.receipt_count);
    append_u64(&mut material, page.total_receipt_count);
    append_u64(&mut material, page.scanned_count);
    append_u64(&mut material, page.matched_count);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.budget_limited as u64);
    append_u64(&mut material, page.truncated as u64);
    metadata_optional_string_filter(&mut material, &page.marker);
    metadata_optional_string_filter(&mut material, &page.next_marker);
    metadata_optional_string_filter(&mut material, &page.operation);
    metadata_optional_string_filter(&mut material, &page.transaction_id);
    append_u64(&mut material, page.skipped_operation_count);
    append_u64(&mut material, page.skipped_transaction_count);
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn append_metadata_transaction_page_filters(
    material: &mut Vec<u8>,
    prefix: &str,
    marker: &Option<String>,
    next_marker: &Option<String>,
    owner: &Option<String>,
    state: &Option<String>,
    only_expired: bool,
    include_terminal: bool,
    transaction_count: u64,
    total_transaction_count: u64,
    scanned_count: u64,
    matched_count: u64,
    skipped_terminal_count: u64,
    skipped_owner_count: u64,
    skipped_state_count: u64,
    skipped_not_expired_count: u64,
    payload_bytes: u64,
    max_payload_bytes: u64,
    budget_limited: bool,
    truncated: bool,
) {
    append_string(material, prefix);
    metadata_optional_string_filter(material, marker);
    metadata_optional_string_filter(material, next_marker);
    metadata_optional_string_filter(material, owner);
    metadata_optional_string_filter(material, state);
    append_u64(material, only_expired as u64);
    append_u64(material, include_terminal as u64);
    append_u64(material, transaction_count);
    append_u64(material, total_transaction_count);
    append_u64(material, scanned_count);
    append_u64(material, matched_count);
    append_u64(material, skipped_terminal_count);
    append_u64(material, skipped_owner_count);
    append_u64(material, skipped_state_count);
    append_u64(material, skipped_not_expired_count);
    append_u64(material, payload_bytes);
    append_u64(material, max_payload_bytes);
    append_u64(material, budget_limited as u64);
    append_u64(material, truncated as u64);
}

pub(crate) fn metadata_transaction_summary_page_proof_checksum(
    page: &MetadataTransactionSummaryPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-TRANSACTION-SUMMARY-PAGE-PROOF1");
    append_metadata_transaction_page_filters(
        &mut material,
        &page.prefix,
        &page.marker,
        &page.next_marker,
        &page.owner,
        &page.state,
        page.only_expired,
        page.include_terminal,
        page.transaction_count,
        page.total_transaction_count,
        page.scanned_count,
        page.matched_count,
        page.skipped_terminal_count,
        page.skipped_owner_count,
        page.skipped_state_count,
        page.skipped_not_expired_count,
        page.payload_bytes,
        page.max_payload_bytes,
        page.budget_limited,
        page.truncated,
    );
    append_u64(&mut material, page.transactions.len() as u64);
    for transaction in &page.transactions {
        append_string(&mut material, &transaction.transaction_id);
        append_string(&mut material, &transaction.owner);
        append_string(&mut material, &transaction.state);
        append_u64(&mut material, transaction.mutation_count);
        append_u64(&mut material, transaction.read_version_count);
        append_u64(&mut material, transaction.begin_time_ms);
        append_u64(&mut material, transaction.update_time_ms);
        append_u64(&mut material, transaction.expire_time_ms);
    }
    etag(&material)
}

pub(crate) fn metadata_transaction_page_proof_checksum(page: &MetadataTransactionPage) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-TRANSACTION-PAGE-PROOF1");
    append_metadata_transaction_page_filters(
        &mut material,
        &page.prefix,
        &None,
        &page.next_marker,
        &page.owner,
        &page.state,
        page.only_expired,
        page.include_terminal,
        page.transactions.len() as u64,
        page.transactions.len() as u64,
        page.scanned_count,
        page.matched_count,
        page.skipped_terminal_count,
        page.skipped_owner_count,
        page.skipped_state_count,
        page.skipped_not_expired_count,
        0,
        0,
        false,
        page.truncated,
    );
    append_u64(&mut material, page.limit);
    append_u64(&mut material, page.transactions.len() as u64);
    for transaction in &page.transactions {
        append_metadata_transaction(&mut material, transaction);
    }
    etag(&material)
}
