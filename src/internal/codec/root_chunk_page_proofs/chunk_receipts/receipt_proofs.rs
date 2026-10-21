// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn chunk_id_encoded_len() -> usize {
    std::mem::size_of::<u64>() * 3
}

pub(crate) fn chunk_write_batch_stats_encoded_len() -> usize {
    std::mem::size_of::<u64>() * 7
}

pub(crate) fn chunk_discard_batch_stats_encoded_len() -> usize {
    std::mem::size_of::<u64>() * 7
}

fn chunk_route_control_proof_encoded_len(proof: &ChunkRouteControlProof) -> usize {
    encoded_string_len(&proof.state_format)
        + encoded_string_len(&proof.state_checksum)
        + std::mem::size_of::<u64>() * 34
}

fn chunk_partial_reclaim_report_encoded_len(report: &ReplicaPartialReclaimReport) -> usize {
    std::mem::size_of::<u64>() * 17
        + encoded_string_len(&report.last_partial_reclaim_failure_status)
}

fn chunk_repair_report_encoded_len(report: &ReplicaRepairReport) -> usize {
    std::mem::size_of::<u64>() * 16
        + report
            .page_id
            .as_ref()
            .map(|_| chunk_id_encoded_len())
            .unwrap_or(0)
        + report
            .first_error
            .as_ref()
            .map(|first_error| encoded_string_len(first_error))
            .unwrap_or(0)
        + chunk_route_control_proof_encoded_len(&report.route_control_proof)
        + encoded_string_len(&report.expected_route_control_checksum)
}

pub(crate) fn chunk_state_install_receipt_encoded_len(receipt: &ReplicaStateInstallReceipt) -> usize {
    chunk_id_encoded_len()
        + encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.payload_checksum)
        + encoded_string_len(&receipt.state_checksum)
        + encoded_string_len(&receipt.idempotency_key)
        + std::mem::size_of::<u64>() * 3
}

pub(crate) fn chunk_reclaim_receipt_encoded_len(receipt: &ReplicaReclaimReceipt) -> usize {
    encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.idempotency_key)
        + std::mem::size_of::<u64>() * 6
        + chunk_partial_reclaim_report_encoded_len(&receipt.report)
}

pub(crate) fn chunk_repair_receipt_encoded_len(receipt: &ReplicaRepairReceipt) -> usize {
    chunk_id_encoded_len()
        + encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.idempotency_key)
        + chunk_repair_report_encoded_len(&receipt.report)
}

fn repair_stats_encoded_len() -> usize {
    std::mem::size_of::<u64>() * 4
}

fn bounded_repair_report_encoded_len(report: &BoundedRepairReport) -> usize {
    std::mem::size_of::<u64>() * 9
        + repair_stats_encoded_len()
        + chunk_route_control_proof_encoded_len(&report.route_control_proof)
        + encoded_string_len(&report.expected_route_control_checksum)
}

fn chunk_replica_audit_item_encoded_len(item: &ChunkReplicaAuditItem) -> usize {
    chunk_id_encoded_len()
        + std::mem::size_of::<u64>() * 6
        + encoded_string_len(&item.status)
        + encoded_string_len(&item.checksum)
}

fn chunk_replica_audit_chunk_encoded_len(page: &ChunkReplicaAuditChunk) -> usize {
    let mut encoded_len = std::mem::size_of::<u64>() * 13;
    for item in &page.items {
        encoded_len += chunk_replica_audit_item_encoded_len(item);
    }
    if page.marker.is_some() {
        encoded_len += chunk_id_encoded_len();
    }
    if page.next_marker.is_some() {
        encoded_len += chunk_id_encoded_len();
    }
    encoded_len
}

fn chunk_replica_audit_repair_report_encoded_len(
    report: &ChunkReplicaAuditRepairReport,
) -> usize {
    chunk_replica_audit_chunk_encoded_len(&report.audit)
        + std::mem::size_of::<u64>() * 5
        + repair_stats_encoded_len()
        + encoded_string_len(&report.repair_reason)
        + chunk_route_control_proof_encoded_len(&report.route_control_proof)
        + encoded_string_len(&report.expected_route_control_checksum)
}

pub(crate) fn page_range_repair_receipt_encoded_len(
    receipt: &ChunkRangeRepairReceipt,
) -> usize {
    std::mem::size_of::<u64>() * 7
        + encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.idempotency_key)
        + bounded_repair_report_encoded_len(&receipt.report)
}

pub(crate) fn page_audit_repair_receipt_encoded_len(
    receipt: &ChunkAuditRepairReceipt,
) -> usize {
    std::mem::size_of::<u64>() * 3
        + receipt
            .after_chunk_id
            .as_ref()
            .map(|_| chunk_id_encoded_len())
            .unwrap_or(0)
        + encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.idempotency_key)
        + chunk_replica_audit_repair_report_encoded_len(&receipt.report)
}

pub(crate) fn chunk_batch_write_receipt_proof_checksum(receipt: &ReplicaBatchWriteReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-CHUNK-WRITE-BATCH-PROOF1")
            + chunk_id_encoded_len()
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.payload_checksum)
            + encoded_string_len(&receipt.idempotency_key)
            + chunk_write_batch_stats_encoded_len(),
    );
    append_string(&mut material, "MXOS-CHUNK-WRITE-BATCH-PROOF1");
    append_page_id(&mut material, &receipt.page_id);
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.payload_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    append_chunk_write_batch_stats(&mut material, &receipt.stats);
    etag(&material)
}

pub(crate) fn chunk_batch_discard_receipt_proof_checksum(
    receipt: &ReplicaBatchDiscardReceipt,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-CHUNK-DISCARD-BATCH-PROOF1")
            + chunk_id_encoded_len()
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.idempotency_key)
            + chunk_discard_batch_stats_encoded_len(),
    );
    append_string(&mut material, "MXOS-CHUNK-DISCARD-BATCH-PROOF1");
    append_page_id(&mut material, &receipt.page_id);
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    append_chunk_discard_batch_stats(&mut material, &receipt.stats);
    etag(&material)
}

pub(crate) fn chunk_state_install_receipt_proof_checksum(
    receipt: &ReplicaStateInstallReceipt,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-CHUNK-STATE-INSTALL-RECEIPT-PROOF1")
            + chunk_state_install_receipt_encoded_len(receipt),
    );
    append_string(&mut material, "MOSRS-CHUNK-STATE-INSTALL-RECEIPT-PROOF1");
    append_chunk_state_install_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn chunk_reclaim_receipt_proof_checksum(receipt: &ReplicaReclaimReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-CHUNK-RECLAIM-RECEIPT-PROOF1")
            + chunk_reclaim_receipt_encoded_len(receipt),
    );
    append_string(&mut material, "MOSRS-CHUNK-RECLAIM-RECEIPT-PROOF1");
    append_chunk_reclaim_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn chunk_repair_receipt_proof_checksum(receipt: &ReplicaRepairReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len(receipt.proof_format()) + chunk_repair_receipt_encoded_len(receipt),
    );
    append_string(&mut material, receipt.proof_format());
    append_chunk_repair_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn page_range_repair_receipt_proof_checksum(
    receipt: &ChunkRangeRepairReceipt,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len(receipt.proof_format()) + page_range_repair_receipt_encoded_len(receipt),
    );
    append_string(&mut material, receipt.proof_format());
    append_page_range_repair_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn page_audit_repair_receipt_proof_checksum(
    receipt: &ChunkAuditRepairReceipt,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len(receipt.proof_format()) + page_audit_repair_receipt_encoded_len(receipt),
    );
    append_string(&mut material, receipt.proof_format());
    append_page_audit_repair_receipt(&mut material, receipt);
    etag(&material)
}
