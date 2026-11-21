// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn append_page_batch_discard_receipt_legacy(
    out: &mut Vec<u8>,
    receipt: &ChunkBatchDiscardReceipt,
) {
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_page_batch_stats_legacy(out, &receipt.stats);
}
