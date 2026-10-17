pub(crate) fn append_metadata_mutation(out: &mut Vec<u8>, mutation: &MetadataMutation) {
    append_string(out, &mutation.key);
    append_string(out, &mutation.value);
    append_u64(out, mutation.expected_version);
    append_u64(out, mutation.delete as u64);
}

pub(crate) fn append_metadata_record(out: &mut Vec<u8>, record: &MetadataRecord) {
    append_string(out, &record.key);
    append_string(out, &record.value);
    append_u64(out, record.version);
    append_u64(out, record.update_time_ms);
    append_u64(out, record.deleted as u64);
}

pub(crate) fn append_metadata_transaction(out: &mut Vec<u8>, transaction: &MetadataTransaction) {
    append_string(out, &transaction.transaction_id);
    append_string(out, &transaction.owner);
    append_u64(out, transaction.expire_time_ms);
    append_string(out, &transaction.state);
    append_u64(out, transaction.begin_time_ms);
    append_u64(out, transaction.update_time_ms);
    append_u64(out, transaction.mutations.len() as u64);
    for mutation in &transaction.mutations {
        append_metadata_mutation(out, mutation);
    }
    append_u64(out, transaction.read_versions.len() as u64);
    for (key, version) in &transaction.read_versions {
        append_string(out, key);
        append_u64(out, *version);
    }
}

pub(crate) fn metadata_transaction_summary(
    transaction: &MetadataTransaction,
) -> MetadataTransactionSummary {
    MetadataTransactionSummary {
        transaction_id: transaction.transaction_id.clone(),
        owner: transaction.owner.clone(),
        expire_time_ms: transaction.expire_time_ms,
        state: transaction.state.clone(),
        begin_time_ms: transaction.begin_time_ms,
        update_time_ms: transaction.update_time_ms,
        mutation_count: transaction.mutations.len() as u64,
        read_version_count: transaction.read_versions.len() as u64,
        mutation_value_bytes: transaction
            .mutations
            .iter()
            .map(|mutation| mutation.key.len().saturating_add(mutation.value.len()) as u64)
            .sum(),
        read_key_bytes: transaction
            .read_versions
            .keys()
            .map(|key| key.len() as u64)
            .sum(),
    }
}

pub(crate) fn metadata_transaction_summary_payload_bytes(
    summary: &MetadataTransactionSummary,
) -> u64 {
    summary
        .transaction_id
        .len()
        .saturating_add(summary.owner.len())
        .saturating_add(summary.state.len())
        .saturating_add(80) as u64
}

pub(crate) fn append_metadata_transaction_receipt(
    out: &mut Vec<u8>,
    receipt: &MetadataTransactionReceipt,
) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.operation);
    append_string(out, &receipt.request_checksum);
    append_metadata_transaction(out, &receipt.transaction);
    append_u64(out, receipt.records.len() as u64);
    for record in &receipt.records {
        append_metadata_record(out, record);
    }
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn metadata_batch_request_checksum(
    mutations: &[MetadataMutation],
    owner_fenced: bool,
    expected_owner: &str,
    expected_epoch: u64,
    lease_owner: Option<&str>,
    lease_epoch: Option<u64>,
    now_ms: u64,
) -> String {
    let mut material = Vec::new();
    append_u64(&mut material, owner_fenced as u64);
    append_string(&mut material, expected_owner);
    append_u64(&mut material, expected_epoch);
    append_string(&mut material, lease_owner.unwrap_or_default());
    append_u64(&mut material, lease_epoch.unwrap_or_default());
    append_u64(&mut material, now_ms);
    append_u64(&mut material, mutations.len() as u64);
    for mutation in mutations {
        append_metadata_mutation(&mut material, mutation);
    }
    etag(&material)
}

pub(crate) fn prepared_metadata_batch_checksum(prepared: &PreparedMetadataBatch) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-BATCH3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_string(
        &mut material,
        prepared.lease_owner.as_deref().unwrap_or_default(),
    );
    append_u64(&mut material, prepared.lease_epoch.unwrap_or_default());
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.idempotency_key);
    append_u64(&mut material, prepared.mutations.len() as u64);
    for mutation in &prepared.mutations {
        append_metadata_mutation(&mut material, mutation);
    }
    append_u64(&mut material, prepared.images.len() as u64);
    for image in &prepared.images {
        append_metadata_record(&mut material, image);
    }
    etag(&material)
}

pub(crate) fn prepared_object_metadata_commit_checksum(
    prepared: &PreparedObjectMetadataCommit,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-OBJECT-META-COMMIT1");
    append_string(&mut material, &prepared.current_key);
    append_string(&mut material, &prepared.object_id.bucket);
    append_string(&mut material, &prepared.object_id.key);
    append_string(&mut material, &prepared.current_value);
    append_string(&mut material, &prepared.object_value);
    append_u64(&mut material, prepared.delete_object as u64);
    append_u64(&mut material, prepared.expected_current_version);
    append_u64(&mut material, prepared.expected_object_version);
    append_string(&mut material, &prepared.object_request_checksum);
    append_string(&mut material, &prepared.batch.raft_entry_checksum);
    append_u64(&mut material, prepared.batch.expected_previous_commit_index);
    append_u64(&mut material, prepared.batch.prepared_commit_index);
    etag(&material)
}

pub(crate) fn prepared_metadata_prefix_delete_checksum(
    prepared: &PreparedMetadataPrefixDelete,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-PREFIX-DELETE3");
    append_string(&mut material, &prepared.prefix);
    append_u64(&mut material, prepared.max_records as u64);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_string(
        &mut material,
        prepared.lease_owner.as_deref().unwrap_or_default(),
    );
    append_u64(&mut material, prepared.lease_epoch.unwrap_or_default());
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.idempotency_key);
    append_u64(&mut material, prepared.mutations.len() as u64);
    for mutation in &prepared.mutations {
        append_metadata_mutation(&mut material, mutation);
    }
    append_u64(&mut material, prepared.images.len() as u64);
    for image in &prepared.images {
        append_metadata_record(&mut material, image);
    }
    etag(&material)
}

pub(crate) fn prepared_metadata_transaction_checksum(
    prepared: &PreparedMetadataTransaction,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-TXN3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_metadata_transaction_receipt(&mut material, &prepared.receipt);
    etag(&material)
}

pub(crate) fn metadata_transaction_gc_request_checksum(
    now_ms: u64,
    max_transactions: usize,
    min_age_ms: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-TXN-GC-REQUEST1");
    append_u64(&mut material, now_ms);
    append_u64(&mut material, max_transactions as u64);
    append_u64(&mut material, min_age_ms);
    etag(&material)
}

pub(crate) fn prepared_metadata_transaction_gc_checksum(
    prepared: &PreparedMetadataTransactionGc,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-TXN-GC3");
    append_u64(&mut material, prepared.now_ms);
    append_u64(&mut material, prepared.max_transactions as u64);
    append_u64(&mut material, prepared.min_age_ms);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_u64(&mut material, prepared.report.scanned_transactions);
    append_u64(&mut material, prepared.report.compacted_transactions);
    append_u64(&mut material, prepared.report.skipped_transactions);
    append_u64(
        &mut material,
        prepared.report.removed_transaction_ids.len() as u64,
    );
    for transaction_id in &prepared.report.removed_transaction_ids {
        append_string(&mut material, transaction_id);
    }
    append_u64(&mut material, prepared.removed_transactions.len() as u64);
    for transaction in &prepared.removed_transactions {
        append_metadata_transaction(&mut material, transaction);
    }
    etag(&material)
}
