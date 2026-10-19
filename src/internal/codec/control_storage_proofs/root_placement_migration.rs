// SPDX-License-Identifier: Apache-2.0
fn encoded_object_id_len(object_id: &ObjectId) -> usize {
    encoded_string_len(&object_id.bucket).saturating_add(encoded_string_len(&object_id.key))
}

fn encoded_string_list_payload_len(values: &[String]) -> usize {
    values.iter().map(|value| encoded_string_len(value)).sum()
}

fn placement_plan_encoded_len(plan: &PlacementPlan) -> usize {
    encoded_object_id_len(&plan.object_id)
        .saturating_add(std::mem::size_of::<u64>() * 2)
        .saturating_add(encoded_string_list_payload_len(&plan.node_ids))
}

fn placement_migration_plan_encoded_len(plan: &PlacementMigrationPlan) -> usize {
    encoded_object_id_len(&plan.object_id)
        .saturating_add(std::mem::size_of::<u64>() * 4)
        .saturating_add(encoded_string_len(&plan.source_node_id))
        .saturating_add(encoded_string_len(&plan.target_node_id))
        .saturating_add(encoded_string_list_payload_len(&plan.before_node_ids))
        .saturating_add(encoded_string_list_payload_len(&plan.after_node_ids))
        .saturating_add(encoded_string_len(&plan.reason))
        .saturating_add(encoded_string_len(&plan.plan_checksum))
}

pub(crate) fn placement_plan_checksum(plan: &PlacementPlan) -> String {
    let mut material = Vec::with_capacity(placement_plan_encoded_len(plan));
    append_object_id(&mut material, &plan.object_id);
    append_u64(&mut material, plan.length);
    append_u64(&mut material, plan.node_ids.len() as u64);
    for node_id in &plan.node_ids {
        append_string(&mut material, node_id);
    }
    etag(&material)
}

pub(crate) fn placement_migration_plan_checksum(plan: &PlacementMigrationPlan) -> String {
    let mut material = Vec::with_capacity(placement_migration_plan_encoded_len(plan));
    append_object_id(&mut material, &plan.object_id);
    append_u64(&mut material, plan.version);
    append_u64(&mut material, plan.length);
    append_string(&mut material, &plan.source_node_id);
    append_string(&mut material, &plan.target_node_id);
    append_u64(&mut material, plan.before_node_ids.len() as u64);
    for node_id in &plan.before_node_ids {
        append_string(&mut material, node_id);
    }
    append_u64(&mut material, plan.after_node_ids.len() as u64);
    for node_id in &plan.after_node_ids {
        append_string(&mut material, node_id);
    }
    append_string(&mut material, &plan.reason);
    etag(&material)
}

pub(crate) fn append_root_placement_migration_plan(
    out: &mut Vec<u8>,
    plan: &PlacementMigrationPlan,
) {
    append_object_id(out, &plan.object_id);
    append_u64(out, plan.version);
    append_u64(out, plan.length);
    append_string(out, &plan.source_node_id);
    append_string(out, &plan.target_node_id);
    append_u64(out, plan.before_node_ids.len() as u64);
    for node_id in &plan.before_node_ids {
        append_string(out, node_id);
    }
    append_u64(out, plan.after_node_ids.len() as u64);
    for node_id in &plan.after_node_ids {
        append_string(out, node_id);
    }
    append_string(out, &plan.reason);
    append_string(out, &plan.plan_checksum);
}

pub(crate) fn root_placement_migration_request_checksum(
    plan: &PlacementMigrationPlan,
    expected_plan_checksum: &str,
    min_free_bytes: u64,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-ROOT-PLACEMENT-MIGRATION-REQUEST2")
            .saturating_add(placement_migration_plan_encoded_len(plan))
            .saturating_add(encoded_string_len(expected_plan_checksum))
            .saturating_add(std::mem::size_of::<u64>()),
    );
    append_string(&mut material, "MOS-RUST-ROOT-PLACEMENT-MIGRATION-REQUEST2");
    append_root_placement_migration_plan(&mut material, plan);
    append_string(&mut material, expected_plan_checksum);
    append_u64(&mut material, min_free_bytes);
    etag(&material)
}

pub(crate) fn append_root_leader_lease(out: &mut Vec<u8>, lease: &RootLeaderLease) {
    append_string(out, &lease.leader_node_id);
    append_u64(out, lease.epoch);
    append_u64(out, lease.acquire_time_ms);
    append_u64(out, lease.expire_time_ms);
    append_u64(out, lease.ttl_ms);
}

pub(crate) fn append_root_leader_lease_receipt(
    out: &mut Vec<u8>,
    receipt: &RootLeaderLeaseReceipt,
) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.operation);
    append_string(out, &receipt.request_checksum);
    append_root_leader_lease(out, &receipt.lease);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn root_leader_lease_receipt_proof_checksum(receipt: &RootLeaderLeaseReceipt) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-ROOT-LEADER-LEASE-RECEIPT-PROOF1");
    append_root_leader_lease_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn root_leader_lease_receipt_page_proof_checksum(
    page: &RootLeaderLeaseReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-ROOT-LEADER-LEASE-RECEIPT-PAGE-PROOF1");
    metadata_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
    );
    append_u64(&mut material, page.skipped_operation_count);
    append_u64(&mut material, page.skipped_leader_count);
    append_u64(&mut material, page.operation.is_some() as u64);
    if let Some(operation) = &page.operation {
        append_string(&mut material, operation);
    }
    append_u64(&mut material, page.leader_node_id.is_some() as u64);
    if let Some(leader_node_id) = &page.leader_node_id {
        append_string(&mut material, leader_node_id);
    }
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn root_read_proof_checksum(proof: &RootReadProof) -> String {
    let mut material = Vec::new();
    if proof.proof_format.is_empty() || proof.proof_format == "MOS-RUST-ROOT-READ-PROOF1" {
        append_string(&mut material, "MOS-RUST-ROOT-READ-PROOF1");
        append_u64(&mut material, proof.requested_root_read_index);
        append_u64(&mut material, proof.root_applied_index);
        append_u64(&mut material, proof.root_read_index_lag);
        append_u64(&mut material, proof.root_raft_term);
        append_string(&mut material, &proof.root_state_fingerprint);
    } else {
        append_string(&mut material, &proof.proof_format);
        append_string(&mut material, &proof.operation);
        append_u64(&mut material, proof.requested_root_read_index);
        append_u64(&mut material, proof.requested_root_commit_index);
        append_u64(&mut material, proof.root_applied_index);
        append_u64(&mut material, proof.root_read_index_lag);
        append_u64(&mut material, proof.root_raft_term);
        append_string(&mut material, &proof.root_state_fingerprint);
    }
    etag(&material)
}

pub(crate) fn prepared_root_placement_migration_checksum(
    prepared: &PreparedRootPlacementMigration,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-PREPARED-ROOT-PLACEMENT-MIGRATION2")
            .saturating_add(placement_migration_plan_encoded_len(&prepared.plan))
            .saturating_add(encoded_string_len(&prepared.expected_plan_checksum))
            .saturating_add(encoded_string_len(&prepared.before_state_fingerprint))
            .saturating_add(encoded_string_len(&prepared.root_read_proof_checksum))
            .saturating_add(encoded_string_len(&prepared.request_checksum))
            .saturating_add(std::mem::size_of::<u64>() * 5),
    );
    append_string(&mut material, "MOS-RUST-PREPARED-ROOT-PLACEMENT-MIGRATION2");
    append_root_placement_migration_plan(&mut material, &prepared.plan);
    append_string(&mut material, &prepared.expected_plan_checksum);
    append_u64(&mut material, prepared.min_free_bytes);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_u64(&mut material, prepared.root_read_index);
    append_u64(&mut material, prepared.root_raft_term);
    append_string(&mut material, &prepared.root_read_proof_checksum);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}

pub(crate) fn append_prepared_root_placement_migration_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedRootPlacementMigrationEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_object_id(out, &envelope.object_id);
    append_u64(out, envelope.version);
    append_u64(out, envelope.length);
    append_string(out, &envelope.source_node_id);
    append_string(out, &envelope.target_node_id);
    append_u64(out, envelope.before_node_ids.len() as u64);
    for node_id in &envelope.before_node_ids {
        append_string(out, node_id);
    }
    append_u64(out, envelope.after_node_ids.len() as u64);
    for node_id in &envelope.after_node_ids {
        append_string(out, node_id);
    }
    append_string(out, &envelope.reason);
    append_string(out, &envelope.expected_plan_checksum);
    append_u64(out, envelope.min_free_bytes);
    append_string(out, &envelope.before_state_fingerprint);
    append_u64(out, envelope.root_read_index);
    append_u64(out, envelope.root_raft_term);
    append_string(out, &envelope.root_read_proof_checksum);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.raft_entry_checksum);
}

pub(crate) fn prepared_root_placement_migration_envelope_checksum(
    envelope: &PreparedRootPlacementMigrationEnvelope,
) -> (String, u64) {
    let mut material = Vec::with_capacity(
        encoded_string_len(&envelope.envelope_format)
            .saturating_add(encoded_object_id_len(&envelope.object_id))
            .saturating_add(encoded_string_len(&envelope.source_node_id))
            .saturating_add(encoded_string_len(&envelope.target_node_id))
            .saturating_add(encoded_string_list_payload_len(&envelope.before_node_ids))
            .saturating_add(encoded_string_list_payload_len(&envelope.after_node_ids))
            .saturating_add(encoded_string_len(&envelope.reason))
            .saturating_add(encoded_string_len(&envelope.expected_plan_checksum))
            .saturating_add(encoded_string_len(&envelope.before_state_fingerprint))
            .saturating_add(encoded_string_len(&envelope.root_read_proof_checksum))
            .saturating_add(encoded_string_len(&envelope.request_checksum))
            .saturating_add(encoded_string_len(&envelope.raft_entry_checksum))
            .saturating_add(std::mem::size_of::<u64>() * 9),
    );
    append_prepared_root_placement_migration_envelope(&mut material, envelope);
    (etag(&material), material.len() as u64)
}

pub(crate) fn prepared_root_placement_migration_envelope(
    prepared: &PreparedRootPlacementMigration,
) -> PreparedRootPlacementMigrationEnvelope {
    let mut envelope = PreparedRootPlacementMigrationEnvelope {
        object_id: prepared.plan.object_id.clone(),
        version: prepared.plan.version,
        length: prepared.plan.length,
        source_node_id: prepared.plan.source_node_id.clone(),
        target_node_id: prepared.plan.target_node_id.clone(),
        before_node_ids: prepared.plan.before_node_ids.clone(),
        after_node_ids: prepared.plan.after_node_ids.clone(),
        reason: prepared.plan.reason.clone(),
        expected_plan_checksum: prepared.expected_plan_checksum.clone(),
        min_free_bytes: prepared.min_free_bytes,
        before_state_fingerprint: prepared.before_state_fingerprint.clone(),
        root_read_index: prepared.root_read_index,
        root_raft_term: prepared.root_raft_term,
        root_read_proof_checksum: prepared.root_read_proof_checksum.clone(),
        request_checksum: prepared.request_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        envelope_format: "MOSRSROOT-PLACEMENT-MIGRATION-ENVELOPE2".to_string(),
        envelope_bytes: 0,
        envelope_checksum: String::new(),
    };
    let (checksum, bytes) = prepared_root_placement_migration_envelope_checksum(&envelope);
    envelope.envelope_bytes = bytes;
    envelope.envelope_checksum = checksum;
    envelope
}

pub(crate) fn append_committed_root_placement_migration_envelope(
    out: &mut Vec<u8>,
    envelope: &CommittedRootPlacementMigrationEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_object_id(out, &envelope.object_id);
    append_u64(out, envelope.version);
    append_u64(out, envelope.length);
    append_string(out, &envelope.source_node_id);
    append_string(out, &envelope.target_node_id);
    append_u64(out, envelope.before_node_ids.len() as u64);
    for node_id in &envelope.before_node_ids {
        append_string(out, node_id);
    }
    append_u64(out, envelope.after_node_ids.len() as u64);
    for node_id in &envelope.after_node_ids {
        append_string(out, node_id);
    }
    append_string(out, &envelope.reason);
    append_string(out, &envelope.expected_plan_checksum);
    append_u64(out, envelope.min_free_bytes);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.raft_entry_checksum);
    append_u64(out, envelope.committed_raft_index);
    append_u64(out, envelope.metadata_version);
    append_u64(out, envelope.metadata_length);
    append_string(out, &envelope.metadata_etag);
    append_string(out, &envelope.metadata_content_type);
    append_u64(out, envelope.metadata_update_time_ms);
}

pub(crate) fn committed_root_placement_migration_envelope_checksum(
    envelope: &CommittedRootPlacementMigrationEnvelope,
) -> (String, u64) {
    let mut material = Vec::with_capacity(
        encoded_string_len(&envelope.envelope_format)
            .saturating_add(encoded_object_id_len(&envelope.object_id))
            .saturating_add(encoded_string_len(&envelope.source_node_id))
            .saturating_add(encoded_string_len(&envelope.target_node_id))
            .saturating_add(encoded_string_list_payload_len(&envelope.before_node_ids))
            .saturating_add(encoded_string_list_payload_len(&envelope.after_node_ids))
            .saturating_add(encoded_string_len(&envelope.reason))
            .saturating_add(encoded_string_len(&envelope.expected_plan_checksum))
            .saturating_add(encoded_string_len(&envelope.request_checksum))
            .saturating_add(encoded_string_len(&envelope.raft_entry_checksum))
            .saturating_add(encoded_string_len(&envelope.metadata_etag))
            .saturating_add(encoded_string_len(&envelope.metadata_content_type))
            .saturating_add(std::mem::size_of::<u64>() * 13),
    );
    append_committed_root_placement_migration_envelope(&mut material, envelope);
    (etag(&material), material.len() as u64)
}

pub(crate) fn committed_root_placement_migration_envelope(
    prepared: &PreparedRootPlacementMigration,
    metadata: &ObjectMetadata,
    committed_raft_index: u64,
) -> CommittedRootPlacementMigrationEnvelope {
    let mut envelope = CommittedRootPlacementMigrationEnvelope {
        object_id: prepared.plan.object_id.clone(),
        version: prepared.plan.version,
        length: prepared.plan.length,
        source_node_id: prepared.plan.source_node_id.clone(),
        target_node_id: prepared.plan.target_node_id.clone(),
        before_node_ids: prepared.plan.before_node_ids.clone(),
        after_node_ids: prepared.plan.after_node_ids.clone(),
        reason: prepared.plan.reason.clone(),
        expected_plan_checksum: prepared.expected_plan_checksum.clone(),
        min_free_bytes: prepared.min_free_bytes,
        request_checksum: prepared.request_checksum.clone(),
        prepared_commit_index: prepared.prepared_commit_index,
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        committed_raft_index,
        metadata_version: metadata.version,
        metadata_length: metadata.length,
        metadata_etag: metadata.etag.to_string(),
        metadata_content_type: metadata.content_type.to_string(),
        metadata_update_time_ms: metadata.update_time_ms,
        envelope_format: "MOSRSROOT-PLACEMENT-MIGRATION-COMMIT-ENVELOPE2".to_string(),
        envelope_bytes: 0,
        envelope_checksum: String::new(),
    };
    let (checksum, bytes) = committed_root_placement_migration_envelope_checksum(&envelope);
    envelope.envelope_bytes = bytes;
    envelope.envelope_checksum = checksum;
    envelope
}

