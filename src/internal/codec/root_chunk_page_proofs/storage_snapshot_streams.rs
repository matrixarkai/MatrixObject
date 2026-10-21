// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn objectstore_module_catalog_route_checksum(
    status: &ObjectStoreModuleStatus,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, &status.catalog_route_checksum_format);
    append_string(&mut material, &status.configured_control_plane_mode);
    append_string(&mut material, &status.control_plane_mode);
    append_string(&mut material, &status.catalog_source);
    append_string(&mut material, &status.catalog_endpoint);
    append_u64(
        &mut material,
        status.metaserver_catalog_endpoint_configured as u64,
    );
    append_u64(
        &mut material,
        status.rootserver_catalog_endpoint_configured as u64,
    );
    append_u64(&mut material, status.rootserver_required as u64);
    etag(&material)
}

pub(crate) fn append_storage_snapshot_proof_material(
    material: &mut Vec<u8>,
    offset: u64,
    payload_len: u64,
    page_checksum: &str,
    proof: &StorageReadProof,
) {
    append_u64(material, offset);
    append_u64(material, payload_len);
    append_string(material, page_checksum);
    append_string(material, &proof.control_kind);
    append_string(material, &proof.control_checksum);
    append_u64(material, proof.requested_storage_read_index);
    append_u64(material, proof.requested_storage_commit_index);
    append_u64(material, proof.storage_applied_index);
    append_u64(material, proof.storage_read_index_lag);
    append_u64(material, proof.storage_commit_index_lag);
}

pub(crate) fn storage_snapshot_proof_material_capacity(
    total_bytes: u64,
    max_payload_bytes: usize,
) -> usize {
    snapshot_proof_material_capacity(total_bytes, max_payload_bytes, 160)
}

pub(crate) fn finish_storage_snapshot_proof_stream_report(
    material: Vec<u8>,
    proof_count: u64,
    source_control_kind: String,
    source_control_checksum: String,
    source_read_index: u64,
    source_commit_index: u64,
    total_payload_bytes: u64,
    first_offset: u64,
    last_end_offset: u64,
    full_snapshot_checksum: String,
) -> StorageSnapshotProofStreamReport {
    StorageSnapshotProofStreamReport {
        format: "MXOS-RUST-STORAGE-SNAPSHOT-READ-PROOFS1".to_string(),
        proof_count,
        aggregate_checksum: etag(&material),
        source_control_kind,
        source_control_checksum,
        source_read_index,
        source_commit_index,
        total_payload_bytes,
        first_offset,
        last_end_offset,
        full_snapshot_checksum,
    }
}

pub(crate) fn verify_storage_snapshot_proof_stream_report(
    expected: &StorageSnapshotProofStreamReport,
    actual: &StorageSnapshotProofStreamReport,
    expected_source_control_kind: &str,
    expected_source_control_checksum: Option<&str>,
    min_read_index: u64,
    min_commit_index: u64,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report kind mismatch: expected {} got {}",
            expected_source_control_kind, expected.source_control_kind
        )));
    }
    if actual.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof stream kind mismatch: expected {} got {}",
            expected_source_control_kind, actual.source_control_kind
        )));
    }
    if let Some(expected_checksum) = expected_source_control_checksum {
        if expected.source_control_checksum != expected_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage snapshot proof report source control checksum mismatch: expected {} got {}",
                expected_checksum, expected.source_control_checksum
            )));
        }
    }
    if expected.source_read_index < min_read_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report read fence {} is below required {}",
            expected.source_read_index, min_read_index
        )));
    }
    if expected.source_commit_index < min_commit_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report commit fence {} is below required {}",
            expected.source_commit_index, min_commit_index
        )));
    }
    if expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report mismatch: expected aggregate {} got {}",
            expected.aggregate_checksum, actual.aggregate_checksum
        )));
    }
    Ok(())
}

pub(crate) fn storage_snapshot_install_source_proof_checksum(
    source_control_kind: &str,
    expected_snapshot_read_proof_checksum: &str,
    expected_source_control_checksum: &str,
    source_read_index: u64,
    source_commit_index: u64,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOS-RUST-STORAGE-SNAPSHOT-INSTALL-SOURCE-PROOF1",
    );
    append_string(&mut material, source_control_kind);
    append_string(&mut material, expected_snapshot_read_proof_checksum);
    append_string(&mut material, expected_source_control_checksum);
    append_u64(&mut material, source_read_index);
    append_u64(&mut material, source_commit_index);
    etag(&material)
}

pub fn storage_snapshot_install_source_proof_report(
    report: &StorageSnapshotProofStreamReport,
) -> StorageSnapshotInstallSourceProofReport {
    let envelope_checksum = storage_snapshot_install_source_proof_checksum(
        &report.source_control_kind,
        &report.aggregate_checksum,
        &report.source_control_checksum,
        report.source_read_index,
        report.source_commit_index,
    );
    StorageSnapshotInstallSourceProofReport {
        format: "MXOS-RUST-STORAGE-SNAPSHOT-INSTALL-SOURCE-PROOF1".to_string(),
        source_control_kind: report.source_control_kind.clone(),
        expected_snapshot_read_proof_checksum: report.aggregate_checksum.clone(),
        expected_source_control_checksum: report.source_control_checksum.clone(),
        source_read_index: report.source_read_index,
        source_commit_index: report.source_commit_index,
        envelope_checksum,
    }
}

pub(crate) fn verify_storage_snapshot_install_source_proof_report(
    expected: &StorageSnapshotInstallSourceProofReport,
    actual: &StorageSnapshotInstallSourceProofReport,
    expected_source_control_kind: &str,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof kind mismatch: expected {} got {}",
            expected_source_control_kind, expected.source_control_kind
        )));
    }
    if actual.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof actual kind mismatch: expected {} got {}",
            expected_source_control_kind, actual.source_control_kind
        )));
    }
    if expected.expected_snapshot_read_proof_checksum.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "storage snapshot install source proof is missing read-proof checksum".to_string(),
        ));
    }
    if expected.expected_source_control_checksum.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "storage snapshot install source proof is missing source control checksum".to_string(),
        ));
    }
    if expected.source_read_index < expected.source_commit_index {
        return Err(ObjectError::PreconditionFailed(
            "storage snapshot install source read index is below source commit index".to_string(),
        ));
    }
    if expected.envelope_checksum != actual.envelope_checksum || expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof mismatch: expected envelope {} got {}",
            expected.envelope_checksum, actual.envelope_checksum
        )));
    }
    Ok(())
}

