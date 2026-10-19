// SPDX-License-Identifier: Apache-2.0
pub(crate) fn service_snapshot_install_source_proof_checksum(
    service: &str,
    expected_snapshot_read_proof_checksum: &str,
    source_state_fingerprint: &str,
    source_read_index: u64,
    source_commit_index: u64,
    source_applied_index: u64,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOS-RUST-SERVICE-SNAPSHOT-INSTALL-SOURCE-PROOF1",
    );
    append_string(&mut material, service);
    append_string(&mut material, expected_snapshot_read_proof_checksum);
    append_string(&mut material, source_state_fingerprint);
    append_u64(&mut material, source_read_index);
    append_u64(&mut material, source_commit_index);
    append_u64(&mut material, source_applied_index);
    etag(&material)
}

pub fn root_service_snapshot_install_source_proof_report(
    report: &RootSnapshotProofStreamReport,
) -> ServiceSnapshotInstallSourceProofReport {
    let service = "rootserver".to_string();
    let envelope_checksum = service_snapshot_install_source_proof_checksum(
        &service,
        &report.aggregate_checksum,
        &report.root_state_fingerprint,
        report.requested_root_read_index,
        report.requested_root_commit_index,
        report.root_applied_index,
    );
    ServiceSnapshotInstallSourceProofReport {
        format: "MXOS-RUST-SERVICE-SNAPSHOT-INSTALL-SOURCE-PROOF1".to_string(),
        service,
        expected_snapshot_read_proof_checksum: report.aggregate_checksum.clone(),
        source_state_fingerprint: report.root_state_fingerprint.clone(),
        source_read_index: report.requested_root_read_index,
        source_commit_index: report.requested_root_commit_index,
        source_applied_index: report.root_applied_index,
        envelope_checksum,
    }
}

pub fn metaserver_snapshot_install_source_proof_report(
    report: &MetaServerSnapshotProofStreamReport,
) -> ServiceSnapshotInstallSourceProofReport {
    let service = "metaserver".to_string();
    let envelope_checksum = service_snapshot_install_source_proof_checksum(
        &service,
        &report.aggregate_checksum,
        &report.full_snapshot_checksum,
        report.consensus_read_index,
        report.metadata_commit_index,
        report.snapshot_last_applied_index,
    );
    ServiceSnapshotInstallSourceProofReport {
        format: "MXOS-RUST-SERVICE-SNAPSHOT-INSTALL-SOURCE-PROOF1".to_string(),
        service,
        expected_snapshot_read_proof_checksum: report.aggregate_checksum.clone(),
        source_state_fingerprint: report.full_snapshot_checksum.clone(),
        source_read_index: report.consensus_read_index,
        source_commit_index: report.metadata_commit_index,
        source_applied_index: report.snapshot_last_applied_index,
        envelope_checksum,
    }
}

pub(crate) fn verify_service_snapshot_install_source_proof_report(
    expected: &ServiceSnapshotInstallSourceProofReport,
    actual: &ServiceSnapshotInstallSourceProofReport,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "service snapshot install source proof format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.service != actual.service {
        return Err(ObjectError::PreconditionFailed(format!(
            "service snapshot install source proof service mismatch: expected {} got {}",
            expected.service, actual.service
        )));
    }
    if expected.expected_snapshot_read_proof_checksum.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "service snapshot install source proof is missing read-proof checksum".to_string(),
        ));
    }
    if expected.source_state_fingerprint.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "service snapshot install source proof is missing state fingerprint".to_string(),
        ));
    }
    if expected.source_read_index < expected.source_commit_index {
        return Err(ObjectError::PreconditionFailed(
            "service snapshot install source read index is below source commit index".to_string(),
        ));
    }
    if expected.source_commit_index < expected.source_applied_index {
        return Err(ObjectError::PreconditionFailed(
            "service snapshot install source commit index is below applied index".to_string(),
        ));
    }
    if expected.envelope_checksum != actual.envelope_checksum || expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "service snapshot install source proof mismatch: expected envelope {} got {}",
            expected.envelope_checksum, actual.envelope_checksum
        )));
    }
    Ok(())
}

