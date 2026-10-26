// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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
    report: &PlacementSnapshotProofStreamReport,
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

/// Check an install source proof against the one this build computes.
///
/// # THE TWO COMPARISON CLAUSES CANNOT DIFFER AS THIS IS CURRENTLY WIRED
///
/// A KNOWN STATE, not an oversight. Measured on 2026-10-26:
///
///  1. This report is CONSTRUCTED in exactly two places, both of them the builder functions
///     above it in this file.
///  2. There is NO DECODER for it anywhere -- it has `to_control_fields` and no counterpart,
///     so it is output-only and is never received from a peer, a frame or a stored byte.
///  3. The two entry points that would accept an externally supplied `expected` --
///     `install_root_snapshot_chunks_with_read_proof_source_report` and
///     `install_raft_metadata_snapshot_chunks_with_read_proof_source_report` -- have ZERO
///     callers. Each computes `actual` locally from the same report the caller passed.
///
/// RE-MEASURING (3) WILL FIND ONE EXTRA OCCURRENCE OF EACH NAME: this comment. It names both
/// functions, so a grep for either now returns its definition and this line. That is the only
/// difference between the counts above and what a fresh grep reports.
///
/// So `expected.format` and `expected.service` are, today, always the values this build just
/// produced, and the two clauses comparing them cannot fail. The remaining clauses are NOT
/// in that position: they check the CONTENT of the expected report -- an empty read-proof
/// checksum, an empty fingerprint, a read index behind the commit index -- and those can fail
/// on a report this build built.
///
/// # WHY IT IS LEFT THIS WAY
///
/// The fix is a real remote sender, not a caller invented to make the check reachable. When
/// the consensus install path gains one, `expected` will arrive from somewhere else, the two
/// comparisons will start meaning something, and the service label becomes a wire contract
/// at that point -- which is also when renaming it stops being free. Whoever adds that sender
/// inherits the question; until then there is nothing here to repair.
///
/// Retiring the control-plane vocabulary therefore does NOT touch the label this file passes:
/// a name hashed into a checksum is a format change, and this one has no reader to break yet.
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

