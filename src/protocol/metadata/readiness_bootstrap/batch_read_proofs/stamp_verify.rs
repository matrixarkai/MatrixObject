// SPDX-License-Identifier: Apache-2.0
pub(crate) fn stamp_object_batch_read_proof(operation: &str, report: &mut ObjectBatchReadReport) {
    report.object_batch_read_proof_format = OBJECT_BATCH_READ_PROOF_FORMAT.to_string();
    report.object_batch_read_proof_checksum = object_batch_read_proof_checksum(operation, report);
}

pub(crate) fn stamp_conditional_object_batch_read_proof(
    operation: &str,
    report: &mut ConditionalObjectBatchReadReport,
) {
    report.object_batch_read_proof_format = OBJECT_BATCH_READ_PROOF_FORMAT.to_string();
    report.object_batch_read_proof_checksum =
        conditional_object_batch_read_proof_checksum(operation, report);
}

pub(crate) fn stamp_versioned_conditional_object_batch_read_proof(
    operation: &str,
    report: &mut VersionedConditionalObjectBatchReadReport,
) {
    report.object_batch_read_proof_format = OBJECT_BATCH_READ_PROOF_FORMAT.to_string();
    report.object_batch_read_proof_checksum =
        versioned_conditional_object_batch_read_proof_checksum(operation, report);
}

pub(crate) fn check_expected_object_batch_read_proof_checksum(
    operation: &str,
    actual_object_batch_read_proof_checksum: &str,
    expected_object_batch_read_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_object_batch_read_proof_checksum {
        if !expected.is_empty() && actual_object_batch_read_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} object batch read proof checksum mismatch: expected {} got {}",
                operation, expected, actual_object_batch_read_proof_checksum
            )));
        }
    }
    Ok(())
}
