pub(crate) fn stamp_object_metadata_batch_read_proof(
    operation: &str,
    batch: &mut ObjectMetadataBatchRead,
    proof: &ObjectReadProof,
) {
    batch.metadata_batch_read_proof_format = METADATA_BATCH_READ_PROOF_FORMAT.to_string();
    batch.metadata_batch_read_proof_checksum =
        object_metadata_batch_read_proof_checksum(operation, batch, proof);
}

pub(crate) fn stamp_object_metadata_batch_read_route_fence(
    batch: &mut ObjectMetadataBatchRead,
    expected_shard_topology_epoch: Option<u64>,
    expected_shard_topology_checksum: Option<&str>,
) {
    batch.expected_metadata_shard_topology_epoch = expected_shard_topology_epoch;
    batch.expected_metadata_shard_topology_checksum = expected_shard_topology_checksum
        .unwrap_or_default()
        .to_string();
}

pub(crate) fn check_expected_metadata_shard_topology_fence(
    operation: &str,
    service: &MatrixMetaService,
    expected_shard_topology_epoch: Option<u64>,
    expected_shard_topology_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    let (actual_epoch, actual_checksum) = metadata_shard_topology_fingerprint(service);
    if let Some(expected_epoch) = expected_shard_topology_epoch {
        if expected_epoch != actual_epoch {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata shard topology epoch mismatch: expected {} got {}",
                operation, expected_epoch, actual_epoch
            )));
        }
    }
    if let Some(expected_checksum) = expected_shard_topology_checksum {
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(format!(
                "{} metadata shard topology checksum expectation is empty",
                operation
            )));
        }
        if expected_checksum != actual_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata shard topology checksum mismatch: expected {} got {}",
                operation, expected_checksum, actual_checksum
            )));
        }
    }
    Ok(())
}

pub(crate) fn stamp_conditional_object_metadata_batch_read_proof(
    operation: &str,
    batch: &mut ConditionalObjectMetadataBatchRead,
    proof: &ObjectReadProof,
) {
    batch.metadata_batch_read_proof_format = METADATA_BATCH_READ_PROOF_FORMAT.to_string();
    batch.metadata_batch_read_proof_checksum =
        conditional_object_metadata_batch_read_proof_checksum(operation, batch, proof);
}

pub(crate) fn check_expected_metadata_batch_read_proof_checksum(
    operation: &str,
    actual: &str,
    expected_metadata_batch_read_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_metadata_batch_read_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata batch read proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}
