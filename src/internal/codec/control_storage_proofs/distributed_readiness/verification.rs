pub fn verify_objectstore_distributed_readiness_checksum(
    readiness: &ObjectStoreDistributedReadiness,
    expected_control_checksum: Option<&str>,
    expected_control_checksum_format: Option<&str>,
    expected_distributed_readiness_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    let recomputed = objectstore_distributed_readiness_checksum(readiness);
    if recomputed != readiness.control_checksum {
        return Err(ObjectError::Corruption(format!(
            "distributed readiness control checksum is corrupt: expected {} got {}",
            recomputed, readiness.control_checksum
        )));
    }
    if let Some(expected_format) = expected_control_checksum_format {
        if !expected_format.is_empty() && expected_format != readiness.control_checksum_format {
            return Err(ObjectError::PreconditionFailed(format!(
                "distributed readiness control checksum format mismatch: expected {} got {}",
                expected_format, readiness.control_checksum_format
            )));
        }
    }
    if let Some(expected_checksum) = expected_control_checksum {
        if !expected_checksum.is_empty() && expected_checksum != readiness.control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "distributed readiness control checksum mismatch: expected {} got {}",
                expected_checksum, readiness.control_checksum
            )));
        }
    }
    if let Some(expected_checksum) = expected_distributed_readiness_checksum {
        if !expected_checksum.is_empty() && expected_checksum != readiness.control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "distributed readiness checksum mismatch: expected {} got {}",
                expected_checksum, readiness.distributed_readiness_checksum
            )));
        }
    }
    Ok(())
}
