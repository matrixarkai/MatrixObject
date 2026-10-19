// SPDX-License-Identifier: Apache-2.0
pub fn verify_objectstore_module_status_checksum(
    status: &ObjectStoreModuleStatus,
    expected_control_checksum: Option<&str>,
    expected_control_checksum_format: Option<&str>,
) -> Result<(), ObjectError> {
    verify_objectstore_module_status_checksum_with_readiness(
        status,
        expected_control_checksum,
        expected_control_checksum_format,
        None,
    )
}

pub fn verify_objectstore_module_status_checksum_with_readiness(
    status: &ObjectStoreModuleStatus,
    expected_control_checksum: Option<&str>,
    expected_control_checksum_format: Option<&str>,
    expected_distributed_readiness_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    let recomputed = objectstore_module_status_checksum(status);
    if recomputed != status.control_checksum {
        return Err(ObjectError::Corruption(format!(
            "module status control checksum is corrupt: expected {} got {}",
            recomputed, status.control_checksum
        )));
    }
    if let Some(expected_format) = expected_control_checksum_format {
        if !expected_format.is_empty() && expected_format != status.control_checksum_format {
            return Err(ObjectError::PreconditionFailed(format!(
                "module status control checksum format mismatch: expected {} got {}",
                expected_format, status.control_checksum_format
            )));
        }
    }
    if let Some(expected_checksum) = expected_control_checksum {
        if !expected_checksum.is_empty() && expected_checksum != status.control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "module status control checksum mismatch: expected {} got {}",
                expected_checksum, status.control_checksum
            )));
        }
    }
    if let Some(expected_checksum) = expected_distributed_readiness_checksum {
        if !expected_checksum.is_empty()
            && expected_checksum != status.distributed_readiness_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "module status distributed readiness checksum mismatch: expected {} got {}",
                expected_checksum, status.distributed_readiness_checksum
            )));
        }
    }
    Ok(())
}
