// SPDX-License-Identifier: Apache-2.0
pub(crate) fn object_error_code(err: &ObjectError) -> &'static str {
    match err {
        ObjectError::NotFound(_) => "NotFound",
        ObjectError::InvalidArgument(_) => "InvalidArgument",
        ObjectError::PreconditionFailed(_) => "PreconditionFailed",
        ObjectError::Corruption(_) => "Corruption",
    }
}

pub(crate) fn metadata_record_payload_bytes(record: &MetadataRecord) -> u64 {
    record
        .key
        .len()
        .saturating_add(record.value.len())
        .saturating_add(32) as u64
}

pub(crate) fn metadata_maintenance_lease_payload_bytes(lease: &MetadataLease) -> u64 {
    lease
        .namespace
        .len()
        .saturating_add(lease.owner.len())
        .saturating_add(48) as u64
}

pub(crate) fn metadata_maintenance_session_payload_bytes(session: &CoordinatorSession) -> u64 {
    session
        .session_id
        .len()
        .saturating_add(session.owner.len())
        .saturating_add(session.endpoint.len())
        .saturating_add(80) as u64
}

pub(crate) fn metadata_maintenance_transaction_payload_bytes(
    transaction: &MetadataTransaction,
) -> u64 {
    transaction
        .transaction_id
        .len()
        .saturating_add(transaction.owner.len())
        .saturating_add(transaction.state.len())
        .saturating_add(
            transaction
                .mutations
                .iter()
                .map(|mutation| mutation.key.len().saturating_add(mutation.value.len()))
                .sum::<usize>(),
        )
        .saturating_add(
            transaction
                .read_versions
                .keys()
                .map(|key| key.len())
                .sum::<usize>(),
        )
        .saturating_add(128) as u64
}
