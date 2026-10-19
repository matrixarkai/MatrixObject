// SPDX-License-Identifier: Apache-2.0
macro_rules! impl_prepared_page_raft_entry_checksum_guard {
    ($ty:ty, $label:literal, $require_method:ident) => {
        impl $ty {
            pub fn verify_expected_raft_entry_checksum(
                &self,
                expected_raft_entry_checksum: impl AsRef<str>,
            ) -> Result<(), ObjectError> {
                let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
                if expected_raft_entry_checksum.is_empty() {
                    return Ok(());
                }
                if self.raft_entry_checksum != expected_raft_entry_checksum {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "prepared page {} raft entry checksum mismatch: expected {} got {}",
                        $label, expected_raft_entry_checksum, self.raft_entry_checksum
                    )));
                }
                Ok(())
            }
        }

        impl ProvenStorageRead<$ty> {
            pub fn $require_method(
                self,
                expected_raft_entry_checksum: impl AsRef<str>,
            ) -> Result<Self, ObjectError> {
                self.value
                    .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
                Ok(self)
            }
        }
    };
}

impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageAllocationReservation,
    "allocation reservation",
    require_page_allocation_reservation_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageBatchAllocationReservation,
    "batch allocation reservation",
    require_page_batch_allocation_reservation_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageAllocationRelease,
    "allocation release",
    require_page_allocation_release_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageReplicaReclaim,
    "replica reclaim",
    require_page_replica_reclaim_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageAuditRepair,
    "audit repair",
    require_page_audit_repair_raft_entry_checksum
);

impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageRangeRepair,
    "range repair",
    require_page_range_repair_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageChunkRepair,
    "chunk repair",
    require_page_chunk_repair_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageRouteQosConfig,
    "route qos",
    require_page_route_qos_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageReceiptRetentionConfig,
    "receipt retention",
    require_page_receipt_retention_raft_entry_checksum
);
impl_prepared_page_raft_entry_checksum_guard!(
    PreparedPageQuorumConfig,
    "quorum",
    require_page_quorum_raft_entry_checksum
);
