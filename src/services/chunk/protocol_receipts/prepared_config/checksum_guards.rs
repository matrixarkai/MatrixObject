// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
macro_rules! impl_proven_prepared_chunk_config_control_fields {
    ($ty:ty) => {
        impl ProvenStorageRead<$ty> {
            pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
                let mut fields = self.value.to_control_fields(field_prefix);
                fields.extend(
                    self.proof
                        .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
                );
                fields
            }
        }
    };
}

impl_proven_prepared_chunk_config_control_fields!(PreparedChunkMutationQosConfig);
impl_proven_prepared_chunk_config_control_fields!(PreparedChunkTrimFlowConfig);
impl_proven_prepared_chunk_config_control_fields!(PreparedChunkReadCacheConfig);
impl_proven_prepared_chunk_config_control_fields!(PreparedChunkReceiptRetentionConfig);
impl_proven_prepared_chunk_config_control_fields!(PreparedChunkCapacityConfig);
impl_proven_prepared_chunk_config_control_fields!(PreparedReplicaServiceModeConfig);

macro_rules! impl_prepared_chunk_raft_entry_checksum_guard {
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
                        "prepared chunk {} raft entry checksum mismatch: expected {} got {}",
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

impl_prepared_chunk_raft_entry_checksum_guard!(
    PreparedChunkReplicaReclaim,
    "replica reclaim",
    require_chunk_replica_reclaim_raft_entry_checksum
);
impl_prepared_chunk_raft_entry_checksum_guard!(
    PreparedChunkMutationQosConfig,
    "mutation qos",
    require_chunk_mutation_qos_raft_entry_checksum
);
impl_prepared_chunk_raft_entry_checksum_guard!(
    PreparedChunkTrimFlowConfig,
    "trim flow",
    require_chunk_trim_flow_raft_entry_checksum
);
impl_prepared_chunk_raft_entry_checksum_guard!(
    PreparedChunkReadCacheConfig,
    "read-cache",
    require_chunk_read_cache_raft_entry_checksum
);
impl_prepared_chunk_raft_entry_checksum_guard!(
    PreparedChunkReceiptRetentionConfig,
    "receipt retention",
    require_chunk_receipt_retention_raft_entry_checksum
);
impl_prepared_chunk_raft_entry_checksum_guard!(
    PreparedChunkCapacityConfig,
    "capacity",
    require_chunk_capacity_raft_entry_checksum
);
impl_prepared_chunk_raft_entry_checksum_guard!(
    PreparedReplicaServiceModeConfig,
    "service-mode",
    require_replica_service_mode_raft_entry_checksum
);
