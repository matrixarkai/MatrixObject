// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_chunk_control_command(
        &mut self,
        command: ObjectCommand,
        _raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::RunMetaMaintenance {
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
            } => {
                let report = self.run_metadata_maintenance(
                    now_ms,
                    &lease_prefix,
                    lease_limit,
                    session_limit,
                    transaction_limit,
                )?;
                Ok(metadata_control_metadata(
                    "_metamaintenance",
                    &lease_prefix,
                    report.total_actions(),
                    report
                        .lease_report
                        .scanned_leases
                        .saturating_add(report.expired_sessions.len() as u64)
                        .saturating_add(report.expired_transactions.len() as u64)
                        .saturating_add(report.route_cache_audit.entries_before),
                    "metaserver/maintenance",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaMaintenance { prepared } => {
                let now_ms = prepared.now_ms;
                let lease_prefix = prepared.lease_prefix.clone();
                let report = self.apply_prepared_metadata_maintenance(prepared)?;
                Ok(metadata_control_metadata(
                    "_metamaintenance",
                    &lease_prefix,
                    report.total_actions(),
                    report
                        .lease_report
                        .scanned_leases
                        .saturating_add(report.expired_sessions.len() as u64)
                        .saturating_add(report.expired_transactions.len() as u64)
                        .saturating_add(report.route_cache_audit.entries_before),
                    "metaserver/maintenance",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyIntegrityFencedMetaMaintenance {
                prepared,
                expected_integrity_plan_checksum,
                actual_integrity_plan_checksum,
            } => {
                if expected_integrity_plan_checksum.is_empty()
                    || actual_integrity_plan_checksum.is_empty()
                {
                    return Err(ObjectError::InvalidArgument(
                        "metaserver integrity maintenance requires plan checksums".to_string(),
                    ));
                }
                if expected_integrity_plan_checksum != actual_integrity_plan_checksum {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "metaserver integrity plan checksum mismatch: expected {} actual {}",
                        expected_integrity_plan_checksum, actual_integrity_plan_checksum
                    )));
                }
                let now_ms = prepared.now_ms;
                let lease_prefix = prepared.lease_prefix.clone();
                let report = self.apply_prepared_metadata_maintenance(prepared)?;
                Ok(metadata_control_metadata(
                    "_metaintegrity",
                    &lease_prefix,
                    report.total_actions(),
                    report
                        .lease_report
                        .scanned_leases
                        .saturating_add(report.expired_sessions.len() as u64)
                        .saturating_add(report.expired_transactions.len() as u64)
                        .saturating_add(report.route_cache_audit.entries_before),
                    "metaserver/integrity-maintenance",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyRaftMembership {
                mutation_id,
                voter_node_ids,
                target_healthy_peer_count,
                expected_previous_commit_index,
            } => {
                let receipt = self.apply_raft_membership_change(
                    mutation_id,
                    voter_node_ids,
                    target_healthy_peer_count,
                    expected_previous_commit_index,
                )?;
                Ok(metadata_control_metadata(
                    "_metaraft",
                    "membership",
                    receipt.membership_epoch,
                    receipt.target_voter_node_ids.len() as u64,
                    "metaserver/raft-membership",
                    receipt.expected_previous_commit_index,
                ))
            }
            ObjectCommand::ApplyPreparedRaftMembership { prepared } => {
                let receipt = self.apply_prepared_raft_membership_change(prepared)?;
                Ok(metadata_control_metadata(
                    "_metaraft",
                    "prepared-membership",
                    receipt.membership_epoch,
                    receipt.target_voter_node_ids.len() as u64,
                    "metaserver/raft-membership-prepared",
                    receipt.expected_previous_commit_index,
                ))
            }
            ObjectCommand::UpdateRaftPeerHealth { healthy_peer_count } => {
                let healthy = self.update_raft_healthy_peer_count(healthy_peer_count)?;
                Ok(metadata_control_metadata(
                    "_metaraft",
                    "peer-health",
                    healthy,
                    self.raft_voter_node_ids.len() as u64,
                    "metaserver/raft-peer-health",
                    self.raft_membership_epoch,
                ))
            }
            ObjectCommand::QuarantinePageReplica {
                replica_index,
                quarantined,
            } => {
                self.set_page_replica_quarantine(replica_index, quarantined)?;
                Ok(metadata_control_metadata(
                    "_blockreplica",
                    &replica_index.to_string(),
                    quarantined as u64,
                    self.quarantined_page_replicas().len() as u64,
                    if quarantined {
                        "blockserver/replica-quarantine"
                    } else {
                        "blockserver/replica-restore"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::FreezePageReplica {
                replica_index,
                frozen,
            } => {
                self.set_page_replica_frozen(replica_index, frozen)?;
                Ok(metadata_control_metadata(
                    "_blockreplica",
                    &replica_index.to_string(),
                    frozen as u64,
                    self.frozen_page_replicas().len() as u64,
                    if frozen {
                        "chunkserver/replica-freeze"
                    } else {
                        "chunkserver/replica-thaw"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigurePageChunkEndpoints {
                endpoints,
                disabled_endpoints,
                write_quorum,
                read_quorum,
            } => {
                self.chunk_service.configure_chunk_endpoints(
                    endpoints,
                    disabled_endpoints,
                    write_quorum,
                    read_quorum,
                )?;
                let proof = self.chunk_service.route_control_proof();
                Ok(metadata_control_metadata(
                    "_blockroute",
                    "chunk-endpoints",
                    proof.chunk_endpoint_count,
                    proof.disabled_chunk_endpoint_count,
                    "blockserver/chunk-endpoints",
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigureChunkReplicaCapacity {
                replica_index,
                capacity_bytes,
            } => {
                if replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
                    self.configure_all_eligible_chunk_replica_capacity(capacity_bytes)?;
                } else {
                    self.chunk_service
                        .set_chunk_replica_capacity(replica_index, capacity_bytes)?;
                }
                Ok(self.chunk_capacity_control_metadata(
                    replica_index,
                    capacity_bytes,
                    None,
                    if replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
                        "chunkserver/all-eligible-capacity"
                    } else {
                        "chunkserver/replica-capacity"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedChunkCapacity { prepared } => {
                self.apply_prepared_chunk_capacity(prepared.clone())?;
                Ok(self.chunk_capacity_control_metadata(
                    prepared.replica_index,
                    prepared.capacity_bytes,
                    Some(prepared.chunk_prepared_replica_count),
                    if prepared.replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
                        "chunkserver/prepared-all-eligible-capacity"
                    } else {
                        "chunkserver/prepared-replica-capacity"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigureChunkReplicaServiceMode {
                replica_index,
                accepting_writes,
                draining,
                frozen,
            } => {
                self.configure_chunk_replica_service_mode(
                    replica_index,
                    accepting_writes,
                    draining,
                    frozen,
                )?;
                Ok(self.replica_service_mode_control_metadata(
                    replica_index,
                    None,
                    if replica_index == PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET {
                        "chunkserver/all-eligible-service-mode"
                    } else {
                        "chunkserver/replica-service-mode"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedReplicaServiceMode { prepared } => {
                self.apply_prepared_replica_service_mode(prepared.clone())?;
                Ok(self.replica_service_mode_control_metadata(
                    prepared.replica_index,
                    Some(prepared.chunk_prepared_replica_count),
                    if prepared.replica_index == PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET {
                        "chunkserver/prepared-all-eligible-service-mode"
                    } else {
                        "chunkserver/prepared-replica-service-mode"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::ConfigureChunkTrimFlow {
                rate_bytes_per_sec,
                burst_bytes,
                now_ms,
            } => {
                self.configure_chunk_trim_flow(rate_bytes_per_sec, burst_bytes, now_ms)?;
                Ok(self.chunk_trim_flow_control_metadata(
                    rate_bytes_per_sec,
                    None,
                    "chunkserver/trim-flow",
                    now_ms,
                ))
            }
            _ => unreachable!("command classified outside ChunkControl"),
        }
    }
}
