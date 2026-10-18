impl MatrixObjectStore {
    fn apply_page_storage_metadata_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ApplyPreparedPageAuditRepair { prepared } => {
                let (report, _) = self.apply_prepared_page_audit_repair(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockauditrepair",
                    "prepared-audit",
                    report.repaired_chunks,
                    report.unrepaired_chunks,
                    if report.audit.has_more {
                        "blockserver/prepared-audit-repair-more"
                    } else {
                        "blockserver/prepared-audit-repair"
                    },
                    report
                        .audit
                        .next_marker
                        .as_ref()
                        .map_or(0, |chunk_id| chunk_id.chunk_index),
                ))
            }
            ObjectCommand::ApplyPreparedPageRangeRepair { prepared } => {
                let (report, _) = self.apply_prepared_page_range_repair(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockrepair",
                    "prepared-range",
                    report.selected_chunk_count,
                    report.stats.repaired_replicas,
                    if report.has_more {
                        "blockserver/prepared-bounded-repair-more"
                    } else {
                        "blockserver/prepared-bounded-repair"
                    },
                    report.next_chunk_index,
                ))
            }
            ObjectCommand::ApplyPreparedPageChunkRepair { prepared } => {
                let (report, _) = self.apply_prepared_page_chunk_repair(prepared)?;
                Ok(metadata_control_metadata(
                    "_blockrepair",
                    "prepared-chunk",
                    report.checked_chunks,
                    report.repaired_replicas,
                    if report.unrepaired_chunks > 0 || report.failed_repairs > 0 {
                        "blockserver/prepared-exact-chunk-repair-partial"
                    } else {
                        "blockserver/prepared-exact-chunk-repair"
                    },
                    report
                        .chunk_id
                        .as_ref()
                        .map_or(0, |chunk_id| chunk_id.chunk_index),
                ))
            }
            ObjectCommand::ScrubPageReplicas { repair } => {
                let report = self.scrub_page_replicas(repair)?;
                Ok(metadata_control_metadata(
                    "_blockscrub",
                    if repair { "repair" } else { "plan" },
                    report.checked_chunks,
                    report.repaired_replicas,
                    if repair {
                        "blockserver/replica-scrub-repair"
                    } else {
                        "blockserver/replica-scrub-plan"
                    },
                    now_millis(),
                ))
            }
            ObjectCommand::RepairPageRangeBounded {
                slab_id,
                offset,
                length,
                max_repair_chunks,
                start_chunk_index,
                expected_route_control_checksum,
            } => {
                self.page_service.check_expected_route_control_checksum(
                    (!expected_route_control_checksum.is_empty())
                        .then_some(expected_route_control_checksum.as_str()),
                )?;
                let mut report = self.page_service.repair_slab_range_bounded(
                    &slab_id,
                    offset,
                    length,
                    max_repair_chunks,
                    start_chunk_index,
                )?;
                report.expected_route_control_checksum = expected_route_control_checksum.clone();
                Ok(metadata_control_metadata(
                    "_blockrepair",
                    &format!("{}:{}", slab_id.volume_id, slab_id.id),
                    report.selected_chunk_count,
                    report.stats.repaired_replicas,
                    if report.has_more {
                        "blockserver/bounded-repair-more"
                    } else {
                        "blockserver/bounded-repair"
                    },
                    report.next_chunk_index,
                ))
            }
            ObjectCommand::RepairPageChunk {
                chunk_id,
                expected_route_control_checksum,
            } => {
                let report = self.repair_page_chunk_fenced(
                    &chunk_id,
                    (!expected_route_control_checksum.is_empty())
                        .then_some(expected_route_control_checksum.as_str()),
                )?;
                Ok(metadata_control_metadata(
                    "_blockrepair",
                    &format!(
                        "{}:{}:{}",
                        chunk_id.slab_id.volume_id, chunk_id.slab_id.id, chunk_id.chunk_index
                    ),
                    report.checked_chunks,
                    report.repaired_replicas,
                    if report.unrepaired_chunks > 0 || report.failed_repairs > 0 {
                        "blockserver/exact-chunk-repair-partial"
                    } else {
                        "blockserver/exact-chunk-repair"
                    },
                    chunk_id.chunk_index,
                ))
            }
            ObjectCommand::RepairAuditedPageReplicas {
                after_chunk_id,
                max_chunks,
                max_payload_bytes,
            } => {
                let report = self.repair_audited_page_replicas(
                    after_chunk_id.as_ref(),
                    max_chunks,
                    max_payload_bytes,
                )?;
                Ok(metadata_control_metadata(
                    "_blockauditrepair",
                    "audit",
                    report.candidate_chunks,
                    report.stats.repaired_replicas,
                    if report.audit.has_more {
                        "blockserver/audit-repair-more"
                    } else {
                        "blockserver/audit-repair"
                    },
                    report
                        .audit
                        .next_marker
                        .as_ref()
                        .map_or(0, |marker| marker.chunk_index),
                ))
            }
            ObjectCommand::ApplyChunkStateHandoffs {
                target_replica_index,
                handoffs,
                expected_page_route_control_checksum,
            } => {
                self.page_service.check_expected_route_control_checksum(
                    (!expected_page_route_control_checksum.is_empty())
                        .then_some(expected_page_route_control_checksum.as_str()),
                )?;
                let stats = self
                    .page_service
                    .apply_chunk_state_handoffs(target_replica_index, handoffs)?;
                Ok(metadata_control_metadata(
                    "_chunkhandoff",
                    &format!("replica-{}", target_replica_index),
                    stats.checked_chunks,
                    stats.repaired_replicas,
                    if stats.failed_repairs > 0 {
                        "chunkserver/state-handoff-partial"
                    } else {
                        "chunkserver/state-handoff"
                    },
                    stats.failed_repairs,
                ))
            }
            ObjectCommand::ReclaimChunkReplicas {
                max_chunks,
                max_rewrite_bytes,
                min_discarded_bytes,
            } => {
                let report =
                    self.reclaim_chunk_replicas(max_chunks, max_rewrite_bytes, min_discarded_bytes);
                Ok(metadata_control_metadata(
                    "_chunkreclaim",
                    "partial",
                    report.reclaimed_chunks,
                    report.reclaimed_discard_bytes,
                    if report.budget_limited {
                        "chunkserver/partial-reclaim-more"
                    } else {
                        "chunkserver/partial-reclaim"
                    },
                    report.skipped_rewrite_bytes,
                ))
            }
            ObjectCommand::ReclaimChunkReplicasWithLimits {
                max_chunks,
                max_rewrite_bytes,
                min_discarded_bytes,
                min_discard_ratio_ppm,
                max_discard_ranges,
                max_trim_bytes,
                now_ms,
            } => {
                let report = self.reclaim_chunk_replicas_with_limits_at(
                    max_chunks,
                    max_rewrite_bytes,
                    min_discarded_bytes,
                    min_discard_ratio_ppm,
                    max_discard_ranges,
                    max_trim_bytes,
                    now_ms,
                );
                Ok(metadata_control_metadata(
                    "_chunkreclaim",
                    "targeted",
                    report.reclaimed_chunks,
                    report.reclaimed_discard_bytes,
                    if report.trim_flow_limited {
                        "chunkserver/targeted-reclaim-throttled"
                    } else if report.budget_limited {
                        "chunkserver/targeted-reclaim-more"
                    } else {
                        "chunkserver/targeted-reclaim"
                    },
                    report.rewritten_bytes,
                ))
            }
            ObjectCommand::RunStorageMaintenance {
                max_actions,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                max_reclaim_chunks,
                max_rewrite_bytes,
                min_discarded_bytes,
                min_discard_ratio_ppm,
                max_discard_ranges,
                max_trim_bytes,
                now_ms,
                expected_plan_checksum,
                expected_page_route_control_checksum,
            } => {
                let report = self.run_storage_maintenance_control_fenced(
                    max_actions,
                    min_capacity_free_bytes,
                    min_reclaimable_bytes,
                    max_reclaim_chunks,
                    max_rewrite_bytes,
                    min_discarded_bytes,
                    min_discard_ratio_ppm,
                    max_discard_ranges,
                    max_trim_bytes,
                    now_ms,
                    &expected_plan_checksum,
                    &expected_page_route_control_checksum,
                )?;
                let content_type = if report.reclaim_continuation_blocked {
                    "storage/maintenance-continuation-blocked"
                } else if report.reclaim.trim_flow_limited {
                    "storage/maintenance-throttled"
                } else if report.plan.truncated || report.reclaim.budget_limited {
                    "storage/maintenance-more"
                } else if report.repair.repaired_chunks > 0 && report.reclaimed {
                    "storage/maintenance-repair-reclaim"
                } else if report.repair.repaired_chunks > 0 {
                    "storage/maintenance-repair"
                } else if report.reclaimed {
                    "storage/maintenance-reclaim"
                } else {
                    "storage/maintenance-skip"
                };
                let mut metadata = metadata_control_metadata(
                    "_storagemaintenance",
                    "run",
                    report.executed_actions,
                    report
                        .reclaim
                        .reclaimed_discard_bytes
                        .saturating_add(report.repair.stats.repaired_replicas),
                    content_type,
                    report.skipped_actions,
                );
                metadata.tags.insert(
                    "route_control_fenced".to_string(),
                    report.route_control_fenced.to_string(),
                );
                metadata.tags.insert(
                    "route_control_fence_matched".to_string(),
                    report.route_control_fence_matched.to_string(),
                );
                metadata.tags.insert(
                    "route_control_checksum".to_string(),
                    report.route_control_proof.state_checksum.clone(),
                );
                metadata.tags.insert(
                    "route_control_commit_index".to_string(),
                    report
                        .route_control_proof
                        .page_route_control_commit_index
                        .to_string(),
                );
                if report.route_control_fenced {
                    metadata.tags.insert(
                        "expected_route_control_checksum".to_string(),
                        report.expected_route_control_checksum.clone(),
                    );
                }
                Ok(metadata)
            }
            _ => unreachable!("command classified outside BlockStorage"),
        }
    }
}
