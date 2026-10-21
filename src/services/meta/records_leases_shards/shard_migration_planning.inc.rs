// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    fn plan_shard_owner_migration(
        &self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        draining: bool,
        reason: &str,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        validate_meta_key(source_owner_node_id)?;
        validate_meta_key(target_owner_node_id)?;
        if source_owner_node_id == target_owner_node_id {
            return Err(ObjectError::InvalidArgument(
                "metadata shard migration owners must differ".to_string(),
            ));
        }
        if max_shards == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata shard migration max_shards must be positive".to_string(),
            ));
        }
        self.validate_shard_owner_session(
            &MetadataShard {
                shard_id: "_planned-target".to_string(),
                start_key: String::new(),
                end_key: String::new(),
                owner_node_id: target_owner_node_id.to_string(),
                epoch: 1,
                draining,
            },
            now_ms,
        )?;
        let (metadata_shard_topology_epoch, metadata_shard_topology_checksum) =
            metadata_shard_topology_fingerprint(self);
        if let Some(expected_epoch) = expected_shard_topology_epoch {
            if expected_epoch != metadata_shard_topology_epoch {
                return Err(ObjectError::PreconditionFailed(format!(
                    "metadata shard migration topology epoch fence mismatch: expected {} actual {}",
                    expected_epoch, metadata_shard_topology_epoch
                )));
            }
        }
        if let Some(expected_checksum) = expected_shard_topology_checksum {
            if expected_checksum.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard migration topology checksum fence cannot be empty".to_string(),
                ));
            }
            if expected_checksum != metadata_shard_topology_checksum {
                return Err(ObjectError::PreconditionFailed(format!(
                    "metadata shard migration topology checksum fence mismatch: expected {} actual {}",
                    expected_checksum, metadata_shard_topology_checksum
                )));
            }
        }
        let metadata_coordinator_session_checksum =
            metadata_route_session_fingerprint(self, now_ms);
        if let Some(expected_checksum) = expected_coordinator_session_checksum {
            if expected_checksum.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard migration coordinator session checksum fence cannot be empty"
                        .to_string(),
                ));
            }
            if expected_checksum != metadata_coordinator_session_checksum {
                return Err(ObjectError::PreconditionFailed(format!(
                    "metadata shard migration coordinator session checksum fence mismatch: expected {} actual {}",
                    expected_checksum, metadata_coordinator_session_checksum
                )));
            }
        }

        let mut report = MetadataShardMigrationReport {
            metadata_shard_topology_epoch,
            metadata_shard_topology_checksum,
            metadata_coordinator_session_checksum,
            expected_metadata_shard_topology_epoch: expected_shard_topology_epoch,
            expected_metadata_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            expected_metadata_coordinator_session_checksum: expected_coordinator_session_checksum
                .unwrap_or_default()
                .to_string(),
            ..MetadataShardMigrationReport::default()
        };
        for shard in self.shards.values() {
            report.scanned_shards = report.scanned_shards.saturating_add(1);
            if shard.owner_node_id != source_owner_node_id {
                continue;
            }
            report.candidate_shards = report.candidate_shards.saturating_add(1);
            if report.actions.len() >= max_shards {
                report.skipped_shards = report.skipped_shards.saturating_add(1);
                continue;
            }
            report.actions.push(MetadataShardMigrationAction {
                shard_id: shard.shard_id.clone(),
                current_owner_node_id: shard.owner_node_id.clone(),
                target_owner_node_id: target_owner_node_id.to_string(),
                expected_epoch: shard.epoch,
                new_epoch: shard.epoch.saturating_add(1),
                draining,
                reason: reason.to_string(),
            });
            report.planned_shards = report.planned_shards.saturating_add(1);
        }
        finalize_metadata_shard_migration_report(&mut report, reason);
        Ok(report)
    }

    fn execute_planned_shard_migrations(
        &mut self,
        report: &mut MetadataShardMigrationReport,
        now_ms: u64,
    ) {
        for action in report.actions.clone() {
            match self.migrate_shard(
                &action.shard_id,
                &action.target_owner_node_id,
                action.expected_epoch,
                action.new_epoch,
                action.draining,
                now_ms,
            ) {
                Ok(_) => {
                    report.migrated_shards = report.migrated_shards.saturating_add(1);
                }
                Err(err) => {
                    report.failed_shards = report.failed_shards.saturating_add(1);
                    report.errors.push(format!("{}: {err}", action.shard_id));
                }
            }
        }
        let operation_reason = report
            .actions
            .first()
            .map(|action| action.reason.as_str())
            .unwrap_or("migration");
        let operation_reason = operation_reason.to_string();
        finalize_metadata_shard_migration_report(report, &operation_reason);
    }

    fn validate_shard_image(&self, shard: &MetadataShard) -> Result<(), ObjectError> {
        if shard.epoch == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata shard epoch must be positive".to_string(),
            ));
        }
        if !shard.end_key.is_empty() && shard.start_key >= shard.end_key {
            return Err(ObjectError::InvalidArgument(
                "metadata shard range is invalid".to_string(),
            ));
        }
        validate_meta_key(&shard.owner_node_id)?;
        Ok(())
    }

    fn validate_shard_owner_session(
        &self,
        shard: &MetadataShard,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if shard.draining {
            return Ok(());
        }
        if is_embedded_coordinator_backend(&self.coordinator_config.backend)
            && self.coordinator_sessions.is_empty()
        {
            return Ok(());
        }
        if self
            .active_fenced_owner_session(&shard.owner_node_id, now_ms)
            .is_some()
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(
            "metadata shard owner has no active endpoint-fenced coordinator session".to_string(),
        ))
    }
}
