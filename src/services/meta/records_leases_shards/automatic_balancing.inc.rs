// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn plan_oversized_shard_splits(
        &self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        self.plan_oversized_shard_splits_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            None,
            None,
        )
    }

    pub fn plan_oversized_shard_splits_fenced(
        &self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        if max_records_per_shard == 0 && max_payload_bytes_per_shard == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata split planner requires a record or payload threshold".to_string(),
            ));
        }
        if max_splits == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata split planner max_splits must be positive".to_string(),
            ));
        }
        let (metadata_shard_topology_epoch, metadata_shard_topology_checksum) =
            metadata_shard_topology_fingerprint(self);
        if let Some(expected_epoch) = expected_shard_topology_epoch {
            if expected_epoch != metadata_shard_topology_epoch {
                return Err(ObjectError::PreconditionFailed(format!(
                    "metadata shard split topology epoch fence mismatch: expected {} actual {}",
                    expected_epoch, metadata_shard_topology_epoch
                )));
            }
        }
        if let Some(expected_checksum) = expected_shard_topology_checksum {
            if expected_checksum.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard split topology checksum fence cannot be empty".to_string(),
                ));
            }
            if expected_checksum != metadata_shard_topology_checksum {
                return Err(ObjectError::PreconditionFailed(format!(
                    "metadata shard split topology checksum fence mismatch: expected {} actual {}",
                    expected_checksum, metadata_shard_topology_checksum
                )));
            }
        }
        let mut report = MetadataShardSplitReport {
            metadata_shard_topology_epoch,
            metadata_shard_topology_checksum,
            expected_metadata_shard_topology_epoch: expected_shard_topology_epoch,
            expected_metadata_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            ..MetadataShardSplitReport::default()
        };
        for shard in self.shards.values() {
            report.scanned_shards = report.scanned_shards.saturating_add(1);
            let mut keys = Vec::new();
            let mut payload_bytes = 0u64;
            for (key, record) in self.records.range::<str, _>((
                std::ops::Bound::Included(shard.start_key.as_str()),
                std::ops::Bound::Unbounded,
            )) {
                if !shard.end_key.is_empty() && key.as_str() >= shard.end_key.as_str() {
                    break;
                }
                report.scanned_records = report.scanned_records.saturating_add(1);
                keys.push(key.clone());
                payload_bytes = payload_bytes.saturating_add(metadata_record_payload_bytes(record));
            }
            let record_count = keys.len();
            let record_pressure = max_records_per_shard > 0 && record_count > max_records_per_shard;
            let payload_pressure =
                max_payload_bytes_per_shard > 0 && payload_bytes > max_payload_bytes_per_shard;
            if !record_pressure && !payload_pressure {
                continue;
            }
            report.candidate_shards = report.candidate_shards.saturating_add(1);
            report.candidate_records = report.candidate_records.saturating_add(record_count as u64);
            report.candidate_payload_bytes =
                report.candidate_payload_bytes.saturating_add(payload_bytes);
            if report.actions.len() >= max_splits {
                report.skipped_shards = report.skipped_shards.saturating_add(1);
                continue;
            }
            if record_count < 2 {
                report.skipped_shards = report.skipped_shards.saturating_add(1);
                continue;
            }
            let mut split_index = record_count / 2;
            while split_index < record_count
                && (keys[split_index].as_str() <= shard.start_key.as_str()
                    || (!shard.end_key.is_empty()
                        && keys[split_index].as_str() >= shard.end_key.as_str()))
            {
                split_index = split_index.saturating_add(1);
            }
            if split_index >= record_count {
                report.skipped_shards = report.skipped_shards.saturating_add(1);
                continue;
            }
            let split_key = keys[split_index].clone();
            let new_epoch = shard.epoch.saturating_add(1);
            let reason = match (record_pressure, payload_pressure) {
                (true, true) => "record_and_payload_pressure",
                (true, false) => "record_pressure",
                (false, true) => "payload_pressure",
                (false, false) => "unknown",
            }
            .to_string();
            report.actions.push(MetadataShardSplitAction {
                source_shard_id: shard.shard_id.clone(),
                split_key: split_key.to_string(),
                left_shard_id: format!("{}:s{}l", shard.shard_id, new_epoch),
                right_shard_id: format!("{}:s{}r", shard.shard_id, new_epoch),
                owner_node_id: shard.owner_node_id.clone(),
                expected_epoch: shard.epoch,
                new_epoch,
                record_count: record_count as u64,
                payload_bytes,
                left_record_count: split_index as u64,
                right_record_count: record_count.saturating_sub(split_index) as u64,
                reason,
            });
            report.planned_shards = report.planned_shards.saturating_add(1);
        }
        finalize_metadata_shard_split_report(&mut report);
        Ok(report)
    }

    pub fn split_oversized_shards(
        &mut self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        self.split_oversized_shards_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            "",
            None,
            None,
        )
    }

    pub fn split_oversized_shards_fenced(
        &mut self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        let mut report = self.plan_oversized_shard_splits_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        )?;
        if !expected_plan_checksum.is_empty() && report.plan_checksum != expected_plan_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard split plan checksum fence mismatch: expected {} actual {}",
                expected_plan_checksum, report.plan_checksum
            )));
        }
        let plan_checksum = report.plan_checksum.clone();
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        for action in report.actions.clone() {
            match self.split_shard(
                &action.source_shard_id,
                &action.split_key,
                &action.left_shard_id,
                &action.right_shard_id,
                &action.owner_node_id,
                &action.owner_node_id,
                action.new_epoch,
                false,
                false,
                now_ms,
            ) {
                Ok(_) => report.split_shards = report.split_shards.saturating_add(1),
                Err(err) => {
                    report.failed_shards = report.failed_shards.saturating_add(1);
                    report
                        .errors
                        .push(format!("{}: {err}", action.source_shard_id));
                }
            }
        }
        finalize_metadata_shard_split_report(&mut report);
        report.plan_checksum = plan_checksum;
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        Ok(report)
    }
}
