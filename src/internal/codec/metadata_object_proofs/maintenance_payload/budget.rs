pub(crate) fn apply_metadata_maintenance_payload_budget(
    prepared: &mut PreparedMetadataMaintenance,
) -> Result<(), ObjectError> {
    let mut payload_bytes = 0_u64;
    let max_payload_bytes = prepared.max_payload_bytes;
    let mut budget_limited = false;
    let mut budget_phase = String::new();
    let mut budget_marker = String::new();

    let mut tombstone_keep = prepared.compacted_tombstones.len();
    for (idx, record) in prepared.compacted_tombstones.iter().enumerate() {
        let item_bytes = metadata_record_payload_bytes(record);
        if max_payload_bytes > 0 && payload_bytes.saturating_add(item_bytes) > max_payload_bytes {
            budget_limited = true;
            budget_phase = "tombstones".to_string();
            budget_marker = if idx == 0 {
                String::new()
            } else {
                prepared.compacted_tombstones[idx - 1].key.to_string()
            };
            tombstone_keep = idx;
            break;
        }
        payload_bytes = payload_bytes.saturating_add(item_bytes);
    }
    if budget_limited {
        prepared.compacted_tombstones.truncate(tombstone_keep);
        prepared.expired_sessions.clear();
        prepared.expired_transactions.clear();
        prepared.lease_report.expired.clear();
        prepared.lease_report.expired_leases = 0;
    } else {
        let mut lease_keep = prepared.lease_report.expired.len();
        for (idx, lease) in prepared.lease_report.expired.iter().enumerate() {
            let item_bytes = metadata_maintenance_lease_payload_bytes(lease);
            if max_payload_bytes > 0 && payload_bytes.saturating_add(item_bytes) > max_payload_bytes
            {
                budget_limited = true;
                budget_phase = "leases".to_string();
                budget_marker = if idx == 0 {
                    String::new()
                } else {
                    prepared.lease_report.expired[idx - 1].namespace.clone()
                };
                lease_keep = idx;
                break;
            }
            payload_bytes = payload_bytes.saturating_add(item_bytes);
        }
        if budget_limited {
            prepared.lease_report.expired.truncate(lease_keep);
            prepared.expired_sessions.clear();
            prepared.expired_transactions.clear();
        } else {
            let mut session_keep = prepared.expired_sessions.len();
            for (idx, session) in prepared.expired_sessions.iter().enumerate() {
                let item_bytes = metadata_maintenance_session_payload_bytes(session);
                if max_payload_bytes > 0
                    && payload_bytes.saturating_add(item_bytes) > max_payload_bytes
                {
                    budget_limited = true;
                    budget_phase = "coordinator_sessions".to_string();
                    budget_marker = if idx == 0 {
                        String::new()
                    } else {
                        prepared.expired_sessions[idx - 1].session_id.clone()
                    };
                    session_keep = idx;
                    break;
                }
                payload_bytes = payload_bytes.saturating_add(item_bytes);
            }
            if budget_limited {
                prepared.expired_sessions.truncate(session_keep);
                prepared.expired_transactions.clear();
            } else {
                let mut transaction_keep = prepared.expired_transactions.len();
                for (idx, transaction) in prepared.expired_transactions.iter().enumerate() {
                    let item_bytes = metadata_maintenance_transaction_payload_bytes(transaction);
                    if max_payload_bytes > 0
                        && payload_bytes.saturating_add(item_bytes) > max_payload_bytes
                    {
                        budget_limited = true;
                        budget_phase = "transactions".to_string();
                        budget_marker = if idx == 0 {
                            String::new()
                        } else {
                            prepared.expired_transactions[idx - 1]
                                .transaction_id
                                .clone()
                        };
                        transaction_keep = idx;
                        break;
                    }
                    payload_bytes = payload_bytes.saturating_add(item_bytes);
                }
                if budget_limited {
                    prepared.expired_transactions.truncate(transaction_keep);
                }
            }
        }
    }

    let old_tombstone_count = prepared.tombstone_report.compacted_records;
    let tombstone_base = prepared
        .tombstone_report
        .compacted_tombstone_count
        .saturating_sub(old_tombstone_count);
    prepared.tombstone_report.compacted_records = prepared.compacted_tombstones.len() as u64;
    prepared.tombstone_report.compacted_bytes = prepared
        .compacted_tombstones
        .iter()
        .map(metadata_record_payload_bytes)
        .sum();
    prepared.tombstone_report.compacted_tombstone_count =
        tombstone_base.saturating_add(prepared.tombstone_report.compacted_records);
    if prepared.tombstone_report.compacted_records == 0 {
        prepared
            .tombstone_report
            .last_tombstone_compaction_commit_index = 0;
    }
    if budget_limited && budget_phase == "tombstones" {
        prepared.tombstone_report.budget_limited = true;
        prepared.tombstone_report.truncated = true;
        prepared.tombstone_report.next_key_marker = if budget_marker.is_empty() {
            None
        } else {
            Some(budget_marker.clone())
        };
    }
    prepared.lease_report.expired_leases = prepared.lease_report.expired.len() as u64;
    prepared.maintenance_payload_bytes = payload_bytes;
    prepared.payload_budget_limited = budget_limited;
    prepared.payload_budget_phase = budget_phase;
    prepared.payload_budget_marker = budget_marker;
    Ok(())
}
