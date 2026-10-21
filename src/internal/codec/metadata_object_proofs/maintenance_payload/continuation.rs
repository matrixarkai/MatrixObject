// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn metadata_maintenance_continuation(
    lease_report: &MetadataLeaseExpiryReport,
    expired_sessions: &[CoordinatorSession],
    expired_transactions: &[MetadataTransaction],
    tombstone_report: &MetadataTombstoneCompactionReport,
    lease_limit: usize,
    session_limit: usize,
    transaction_limit: usize,
    payload_budget_limited: bool,
    payload_budget_phase: &str,
    payload_budget_marker: &str,
) -> MetadataMaintenanceContinuation {
    let tombstone_marker = tombstone_report.next_key_marker.clone().unwrap_or_default();
    let tombstone_has_more = tombstone_report.truncated || tombstone_report.budget_limited;
    let tombstone_marker_advanced = !tombstone_has_more || !tombstone_marker.is_empty();
    let lease_marker = lease_report
        .expired
        .last()
        .map(|lease| lease.namespace.clone())
        .unwrap_or_default();
    let lease_has_more = lease_limit > 0 && lease_report.expired.len() >= lease_limit;
    let lease_marker_advanced = !lease_has_more || !lease_marker.is_empty();
    let session_marker = expired_sessions
        .last()
        .map(|session| session.session_id.clone())
        .unwrap_or_default();
    let session_has_more = session_limit > 0 && expired_sessions.len() >= session_limit;
    let session_marker_advanced = !session_has_more || !session_marker.is_empty();
    let transaction_marker = expired_transactions
        .last()
        .map(|transaction| transaction.transaction_id.clone())
        .unwrap_or_default();
    let transaction_has_more =
        transaction_limit > 0 && expired_transactions.len() >= transaction_limit;
    let transaction_marker_advanced = !transaction_has_more || !transaction_marker.is_empty();
    let payload_budget_marker_advanced =
        !payload_budget_limited || !payload_budget_marker.is_empty();
    let payload_phase_advanced = |phase: &str, phase_marker_advanced: bool| {
        if payload_budget_limited && payload_budget_phase == phase {
            payload_budget_marker_advanced
        } else {
            phase_marker_advanced
        }
    };
    let effective_tombstone_marker_advanced =
        payload_phase_advanced("tombstones", tombstone_marker_advanced);
    let effective_lease_marker_advanced = payload_phase_advanced("leases", lease_marker_advanced);
    let effective_session_marker_advanced =
        payload_phase_advanced("coordinator_sessions", session_marker_advanced);
    let effective_transaction_marker_advanced =
        payload_phase_advanced("transactions", transaction_marker_advanced);
    let mut continuation = MetadataMaintenanceContinuation {
        has_more: tombstone_has_more
            || lease_has_more
            || session_has_more
            || transaction_has_more
            || payload_budget_limited,
        continuation_safe: effective_tombstone_marker_advanced
            && effective_lease_marker_advanced
            && effective_session_marker_advanced
            && effective_transaction_marker_advanced
            && payload_budget_marker_advanced,
        payload_budget_limited,
        tombstone_marker_advanced: effective_tombstone_marker_advanced,
        lease_marker_advanced: effective_lease_marker_advanced,
        coordinator_session_marker_advanced: effective_session_marker_advanced,
        transaction_marker_advanced: effective_transaction_marker_advanced,
        ..MetadataMaintenanceContinuation::default()
    };
    if tombstone_has_more {
        continuation.phase = "tombstones".to_string();
        continuation.marker = if payload_budget_limited && payload_budget_phase == "tombstones" {
            payload_budget_marker.to_string()
        } else {
            tombstone_marker
        };
        continuation.reason = if payload_budget_limited && payload_budget_phase == "tombstones" {
            "metadata_maintenance_payload_budget_limited".to_string()
        } else if tombstone_report.budget_limited {
            "metadata_tombstone_compaction_budget_limited".to_string()
        } else {
            "metadata_tombstone_compaction_truncated".to_string()
        };
    } else if lease_has_more {
        continuation.phase = "leases".to_string();
        continuation.marker = if payload_budget_limited && payload_budget_phase == "leases" {
            payload_budget_marker.to_string()
        } else {
            lease_marker
        };
        continuation.reason = if payload_budget_limited && payload_budget_phase == "leases" {
            "metadata_maintenance_payload_budget_limited".to_string()
        } else {
            "metadata_lease_expiry_truncated".to_string()
        };
    } else if session_has_more {
        continuation.phase = "coordinator_sessions".to_string();
        continuation.marker =
            if payload_budget_limited && payload_budget_phase == "coordinator_sessions" {
                payload_budget_marker.to_string()
            } else {
                session_marker
            };
        continuation.reason =
            if payload_budget_limited && payload_budget_phase == "coordinator_sessions" {
                "metadata_maintenance_payload_budget_limited".to_string()
            } else {
                "metadata_coordinator_session_expiry_truncated".to_string()
            };
    } else if transaction_has_more {
        continuation.phase = "transactions".to_string();
        continuation.marker = if payload_budget_limited && payload_budget_phase == "transactions" {
            payload_budget_marker.to_string()
        } else {
            transaction_marker
        };
        continuation.reason = if payload_budget_limited && payload_budget_phase == "transactions" {
            "metadata_maintenance_payload_budget_limited".to_string()
        } else {
            "metadata_transaction_gc_truncated".to_string()
        };
    } else if payload_budget_limited {
        continuation.phase = if payload_budget_phase.is_empty() {
            "payload_budget".to_string()
        } else {
            payload_budget_phase.to_string()
        };
        continuation.marker = payload_budget_marker.to_string();
        continuation.reason = "metadata_maintenance_payload_budget_limited".to_string();
    }
    continuation
}
