// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn append_readiness_identity(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_string(&mut material, "MOSRSOBJECTREADY77");
    append_u64(&mut material, report.ready as u64);
    append_string(&mut material, &report.reason);
    append_u64(&mut material, report.metadata_ready as u64);
    append_u64(&mut material, report.page_ready as u64);
    append_u64(&mut material, report.placement_ready as u64);
    append_string(&mut material, &report.control_plane_mode);
    append_u64(&mut material, report.placement_required as u64);
    append_string(&mut material, &report.root_status);
    append_u64(&mut material, report.placement_leader_ready as u64);
    append_u64(&mut material, report.placement_read_ready as u64);
    append_u64(&mut material, report.placement_metadata_read_ready as u64);
    append_u64(&mut material, report.placement_metadata_write_ready as u64);
    append_u64(&mut material, report.root_pageserver_read_ready as u64);
    append_u64(&mut material, report.root_pageserver_write_ready as u64);
    append_u64(&mut material, report.root_replicaserver_read_ready as u64);
    append_u64(&mut material, report.root_replicaserver_write_ready as u64);
    append_u64(&mut material, report.placement_readiness_gate_read_ready as u64);
    append_u64(&mut material, report.placement_readiness_gate_write_ready as u64);
    append_string(&mut material, &report.metadata_state_fingerprint);
    append_string(&mut material, &report.placement_state_fingerprint);
    append_string(&mut material, &report.placement_raft_status_checksum_format);
    append_string(&mut material, &report.root_raft_status_checksum);
    append_string(&mut material, &report.page_state_fingerprint);
    append_string(&mut material, &report.page_state_fingerprint_format);
    append_u64(&mut material, report.page_state_fingerprint_bytes);
    append_string(&mut material, &report.chunk_state_fingerprint);
    append_string(&mut material, &report.chunk_state_fingerprint_format);
    append_u64(&mut material, report.chunk_state_fingerprint_bytes);
}
