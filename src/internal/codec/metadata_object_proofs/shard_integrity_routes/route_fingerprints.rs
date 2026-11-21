// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn metadata_shard_topology_fingerprint(meta: &MatrixMetaService) -> (u64, String) {
    let mut material = Vec::new();
    let mut topology_epoch = 0u64;
    append_u64(&mut material, meta.shards.len() as u64);
    for shard in meta.shards.values() {
        append_string(&mut material, &shard.shard_id);
        append_string(&mut material, &shard.start_key);
        append_string(&mut material, &shard.end_key);
        append_string(&mut material, &shard.owner_node_id);
        append_u64(&mut material, shard.epoch);
        append_u64(&mut material, shard.draining as u64);
        topology_epoch = topology_epoch.max(shard.epoch);
    }
    (topology_epoch, etag(&material))
}

pub(crate) fn metadata_route_session_fingerprint(meta: &MatrixMetaService, now_ms: u64) -> String {
    let mut material = Vec::new();
    for session in meta.coordinator_sessions.values() {
        if now_ms > 0 && session.expire_time_ms <= now_ms {
            continue;
        }
        append_string(&mut material, &session.session_id);
        append_string(&mut material, &session.owner);
        append_string(&mut material, &session.endpoint);
        append_u64(&mut material, session.epoch);
        append_u64(&mut material, session.expire_time_ms);
    }
    etag(&material)
}
