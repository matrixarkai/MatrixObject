// SPDX-License-Identifier: Apache-2.0
pub(crate) fn metadata_route_cache_expire_time_ms(
    route: &MetadataShardRoute,
    now_ms: u64,
    cache_ttl_ms: u64,
) -> u64 {
    let ttl_expire = now_ms.saturating_add(cache_ttl_ms);
    route
        .active_owner_session
        .as_ref()
        .map_or(ttl_expire, |session| ttl_expire.min(session.expire_time_ms))
}

pub(crate) fn metadata_route_topology_fingerprint(meta: &MatrixMetaService) -> (u64, String) {
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
    append_u64(&mut material, meta.coordinator_sessions.len() as u64);
    for session in meta.coordinator_sessions.values() {
        append_string(&mut material, &session.session_id);
        append_string(&mut material, &session.owner);
        append_string(&mut material, &session.endpoint);
        append_u64(&mut material, session.epoch);
        append_u64(&mut material, session.expire_time_ms);
        topology_epoch = topology_epoch.max(session.epoch);
    }
    (topology_epoch, etag(&material))
}

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
