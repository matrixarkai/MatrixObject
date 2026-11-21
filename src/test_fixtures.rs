// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Shared fixtures for the tests of the metadata plane.
use super::*;

pub(crate) fn placement_node(node_id: &str, domain: &str, capacity_bytes: u64) -> PlacementNode {
    PlacementNode {
        node_id: node_id.to_string(),
        endpoint: format!("{node_id}.matrix.local:9102"),
        service_kind: "chunkserver".to_string(),
        failure_domain: domain.to_string(),
        metadata_eligible: false,
        data_eligible: true,
        gateway_eligible: false,
        serves_page_tier: false,
        metadata_value_bytes: 0,
        capacity_bytes,
        used_bytes: 0,
        committed_bytes: 0,
        draining: false,
        online: true,
        last_heartbeat_ms: 1,
        page_allocator_shard_index: 0,
        page_allocator_shard_count: 1,
        meta_health: NodeMetaHealth::default(),
        page_health: NodePageHealth::default(),
        chunk_health: NodeChunkHealth::default(),
    }
}

pub(crate) fn manifest_publication_fixture() -> (
    MatrixMetaService, crate::placement_outcome::PlacementOutcome, crate::placement_outcome::PlacementOutcome
) {
    use crate::object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest};
    use crate::placement_outcome::PlacementOutcome;
    let mut service = MatrixMetaService::new();
    for name in ["group-a", "group-b"] {
        let mut node = placement_node(name, name, 1000);
        node.meta_health.control_proof.coordinator_backend = "embedded-raft".into();
        node.chunk_health.accepting_writes = true;
        node.chunk_health.raft_writable = true;
        service.register_node(node).unwrap();
    }
    let one = ObjectManifest {
        format_version: 1, bucket: "bucket".into(), key: "object".into(),
        generation: 1, length: 4, deleted: false,
        extents: vec![ObjectExtent {
            group: "group-a".into(), volume_id: 1, chunk_id: 1, page_index: 0,
            object_offset: 0, page_offset: 0, length: 4, write_sequence: 1, data_checksum: 42,
            data_sha256: Vec::new(), ..Default::default()
        }],
    };
    let create = PlacementOutcome::publish_manifest(
        &service, ManifestOutcome::decide(None, one.clone()).unwrap(), 10
    ).unwrap();
    let mut working = service.clone();
    create.apply(&mut working).unwrap();
    let mut two = one.clone();
    two.generation = 2;
    two.length = 8;
    two.extents[0].length = 8;
    two.extents[0].group = "group-b".into();
    let replace = PlacementOutcome::publish_manifest(
        &working, ManifestOutcome::decide(Some(one), two).unwrap(), 20
    ).unwrap();
    (service, create, replace)
}
