// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Which gateways a client may talk to.
//!
//! The metaserver already keeps a roster with liveness. A gateway is a roster node of service
//! kind `objectstore`: the operator declares it (so it starts offline), the gateway heartbeats
//! it online, the metaserver's expiry loop takes it offline when the heartbeats stop, and a
//! gateway that is shutting down heartbeats `draining` first. Nothing about a gateway is
//! durable and nothing a client needs is pinned to one gateway, so the only question a client
//! has is "which of them is up", and this is the answer.

use super::{PlacementNodeSummary, RemoteMetaClient};
use crate::ObjectError;

/// The roster service kind of an object gateway.
pub const GATEWAY_SERVICE_KIND: &str = "objectstore";

/// One gateway a client may use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayEndpoint {
    pub node_id: String,
    /// `host:port` of the gateway's HTTP listener.
    pub endpoint: String,
    pub failure_domain: String,
}

/// The gateways in `nodes` that are fit to receive a request now: gateway kind, eligible,
/// online, and not draining. Sorted by node id so every caller sees the same order.
pub fn live_gateways(nodes: &[PlacementNodeSummary]) -> Vec<GatewayEndpoint> {
    let mut live: Vec<GatewayEndpoint> = nodes
        .iter()
        .filter(|node| {
            node.service_kind == GATEWAY_SERVICE_KIND
                && node.gateway_eligible
                && node.online
                && !node.draining
        })
        .map(|node| GatewayEndpoint {
            node_id: node.node_id.clone(),
            endpoint: node.endpoint.clone(),
            failure_domain: node.failure_domain.clone(),
        })
        .collect();
    live.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    live
}

impl RemoteMetaClient {
    /// The gateways that are up and accepting work, from the metaserver roster.
    pub fn list_gateways(&mut self) -> Result<Vec<GatewayEndpoint>, ObjectError> {
        Ok(live_gateways(&self.list_placement_nodes()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, kind: &str, eligible: bool, online: bool, draining: bool) -> PlacementNodeSummary {
        PlacementNodeSummary {
            node_id: id.into(),
            endpoint: format!("127.0.0.1:{}", 9000 + id.len()),
            service_kind: kind.into(),
            failure_domain: "rack-a".into(),
            metadata_eligible: false,
            data_eligible: kind == "chunkserver",
            gateway_eligible: eligible,
            draining,
            online,
            metadata_value_bytes: 0,
            capacity_bytes: 0,
            used_bytes: 0,
            committed_bytes: 0,
            last_heartbeat_ms: 1,
            allocator_shard_index: 0,
            allocator_shard_count: 1,
        }
    }

    #[test]
    fn only_online_undrained_gateway_nodes_are_listed_in_id_order() {
        let nodes = vec![
            node("gw-b", "objectstore", true, true, false),
            node("gw-a", "objectstore", true, true, false),
            node("gw-offline", "objectstore", true, false, false),
            node("gw-draining", "objectstore", true, true, true),
            node("gw-ineligible", "objectstore", false, true, false),
            node("chunk-1", "chunkserver", false, true, false),
            node("chunk-2", "chunkserver", true, true, false),
        ];
        let live: Vec<_> = live_gateways(&nodes).into_iter().map(|g| g.node_id).collect();
        assert_eq!(live, ["gw-a", "gw-b"]);
    }

    #[test]
    fn no_gateways_is_an_empty_list_not_an_error() {
        assert!(live_gateways(&[]).is_empty());
        assert!(live_gateways(&[node("chunk-1", "chunkserver", false, true, false)]).is_empty());
    }
}
