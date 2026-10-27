// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementNode {
    pub node_id: String,
    pub endpoint: String,
    pub service_kind: String,
    pub failure_domain: String,
    pub metadata_eligible: bool,
    pub data_eligible: bool,
    pub gateway_eligible: bool,
    /// WHETHER THIS NODE SERVES THE PAGE TIER.
    ///
    /// The store-plane node kinds merge, so the kind can no longer answer this: a node
    /// serving pages and a node serving chunks are both `chunkserver`. This field carries
    /// what the kind used to imply.
    ///
    /// A record written before this field existed does not carry it, and the decoder
    /// derives it from the kind that record was written with -- so every existing record
    /// keeps its exact current meaning and nothing has to be rewritten.
    pub serves_page_tier: bool,
    pub metadata_value_bytes: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub committed_bytes: u64,
    pub draining: bool,
    pub online: bool,
    pub last_heartbeat_ms: u64,
    pub page_allocator_shard_index: u64,
    pub page_allocator_shard_count: u64,
    pub meta_health: NodeMetaHealth,
    pub page_health: NodePageHealth,
    pub chunk_health: NodeChunkHealth,
}

impl PlacementNode {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("node_id", &self.node_id),
            ("endpoint", &self.endpoint),
            ("service_kind", &self.service_kind),
            ("failure_domain", &self.failure_domain),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("metadata_eligible", self.metadata_eligible),
            ("data_eligible", self.data_eligible),
            ("gateway_eligible", self.gateway_eligible),
            ("draining", self.draining),
            ("online", self.online),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("metadata_value_bytes", self.metadata_value_bytes),
            ("capacity_bytes", self.capacity_bytes),
            ("used_bytes", self.used_bytes),
            ("committed_bytes", self.committed_bytes),
            ("last_heartbeat_ms", self.last_heartbeat_ms),
            (
                "page_allocator_shard_index",
                self.page_allocator_shard_index,
            ),
            (
                "page_allocator_shard_count",
                self.page_allocator_shard_count,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}


impl PlacementNode {
    /// WHETHER THIS NODE SERVES THE PAGE TIER.
    ///
    /// The body is today's kind comparison, unchanged, so introducing this changes no
    /// behaviour. The point is that eight predicates previously spelled this question
    /// out for themselves, which made it eight places to get wrong and eight places to
    /// find. It is now one.
    ///
    /// It is deliberately NOT `gateway_eligible`, even though that flag happens to be
    /// true for exactly the page registrations today: `gateway_eligible` means the node
    /// can serve as an object gateway, and both of its own call sites pair it with
    /// `service_kind == "objectstore"`. A rule gated on a label that merely correlates
    /// is the trap this method exists to make visible rather than to repeat.
    ///
    /// When the store-plane kinds merge, the body of THIS function is what changes --
    /// one line, one place -- and whether that needs a new stored field becomes a
    /// question that can be answered by trying it.
    pub fn serves_page_tier(&self) -> bool {
        self.serves_page_tier
    }

    /// WHETHER THIS NODE SERVES THE CHUNK TIER.
    ///
    /// The mirror of `serves_page_tier`, and it exists for the same reason: once the store
    /// plane became ONE kind, eight predicates that asked `service_kind == "chunkserver"`
    /// to mean "a chunk-tier node" started admitting page-tier nodes too. Only one of the
    /// eight was covered by a test that could tell the difference.
    ///
    /// A store-plane node that is not a page node is a chunk node, which restores exactly
    /// the division the two kinds used to draw. If a node ever needs to serve BOTH tiers
    /// this is the one line that has to change -- and today nothing produces such a node,
    /// which `a_registration_serves_one_tier_or_the_other` asserts.
    pub fn serves_chunk_tier(&self) -> bool {
        self.service_kind == "chunkserver" && !self.serves_page_tier
    }
}
