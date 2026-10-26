// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPlacement {
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    pub node_ids: Vec<String>,
    pub update_time_ms: u64,
}

impl ObjectPlacement {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}bucket"),
            self.object_id.bucket.to_string(),
        );
        fields.insert(format!("{field_prefix}key"), self.object_id.key.to_string());
        fields.insert(format!("{field_prefix}version"), self.version.to_string().into());
        fields.insert(format!("{field_prefix}length"), self.length.to_string().into());
        fields.insert(
            format!("{field_prefix}node_count"),
            self.node_ids.len().to_string().into(),
        );
        for (index, node_id) in self.node_ids.iter().enumerate() {
            fields.insert(format!("{field_prefix}node.{index}"), node_id.clone().into());
        }
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string().into(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootLeaderLease {
    pub leader_node_id: String,
    pub epoch: u64,
    pub acquire_time_ms: u64,
    pub expire_time_ms: u64,
    pub ttl_ms: u64,
}

impl RootLeaderLease {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}leader_node_id"),
            self.leader_node_id.clone(),
        );
        fields.insert(format!("{field_prefix}lease_epoch"), self.epoch.to_string());
        fields.insert(
            format!("{field_prefix}lease_acquire_time_ms"),
            self.acquire_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}lease_expire_time_ms"),
            self.expire_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}lease_ttl_ms"),
            self.ttl_ms.to_string(),
        );
        fields
    }
}

/// Whether `node` may hold this store's control-plane leader lease.
///
/// # WHY THIS IS ONE FUNCTION
///
/// The rule was written out at four sites: the live gate that `campaign_leader` and
/// `renew_leader` share, the `leader_active` flag the raft status reports, the snapshot
/// DECODE gate that refuses a lease whose leader is not serving, and the environment-driven
/// catalog bootstrap. Four copies of a membership rule is three chances for the live gate and
/// the decode gate to disagree -- and that pair disagreeing is the worst case available,
/// because it means a store writes a snapshot it cannot read back.
///
/// # THE LEASE IS THE METASERVER'S NOW
///
/// This store has one control plane and it is the metaserver, so the metaserver node kind is
/// where the lease belongs. It used to require the `rootserver` kind, which came from the
/// service whose placement state was folded into the metaserver.
///
/// `rootserver` is still accepted, and that arm is COMPATIBILITY rather than policy: a
/// snapshot written before this change carries a lease whose leader is a `rootserver` node,
/// and the decode gate above would refuse to load it. Refusing to load an existing store is
/// not a retirement, it is a break. So the old kind still passes, here, until the kind itself
/// is gone -- and because this is one function, that removal is one line rather than four.
/// What a node kind this build no longer writes is called now.
///
/// # WHY A NODE KIND NEEDS THIS AT ALL
///
/// A node kind is a STRING IN DURABLE STATE. A store written by an older build holds the
/// spelling that build used, and that store has to keep loading. The two ways to honour that
/// are to carry every retired spelling in every predicate forever, or to rewrite it once
/// where the bytes are read. The first is what the leader-lease predicate was doing, and it
/// does not scale: every predicate about every kind would have to carry every spelling, and
/// a predicate that forgot one would be wrong only for old stores.
///
/// So the rewrite happens in `read_catalog_roster`, the one place a roster is read back from
/// bytes, and the live predicates name one spelling each.
///
/// This is deliberately a rewrite on the way IN and not a live policy: an in-memory node
/// carrying a retired kind is refused, because nothing in this build should be producing one.
pub(crate) fn current_service_kind_spelling(service_kind: &str) -> &str {
    match service_kind {
        // The control plane folded into the metaserver, so a node recorded under the retired
        // kind is a metaserver node -- including when it is the one holding the leader lease,
        // which is the case that kept the second spelling alive in the predicate below.
        "rootserver" => "metaserver",
        other => other,
    }
}

pub(crate) fn is_control_plane_leader_candidate(node: &RootNode) -> bool {
    // ORDER MATTERS FOR THE MESSAGE, NOT THE VERDICT: a caller reporting why a candidate was
    // refused wants to know it was the kind rather than the liveness, so the two are kept
    // separable by `control_plane_leader_refusal` below.
    node.service_kind == "metaserver" && node.online && !node.draining
}

/// Why `node` may not hold the leader lease, or `None` if it may.
///
/// Separate from the predicate so that the two callers which return an error can say which
/// clause refused, and the two which only need a boolean do not have to format a string.
pub(crate) fn control_plane_leader_refusal(node: &RootNode) -> Option<String> {
    if node.service_kind != "metaserver" {
        return Some(format!(
            "a control-plane leader must be a metaserver node, and {:?} is a {:?}",
            node.node_id, node.service_kind
        ));
    }
    if !node.online {
        return Some(format!(
            "the control-plane leader candidate {:?} is offline",
            node.node_id
        ));
    }
    if node.draining {
        return Some(format!(
            "the control-plane leader candidate {:?} is draining",
            node.node_id
        ));
    }
    None
}

