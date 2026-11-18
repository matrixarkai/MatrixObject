// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Keeping a named group of gateways at its declared size.
//!
//! A group says "this many gateways, within this location". The planner compares each group's
//! target with the members that are actually serving and returns what to change: attach idle
//! spare gateways to a group that is short, release members of a group that has too many or
//! that no longer exists. It is pure and deterministic: the same state always gives the same
//! plan, groups are visited in name order and every tie breaks on the node id.
//!
//! Nothing here applies a plan. Attaching a gateway moves no data (a gateway is stateless), so
//! applying a plan is a membership change the metaserver replicates; that is a separate step.
//!
//! Location is hierarchical: a group at `us-east/dc1` draws from any gateway whose failure
//! domain is `us-east/dc1` or beneath it. An empty location means anywhere.

use std::collections::{BTreeMap, BTreeSet};

/// A declared pool of gateways.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayGroup {
    pub name: String,
    /// Where members may come from; empty means anywhere.
    pub location: String,
    /// How many serving gateways the group wants.
    pub instance_num: usize,
    /// A group that is not active is treated as gone: its members are released.
    pub active: bool,
}

/// A gateway as the planner sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayMember {
    pub node_id: String,
    /// The group it is attached to; empty when idle.
    pub group: String,
    pub failure_domain: String,
    pub online: bool,
    pub draining: bool,
    /// Whether the gateway has ever heartbeated. A declared gateway that never has is a roster
    /// entry, not capacity.
    pub heard_from: bool,
}

impl GatewayMember {
    fn serving(&self) -> bool {
        self.online && !self.draining
    }

    fn is_idle_candidate(&self) -> bool {
        self.group.is_empty() && self.serving() && self.heard_from
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CalibrationOptions {
    /// Most attach and detach decisions in one round, so a large change lands gradually.
    pub max_changes_per_round: usize,
}

impl Default for CalibrationOptions {
    fn default() -> Self {
        Self { max_changes_per_round: 16 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attachment {
    pub node_id: String,
    pub group: String,
}

/// A group that could not be filled this round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Shortfall {
    pub group: String,
    pub wanted: usize,
    pub attached: usize,
    /// Idle gateways that were eligible; zero means the location has no spare capacity.
    pub available: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CalibrationPlan {
    pub attach: Vec<Attachment>,
    pub detach: Vec<Attachment>,
    pub shortfalls: Vec<Shortfall>,
    /// Changes the per-round cap held back.
    pub capped: usize,
}

impl CalibrationPlan {
    pub fn is_empty(&self) -> bool {
        self.attach.is_empty() && self.detach.is_empty()
    }
}

/// Is `location` equal to or beneath `pattern`? Components are separated by `/`.
fn belongs_to(location: &str, pattern: &str) -> bool {
    pattern.is_empty()
        || location == pattern
        || location.strip_prefix(pattern).is_some_and(|rest| rest.starts_with('/'))
}

/// Bring every active group to its declared size.
///
/// Detachments are decided before attachments, so a gateway released in this round is not also
/// offered to another group in the same round; it is available on the next one.
pub fn plan_calibration(
    groups: &[GatewayGroup],
    members: &[GatewayMember],
    options: CalibrationOptions,
) -> CalibrationPlan {
    let mut plan = CalibrationPlan::default();
    let live: BTreeMap<&str, &GatewayGroup> =
        groups.iter().filter(|g| g.active).map(|g| (g.name.as_str(), g)).collect();
    let mut sorted: Vec<&GatewayMember> = members.iter().collect();
    sorted.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    // 1. Release members of a group that is gone.
    let mut wanted_detach: Vec<Attachment> = Vec::new();
    for member in &sorted {
        if !member.group.is_empty() && !live.contains_key(member.group.as_str()) {
            wanted_detach.push(Attachment { node_id: member.node_id.clone(), group: member.group.clone() });
        }
    }

    // A gateway that is down or draining is attached but not serving, so it does not count
    // toward the target; otherwise a group would silently run short whenever one member fails.
    let mut serving_by_group: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for member in &sorted {
        if !member.group.is_empty() && member.serving() {
            serving_by_group.entry(member.group.as_str()).or_default().push(member.node_id.as_str());
        }
    }

    // 2. Compare each group's target with what is serving.
    let mut wanted_attach: Vec<Attachment> = Vec::new();
    let mut claimed: BTreeSet<&str> = BTreeSet::new();
    for (name, group) in &live {
        let serving = serving_by_group.get(name).cloned().unwrap_or_default();
        if serving.len() > group.instance_num {
            // Shed from the end, so the members that sort first stay.
            for id in serving.iter().skip(group.instance_num).rev() {
                wanted_detach.push(Attachment { node_id: (*id).to_string(), group: (*name).to_string() });
            }
            continue;
        }
        let short = group.instance_num - serving.len();
        if short == 0 {
            continue;
        }
        let eligible: Vec<&str> = sorted
            .iter()
            .filter(|m| m.is_idle_candidate())
            .filter(|m| belongs_to(&m.failure_domain, &group.location))
            .filter(|m| !claimed.contains(m.node_id.as_str()))
            .map(|m| m.node_id.as_str())
            .collect();
        for id in eligible.iter().take(short) {
            claimed.insert(*id);
            wanted_attach.push(Attachment { node_id: (*id).to_string(), group: (*name).to_string() });
        }
        if eligible.len() < short {
            plan.shortfalls.push(Shortfall {
                group: (*name).to_string(),
                wanted: group.instance_num,
                attached: serving.len(),
                available: eligible.len(),
            });
        }
    }

    wanted_detach.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    wanted_detach.dedup_by(|a, b| a.node_id == b.node_id);
    wanted_attach.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    // 3. Spend the round's budget, detachments first: releasing is always safe, an attachment
    //    commits capacity.
    let total = wanted_detach.len() + wanted_attach.len();
    let mut budget = options.max_changes_per_round;
    for item in wanted_detach {
        if budget == 0 {
            break;
        }
        budget -= 1;
        plan.detach.push(item);
    }
    for item in wanted_attach {
        if budget == 0 {
            break;
        }
        budget -= 1;
        plan.attach.push(item);
    }
    plan.capped = total - plan.detach.len() - plan.attach.len();
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(name: &str, location: &str, n: usize) -> GatewayGroup {
        GatewayGroup { name: name.into(), location: location.into(), instance_num: n, active: true }
    }

    fn member(id: &str, group: &str, domain: &str) -> GatewayMember {
        GatewayMember {
            node_id: id.into(),
            group: group.into(),
            failure_domain: domain.into(),
            online: true,
            draining: false,
            heard_from: true,
        }
    }

    fn ids(items: &[Attachment]) -> Vec<(&str, &str)> {
        items.iter().map(|a| (a.node_id.as_str(), a.group.as_str())).collect()
    }

    #[test]
    fn a_short_group_takes_idle_gateways_in_id_order() {
        let plan = plan_calibration(
            &[group("g", "", 2)],
            &[member("c", "", "r"), member("a", "", "r"), member("b", "", "r")],
            CalibrationOptions::default(),
        );
        assert_eq!(ids(&plan.attach), [("a", "g"), ("b", "g")]);
        assert!(plan.detach.is_empty() && plan.shortfalls.is_empty());
    }

    #[test]
    fn a_member_that_is_down_or_draining_does_not_count_and_is_replaced() {
        let mut down = member("a", "g", "r");
        down.online = false;
        let mut draining = member("b", "g", "r");
        draining.draining = true;
        let plan = plan_calibration(
            &[group("g", "", 2)],
            &[down, draining, member("c", "g", "r"), member("d", "", "r"), member("e", "", "r")],
            CalibrationOptions::default(),
        );
        assert_eq!(ids(&plan.attach), [("d", "g")], "one serving member, one short");
    }

    #[test]
    fn a_group_with_too_many_serving_members_sheds_from_the_end() {
        let plan = plan_calibration(
            &[group("g", "", 1)],
            &[member("a", "g", "r"), member("b", "g", "r"), member("c", "g", "r")],
            CalibrationOptions::default(),
        );
        assert_eq!(ids(&plan.detach), [("b", "g"), ("c", "g")]);
        assert!(plan.attach.is_empty());
    }

    #[test]
    fn members_of_a_dropped_or_inactive_group_are_released() {
        let mut gone = group("old", "", 3);
        gone.active = false;
        let plan = plan_calibration(
            &[gone],
            &[member("a", "old", "r"), member("b", "missing", "r")],
            CalibrationOptions::default(),
        );
        assert_eq!(ids(&plan.detach), [("a", "old"), ("b", "missing")]);
    }

    #[test]
    fn a_gateway_that_never_heartbeated_or_is_down_is_not_spare_capacity() {
        let mut silent = member("a", "", "r");
        silent.heard_from = false;
        let mut down = member("b", "", "r");
        down.online = false;
        let plan = plan_calibration(&[group("g", "", 1)], &[silent, down], CalibrationOptions::default());
        assert!(plan.attach.is_empty());
        assert_eq!(plan.shortfalls, [Shortfall { group: "g".into(), wanted: 1, attached: 0, available: 0 }]);
    }

    #[test]
    fn location_is_hierarchical_and_not_a_string_prefix() {
        let plan = plan_calibration(
            &[group("g", "us/dc1", 3)],
            &[
                member("a", "", "us/dc1"),
                member("b", "", "us/dc1/rack2"),
                member("c", "", "us/dc10"),
                member("d", "", "eu/dc1"),
            ],
            CalibrationOptions::default(),
        );
        assert_eq!(ids(&plan.attach), [("a", "g"), ("b", "g")]);
        assert_eq!(plan.shortfalls[0].available, 2);
    }

    #[test]
    fn one_spare_is_never_promised_to_two_groups_in_one_round() {
        let plan = plan_calibration(
            &[group("g1", "", 1), group("g2", "", 1)],
            &[member("a", "", "r")],
            CalibrationOptions::default(),
        );
        assert_eq!(ids(&plan.attach), [("a", "g1")]);
        assert_eq!(plan.shortfalls.len(), 1);
        assert_eq!(plan.shortfalls[0].group, "g2");
    }

    #[test]
    fn releases_come_before_attachments_and_the_round_budget_holds_back_the_rest() {
        let plan = plan_calibration(
            &[group("g", "", 3)],
            &[
                member("x", "gone", "r"),
                member("a", "", "r"),
                member("b", "", "r"),
                member("c", "", "r"),
            ],
            CalibrationOptions { max_changes_per_round: 2 },
        );
        assert_eq!(ids(&plan.detach), [("x", "gone")]);
        assert_eq!(ids(&plan.attach), [("a", "g")]);
        assert_eq!(plan.capped, 2);
        // A released gateway is not offered to a group in the same round.
        assert!(!plan.attach.iter().any(|a| a.node_id == "x"));
    }

    #[test]
    fn the_same_state_gives_the_same_plan_whatever_order_it_is_listed_in() {
        let groups = [group("g2", "", 1), group("g1", "", 2)];
        let mut members = vec![member("c", "", "r"), member("a", "", "r"), member("b", "", "r")];
        let first = plan_calibration(&groups, &members, CalibrationOptions::default());
        members.reverse();
        let second = plan_calibration(&[groups[1].clone(), groups[0].clone()], &members, CalibrationOptions::default());
        assert_eq!(first, second);
        assert_eq!(ids(&first.attach), [("a", "g1"), ("b", "g1"), ("c", "g2")]);
    }

    #[test]
    fn a_full_group_needs_no_change() {
        let plan = plan_calibration(
            &[group("g", "", 2)],
            &[member("a", "g", "r"), member("b", "g", "r"), member("c", "", "r")],
            CalibrationOptions::default(),
        );
        assert!(plan.is_empty() && plan.shortfalls.is_empty() && plan.capped == 0);
    }
}
