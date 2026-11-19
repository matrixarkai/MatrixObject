// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Deciding that a gateway is gone: an adaptive heartbeat detector with two guards.
//!
//! A fixed time-to-live applies one threshold to every node and has three failure modes that
//! turn a small incident into a large one:
//!
//! 1. It is not adaptive: tight enough to catch a fast heartbeater, it falsely convicts a slow
//!    one; loose enough for the slow one, it detects the fast one late.
//! 2. A stall of the deciding process convicts everyone: every node looks silent at once.
//! 3. A correlated failure (a rack, a switch) is treated as many independent ones, and
//!    taking them all out of rotation drains the tier that was still partly working.
//!
//! [`GatewayFailureDetector`] learns each gateway's own heartbeat interval and reports
//! suspicion as `phi`, so one threshold means the same confidence for every node, and it
//! refuses to convict while the detector itself has just stalled. `plan_conviction` groups
//! gateways by location and holds a location back ("safe mode") when too large a fraction of
//! it is abnormal at once; an isolated failure is still convicted immediately.
//!
//! Everything is pure and takes the time as an argument, so a whole decision is testable
//! without a cluster. All timestamps given to one detector must come from one clock.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// log10(e): turns the exponential-model silence ratio into phi, "how many nines of confidence
/// that the node is gone".
const LOG10_E: f64 = std::f64::consts::LOG10_E;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DetectorOptions {
    /// How many recent heartbeat intervals per gateway feed the mean.
    pub sample_capacity: usize,
    /// Interval assumed for a gateway's first heartbeat. Large on purpose: it biases early phi
    /// downward so a new gateway is never convicted on one sample.
    pub initial_interval_ms: u64,
    /// Longer intervals are dropped, not averaged: they record a stall, not the node's cadence.
    pub max_interval_ms: u64,
    /// Convict when phi exceeds this. 5.0 is about one in 10^5 for a live node.
    pub phi_failure_threshold: f64,
    /// If the detector did not run for longer than this, suspicion is suppressed for the same
    /// duration again.
    pub max_round_pause_ms: u64,
}

impl Default for DetectorOptions {
    fn default() -> Self {
        Self {
            sample_capacity: 1000,
            initial_interval_ms: 30_000,
            max_interval_ms: 60_000,
            phi_failure_threshold: 5.0,
            max_round_pause_ms: 5_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Diagnosis {
    /// The detector itself stalled or just started: suspicion is not trustworthy. Never convict.
    Unknown,
    /// No heartbeat was ever seen, so there is no cadence to judge against. Never convict.
    NotObserved,
    Healthy,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DamageSeverity {
    Normal,
    Warning,
    Critical,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConvictionPolicy {
    /// Abnormal percentage of a location above which it is `Warning`.
    pub warning_ratio_percent: u64,
    /// Abnormal percentage of a location above which it is `Critical`.
    pub critical_ratio_percent: u64,
    /// A location needs at least this many abnormal gateways before safe mode can engage:
    /// safe mode is for correlated failure, and a percentage alone would put a two-gateway
    /// location into safe mode on its first dead node.
    pub min_abnormal_for_safe_mode: usize,
    pub safe_mode_enabled: bool,
    /// Off means a dry run: the plan is computed but nobody is named for conviction.
    pub convict_enabled: bool,
}

impl Default for ConvictionPolicy {
    fn default() -> Self {
        Self {
            warning_ratio_percent: 5,
            critical_ratio_percent: 8,
            min_abnormal_for_safe_mode: 2,
            safe_mode_enabled: true,
            convict_enabled: true,
        }
    }
}

/// One gateway as a detection round sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayObservation {
    pub node_id: String,
    /// Failure domain, used to group gateways for the correlated-failure gate.
    pub location: String,
    /// False when the roster already has it offline: it counts as damage but is not convicted
    /// again.
    pub serving: bool,
    pub last_heartbeat_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocationDamage {
    pub location: String,
    pub severity: DamageSeverity,
    pub total: usize,
    pub abnormal: usize,
    /// True when this location is held back from conviction this round.
    pub safe_mode: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConvictionPlan {
    /// Gateways to take out of service, by node id.
    pub convict: Vec<String>,
    /// Gateways the detector called failed but safe mode (or a dry run) held back.
    pub held_by_safe_mode: Vec<String>,
    pub damage: Vec<LocationDamage>,
    /// True when the detector was paused, so nobody could be convicted whatever the silence.
    pub detector_paused: bool,
}

/// Damage severity for one location, compared without integer division so a small location is
/// not rounded into safe mode.
fn assess_severity(total: usize, abnormal: usize, policy: ConvictionPolicy) -> DamageSeverity {
    if total == 0 || abnormal < policy.min_abnormal_for_safe_mode.max(1) {
        return DamageSeverity::Normal;
    }
    let scaled = abnormal as u128 * 100;
    let total = total as u128;
    if scaled > total * policy.critical_ratio_percent as u128 {
        DamageSeverity::Critical
    } else if scaled > total * policy.warning_ratio_percent as u128 {
        DamageSeverity::Warning
    } else {
        DamageSeverity::Normal
    }
}

/// Sliding window of one gateway's recent heartbeat intervals.
#[derive(Clone, Debug)]
struct ArrivalWindow {
    intervals: VecDeque<u64>,
    sum: u64,
    last_arrival_ms: u64,
}

impl ArrivalWindow {
    fn new(first_arrival_ms: u64, options: &DetectorOptions) -> Self {
        let mut window = Self { intervals: VecDeque::new(), sum: 0, last_arrival_ms: first_arrival_ms };
        // Seeded so the mean is defined from the first observation and starts biased against
        // conviction.
        window.push(options.initial_interval_ms.max(1), options);
        window
    }

    fn push(&mut self, interval_ms: u64, options: &DetectorOptions) {
        let capacity = options.sample_capacity.max(1);
        while self.intervals.len() >= capacity {
            if let Some(evicted) = self.intervals.pop_front() {
                self.sum -= evicted;
            }
        }
        self.intervals.push_back(interval_ms);
        self.sum += interval_ms;
    }

    fn observe(&mut self, arrival_ms: u64, options: &DetectorOptions) {
        if arrival_ms <= self.last_arrival_ms {
            // Not a new heartbeat, or the clock went backwards: nothing to learn.
            return;
        }
        let interval = arrival_ms - self.last_arrival_ms;
        self.last_arrival_ms = arrival_ms;
        if interval <= options.max_interval_ms {
            self.push(interval, options);
        }
    }

    fn mean_ms(&self) -> f64 {
        if self.intervals.is_empty() {
            return 0.0;
        }
        self.sum as f64 / self.intervals.len() as f64
    }

    fn phi(&self, now_ms: u64) -> f64 {
        let mean = self.mean_ms();
        if mean <= 0.0 || now_ms <= self.last_arrival_ms {
            return 0.0;
        }
        LOG10_E * (now_ms - self.last_arrival_ms) as f64 / mean
    }
}

/// Per-gateway phi-accrual detector with a stall guard.
///
/// Feed it one round at a time: [`Self::begin_round`], [`Self::observe`] for every gateway's
/// latest heartbeat time, then [`Self::diagnose`]; or call [`Self::plan_round`], which does
/// all of that and applies the correlated-failure gate.
#[derive(Clone, Debug)]
pub struct GatewayFailureDetector {
    options: DetectorOptions,
    windows: BTreeMap<String, ArrivalWindow>,
    last_round_ms: u64,
    paused_until_ms: u64,
}

impl Default for GatewayFailureDetector {
    fn default() -> Self {
        Self::new(DetectorOptions::default())
    }
}

impl GatewayFailureDetector {
    pub fn new(options: DetectorOptions) -> Self {
        Self { options, windows: BTreeMap::new(), last_round_ms: 0, paused_until_ms: 0 }
    }

    pub fn tracked(&self) -> usize {
        self.windows.len()
    }

    /// Open a round. Returns false when suspicion is not trustworthy: the detector just
    /// started, or it did not run for longer than `max_round_pause_ms`, so every gateway looks
    /// silent through no fault of its own. A stall re-arms the grace window.
    pub fn begin_round(&mut self, now_ms: u64) -> bool {
        let previous = self.last_round_ms;
        self.last_round_ms = now_ms;
        if previous == 0 || now_ms.saturating_sub(previous) > self.options.max_round_pause_ms {
            self.paused_until_ms = now_ms.saturating_add(self.options.max_round_pause_ms);
            return false;
        }
        now_ms >= self.paused_until_ms
    }

    pub fn is_active(&self, now_ms: u64) -> bool {
        self.last_round_ms != 0 && now_ms >= self.paused_until_ms
    }

    /// Record a gateway's newest heartbeat time. Repeating an unchanged time is a no-op.
    pub fn observe(&mut self, node_id: &str, last_heartbeat_ms: u64) {
        if last_heartbeat_ms == 0 {
            return;
        }
        match self.windows.get_mut(node_id) {
            Some(window) => window.observe(last_heartbeat_ms, &self.options),
            None => {
                self.windows.insert(node_id.to_string(), ArrivalWindow::new(last_heartbeat_ms, &self.options));
            }
        }
    }

    pub fn forget(&mut self, node_id: &str) {
        self.windows.remove(node_id);
    }

    /// Drop every gateway that is no longer in the roster.
    pub fn retain_only(&mut self, present: &BTreeSet<String>) {
        self.windows.retain(|id, _| present.contains(id));
    }

    pub fn phi(&self, node_id: &str, now_ms: u64) -> Option<f64> {
        self.windows.get(node_id).map(|window| window.phi(now_ms))
    }

    pub fn mean_interval_ms(&self, node_id: &str) -> Option<f64> {
        self.windows.get(node_id).map(|window| window.mean_ms())
    }

    /// `Unknown` whenever the detector is paused, so a stalled process can never convict.
    pub fn diagnose(&self, node_id: &str, now_ms: u64) -> Diagnosis {
        if !self.is_active(now_ms) {
            return Diagnosis::Unknown;
        }
        let Some(window) = self.windows.get(node_id) else {
            return Diagnosis::NotObserved;
        };
        if window.phi(now_ms) > self.options.phi_failure_threshold {
            Diagnosis::Failed
        } else {
            Diagnosis::Healthy
        }
    }

    /// One full round: learn from every heartbeat, diagnose every serving gateway, apply the
    /// correlated-failure gate. The plan names exactly the gateways to take out of service.
    pub fn plan_round(
        &mut self,
        gateways: &[GatewayObservation],
        now_ms: u64,
        policy: ConvictionPolicy,
    ) -> ConvictionPlan {
        let active = self.begin_round(now_ms);
        let mut present = BTreeSet::new();
        for gateway in gateways {
            present.insert(gateway.node_id.clone());
            self.observe(&gateway.node_id, gateway.last_heartbeat_ms);
        }
        self.retain_only(&present);

        let mut by_location: BTreeMap<&str, Vec<(&str, bool, bool)>> = BTreeMap::new();
        for gateway in gateways {
            let failed = active
                && gateway.serving
                && self.diagnose(&gateway.node_id, now_ms) == Diagnosis::Failed;
            by_location
                .entry(gateway.location.as_str())
                .or_default()
                .push((gateway.node_id.as_str(), !gateway.serving, failed));
        }
        let mut plan = plan_conviction(&by_location, policy);
        plan.detector_paused = !active;
        plan
    }
}

/// The correlated-failure gate. `by_location` maps a location to its gateways as
/// `(node id, already abnormal, diagnosed failed)`. Deterministic: every output is sorted.
fn plan_conviction(
    by_location: &BTreeMap<&str, Vec<(&str, bool, bool)>>,
    policy: ConvictionPolicy,
) -> ConvictionPlan {
    let mut plan = ConvictionPlan::default();
    for (location, members) in by_location {
        let total = members.len();
        // A gateway about to be convicted counts as damage too: it is about to stop serving.
        let abnormal = members.iter().filter(|(_, abnormal, failed)| *abnormal || *failed).count();
        let severity = assess_severity(total, abnormal, policy);
        let safe_mode = policy.safe_mode_enabled && severity != DamageSeverity::Normal;
        let mut failed: Vec<String> = members
            .iter()
            .filter(|(_, abnormal, failed)| *failed && !*abnormal)
            .map(|(id, _, _)| (*id).to_string())
            .collect();
        failed.sort();
        if safe_mode || !policy.convict_enabled {
            plan.held_by_safe_mode.extend(failed);
        } else {
            plan.convict.extend(failed);
        }
        plan.damage.push(LocationDamage {
            location: (*location).to_string(),
            severity,
            total,
            abnormal,
            safe_mode,
        });
    }
    plan.convict.sort();
    plan.held_by_safe_mode.sort();
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> DetectorOptions {
        DetectorOptions {
            sample_capacity: 1000,
            initial_interval_ms: 1_000,
            max_interval_ms: 60_000,
            phi_failure_threshold: 5.0,
            max_round_pause_ms: 5_000,
        }
    }

    /// A detector that is past its start-up grace and has heard `id` every `interval_ms`.
    fn trained(id: &str, interval_ms: u64, beats: u64) -> (GatewayFailureDetector, u64) {
        let mut detector = GatewayFailureDetector::new(options());
        let mut t = 100_000;
        assert!(!detector.begin_round(t));
        for _ in 0..beats {
            t += interval_ms;
            detector.observe(id, t);
        }
        // Rounds that keep arriving on time end the grace window.
        let mut now = t;
        while !detector.begin_round(now) {
            now += 1_000;
        }
        (detector, t)
    }

    #[test]
    fn phi_scales_with_the_nodes_own_cadence() {
        // Fast heartbeater: mean 1 s. Silent for 11 s is still plausible, 12 s is not.
        let (fast, last) = trained("fast", 1_000, 20);
        assert_eq!(fast.diagnose("fast", last + 11_000), Diagnosis::Healthy);
        assert_eq!(fast.diagnose("fast", last + 12_000), Diagnosis::Failed);
        // Slow heartbeater: the first beat only opens the window, so five beats teach four
        // 10 s intervals; with the 1 s seed the mean is 8.2 s. The same 12 s of silence is
        // nothing.
        let (slow, last) = trained("slow", 10_000, 5);
        assert!((slow.mean_interval_ms("slow").unwrap() - 8_200.0).abs() < 1.0);
        assert_eq!(slow.diagnose("slow", last + 12_000), Diagnosis::Healthy);
        assert_eq!(slow.diagnose("slow", last + 100_000), Diagnosis::Failed);
    }

    #[test]
    fn nobody_is_convicted_before_the_detector_has_a_baseline() {
        let mut detector = GatewayFailureDetector::new(options());
        assert!(!detector.begin_round(1_000), "the first round carries no information");
        detector.observe("g", 500);
        // Inside the start-up grace window even a long silence is not evidence.
        assert_eq!(detector.diagnose("g", 3_000), Diagnosis::Unknown);
        assert_eq!(detector.diagnose("never-seen", 3_000), Diagnosis::Unknown);
    }

    #[test]
    fn a_gateway_never_heard_from_is_not_observed_rather_than_failed() {
        let (detector, last) = trained("g", 1_000, 10);
        assert_eq!(detector.diagnose("stranger", last + 1_000_000), Diagnosis::NotObserved);
    }

    #[test]
    fn a_stall_of_the_detector_suppresses_conviction_until_the_fleet_is_heard_again() {
        let (mut detector, last) = trained("g", 1_000, 20);
        // The deciding process paused for a minute: every gateway looks silent.
        let resume = last + 60_000;
        assert!(!detector.begin_round(resume));
        assert_eq!(detector.diagnose("g", resume), Diagnosis::Unknown);
        assert_eq!(detector.diagnose("g", resume + 4_000), Diagnosis::Unknown);
        // After a full pause window with no further stall, suspicion returns.
        assert!(!detector.begin_round(resume + 1_000), "still inside the grace window");
        assert!(detector.begin_round(resume + 5_000));
        assert_eq!(detector.diagnose("g", resume + 5_000), Diagnosis::Failed);
    }

    #[test]
    fn an_interval_longer_than_the_cap_does_not_inflate_the_mean() {
        let (mut detector, last) = trained("g", 1_000, 20);
        let before = detector.mean_interval_ms("g").unwrap();
        detector.observe("g", last + 90_000);
        assert_eq!(detector.mean_interval_ms("g").unwrap(), before);
    }

    #[test]
    fn a_repeated_or_older_timestamp_teaches_nothing() {
        let (mut detector, last) = trained("g", 1_000, 20);
        let before = detector.mean_interval_ms("g").unwrap();
        detector.observe("g", last);
        detector.observe("g", last - 500);
        assert_eq!(detector.mean_interval_ms("g").unwrap(), before);
    }

    #[test]
    fn departed_gateways_are_forgotten() {
        let (mut detector, _) = trained("g", 1_000, 5);
        detector.observe("h", 100_500);
        assert_eq!(detector.tracked(), 2);
        detector.retain_only(&BTreeSet::from(["g".to_string()]));
        assert_eq!(detector.tracked(), 1);
        detector.forget("g");
        assert_eq!(detector.tracked(), 0);
    }

    fn obs(id: &str, location: &str, serving: bool, last: u64) -> GatewayObservation {
        GatewayObservation { node_id: id.into(), location: location.into(), serving, last_heartbeat_ms: last }
    }

    /// Run rounds so the detector is active, with every gateway heartbeating every second until
    /// `silent_from`, after which the ones in `silent` stop.
    fn round_with(
        gateways: &[(&str, &str, bool)],
        silent: &[&str],
        policy: ConvictionPolicy,
    ) -> ConvictionPlan {
        let mut detector = GatewayFailureDetector::new(options());
        let mut plan = ConvictionPlan::default();
        let silent_from = 120_000;
        let mut now = 100_000;
        while now <= 140_000 {
            let view: Vec<_> = gateways
                .iter()
                .map(|(id, location, serving)| {
                    let last = if silent.contains(id) { now.min(silent_from) } else { now };
                    obs(id, location, *serving, last)
                })
                .collect();
            plan = detector.plan_round(&view, now, policy);
            now += 1_000;
        }
        plan
    }

    #[test]
    fn an_isolated_failure_is_convicted() {
        let gateways: Vec<(&str, &str, bool)> = vec![
            ("a", "rack1", true), ("b", "rack1", true), ("c", "rack1", true), ("d", "rack1", true),
            ("e", "rack2", true), ("f", "rack2", true), ("g", "rack2", true), ("h", "rack2", true),
        ];
        let plan = round_with(&gateways, &["c"], ConvictionPolicy::default());
        assert_eq!(plan.convict, ["c"]);
        assert!(plan.held_by_safe_mode.is_empty() && !plan.detector_paused);
    }

    #[test]
    fn a_correlated_failure_is_held_back_not_convicted() {
        let gateways: Vec<(&str, &str, bool)> = vec![
            ("a", "rack1", true), ("b", "rack1", true), ("c", "rack1", true), ("d", "rack1", true),
            ("e", "rack2", true), ("f", "rack2", true), ("g", "rack2", true), ("h", "rack2", true),
        ];
        // Three of rack1's four go quiet at once: a switch, not three coincidences.
        let plan = round_with(&gateways, &["a", "b", "c"], ConvictionPolicy::default());
        assert!(plan.convict.is_empty(), "{plan:?}");
        assert_eq!(plan.held_by_safe_mode, ["a", "b", "c"]);
        let rack1 = plan.damage.iter().find(|d| d.location == "rack1").unwrap();
        assert!(rack1.safe_mode && rack1.severity == DamageSeverity::Critical);
        let rack2 = plan.damage.iter().find(|d| d.location == "rack2").unwrap();
        assert!(!rack2.safe_mode && rack2.abnormal == 0);
    }

    #[test]
    fn one_failure_in_a_two_gateway_location_is_still_convicted() {
        // Without the floor a single dead node of two is 50% abnormal and safe mode would
        // refuse to ever convict in a small location.
        let gateways: Vec<(&str, &str, bool)> = vec![("a", "r", true), ("b", "r", true)];
        let plan = round_with(&gateways, &["a"], ConvictionPolicy::default());
        assert_eq!(plan.convict, ["a"]);
    }

    #[test]
    fn gateways_already_offline_count_as_damage() {
        let gateways: Vec<(&str, &str, bool)> =
            vec![("a", "r", false), ("b", "r", false), ("c", "r", true), ("d", "r", true), ("e", "r", true)];
        let plan = round_with(&gateways, &["c"], ConvictionPolicy::default());
        assert!(plan.convict.is_empty());
        assert_eq!(plan.held_by_safe_mode, ["c"]);
        assert_eq!(plan.damage[0].abnormal, 3);
    }

    #[test]
    fn a_dry_run_names_nobody() {
        let gateways: Vec<(&str, &str, bool)> =
            vec![("a", "r1", true), ("b", "r1", true), ("c", "r2", true), ("d", "r2", true)];
        let policy = ConvictionPolicy { convict_enabled: false, ..ConvictionPolicy::default() };
        let plan = round_with(&gateways, &["a"], policy);
        assert!(plan.convict.is_empty());
        assert_eq!(plan.held_by_safe_mode, ["a"]);
    }

    #[test]
    fn a_stalled_round_convicts_nobody_even_when_everyone_is_silent() {
        let mut detector = GatewayFailureDetector::new(options());
        let gateways: Vec<(&str, &str, bool)> = vec![("a", "r1", true), ("b", "r2", true)];
        let mut now = 100_000;
        while now <= 110_000 {
            let view: Vec<_> = gateways.iter().map(|(id, l, s)| obs(id, l, *s, now)).collect();
            detector.plan_round(&view, now, ConvictionPolicy::default());
            now += 1_000;
        }
        // The process stalls for two minutes; the heartbeats' timestamps are stale.
        let stale: Vec<_> = gateways.iter().map(|(id, l, s)| obs(id, l, *s, 110_000)).collect();
        let plan = detector.plan_round(&stale, 230_000, ConvictionPolicy::default());
        assert!(plan.detector_paused);
        assert!(plan.convict.is_empty() && plan.held_by_safe_mode.is_empty(), "{plan:?}");
    }
}
