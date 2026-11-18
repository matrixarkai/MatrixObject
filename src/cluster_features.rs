// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The cluster feature set: which optional wire and durable shapes the whole metadata group may
//! emit.
//!
//! A shape that an older node cannot read (a new outcome version, a new manifest format) must
//! not be written until every node that has to read it can. Nothing in the cluster told a
//! gateway or a leader whether that was so; upgrading voters before gateways was a rule in a
//! document. This is the one place that records it, as replicated metadata state:
//!
//! * `declared`: each metadata voter's supported feature bits, written by the voter itself
//!   through the log (so a late joiner and a replay see the same evidence as everyone else);
//! * `raised`: the bits the group has agreed to emit, changed only by a committed command that
//!   the leader accepts only when every declared voter supports them.
//!
//! An entry already committed is always applied: the check is made before a bit is raised, never
//! when an entry is applied, and a node that cannot read what was committed fails loudly rather
//! than skipping it.
use std::collections::BTreeMap;

use crate::ObjectError;

/// Compact append (outcome version 11).
pub const FEATURE_COMPACT_APPEND: u64 = 1 << 0;
/// Compact append that reserves the next allocation (outcome version 13).
pub const FEATURE_RESERVE_NEXT: u64 = 1 << 1;
/// Manifests whose extents name log chunks (manifest format 3).
pub const FEATURE_CHUNK_MANIFEST: u64 = 1 << 2;
/// Bits 8 through 15 are held for the gateway recovery records.
pub const FEATURE_GATEWAY_RESERVED_MASK: u64 = 0xff << 8;
/// Every bit this build knows how to read.
pub const SUPPORTED_FEATURES: u64 =
    FEATURE_COMPACT_APPEND | FEATURE_RESERVE_NEXT | FEATURE_CHUNK_MANIFEST | FEATURE_GATEWAY_RESERVED_MASK;

/// Most voters a declaration table holds.
pub const MAX_DECLARED_VOTERS: usize = 1024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterFeatures {
    /// Bits the group has agreed to emit.
    pub raised: u64,
    /// Voter id -> the bits that voter reads.
    pub declared: BTreeMap<u64, u64>,
}

impl ClusterFeatures {
    /// Whether `feature` (one or more bits) is raised.
    pub fn has(&self, feature: u64) -> bool {
        feature != 0 && self.raised & feature == feature
    }
    /// True while nothing has ever been declared or raised: the snapshot of such a state has
    /// exactly the encoding it had before this existed.
    pub fn is_default(&self) -> bool {
        self.raised == 0 && self.declared.is_empty()
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.declared.len() > MAX_DECLARED_VOTERS || self.declared.contains_key(&0) {
            return Err(ObjectError::Corruption("invalid cluster feature declarations".into()));
        }
        Ok(())
    }
    /// The bits every one of `voters` declares support for (zero if one has declared nothing).
    pub fn common_support(&self, voters: &[u64]) -> u64 {
        if voters.is_empty() {
            return 0;
        }
        voters
            .iter()
            .map(|voter| self.declared.get(voter).copied().unwrap_or(0))
            .fold(u64::MAX, |all, bits| all & bits)
    }
}
