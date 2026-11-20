// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The commands that change and read the cluster feature set.
//!
//! * declare (action 1): a metadata voter records the feature bits it reads. Written by the voter
//!   itself, so a late joiner and a replay see the same evidence as everyone else.
//! * raise (action 2): the group agrees to emit `bits`. Refused unless every current journal reader
//!   has declared support for every one of them, and unless this build reads them itself.
//!   A bit is never lowered.
//! * read (action 3): the current set, from the committed state.
//!
//! The check is made here, on the leader, against the state the outcome is bound to: the outcome
//! carries the whole set it was decided from and applies only to exactly that set, so a leader
//! change between the decision and the commit cannot slip a bit past the check. An entry that is
//! already committed is applied unconditionally; a node that cannot read it fails, it does not skip.
use super::meta_message::MetaReply;
use crate::cluster_features::{ClusterFeatures, SUPPORTED_FEATURES};
use crate::placement_outcome::{FeatureTable, PlacementOutcome};
use crate::{MatrixMetaService, ObjectError};
use prost::Message;

pub const ACTION_DECLARE: u32 = 1;
pub const ACTION_RAISE: u32 = 2;
pub const ACTION_READ: u32 = 3;
const MAX_VOTERS: usize = crate::cluster_features::MAX_DECLARED_VOTERS;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct FeaturesRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub action: u32,
    /// Declare: the voter making the declaration. Zero otherwise.
    #[prost(uint64, tag = "3")]
    pub voter: u64,
    /// Declare: the bits the voter reads. Raise: the bits to raise. Zero for a read.
    #[prost(uint64, tag = "4")]
    pub bits: u64,
    /// Raise: the sorted union of committed voters, outgoing voters and learners.
    /// The consensus service verifies the set against its own membership.
    #[prost(uint64, repeated, tag = "5")]
    pub voters: Vec<u64>,
}

impl FeaturesRequest {
    pub fn declare(voter: u64, bits: u64) -> Self {
        Self { version: 1, action: ACTION_DECLARE, voter, bits, voters: Vec::new() }
    }
    pub fn raise(bits: u64, voters: Vec<u64>) -> Self {
        Self { version: 1, action: ACTION_RAISE, voter: 0, bits, voters }
    }
    pub fn read() -> Self {
        Self { version: 1, action: ACTION_READ, ..Default::default() }
    }
    pub fn is_read(&self) -> bool {
        self.action == ACTION_READ
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        let shaped = self.version == 1
            && match self.action {
                ACTION_DECLARE => self.voter != 0 && self.voters.is_empty(),
                ACTION_RAISE => {
                    self.voter == 0
                        && self.bits != 0
                        && !self.voters.is_empty()
                        && self.voters.len() <= MAX_VOTERS
                        && self.voters.windows(2).all(|pair| pair[0] < pair[1])
                        && !self.voters.contains(&0)
                }
                ACTION_READ => self.voter == 0 && self.bits == 0 && self.voters.is_empty(),
                _ => false,
            };
        if !shaped {
            return Err(ObjectError::InvalidArgument("invalid cluster feature request".into()));
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 16 * 1024 {
            return Err(ObjectError::Corruption("cluster feature request exceeds its bound".into()));
        }
        let request = Self::decode(bytes).map_err(|_| ObjectError::Corruption("invalid cluster feature request".into()))?;
        if request.encode_to_vec() != bytes {
            return Err(ObjectError::Corruption("noncanonical cluster feature request".into()));
        }
        request.validate().map_err(|_| ObjectError::Corruption("invalid cluster feature request".into()))?;
        Ok(request)
    }
}

/// Decode a reply payload, refusing anything that does not re-encode to the same bytes.
pub fn reply_from_payload(bytes: &[u8]) -> Result<FeatureTable, ObjectError> {
    if bytes.len() > 64 * 1024 {
        return Err(ObjectError::Corruption("cluster feature reply exceeds its bound".into()));
    }
    let table = FeatureTable::decode(bytes).map_err(|_| ObjectError::Corruption("invalid cluster feature reply".into()))?;
    if table.encode_to_vec() != bytes {
        return Err(ObjectError::Corruption("noncanonical cluster feature reply".into()));
    }
    table.to_features().map_err(|_| ObjectError::Corruption("invalid cluster feature reply".into()))?;
    Ok(table)
}

/// The reply of every action: the set as it stands after the command.
pub fn reply(features: &ClusterFeatures) -> MetaReply {
    MetaReply::Features(FeatureTable::from_features(features))
}

/// Decide a declare or a raise against `service`. `Ok(None)` means the command changes nothing (the
/// voter already declared those bits, or they are already raised): the caller answers with the
/// current set and proposes nothing.
pub(super) fn prepare(
    service: &MatrixMetaService,
    request: &FeaturesRequest,
) -> Result<Option<PlacementOutcome>, ObjectError> {
    request.validate()?;
    let mut after = service.cluster_features.clone();
    match request.action {
        ACTION_DECLARE => {
            if after.declared.get(&request.voter) == Some(&request.bits) {
                return Ok(None);
            }
            after.declared.insert(request.voter, request.bits);
        }
        ACTION_RAISE => {
            if request.bits & !SUPPORTED_FEATURES != 0 {
                return Err(ObjectError::PreconditionFailed("a requested feature is not one this build reads".into()));
            }
            let common = service.cluster_features.common_support(&request.voters);
            if request.bits & !common != 0 {
                return Err(ObjectError::PreconditionFailed(
                    "a voter has not declared support for every requested feature".into(),
                ));
            }
            if after.raised & request.bits == request.bits {
                return Ok(None);
            }
            after.raised |= request.bits;
        }
        _ => return Err(ObjectError::InvalidArgument("a read is not decided".into())),
    }
    PlacementOutcome::features_change(service, &after).map(Some)
}

pub(super) fn read(service: &MatrixMetaService, request: &FeaturesRequest) -> Result<MetaReply, ObjectError> {
    request.validate()?;
    Ok(reply(&service.cluster_features))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::meta_wire::MetaRequestCode;
    use crate::cluster_features::*;

    fn declare(service: &mut MatrixMetaService, voter: u64, bits: u64) -> bool {
        match prepare(service, &FeaturesRequest::declare(voter, bits)).unwrap() {
            Some(outcome) => {
                // every committed entry goes through the payload, as it does in the log
                let payload = outcome.to_payload().unwrap();
                PlacementOutcome::from_payload(&payload).unwrap().apply(service).unwrap();
                true
            }
            None => false,
        }
    }
    fn raise(service: &mut MatrixMetaService, bits: u64, voters: &[u64]) -> Result<bool, ObjectError> {
        Ok(match prepare(service, &FeaturesRequest::raise(bits, voters.to_vec()))? {
            Some(outcome) => {
                let payload = outcome.to_payload().unwrap();
                PlacementOutcome::from_payload(&payload).unwrap().apply(service).unwrap();
                true
            }
            None => false,
        })
    }
    const BOTH: u64 = FEATURE_COMPACT_APPEND | FEATURE_RESERVE_NEXT;

    #[test]
    fn the_read_grace_bit_is_known_and_is_raised_like_the_others() {
        use crate::cluster_features::FEATURE_READ_GRACE;
        assert_ne!(SUPPORTED_FEATURES & FEATURE_READ_GRACE, 0);
        assert_eq!(FEATURE_READ_GRACE & (FEATURE_COMPACT_APPEND | FEATURE_RESERVE_NEXT), 0);
        let mut service = MatrixMetaService::new();
        for voter in 1..=3 {
            declare(&mut service, voter, FEATURE_READ_GRACE);
        }
        assert!(!service.cluster_features.has(FEATURE_READ_GRACE), "declared is not raised");
        assert!(raise(&mut service, FEATURE_READ_GRACE, &[1, 2, 3]).unwrap());
        assert!(service.cluster_features.has(FEATURE_READ_GRACE));
        assert!(!service.cluster_features.has(FEATURE_COMPACT_APPEND));
        // one voter that has not declared it keeps it down
        let mut mixed = MatrixMetaService::new();
        declare(&mut mixed, 1, FEATURE_READ_GRACE);
        declare(&mut mixed, 2, FEATURE_COMPACT_APPEND);
        assert!(raise(&mut mixed, FEATURE_READ_GRACE, &[1, 2]).is_err());
    }

    #[test]
    fn a_declaration_is_recorded_once_and_a_repeat_changes_nothing() {
        let mut service = MatrixMetaService::new();
        let index = service.placement_commit_index;
        assert!(declare(&mut service, 1, BOTH));
        assert_eq!(service.cluster_features.declared.get(&1), Some(&BOTH));
        assert_eq!(service.placement_commit_index, index + 1);
        assert!(!declare(&mut service, 1, BOTH), "a restart repeating its declaration proposes nothing");
        assert_eq!(service.placement_commit_index, index + 1);
        // a voter whose build changed declares again and replaces its entry
        assert!(declare(&mut service, 1, FEATURE_COMPACT_APPEND));
        assert_eq!(service.cluster_features.declared.get(&1), Some(&FEATURE_COMPACT_APPEND));
    }

    #[test]
    fn a_mixed_set_keeps_the_bit_low_until_every_voter_declares_it() {
        let mut service = MatrixMetaService::new();
        declare(&mut service, 1, BOTH);
        declare(&mut service, 2, BOTH);
        // voter 3 runs an older build: it declared less, or nothing at all
        for refused in [BOTH, FEATURE_RESERVE_NEXT] {
            let index = service.placement_commit_index;
            assert!(raise(&mut service, refused, &[1, 2, 3]).is_err(), "voter 3 has not declared anything");
            assert_eq!(service.cluster_features.raised, 0);
            assert_eq!(service.placement_commit_index, index, "a refused raise writes nothing");
        }
        declare(&mut service, 3, FEATURE_COMPACT_APPEND);
        assert!(raise(&mut service, BOTH, &[1, 2, 3]).is_err(), "voter 3 reads only one of the two");
        assert!(raise(&mut service, FEATURE_COMPACT_APPEND, &[1, 2, 3]).unwrap());
        assert!(service.cluster_features.has(FEATURE_COMPACT_APPEND));
        assert!(!service.cluster_features.has(FEATURE_RESERVE_NEXT));
        // voter 3 upgrades and declares; now the second bit may be raised
        declare(&mut service, 3, BOTH);
        assert!(raise(&mut service, FEATURE_RESERVE_NEXT, &[1, 2, 3]).unwrap());
        assert!(service.cluster_features.has(BOTH));
        // raising what is raised is a no-op
        assert!(!raise(&mut service, BOTH, &[1, 2, 3]).unwrap());
    }

    #[test]
    fn a_voter_that_is_not_named_cannot_be_forgotten_and_an_unknown_bit_is_refused() {
        let mut service = MatrixMetaService::new();
        declare(&mut service, 1, u64::MAX);
        // the caller names voter 2, which never declared
        assert!(raise(&mut service, FEATURE_COMPACT_APPEND, &[1, 2]).is_err());
        // a bit this build does not read is refused even when the voter declares it
        assert!(raise(&mut service, 1 << 40, &[1]).is_err());
        assert_eq!(service.cluster_features.raised, 0);
    }

    #[test]
    fn an_outcome_applies_only_to_the_set_it_was_decided_from() {
        let mut service = MatrixMetaService::new();
        declare(&mut service, 1, BOTH);
        // a leader decides a raise; before it commits, another declaration commits
        let stale = prepare(&service, &FeaturesRequest::raise(FEATURE_COMPACT_APPEND, vec![1])).unwrap().unwrap();
        declare(&mut service, 2, BOTH);
        assert!(stale.apply(&mut service).is_err(), "decided against a set that no longer exists");
        assert_eq!(service.cluster_features.raised, 0);
        // and the same outcome cannot be applied twice
        let fresh = prepare(&service, &FeaturesRequest::raise(FEATURE_COMPACT_APPEND, vec![1, 2])).unwrap().unwrap();
        fresh.apply(&mut service).unwrap();
        assert!(fresh.apply(&mut service).is_err());
    }

    #[test]
    fn the_outcome_shape_is_strict() {
        let mut service = MatrixMetaService::new();
        declare(&mut service, 1, BOTH);
        declare(&mut service, 2, BOTH);
        raise(&mut service, FEATURE_COMPACT_APPEND, &[1, 2]).unwrap();
        let base = service.cluster_features.clone();
        let decide = |after: ClusterFeatures| PlacementOutcome::features_change(&service, &after);
        let mut lowered = base.clone();
        lowered.raised = 0;
        assert!(decide(lowered).is_err(), "a bit never goes down");
        let mut two = base.clone();
        two.declared.insert(1, 0);
        two.declared.insert(2, 0);
        assert!(decide(two).is_err(), "one voter at a time");
        let mut removed = base.clone();
        removed.declared.remove(&1);
        assert!(decide(removed).is_err(), "a declaration is replaced, not removed");
        let mut both = base.clone();
        both.raised |= FEATURE_RESERVE_NEXT;
        both.declared.insert(1, 0);
        assert!(decide(both).is_err(), "a raise and a declaration are separate commands");
        assert!(decide(base.clone()).is_err(), "no change is not an outcome");
        let mut unknown = base.clone();
        unknown.raised |= 1 << 40;
        assert!(decide(unknown).is_err());
        // the version and the field go together
        let good = prepare(&service, &FeaturesRequest::declare(3, 1)).unwrap().unwrap();
        let mut wrong_version = good.clone();
        wrong_version.version = 12;
        assert!(wrong_version.to_payload().is_err());
        let mut no_change = good.clone();
        no_change.features = None;
        assert!(no_change.to_payload().is_err());
        let mut extra = good.clone();
        extra.after_index += 1;
        assert!(extra.to_payload().is_err());
    }

    #[test]
    fn a_replay_of_the_log_and_a_late_joiner_reach_the_same_set() {
        let mut live = MatrixMetaService::new();
        let mut log: Vec<Vec<u8>> = Vec::new();
        for request in [
            FeaturesRequest::declare(1, BOTH),
            FeaturesRequest::declare(2, BOTH),
            FeaturesRequest::declare(3, FEATURE_COMPACT_APPEND),
            FeaturesRequest::raise(FEATURE_COMPACT_APPEND, vec![1, 2, 3]),
        ] {
            let outcome = prepare(&live, &request).unwrap().unwrap();
            let payload = outcome.to_payload().unwrap();
            PlacementOutcome::from_payload(&payload).unwrap().apply(&mut live).unwrap();
            log.push(payload);
        }
        assert_eq!(log.len(), 4);
        // restart: a fresh node replays the log from the start
        let mut replayed = MatrixMetaService::new();
        for payload in &log {
            PlacementOutcome::from_payload(payload).unwrap().apply(&mut replayed).unwrap();
        }
        assert_eq!(replayed.cluster_features, live.cluster_features);
        assert_eq!(replayed.placement_commit_index, live.placement_commit_index);
        // late joiner: installs a snapshot taken after the second entry, then replays the rest
        let mut early = MatrixMetaService::new();
        for payload in &log[..2] {
            PlacementOutcome::from_payload(payload).unwrap().apply(&mut early).unwrap();
        }
        let snapshot = crate::MetaSnapshot::from_bytes(&early.export_snapshot().to_bytes().unwrap()).unwrap();
        let mut joiner = MatrixMetaService::new();
        joiner.install_snapshot(snapshot.clone()).unwrap();
        joiner.install_placement_roster(&snapshot).unwrap();
        for payload in &log[2..] {
            PlacementOutcome::from_payload(payload).unwrap().apply(&mut joiner).unwrap();
        }
        assert_eq!(joiner.cluster_features, live.cluster_features);
        assert!(joiner.cluster_features.has(FEATURE_COMPACT_APPEND));
    }

    #[test]
    fn the_requests_have_one_canonical_shape() {
        for good in [FeaturesRequest::declare(7, 3), FeaturesRequest::raise(1, vec![1, 2, 3]), FeaturesRequest::read()] {
            let bytes = good.encode_to_vec();
            assert_eq!(FeaturesRequest::from_payload(&bytes).unwrap(), good);
            assert!(FeaturesRequest::from_payload(&[bytes.clone(), vec![0]].concat()).is_err(), "trailing byte");
        }
        let bad = [
            FeaturesRequest { version: 2, ..FeaturesRequest::read() },
            FeaturesRequest { action: 9, ..FeaturesRequest::read() },
            FeaturesRequest::declare(0, 1),
            FeaturesRequest { voters: vec![1], ..FeaturesRequest::declare(1, 1) },
            FeaturesRequest::raise(0, vec![1]),
            FeaturesRequest::raise(1, vec![]),
            FeaturesRequest::raise(1, vec![2, 1]),
            FeaturesRequest::raise(1, vec![1, 1]),
            FeaturesRequest::raise(1, vec![0, 1]),
            FeaturesRequest { bits: 1, ..FeaturesRequest::read() },
        ];
        for request in bad {
            assert!(request.validate().is_err(), "{request:?}");
            assert!(FeaturesRequest::from_payload(&request.encode_to_vec()).is_err());
        }
        assert_eq!(MetaRequestCode::from_u8(96), Some(MetaRequestCode::Features));
        assert!(MetaRequestCode::all().contains(&MetaRequestCode::Features));
        let reply_table = FeatureTable::from_features(&ClusterFeatures { raised: 1, declared: [(1, 3)].into() });
        assert!(reply_from_payload(&reply_table.encode_to_vec()).is_ok());
        assert!(reply_from_payload(&[reply_table.encode_to_vec(), vec![0]].concat()).is_err());
    }
}
