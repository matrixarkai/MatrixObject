// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::cluster_features::*;
use crate::{MatrixMetaService, MetaSnapshot};

fn contains(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|w| w == needle)
}
fn with_features(raised: u64, declared: &[(u64, u64)]) -> ClusterFeatures {
    ClusterFeatures { raised, declared: declared.iter().copied().collect() }
}
fn service_with(features: ClusterFeatures) -> MatrixMetaService {
    let mut service = MatrixMetaService::new();
    service.cluster_features = features;
    service
}

struct Short<'a>(&'a [u8]);
impl std::io::Read for Short<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = out.len().min(5).min(self.0.len());
        out[..n].copy_from_slice(&self.0[..n]);
        self.0 = &self.0[n..];
        Ok(n)
    }
}

#[test]
fn an_empty_feature_set_leaves_the_snapshot_encoding_exactly_as_it_was() {
    let snapshot = MatrixMetaService::new().export_snapshot();
    assert!(snapshot.cluster_features.is_default());
    let bytes = snapshot.to_bytes().unwrap();
    // the version string is the one a build without the feature set wrote
    assert!(contains(&bytes[..32], b"MOSRSMETA18"), "empty set must keep the old magic");
    assert!(!contains(&bytes, b"MOSRSMETA21"));
    let back = MetaSnapshot::from_bytes(&bytes).unwrap();
    assert_eq!(back, snapshot);
    // and an old snapshot, which has no feature set, reads as the empty one
    assert!(back.cluster_features.is_default());
}

#[test]
fn a_feature_set_survives_the_snapshot_encoding_both_readers_and_a_late_joiner() {
    let features = with_features(FEATURE_COMPACT_APPEND | FEATURE_CHUNK_MANIFEST, &[(1, 7), (2, 5), (3, u64::MAX)]);
    let leader = service_with(features.clone());
    let snapshot = leader.export_snapshot();
    assert_eq!(snapshot.cluster_features, features);
    let bytes = snapshot.to_bytes().unwrap();
    assert!(contains(&bytes[..32], b"MOSRSMETA21"));
    assert_eq!(MetaSnapshot::from_bytes(&bytes).unwrap(), snapshot);
    assert_eq!(MetaSnapshot::read_from(&mut Short(&bytes), bytes.len() as u64).unwrap(), snapshot);
    // a node that joins late installs the snapshot and holds the same set
    let mut late = MatrixMetaService::new();
    assert!(late.cluster_features.is_default());
    late.install_snapshot(MetaSnapshot::from_bytes(&bytes).unwrap()).unwrap();
    assert_eq!(late.cluster_features, features);
    assert_eq!(late.export_snapshot(), snapshot);
    // restart: encode what the late joiner holds, read it back, install again
    let mut restarted = MatrixMetaService::new();
    restarted
        .install_snapshot(MetaSnapshot::from_bytes(&late.export_snapshot().to_bytes().unwrap()).unwrap())
        .unwrap();
    assert_eq!(restarted.cluster_features, features);
}

#[test]
fn a_declaration_alone_without_a_raised_bit_is_state_too() {
    let features = with_features(0, &[(1, FEATURE_COMPACT_APPEND)]);
    let snapshot = service_with(features.clone()).export_snapshot();
    let bytes = snapshot.to_bytes().unwrap();
    assert!(contains(&bytes[..32], b"MOSRSMETA21"));
    assert_eq!(MetaSnapshot::from_bytes(&bytes).unwrap().cluster_features, features);
}

#[test]
fn the_feature_set_is_covered_by_the_checksum_and_damage_is_refused() {
    let features = with_features(FEATURE_COMPACT_APPEND, &[(1, 3)]);
    let snapshot = service_with(features).export_snapshot();
    let bytes = snapshot.to_bytes().unwrap();
    let plain = MatrixMetaService::new().export_snapshot();
    assert_ne!(snapshot.checksum, plain.checksum);
    // flip every byte of the encoded set (it sits just before the checksum string): never accepted
    let tail = bytes.len() - 80;
    let mut refused = 0;
    for at in tail..bytes.len() {
        let mut damaged = bytes.clone();
        damaged[at] ^= 0x01;
        if MetaSnapshot::from_bytes(&damaged).map(|s| s != snapshot).unwrap_or(true) {
            refused += 1;
        }
    }
    assert_eq!(refused, bytes.len() - tail, "every damaged byte near the set must be refused or visible");
    for cut in [0, 1, bytes.len() / 2, bytes.len() - 1] {
        assert!(MetaSnapshot::from_bytes(&bytes[..cut]).is_err());
        assert!(MetaSnapshot::read_from(&mut Short(&bytes[..cut]), bytes.len() as u64).is_err());
    }
}

#[test]
fn a_changed_raised_bit_alone_changes_the_checksum() {
    let a = service_with(with_features(FEATURE_COMPACT_APPEND, &[(1, 3)])).export_snapshot();
    let b = service_with(with_features(FEATURE_RESERVE_NEXT, &[(1, 3)])).export_snapshot();
    let c = service_with(with_features(FEATURE_COMPACT_APPEND, &[(1, 5)])).export_snapshot();
    assert_ne!(a.checksum, b.checksum);
    assert_ne!(a.checksum, c.checksum);
}

#[test]
fn an_invalid_table_is_refused() {
    let zero = service_with(with_features(0, &[(0, 1)])).export_snapshot();
    assert!(zero.validate().is_err(), "voter id zero");
    let mut many = ClusterFeatures::default();
    for voter in 1..=(MAX_DECLARED_VOTERS as u64 + 1) {
        many.declared.insert(voter, 1);
    }
    assert!(service_with(many).export_snapshot().validate().is_err(), "too many voters");
}

#[test]
fn has_and_common_support_read_the_set() {
    let f = with_features(FEATURE_COMPACT_APPEND | FEATURE_CHUNK_MANIFEST, &[(1, 0b111), (2, 0b101), (3, 0b001)]);
    assert!(f.has(FEATURE_COMPACT_APPEND));
    assert!(f.has(FEATURE_COMPACT_APPEND | FEATURE_CHUNK_MANIFEST));
    assert!(!f.has(FEATURE_RESERVE_NEXT));
    assert!(!f.has(0));
    assert_eq!(f.common_support(&[1, 2]), 0b101);
    assert_eq!(f.common_support(&[1, 2, 3]), 0b001);
    // a voter that never declared counts as supporting nothing; an empty roster supports nothing
    assert_eq!(f.common_support(&[1, 9]), 0);
    assert_eq!(f.common_support(&[]), 0);
    assert!(SUPPORTED_FEATURES & FEATURE_CHUNK_MANIFEST != 0);
}
