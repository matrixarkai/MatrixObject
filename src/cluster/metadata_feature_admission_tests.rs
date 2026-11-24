// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster_features::{FEATURE_COMPACT_APPEND, FEATURE_RESERVE_NEXT};
const BOTH: u64 = FEATURE_COMPACT_APPEND | FEATURE_RESERVE_NEXT;

fn state() -> MembershipState {
    MembershipState {
        voters: vec![1, 2, 4],
        outgoing_voters: vec![1, 2, 3],
        learners: vec![5],
        next_learners: vec![3],
        auto_leave: false,
    }
}
fn service() -> MatrixMetaService {
    let mut service = MatrixMetaService::new();
    for id in 1..=5 {
        service.cluster_features.declared.insert(id, BOTH);
    }
    service
}
fn admitted(service: &MatrixMetaService, command: FeaturesRequest) -> Result<(), ObjectError> {
    check_feature_request(service, &command, &state())?;
    super::super::super::features_commands::prepare(service, &command).map(|_| ())
}
fn membership(operation: MembershipOperation, node_id: u64) -> MembershipRequest {
    MembershipRequest {
        version: 1,
        bootstrap_id: 1,
        operation: operation as i32,
        node_id,
    }
}
#[test]
fn metadata_feature_admission_requires_the_exact_joint_reader_union() {
    let service = service();
    assert!(admitted(&service, FeaturesRequest::raise(BOTH, vec![1, 2, 3, 4, 5])).is_ok());
    for readers in [vec![1, 2, 4, 5], vec![1, 2, 3, 4], vec![1, 2, 3, 4, 5, 9]] {
        assert!(matches!(
            admitted(&service, FeaturesRequest::raise(BOTH, readers)),
            Err(ObjectError::PreconditionFailed(_))
        ));
    }
}
#[test]
fn metadata_feature_admission_requires_support_from_outgoing_voters_and_learners() {
    for id in [3, 5] {
        let mut service = service();
        service
            .cluster_features
            .declared
            .insert(id, FEATURE_COMPACT_APPEND);
        assert!(admitted(&service, FeaturesRequest::raise(BOTH, vec![1, 2, 3, 4, 5])).is_err());
        assert!(admitted(
            &service,
            FeaturesRequest::raise(FEATURE_COMPACT_APPEND, vec![1, 2, 3, 4, 5])
        )
        .is_ok());
    }
}
#[test]
fn metadata_feature_admission_blocks_reader_downgrades_but_allows_standby_declarations() {
    let mut service = service();
    service.cluster_features.raised = BOTH;
    for id in 1..=5 {
        assert!(matches!(
            admitted(
                &service,
                FeaturesRequest::declare(id, FEATURE_COMPACT_APPEND)
            ),
            Err(ObjectError::PreconditionFailed(_))
        ));
        assert!(admitted(&service, FeaturesRequest::declare(id, BOTH)).is_ok());
    }
    assert!(admitted(
        &service,
        FeaturesRequest::declare(9, FEATURE_COMPACT_APPEND)
    )
    .is_ok());
}
#[test]
fn metadata_feature_admission_blocks_incompatible_joins_and_promotions() {
    let mut service = service();
    service.cluster_features.raised = BOTH;
    for operation in [
        MembershipOperation::AddLearner,
        MembershipOperation::Promote,
    ] {
        let request = membership(operation, 6);
        assert!(check_membership_request(&service, &request).is_err());
        service
            .cluster_features
            .declared
            .insert(6, FEATURE_COMPACT_APPEND);
        assert!(check_membership_request(&service, &request).is_err());
        service.cluster_features.declared.insert(6, BOTH);
        assert!(check_membership_request(&service, &request).is_ok());
        service.cluster_features.declared.remove(&6);
    }
}
#[test]
fn metadata_feature_admission_allows_membership_removal_to_repair_support() {
    let mut service = service();
    service.cluster_features.raised = BOTH;
    service.cluster_features.declared.clear();
    for request in [
        membership(MembershipOperation::Remove, 3),
        membership(MembershipOperation::LeaveJoint, 0),
    ] {
        assert!(check_membership_request(&service, &request).is_ok());
    }
}
#[test]
fn metadata_feature_admission_preserves_unraised_cluster_bootstrapping() {
    let service = MatrixMetaService::new();
    for operation in [
        MembershipOperation::AddLearner,
        MembershipOperation::Promote,
    ] {
        assert!(check_membership_request(&service, &membership(operation, 6)).is_ok());
    }
}
