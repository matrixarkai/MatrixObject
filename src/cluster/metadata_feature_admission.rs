// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Membership-dependent checks performed before metadata proposals are admitted.
use super::super::features_commands::{FeaturesRequest, ACTION_DECLARE, ACTION_RAISE};
use super::super::peer_membership::{MembershipOperation, MembershipRequest};
use crate::{MatrixMetaService, ObjectError};
use matrixraft::peer_runtime::MembershipState;
use std::collections::BTreeSet;

/// Feature preparation separately checks the declared support of every named reader.
pub(super) fn check_feature_request(
    service: &MatrixMetaService,
    request: &FeaturesRequest,
    membership: &MembershipState,
) -> Result<(), ObjectError> {
    request.validate()?;
    let readers: BTreeSet<_> = membership
        .voters
        .iter()
        .chain(&membership.outgoing_voters)
        .chain(&membership.learners)
        .chain(&membership.next_learners)
        .copied()
        .collect();
    if request.action == ACTION_RAISE {
        if request.voters.iter().copied().ne(readers.iter().copied()) {
            return Err(ObjectError::PreconditionFailed(
                "feature raise must name every current metadata journal reader".into(),
            ));
        }
    } else if request.action == ACTION_DECLARE
        && readers.contains(&request.voter)
        && service.cluster_features.raised & !request.bits != 0
    {
        return Err(ObjectError::PreconditionFailed(
            "a current metadata journal reader must support every raised feature".into(),
        ));
    }
    Ok(())
}

pub(super) fn check_membership_request(
    service: &MatrixMetaService,
    request: &MembershipRequest,
) -> Result<(), ObjectError> {
    request.validate()?;
    if matches!(
        MembershipOperation::from_i32(request.operation),
        Some(MembershipOperation::AddLearner | MembershipOperation::Promote)
    ) {
        let supported = service
            .cluster_features
            .declared
            .get(&request.node_id)
            .copied()
            .unwrap_or(0);
        if service.cluster_features.raised & !supported != 0 {
            return Err(ObjectError::PreconditionFailed(
                "a joining metadata journal reader must declare every raised feature".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "metadata_feature_admission_tests.rs"]
mod tests;
