// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// A feature declaration or raise prepared against the proposal draft must leave the live service
// as it was. The leader applies each outcome to the draft so the next request is prepared against
// the prefix, then restores the service; a restore that forgot the feature set left the change in
// the live service, and when the entry committed the leader found its "before" no longer matching
// and stopped the whole consensus worker.
#[test]
fn a_feature_outcome_applied_to_the_draft_is_undone_with_it() {
    use crate::cluster::{FeaturesRequest, MetaRequestCode};
    use crate::cluster_features::{FEATURE_COMPACT_APPEND, FEATURE_CHUNK_MANIFEST};
    let (mut service, _groups) = fixture(1, 0);
    let index = service.placement_commit_index;
    let before = service.export_snapshot().to_bytes().unwrap();
    {
        let draft = PlacementDraft::with_records(&mut service, Vec::new(), Vec::new());
        // declared by two voters in one batch: the second is prepared against the first
        for (voter, bits) in [(1, FEATURE_CHUNK_MANIFEST), (2, FEATURE_CHUNK_MANIFEST | FEATURE_COMPACT_APPEND)] {
            let outcome = super::super::features_commands::prepare(
                draft.service,
                &FeaturesRequest::declare(voter, bits),
            )
            .unwrap()
            .unwrap();
            // what the proposal pipeline does with the payload it will propose
            PlacementOutcome::from_payload(&outcome.to_payload().unwrap())
                .unwrap()
                .apply(draft.service)
                .unwrap();
        }
        assert_eq!(draft.service.cluster_features.declared.len(), 2, "the draft saw both");
        let _ = MetaRequestCode::Features;
    }
    assert!(
        service.cluster_features.is_default(),
        "the live service kept a change that had not committed: {:?}",
        service.cluster_features
    );
    assert_eq!(service.placement_commit_index, index);
    assert_eq!(
        service.export_snapshot().to_bytes().unwrap(),
        before,
        "the draft left the live service different from before: the whole persisted state is compared, so a field it forgets to restore shows here"
    );
    // and the entry still applies cleanly when it commits
    let outcome = super::super::features_commands::prepare(&service, &FeaturesRequest::declare(1, FEATURE_CHUNK_MANIFEST))
        .unwrap()
        .unwrap();
    PlacementOutcome::from_payload(&outcome.to_payload().unwrap()).unwrap().apply(&mut service).unwrap();
    assert_eq!(service.cluster_features.declared.get(&1), Some(&FEATURE_CHUNK_MANIFEST));
}
