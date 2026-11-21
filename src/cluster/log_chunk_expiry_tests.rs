// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn fixture() -> MatrixMetaService {
    let mut service = MatrixMetaService::new();
    service.placement_commit_index = 50;
    for (key, epoch, deadline) in [("a", 1, 90), ("b", 2, 50), ("c", 3, 80), ("live", 4, 100)] {
        service.writer_leases.set(
            "bucket",
            key,
            &WriterLease {
                holder: "writer".into(),
                epoch,
                expires_at_ms: deadline,
            },
        );
    }
    service
}

#[test]
fn sweep_sorts_selected_deadlines_before_building_the_outcome() {
    let service = fixture();
    let requests = expired(&service, 100);
    let [super::super::MetaRequest::LogChunk(request)] = requests.as_slice() else {
        panic!("one sweep command required")
    };
    request.validate().unwrap();
    assert_eq!(
        request
            .drops
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    let Decision::Change(outcome, _) = sweep(&service, request, 100).unwrap() else {
        panic!("expired leases must be proposed")
    };
    let batch = outcome.lease_batch.unwrap();
    batch.validate(50, 51).unwrap();
    batch.check_leases(&service.writer_leases).unwrap();
}

#[test]
fn queued_sweep_preserves_a_lease_renewed_after_selection() {
    let mut service = fixture();
    let requests = expired(&service, 100);
    let [super::super::MetaRequest::LogChunk(request)] = requests.as_slice() else {
        panic!("one sweep command required")
    };
    service.writer_leases.set(
        "bucket",
        "b",
        &WriterLease {
            holder: "writer".into(),
            epoch: 2,
            expires_at_ms: 200,
        },
    );
    let Decision::Change(outcome, _) = sweep(&service, request, 100).unwrap() else {
        panic!("other expired leases remain")
    };
    let batch = outcome.lease_batch.unwrap();
    assert_eq!(
        batch
            .dropped
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "c"]
    );
    batch.check_leases(&service.writer_leases).unwrap();
    batch.apply_leases(&mut service.writer_leases);
    assert_eq!(service.writer_leases.len(), 2);
    assert_eq!(
        service
            .writer_leases
            .get("bucket", "b")
            .unwrap()
            .expires_at_ms,
        200
    );
    assert!(service
        .writer_leases
        .expired_before(200)
        .all(|(_, key, _)| key == "live"));
}
