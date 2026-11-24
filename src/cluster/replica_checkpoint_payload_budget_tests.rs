// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::super::super::{replica_checkpoint_index as index, replica_checkpoint_recovery};
use super::*;

fn budget_fixture(
    keep_first: bool,
) -> (
    Dir,
    SegmentedOutcomeLog,
    CheckpointState,
    StoredOutcomePosition,
    Arc<PayloadExtent>,
) {
    let dir = Dir::new();
    let log = dir.log();
    let mut managed = CheckpointState::new(replica_checkpoint_recovery::scalar_template()).unwrap();
    let first = payload(&log, vec![0x51; 1024 * 1024]);
    for _ in 0..23 {
        payload(&log, vec![0x63; 1024 * 1024]);
    }
    let mut attach = |number: u64, extent: Arc<PayloadExtent>, byte: u8| {
        let page = state(vec![byte; 1024 * 1024]);
        let key = id(number);
        managed.catalog.pages.insert(
            key.clone(),
            CatalogPage {
                metadata: CatalogPageMetadata::from_state(&key, &page).unwrap(),
                spans: vec![PayloadSpan {
                    offset: 0,
                    length: extent.length,
                    extent,
                    extent_offset: 0,
                }],
            },
        );
    };
    if keep_first {
        attach(0, first.clone(), 0x51);
    }
    let count = if keep_first { 24 } else { 28 };
    for number in 1..=count {
        attach(number, payload(&log, vec![0x32; 1024 * 1024]), 0x32);
    }
    let marker = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![12; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x31],
    };
    let through = log
        .append_decided_batch(&[DecidedMutation::CheckpointFragment(&marker)])
        .unwrap()
        .position;
    let metadata = managed.metadata_bytes().unwrap();
    let publication = index::prepare(
        &log,
        &managed.catalog,
        &mut managed.index,
        &mut managed.dirty_pages,
        metadata,
        Some(through),
        1,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let cut = publication.1.clone();
    managed.publication = Some(publication);
    (dir, log, managed, cut, first)
}

#[test]
fn compaction_payload_budget_defers_copies_and_resumes_when_space_pressure_rises() {
    let (_dir, log, mut managed, mut cut, first) = budget_fixture(true);
    let payload_bytes = managed
        .index
        .as_ref()
        .unwrap()
        .planned_payload_lower_bound(managed.catalog.pages.len())
        .unwrap();
    assert_eq!(payload_bytes, 25 * 1024 * 1024);
    assert!(log.logical_length().unwrap() <= payload_bytes * 2);
    let before = log.write_statistics().unwrap();
    let proposed = plan(&log, &mut managed, &cut, 1, &|| Ok(())).unwrap();
    assert!(proposed.selected.is_none());
    assert!(apply(&log, &mut managed, proposed, &|| Ok(()))
        .unwrap()
        .is_empty());
    let delta = log
        .write_statistics()
        .unwrap()
        .checked_delta(&before)
        .unwrap();
    assert_eq!(delta.maintenance_payload_bytes, 0);
    let filler = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![14; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x33; 1024 * 1024],
    };
    for _ in 0..4 {
        cut = log
            .append_decided_batch(&[DecidedMutation::CheckpointFragment(&filler)])
            .unwrap()
            .position;
    }
    assert!(log.logical_length().unwrap() > payload_bytes * 2);
    let proposed = plan(&log, &mut managed, &cut, 1, &|| Ok(())).unwrap();
    assert_eq!(
        proposed.selected.as_ref().unwrap().store_id.as_slice(),
        first.store_id
    );
    let before = log.write_statistics().unwrap();
    assert!(!apply(&log, &mut managed, proposed, &|| Ok(()))
        .unwrap()
        .is_empty());
    let delta = log
        .write_statistics()
        .unwrap()
        .checked_delta(&before)
        .unwrap();
    assert_eq!(delta.maintenance_payload_bytes, 1024 * 1024);
    assert_eq!(
        managed.catalog.pages[&id(0)]
            .materialize(|extent, offset, length| log.read_payload_range(extent, offset, length))
            .unwrap(),
        state(vec![0x51; 1024 * 1024])
    );
}

#[test]
fn compaction_payload_budget_still_retires_wholly_unreachable_files() {
    let (_dir, log, mut managed, cut, unused) = budget_fixture(false);
    let bytes = managed
        .index
        .as_ref()
        .unwrap()
        .planned_payload_lower_bound(managed.catalog.pages.len())
        .unwrap();
    assert!(log.logical_length().unwrap() <= bytes * 2);
    let proposed = plan(&log, &mut managed, &cut, 1, &|| Ok(())).unwrap();
    assert!(proposed.selected.is_none());
    assert!(proposed.unreferenced > 0);
    assert!(proposed.has_work());
    let (commit, position) = managed.publication.as_ref().unwrap();
    let keep = managed
        .index
        .as_ref()
        .unwrap()
        .protected_segments(&log, &managed.catalog, commit, position, &|| Ok(()))
        .unwrap();
    assert!(!keep.contains(&unused.store_id));
    let stats = log
        .prepare_retention(&keep, commit.through.as_ref().unwrap(), &|| Ok(()))
        .unwrap()
        .unwrap()
        .commit()
        .unwrap()
        .finish()
        .unwrap();
    assert!(stats.removed > 0);
    assert!(log.read_payload_range(&unused, 0, 1).is_err());
    assert_eq!(
        managed.catalog.pages[&id(1)]
            .materialize(|extent, offset, length| log.read_payload_range(extent, offset, length))
            .unwrap(),
        state(vec![0x32; 1024 * 1024])
    );
}
