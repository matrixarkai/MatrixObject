// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::collections::BTreeMap;

fn held(epoch: u64, deadline: u64) -> WriterLease {
    WriterLease {
        holder: "writer".into(),
        epoch,
        expires_at_ms: deadline,
    }
}
fn expired(table: &WriterLeaseTable, now: u64) -> Vec<(String, String, u64)> {
    table
        .expired_before(now)
        .map(|(bucket, key, epoch)| (bucket.to_owned(), key.to_owned(), epoch))
        .collect()
}

#[test]
fn expiry_index_tracks_renewal_fencing_removal_and_rollback() {
    let mut table = WriterLeaseTable::default();
    let mut model = BTreeMap::new();
    for step in 1..=2048u64 {
        let key = format!("object-{:03}", (step * 17) % 97);
        let identity = ("bucket".to_owned(), key.clone());
        let before = table.get("bucket", &key);
        let prior = table.clone();
        match step % 5 {
            0 => {
                table.remove("bucket", &key);
                model.remove(&identity);
            }
            1 => {
                table.restore("bucket", &key, None);
                model.remove(&identity);
            }
            _ => {
                let lease = held(step, 10 + (step % 13));
                table.set("bucket", &key, &lease);
                model.insert(identity.clone(), lease);
            }
        }
        if step % 7 == 0 {
            table.restore("bucket", &key, before.as_ref());
            match before {
                Some(lease) => {
                    model.insert(identity, lease);
                }
                None => {
                    model.remove(&identity);
                }
            }
            assert_eq!(table, prior);
        }
        if step % 23 == 0 {
            table.validate().unwrap();
            for now in [0, 10, 11, 17, 23, u64::MAX] {
                let mut expected: Vec<_> = model
                    .iter()
                    .filter(|(_, lease)| lease.expires_at_ms < now)
                    .map(|((bucket, key), lease)| {
                        (
                            lease.expires_at_ms,
                            bucket.clone(),
                            key.clone(),
                            lease.epoch,
                        )
                    })
                    .collect();
                expected.sort();
                let expected: Vec<_> = expected
                    .into_iter()
                    .map(|(_, bucket, key, epoch)| (bucket, key, epoch))
                    .collect();
                assert_eq!(expired(&table, now), expected);
            }
        }
    }
    table.set("bucket", "same-deadline", &held(3000, 1));
    table.set("bucket", "same-deadline", &held(3001, 1));
    assert_eq!(
        table
            .expired_before(2)
            .find(|(_, key, _)| *key == "same-deadline")
            .unwrap()
            .2,
        3001
    );
    assert_eq!(table.deadlines.len(), table.len());
}

#[test]
fn expiry_index_finds_old_leases_after_many_live_names() {
    let mut table = WriterLeaseTable::default();
    for index in 0..65536 {
        table.set("bucket", &format!("object-{index:08}"), &held(1, 100000));
    }
    let capture = table.clone();
    assert!(capture.leases.ptr_eq(&table.leases));
    assert!(capture.deadlines.ptr_eq(&table.deadlines));
    for (index, deadline) in [(65535, 10), (65534, 11), (65533, 12)] {
        table.set("bucket", &format!("object-{index:08}"), &held(2, deadline));
    }
    assert!(capture.expired_before(100000).next().is_none());
    assert!(table.expired_before(10).next().is_none());
    assert_eq!(
        expired(&table, 12),
        vec![
            ("bucket".into(), "object-00065535".into(), 2),
            ("bucket".into(), "object-00065534".into(), 2)
        ]
    );
    assert_eq!(table.expired_before(13).count(), 3);
    assert_eq!(table.expired_before(100000).count(), 3);
    assert_eq!(table.expired_before(100001).count(), 65536);
    table.validate().unwrap();
    let lookup = ("bucket".to_owned(), "object-00000000".to_owned());
    let (identity, _) = table.leases.get_key_value(&lookup).unwrap();
    let (_, indexed, _) = table
        .deadlines
        .iter()
        .find(|(_, candidate, _)| candidate.as_ref() == &lookup)
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(identity, indexed));
}

#[test]
fn snapshot_decoding_rebuilds_expiry_index_without_changing_bytes() {
    let mut service = crate::MatrixMetaService::new();
    for index in 1..=128 {
        service.writer_leases.set(
            "bucket",
            &format!("object-{index:03}"),
            &held(index, 10 + index % 7),
        );
    }
    let original = service.export_snapshot().to_bytes().unwrap();
    let decoded = crate::MetaSnapshot::from_bytes(&original).unwrap();
    assert_eq!(decoded.to_bytes().unwrap(), original);
    assert_eq!(decoded.writer_leases, service.writer_leases);
    assert_eq!(
        expired(&decoded.writer_leases, 15),
        expired(&service.writer_leases, 15)
    );
    let mut restored = crate::MatrixMetaService::new();
    restored.install_placement_roster(&decoded).unwrap();
    restored.install_snapshot(decoded).unwrap();
    assert_eq!(restored.export_snapshot().to_bytes().unwrap(), original);
    restored.writer_leases.remove("bucket", "object-001");
    assert_ne!(
        expired(&restored.writer_leases, 15),
        expired(&service.writer_leases, 15)
    );
}
