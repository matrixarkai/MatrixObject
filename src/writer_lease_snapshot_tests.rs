// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn lease(epoch: u64) -> WriterLease {
    WriterLease {
        holder: "writer".into(),
        epoch,
        expires_at_ms: 100000,
    }
}

#[test]
fn lease_captures_share_the_root_and_preserve_key_order() {
    let mut table = WriterLeaseTable::default();
    let mut next = 0;
    for count in [1, 512, 8192] {
        while next < count {
            table.set("bucket", &format!("object-{next:08}"), &lease(1));
            next += 1;
        }
        table.validate().unwrap();
        let captured = table.clone();
        assert!(captured.leases.ptr_eq(&table.leases));
        let keys: Vec<_> = captured.iter().map(|(_, key, _)| key.to_string()).collect();
        let expected: Vec<_> = (0..count)
            .map(|index| format!("object-{index:08}"))
            .collect();
        assert_eq!(keys, expected);
    }
}

#[test]
fn captured_leases_survive_renewal_removal_new_owners_and_rollback() {
    let mut table = WriterLeaseTable::default();
    table.set("bucket", "first", &lease(1));
    table.set("bucket", "second", &lease(2));
    let captured = table.clone();
    let saved = table.get("bucket", "first");
    table.set("bucket", "first", &lease(3));
    assert!(!captured.leases.ptr_eq(&table.leases));
    table.restore("bucket", "first", saved.as_ref());
    assert_eq!(table, captured);
    table.remove("bucket", "second");
    table.set("bucket", "third", &lease(4));
    table.restore("bucket", "third", None);
    assert_eq!(table.get("bucket", "second"), None);
    assert_eq!(table.get("bucket", "third"), None);
    assert_eq!(captured.get("bucket", "second"), Some(lease(2)));
    drop(table);
    let captured = std::thread::spawn(move || {
        captured.validate().unwrap();
        captured
    })
    .join()
    .unwrap();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured.get("bucket", "first"), Some(lease(1)));
}
