// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::outcome::PageOutcome;

fn page() -> PageId {
    PageId {
        chunk_id: ChunkId::new(7, 8).unwrap(),
        page_index: 0,
    }
}
fn service(prefix: &[u8]) -> MatrixReplicaService {
    let mut service = MatrixReplicaService::new();
    service.set_keep_ec_sidecar(false);
    service.write_chunk(page(), 0, prefix).unwrap();
    service
}

#[test]
fn journal_append_outcomes_match_general_binary_decisions_and_recover() {
    let suffixes = [
        vec![0x7b; 4096],
        vec![0; 4096],
        (0..4096).map(|i| if i % 17 == 0 { 5 } else { 0 }).collect(),
    ];
    for length in [1, 4095, 4096, 4097, 1024 * 1024] {
        for suffix in &suffixes {
            let mut service = service(&vec![0x31; length]);
            let before = service.read_chunk_state(&page()).unwrap();
            let counters = PageOutcome::counters(&service);
            let mut recovered = service.clone();
            let prepared = service
                .prepare_journal_append(&page(), length as u64, suffix)
                .unwrap()
                .unwrap();
            service.write_chunk(page(), length as u64, suffix).unwrap();
            service.verify_journal_append(&prepared).unwrap();
            let fast = prepared
                .outcome
                .with_counters(counters, PageOutcome::counters(&service));
            let after = service.read_chunk_state(&page()).unwrap();
            let general = PageOutcome::between(&page(), Some(&before), &after)
                .unwrap()
                .with_counters(counters, PageOutcome::counters(&service));
            assert_eq!(fast.to_payload().unwrap(), general.to_payload().unwrap());
            assert!(fast.to_payload().unwrap().len() <= suffix.len() + 512);
            PageOutcome::from_payload(&fast.to_payload().unwrap())
                .unwrap()
                .apply(&mut recovered, 10)
                .unwrap();
            assert_eq!(recovered.read_chunk_state(&page()).unwrap(), after);
            assert_eq!(
                PageOutcome::counters(&recovered),
                PageOutcome::counters(&service)
            );
        }
    }
}

#[test]
fn journal_append_preparation_keeps_the_payload_exclusively_owned() {
    let mut service = service(&vec![0x31; 1024 * 1024]);
    // Isolate snapshot copying from the separate geometric allocation policy.
    let state = std::sync::Arc::make_mut(service.pages.get_mut(&page()).unwrap());
    std::sync::Arc::make_mut(&mut state.data).reserve_exact(8192);
    let address = service.pages[&page()].data.as_ptr();
    let prepared = service
        .prepare_journal_append(&page(), 1024 * 1024, &[9; 4096])
        .unwrap()
        .unwrap();
    assert_eq!(
        std::sync::Arc::strong_count(&service.pages[&page()].data),
        1
    );
    service
        .write_chunk(page(), 1024 * 1024, &[9; 4096])
        .unwrap();
    service.verify_journal_append(&prepared).unwrap();
    assert_eq!(service.pages[&page()].data.as_ptr(), address);
}

#[test]
fn journal_append_recovery_policy_refuses_an_active_mutation_batch() {
    let mut service = service(&[0x31; 4096]);
    service.begin_mutation_batch("active").unwrap();
    let before = service.read_chunk_state(&page()).unwrap();
    assert!(matches!(
        service.configure_journal_recovery(true),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(service.read_chunk_state(&page()).unwrap(), before);
    assert!(!service.keep_ec_sidecar);
}

#[test]
fn journal_append_uses_general_decisions_for_other_mutation_shapes() {
    let mut service = service(&[3; 4096]);
    for (offset, suffix) in [(0, b"x".as_slice()), (4097, b"x"), (4096, b"")] {
        assert!(service
            .prepare_journal_append(&page(), offset, suffix)
            .unwrap()
            .is_none());
    }
    service.discard_chunk(&page(), 0, 1).unwrap();
    assert!(service
        .prepare_journal_append(&page(), 4096, b"x")
        .unwrap()
        .is_none());
    let mut sidecar = MatrixReplicaService::new();
    sidecar.write_chunk(page(), 0, &[2; 4096]).unwrap();
    assert!(sidecar
        .prepare_journal_append(&page(), 4096, b"x")
        .unwrap()
        .is_none());
}

#[test]
fn journal_append_rejects_invalidated_prefix_and_changed_suffix() {
    let mut service = service(&vec![0x31; 16384]);
    drop(
        service
            .prepare_journal_append(&page(), 16384, b"tail")
            .unwrap()
            .unwrap(),
    );
    service.corrupt_chunk_payload_for_repair(&page()).unwrap();
    assert!(matches!(
        service.prepare_journal_append(&page(), 16384, b"tail"),
        Err(ObjectError::Corruption(_))
    ));

    let mut service = self::service(&[0x31; 4096]);
    let prepared = service
        .prepare_journal_append(&page(), 4096, b"tail")
        .unwrap()
        .unwrap();
    service.write_chunk(page(), 4096, b"tail").unwrap();
    let state = std::sync::Arc::make_mut(service.pages.get_mut(&page()).unwrap());
    std::sync::Arc::make_mut(&mut state.data)[4097] ^= 1;
    assert!(service.verify_journal_append(&prepared).is_err());
}

#[test]
fn journal_append_preserves_unread_prefix_corruption_detection_and_clean_recovery() {
    let mut service = service(&vec![0x31; 16384]);
    let mut recovered = service.clone();
    drop(
        service
            .prepare_journal_append(&page(), 16384, b"tail")
            .unwrap()
            .unwrap(),
    );
    // Simulate an untracked memory fault outside the touched tail block. The
    // existing validation memo must still reject that block when it is read;
    // its damaged bytes must never enter the durable suffix decision.
    let state = std::sync::Arc::make_mut(service.pages.get_mut(&page()).unwrap());
    std::sync::Arc::make_mut(&mut state.data)[0] ^= 1;
    let prepared = service
        .prepare_journal_append(&page(), 16384, b"tail")
        .unwrap()
        .unwrap();
    let counters = PageOutcome::counters(&service);
    service.write_chunk(page(), 16384, b"tail").unwrap();
    service.verify_journal_append(&prepared).unwrap();
    assert!(service.read_chunk(&page(), 0, 4096).is_err());
    let outcome = prepared
        .outcome
        .with_counters(counters, PageOutcome::counters(&service));
    outcome.apply(&mut recovered, 10).unwrap();
    assert_eq!(
        recovered.read_chunk(&page(), 0, 16384).unwrap().data,
        vec![0x31; 16384]
    );
    assert_eq!(
        recovered.read_chunk(&page(), 16384, 4).unwrap().data,
        b"tail"
    );
}
