// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use prost::Message;
fn identified(tag: [u8; 16]) -> LogOpRequest {
    LogOpRequest::append_identified(V, C, 1, 5, 0, b"hello".to_vec(), tag)
}
fn query() -> LogOpRequest {
    LogOpRequest::identity(V, C, 1, 0)
}
#[test]
fn identified_requests_round_trip_and_owned_decode_reuses_the_payload_buffer() {
    let mut first = [0; 16];
    first[..8].fill(3);
    let mut second = [0; 16];
    second[8..].fill(4);
    for tag in [[0; 16], [255; 16], [1; 16], first, second] {
        for size in [1, 4096, MAX_OP_DATA] {
            let request = LogOpRequest::append_identified(V, C, 1, 5, 0, vec![7; size], tag);
            let encoded = ReplicaRequest::LogOp(request.clone()).encode_payload();
            let pointer = encoded.as_ptr();
            let capacity = encoded.capacity();
            assert_eq!(LogOpRequest::from_payload(&encoded).unwrap(), request);
            let actual = LogOpRequest::from_payload_owned(encoded).unwrap();
            assert_eq!(actual, request);
            assert_eq!(actual.attempt_tag(), Some(tag));
            assert_eq!(actual.data.as_ptr(), pointer);
            assert_eq!(actual.data.capacity(), capacity);
        }
    }
}
#[test]
fn old_request_encoding_is_unchanged_and_zero_tags_need_no_extra_wire_fields() {
    let old = LogOpRequest::append(V, C, 1, 5, 0, b"abc".to_vec());
    assert_eq!(
        old.encode_to_vec(),
        vec![8, 1, 16, 1, 24, 3, 32, 11, 40, 1, 48, 5, 82, 3, 97, 98, 99]
    );
    let zero = LogOpRequest::append_identified(V, C, 1, 5, 0, b"abc".to_vec(), [0; 16]);
    let mut expected = old.encode_to_vec();
    expected[1] = 2;
    assert_eq!(zero.encode_to_vec(), expected);
    let nonzero = LogOpRequest::append_identified(V, C, 1, 5, 0, b"abc".to_vec(), [1; 16]);
    assert_eq!(nonzero.encoded_len(), zero.encoded_len() + 18);
}
#[test]
fn malformed_identified_requests_are_rejected_before_storage() {
    let bad = [
        LogOpRequest {
            version: 1,
            attempt_hi: 1,
            ..identified([0; 16])
        },
        LogOpRequest {
            version: 3,
            ..identified([0; 16])
        },
        LogOpRequest {
            version: 2,
            ..LogOpRequest::tail(V, C, 1)
        },
        LogOpRequest {
            epoch: 1,
            ..query()
        },
        LogOpRequest {
            attempt_lo: 1,
            ..query()
        },
        LogOpRequest {
            data: vec![1],
            ..query()
        },
    ];
    for request in bad {
        assert!(request.validate().is_err());
        assert!(LogOpRequest::from_payload(&request.encode_to_vec()).is_err());
    }
    let canonical = identified([1; 16]).encode_to_vec();
    let mut short = canonical.clone();
    short.pop();
    assert!(LogOpRequest::from_payload(&short).is_err());
    let mut duplicate = canonical.clone();
    duplicate.extend_from_slice(&[97, 1, 0, 0, 0, 0, 0, 0, 0]);
    assert!(LogOpRequest::from_payload(&duplicate).is_err());
    let mut explicit_zero = identified([0; 16]).encode_to_vec();
    explicit_zero.extend_from_slice(&[89, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert!(LogOpRequest::from_payload(&explicit_zero).is_err());
    let mut wrong_wire = identified([0; 16]).encode_to_vec();
    wrong_wire.extend_from_slice(&[88, 1]);
    assert!(LogOpRequest::from_payload(&wrong_wire).is_err());
}
#[test]
fn durable_identity_survives_a_replica_dispatcher_restart() {
    let dir = Dir::new("identified-restart");
    let mut n = node(Some(&dir.0));
    call(&mut n, identified([9; 16])).unwrap();
    drop(n);
    let mut n = node(Some(&dir.0));
    let reply = call(&mut n, query()).unwrap();
    let record = reply.record_identity().unwrap();
    assert_eq!(
        (record.offset, record.length, record.attempt_tag),
        (0, 5, Some([9; 16]))
    );
    let wire = ReplicaReply::LogOp(reply.clone()).encode_payload();
    assert_eq!(LogOpReply::from_payload(&wire).unwrap(), reply);
    assert_eq!(LogOpReply::from_payload_owned(wire).unwrap(), reply);
    assert_eq!(
        call(&mut n, LogOpRequest::read(V, C, 1, 0, 5))
            .unwrap()
            .data,
        b"hello"
    );
}
#[test]
fn replica_refuses_equal_bytes_with_a_different_attempt_or_range() {
    let dir = Dir::new("identified-conflict");
    let mut n = node(Some(&dir.0));
    call(&mut n, identified([1; 16])).unwrap();
    assert!(matches!(
        call(&mut n, identified([2; 16])),
        Err(ObjectError::PreconditionFailed(_))
    ));
    let partial = LogOpRequest::append_identified(V, C, 1, 5, 1, b"ello".to_vec(), [1; 16]);
    assert!(matches!(
        call(&mut n, partial),
        Err(ObjectError::PreconditionFailed(_))
    ));
    let reply = call(&mut n, query()).unwrap();
    assert_eq!(reply.record_identity().unwrap().attempt_tag, Some([1; 16]));
    assert_eq!(reply.durable_length, 5);
}
#[test]
fn missing_identity_query_does_not_create_a_chunk_or_claim_the_directory() {
    let dir = Dir::new("identified-missing");
    let mut store = LogChunkStore::open(&dir.0).unwrap();
    assert!(!store.apply(&query()).unwrap().record_present);
    assert_eq!(store.open_chunks(), 0);
    assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0);
}
#[test]
fn legacy_records_have_no_tag_and_interior_queries_have_no_complete_record() {
    let dir = Dir::new("identified-legacy");
    let mut n = node(Some(&dir.0));
    call(
        &mut n,
        LogOpRequest::append(V, C, 1, 5, 0, b"hello".to_vec()),
    )
    .unwrap();
    let r = call(&mut n, query()).unwrap();
    assert!(r.record_present);
    assert!(!r.record_identified);
    assert_eq!(r.record_identity().unwrap().attempt_tag, None);
    assert!(
        !call(&mut n, LogOpRequest::identity(V, C, 1, 1))
            .unwrap()
            .record_present
    );
    assert!(call(&mut n, LogOpRequest::identity(V, C, 2, 0)).is_err());
}
#[test]
fn identity_is_not_visible_until_deferred_replica_sync_completes() {
    let dir = Dir::new("identified-deferred");
    let mut store = LogChunkStore::open(&dir.0).unwrap();
    store.apply_deferred(&identified([4; 16])).unwrap();
    assert!(!store.apply(&query()).unwrap().record_present);
    let pending = store.take_pending_syncs().unwrap();
    assert_eq!(pending.len(), 1);
    crate::cluster::log_chunk_store::sync_pending(&pending).unwrap();
    store.note_synced(&pending);
    assert_eq!(
        store
            .apply(&query())
            .unwrap()
            .record_identity()
            .unwrap()
            .attempt_tag,
        Some([4; 16])
    );
}
#[test]
fn pending_rpc_records_preserve_tags_when_the_missing_prefix_arrives() {
    let dir = Dir::new("identified-pending");
    let mut n = node(Some(&dir.0));
    call(
        &mut n,
        LogOpRequest::append_identified(V, C, 1, 5, 3, b"two".to_vec(), [2; 16]),
    )
    .unwrap();
    assert!(
        !call(&mut n, LogOpRequest::identity(V, C, 1, 3))
            .unwrap()
            .record_present
    );
    call(
        &mut n,
        LogOpRequest::append_identified(V, C, 1, 5, 0, b"one".to_vec(), [1; 16]),
    )
    .unwrap();
    assert_eq!(
        call(&mut n, query())
            .unwrap()
            .record_identity()
            .unwrap()
            .attempt_tag,
        Some([1; 16])
    );
    assert_eq!(
        call(&mut n, LogOpRequest::identity(V, C, 1, 3))
            .unwrap()
            .record_identity()
            .unwrap()
            .attempt_tag,
        Some([2; 16])
    );
}
#[test]
fn reply_presence_flags_and_durable_range_are_checked_independently_of_tag_value() {
    for bad in [
        LogOpReply {
            record_present: true,
            record_length: 5,
            durable_length: 5,
            length: 4,
            ..Default::default()
        },
        LogOpReply {
            attempt_hi: 1,
            ..Default::default()
        },
        LogOpReply {
            record_identified: true,
            ..Default::default()
        },
        LogOpReply {
            record_present: true,
            record_offset: u64::MAX,
            record_length: 1,
            durable_length: u64::MAX,
            ..Default::default()
        },
        LogOpReply {
            record_present: true,
            record_length: 5,
            durable_length: 4,
            ..Default::default()
        },
        LogOpReply {
            record_present: true,
            record_length: 1,
            durable_length: 1,
            attempt_lo: 1,
            ..Default::default()
        },
    ] {
        let bytes = bad.encode_to_vec();
        assert!(LogOpReply::from_payload(&bytes).is_err());
        assert!(LogOpReply::from_payload_owned(bytes).is_err());
    }
    let zero = LogOpReply {
        length: 5,
        record_present: true,
        record_identified: true,
        record_length: 5,
        durable_length: 5,
        ..Default::default()
    };
    assert_eq!(
        LogOpReply::from_payload(&zero.encode_to_vec())
            .unwrap()
            .record_identity()
            .unwrap()
            .attempt_tag,
        Some([0; 16])
    );
}
