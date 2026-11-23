// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use super::super::MAX_OP_DATA;
use crate::cluster::{ReplicaReply, ReplicaRequest};
use crate::ObjectError;

fn same<T: PartialEq>(a: Result<T, ObjectError>, b: Result<T, ObjectError>, bytes: usize) {
    match (a,b) {
        (Ok(a),Ok(b)) => assert!(a==b,"decoded values differ for {bytes} bytes"),
        (Err(a),Err(b)) => {
            assert_eq!(std::mem::discriminant(&a),std::mem::discriminant(&b));
            assert_eq!(a.to_string(),b.to_string());
        }
        _ => panic!("acceptance differs for {bytes} bytes"),
    }
}
fn compare(bytes: &[u8]) {
    same(LogOpRequest::from_payload(bytes),LogOpRequest::from_payload_owned(bytes.to_vec()),bytes.len());
    same(LogOpReply::from_payload(bytes),LogOpReply::from_payload_owned(bytes.to_vec()),bytes.len());
}

#[test]
fn owned_request_keeps_the_rpc_allocation_for_data() {
    for size in [1,32,127,128,4096,MAX_OP_DATA] {
        let expected=LogOpRequest::append(1,1,u32::MAX,u64::MAX,u64::MAX,vec![7;size]);
        let bytes=ReplicaRequest::LogOp(expected.clone()).encode_payload();
        let pointer=bytes.as_ptr();let capacity=bytes.capacity();
        let actual=LogOpRequest::from_payload_owned(bytes).unwrap();
        assert!(actual==expected);assert_eq!(actual.data.as_ptr(),pointer);
        assert_eq!(actual.data.capacity(),capacity);
    }
}

#[test]
fn owned_reply_keeps_data_and_preserves_a_preceding_seal_digest() {
    for size in [1,32,127,128,4096,MAX_OP_DATA] {
        let expected=LogOpReply{length:u64::MAX,durable_length:u64::MAX,epoch:u64::MAX,sealed:true,
            crc:u32::MAX,sha256:vec![11;32],data:vec![7;size],..Default::default()};
        let bytes=ReplicaReply::LogOp(expected.clone()).encode_payload();
        let pointer=bytes.as_ptr();let capacity=bytes.capacity();
        let actual=LogOpReply::from_payload_owned(bytes).unwrap();
        assert!(actual==expected);assert_eq!(actual.data.as_ptr(),pointer);
        assert_eq!(actual.data.capacity(),capacity);
    }
}

#[test]
fn owned_metadata_drops_empty_data_buffers_and_reuses_a_digest_only_body() {
    let expected=LogOpRequest::tail(1,1,1);
    let actual=LogOpRequest::from_payload_owned(ReplicaRequest::LogOp(expected.clone()).encode_payload()).unwrap();
    assert!(actual==expected);assert_eq!(actual.data.capacity(),0);
    let expected=LogOpReply{length:128,durable_length:128,epoch:9,..Default::default()};
    let actual=LogOpReply::from_payload_owned(ReplicaReply::LogOp(expected.clone()).encode_payload()).unwrap();
    assert!(actual==expected);assert_eq!(actual.data.capacity(),0);assert_eq!(actual.sha256.capacity(),0);
    let expected=LogOpReply{length:128,durable_length:128,epoch:9,sealed:true,crc:17,sha256:vec![8;32],..Default::default()};
    let bytes=ReplicaReply::LogOp(expected.clone()).encode_payload();let pointer=bytes.as_ptr();let capacity=bytes.capacity();
    let actual=LogOpReply::from_payload_owned(bytes).unwrap();assert!(actual==expected);
    assert_eq!(actual.sha256.as_ptr(),pointer);assert_eq!(actual.sha256.capacity(),capacity);
}

#[test]
fn rejected_owned_wire_is_unchanged_before_error_classification() {
    for bytes in [vec![88,1],vec![8,1,8,1],vec![16,1,8,1],vec![8,0],vec![0x88,0,1],
        vec![8,0x81,0],vec![82,0],vec![82,0x81,0,7],vec![82,0x80],vec![82,5,1,2],vec![0,1]] {
        if parse(&bytes,REQUEST_FIELDS).is_none() {assert_eq!(request_owned(bytes.clone()).unwrap_err(),bytes);}
        if parse(&bytes,REPLY_FIELDS).is_none() {assert_eq!(reply_owned(bytes.clone()).unwrap_err(),bytes);}
        compare(&bytes);
    }
    compare(&vec![0;MAX_OP_DATA+257]);
    compare(&ReplicaRequest::LogOp(LogOpRequest::append(1,1,1,1,0,vec![7;MAX_OP_DATA+1])).encode_payload());
    compare(&ReplicaReply::LogOp(LogOpReply{data:vec![7;MAX_OP_DATA+1],..Default::default()}).encode_payload());
    for size in [1,31,33] {compare(&ReplicaReply::LogOp(LogOpReply{sha256:vec![7;size],data:vec![9;32],..Default::default()}).encode_payload());}
}

#[test]
fn owned_decode_matches_borrowed_decode_across_mutations() {
    let fixtures=[ReplicaRequest::LogOp(LogOpRequest::append(127,128,16384,1<<63,4096,vec![9;32])).encode_payload(),
        ReplicaReply::LogOp(LogOpReply{length:u64::MAX,durable_length:128,epoch:1,sealed:true,crc:u32::MAX,sha256:vec![7;32],data:vec![9;32],..Default::default()}).encode_payload()];
    for fixture in fixtures {
        compare(&fixture);
        for index in 0..fixture.len() {
            for byte in 0..=255 {let mut changed=fixture.clone();changed[index]=byte;compare(&changed);}
            compare(&fixture[..index]);
        }
    }
}
