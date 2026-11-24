// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use super::super::MAX_OP_DATA;
use crate::ObjectError;
use crate::cluster::{ReplicaReply, ReplicaRequest};
use prost::Message;
// Independent general decoding and canonical re-encoding retain the previous
// acceptance rules and error text, including asymmetric oversized-frame errors.
fn prost_request(bytes: &[u8]) -> Result<LogOpRequest, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Err(ObjectError::InvalidArgument("log chunk operation exceeds its bound".into()));
        }
        let value = LogOpRequest::decode(bytes).map_err(|_| ObjectError::Corruption("invalid log chunk operation".into()))?;
        if value.encode_to_vec() != bytes {
            return Err(ObjectError::Corruption("noncanonical log chunk operation".into()));
        }
        value.validate().map_err(|_| ObjectError::Corruption("invalid log chunk operation".into()))?;
        Ok(value)
    }
fn prost_reply(bytes: &[u8]) -> Result<LogOpReply, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Err(ObjectError::Corruption("log chunk reply exceeds its bound".into()));
        }
        let value = LogOpReply::decode(bytes).map_err(|_| ObjectError::Corruption("invalid log chunk reply".into()))?;
        if value.encode_to_vec() != bytes || !(value.sha256.is_empty() || value.sha256.len() == 32) || !value.identity_shape_valid() {
            return Err(ObjectError::Corruption("noncanonical log chunk reply".into()));
        }
        Ok(value)
    }
fn category<T>(result:&Result<T,ObjectError>)-> &'static str {match result {Ok(_)=>"ok",Err(ObjectError::Corruption(_))=>"corruption",Err(ObjectError::InvalidArgument(_))=>"invalid argument",_=>"other"}}
fn compare(bytes:&[u8]) {
    match (prost_request(bytes),LogOpRequest::from_payload(bytes)) {
        (Ok(a),Ok(b))=>assert!(a==b,"decoded value differs; payload length {}",bytes.len()),
        (Err(ObjectError::Corruption(a)),Err(ObjectError::Corruption(b)))=>assert_eq!(a,b),
        (Err(ObjectError::InvalidArgument(a)),Err(ObjectError::InvalidArgument(b)))=>assert_eq!(a,b),
        pair=>panic!("request acceptance differs: input bytes={}, prefix={:02x?}, categories={:?}",bytes.len(),&bytes[..bytes.len().min(64)],(category(&pair.0),category(&pair.1))),
    }
    match (prost_reply(bytes),LogOpReply::from_payload(bytes)) {
        (Ok(a),Ok(b))=>assert!(a==b,"decoded value differs; payload length {}",bytes.len()),
        (Err(ObjectError::Corruption(a)),Err(ObjectError::Corruption(b)))=>assert_eq!(a,b),
        (Err(ObjectError::InvalidArgument(a)),Err(ObjectError::InvalidArgument(b)))=>assert_eq!(a,b),
        pair=>panic!("reply acceptance differs: input bytes={}, prefix={:02x?}, categories={:?}",bytes.len(),&bytes[..bytes.len().min(64)],(category(&pair.0),category(&pair.1))),
    }
}
#[test]
fn public_valid_operations_and_maximum_payloads_decode_identically() {
    for value in [LogOpRequest::tail(1,1,1),LogOpRequest::read(1,1,1,0,0),
        LogOpRequest::read(1,1,1,u64::MAX,MAX_OP_DATA as u32),
        LogOpRequest::truncate(1,1,1,u64::MAX,u64::MAX),
        LogOpRequest::seal(1,1,1,1,false),LogOpRequest::seal(1,1,1,1,true)] {
        let bytes=ReplicaRequest::LogOp(value.clone()).encode_payload();compare(&bytes);assert_eq!(LogOpRequest::from_payload(&bytes).unwrap(),value);
    }
    for size in [1,32,127,128,16383,16384,MAX_OP_DATA] {
        let value=LogOpRequest::append(1,1,u32::MAX,u64::MAX,u64::MAX,vec![7;size]);
        let bytes=ReplicaRequest::LogOp(value.clone()).encode_payload();compare(&bytes);assert_eq!(LogOpRequest::from_payload(&bytes).unwrap(),value);
        let value=LogOpReply{length:u64::MAX,durable_length:u64::MAX,epoch:u64::MAX,sealed:true,crc:u32::MAX,sha256:vec![11;32],data:vec![7;size],..Default::default()};
        let bytes=ReplicaReply::LogOp(value.clone()).encode_payload();compare(&bytes);assert_eq!(LogOpReply::from_payload(&bytes).unwrap(),value);
    }
    compare(&ReplicaRequest::LogOp(LogOpRequest::append(1,1,1,1,0,vec![7;MAX_OP_DATA+1])).encode_payload());
    compare(&ReplicaReply::LogOp(LogOpReply{data:vec![7;MAX_OP_DATA+1],..Default::default()}).encode_payload());
    for length in [1,31,33] {
        compare(&ReplicaReply::LogOp(LogOpReply{sha256:vec![7;length],data:vec![9;32],..Default::default()}).encode_payload());
    }
    compare(&vec![0;MAX_OP_DATA+257]);
}
#[test]
fn malformed_varints_defaults_reordering_unknowns_and_shapes_match() {
    for bytes in [vec![],vec![88,1],vec![8,1,8,1],vec![16,1,8,1],vec![8,0],
        vec![0x88,0,1],vec![8,0x81,0],vec![82,0],vec![82,0x81,0,7],vec![32,2],
        vec![8,0x80,0x80,0x80,0x80,0x10],
        vec![8,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,2],
        vec![82,5,1,2],vec![0,1],vec![82,0x80],vec![9,1,0,0,0,0,0,0,0]] {compare(&bytes);}
    for n in [0,1,127,128,16383,16384,u32::MAX as u64,u32::MAX as u64+1,1<<63,u64::MAX] {
        compare(&ReplicaRequest::LogOp(LogOpRequest{version:n as u32,op:n as u32,volume:n,chunk:n,generation:n as u32,epoch:n,offset:n,length:n as u32,flags:n as u32,data:vec![n as u8;129],..Default::default()}).encode_payload());
        compare(&ReplicaReply::LogOp(LogOpReply{length:n,durable_length:n,epoch:n,sealed:n!=0,crc:n as u32,sha256:vec![n as u8;32],data:vec![7;129],..Default::default()}).encode_payload());
    }
}
#[test]
fn mutations_and_arbitrary_bytes_match_the_public_decoder() {
    let mut seed=0x3bf2_9ad7_4121_8e05u64;
    let mut next=||{seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;seed};
    let fixtures=[ReplicaRequest::LogOp(LogOpRequest::append(127,128,16384,1<<63,4096,vec![9;32])).encode_payload(),
        ReplicaReply::LogOp(LogOpReply{length:u64::MAX,durable_length:128,epoch:1,sealed:true,crc:u32::MAX,sha256:vec![7;32],data:vec![9;32],..Default::default()}).encode_payload()];
    for fixture in fixtures {
        compare(&fixture);
        for index in 0..fixture.len() {
            for byte in 0..=255 {let mut mutated=fixture.clone();mutated[index]=byte;compare(&mutated);}
            compare(&fixture[..index]);
        }
        for _ in 0..4000 {
            let mut mutated=fixture.clone();
            for _ in 0..1+next()%4 {let index=next() as usize%mutated.len();mutated[index]=next() as u8;}
            compare(&mutated);
        }
    }
    for length in 0..65 {for _ in 0..256 {compare(&(0..length).map(|_|next() as u8).collect::<Vec<_>>());}}
}
