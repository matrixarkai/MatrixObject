// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Strict flat protobuf decoding with one owned copy per populated byte field.
//! Ordered unique tags, omitted defaults, minimal varints and checked widths
//! preserve canonical encodings. Each scalar and byte field is bound explicitly;
//! complete message construction makes schema additions a compile obligation.
use super::{LogOpReply, LogOpRequest};

#[derive(Clone, Copy)]
enum Field {
    U32,
    U64,
    Bool,
    Bytes,
    Fixed64,
}
const REQUEST_FIELDS: &[Field] = &[
    Field::U32,
    Field::U32,
    Field::U64,
    Field::U64,
    Field::U32,
    Field::U64,
    Field::U64,
    Field::U32,
    Field::U32,
    Field::Bytes,
    Field::Fixed64,
    Field::Fixed64,
];
const REPLY_FIELDS: &[Field] = &[
    Field::U64,
    Field::U64,
    Field::U64,
    Field::Bool,
    Field::U32,
    Field::Bytes,
    Field::Bytes,
    Field::U64,
    Field::U32,
    Field::Fixed64,
    Field::Fixed64,
    Field::Bool,
    Field::Bool,
    Field::U64,
    Field::Bool,
    Field::U64,
    Field::Bool,
    Field::Bool,
    Field::Bool,
];
struct Fields {
    scalars: [u64; 20],
    bytes: [(usize, usize); 20],
}
fn varint(bytes: &[u8], position: &mut usize) -> Option<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*position)?;
        *position += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return (shift == 0 || byte != 0).then_some(value);
        }
    }
    None
}
fn parse(bytes: &[u8], schema: &[Field]) -> Option<Fields> {
    let mut fields = Fields {
        scalars: [0; 20],
        bytes: [(0, 0); 20],
    };
    let mut position = 0;
    let mut previous = 0;
    while position < bytes.len() {
        let key = varint(bytes, &mut position)?;
        let tag = key >> 3;
        if tag <= previous || tag > schema.len() as u64 {
            return None;
        }
        previous = tag;
        let tag = tag as usize;
        if matches!(schema[tag - 1], Field::Fixed64) {
            if key & 7 != 1 {
                return None;
            }
            let end = position.checked_add(8)?;
            let value = u64::from_le_bytes(bytes.get(position..end)?.try_into().ok()?);
            if value == 0 {
                return None;
            }
            fields.scalars[tag] = value;
            position = end;
            continue;
        }
        let value = varint(bytes, &mut position)?;
        if value == 0 {
            return None;
        }
        match (schema[tag - 1], key & 7) {
            (Field::Bytes, 2) => {
                let end = position.checked_add(usize::try_from(value).ok()?)?;
                bytes.get(position..end)?;
                fields.bytes[tag] = (position, end);
                position = end;
            }
            (Field::U64, 0) => fields.scalars[tag] = value,
            (Field::U32, 0) if value <= u64::from(u32::MAX) => fields.scalars[tag] = value,
            (Field::Bool, 0) if value == 1 => fields.scalars[tag] = value,
            _ => return None,
        }
    }
    Some(fields)
}

fn field_bytes<'a>(bytes: &'a [u8], range: (usize, usize)) -> &'a [u8] {
    &bytes[range.0..range.1]
}

fn request_value(scalars: [u64; 20], data: Vec<u8>) -> LogOpRequest {
    LogOpRequest {
        version: scalars[1] as u32,
        op: scalars[2] as u32,
        volume: scalars[3],
        chunk: scalars[4],
        generation: scalars[5] as u32,
        epoch: scalars[6],
        offset: scalars[7],
        length: scalars[8] as u32,
        flags: scalars[9] as u32,
        data,
        attempt_hi: scalars[11],
        attempt_lo: scalars[12],
    }
}

fn reply_value(scalars: [u64; 20], sha256: Vec<u8>, data: Vec<u8>) -> LogOpReply {
    LogOpReply {
        length: scalars[1],
        durable_length: scalars[2],
        epoch: scalars[3],
        sealed: scalars[4] == 1,
        crc: scalars[5] as u32,
        sha256,
        data,
        record_offset: scalars[8],
        record_length: scalars[9] as u32,
        attempt_hi: scalars[10],
        attempt_lo: scalars[11],
        record_present: scalars[12] == 1,
        record_identified: scalars[13] == 1,
        published_length: scalars[14],
        publication_mode: scalars[15] == 1,
        record_published_length: scalars[16],
        publication_supported: scalars[17] == 1,
        record_publication: scalars[18] == 1,
        chunk_present: scalars[19] == 1,
    }
}

pub(super) fn request(bytes: &[u8]) -> Option<LogOpRequest> {
    let f = parse(bytes, REQUEST_FIELDS)?;
    Some(request_value(
        f.scalars,
        field_bytes(bytes, f.bytes[10]).to_vec(),
    ))
}

pub(super) fn reply(bytes: &[u8]) -> Option<LogOpReply> {
    let f = parse(bytes, REPLY_FIELDS)?;
    Some(reply_value(
        f.scalars,
        field_bytes(bytes, f.bytes[6]).to_vec(),
        field_bytes(bytes, f.bytes[7]).to_vec(),
    ))
}

// Ranges come only from the checked parser. No allocation or capacity growth:
// move the byte field to the start of the existing owned RPC payload buffer.
fn take_data(mut bytes: Vec<u8>, range: (usize, usize)) -> Vec<u8> {
    if range.0 == range.1 {
        return Vec::new();
    }
    bytes.copy_within(range.0..range.1, 0);
    bytes.truncate(range.1 - range.0);
    bytes
}

pub(super) fn request_owned(bytes: Vec<u8>) -> Result<LogOpRequest, Vec<u8>> {
    let Some(f) = parse(&bytes, REQUEST_FIELDS) else {
        return Err(bytes);
    };
    Ok(request_value(f.scalars, take_data(bytes, f.bytes[10])))
}

pub(super) fn reply_owned(bytes: Vec<u8>) -> Result<LogOpReply, Vec<u8>> {
    let Some(f) = parse(&bytes, REPLY_FIELDS) else {
        return Err(bytes);
    };
    if f.bytes[7].0 == f.bytes[7].1 {
        return Ok(reply_value(
            f.scalars,
            take_data(bytes, f.bytes[6]),
            Vec::new(),
        ));
    }
    // Preserve the small seal digest before compacting the data field in place.
    let sha256 = field_bytes(&bytes, f.bytes[6]).to_vec();
    Ok(reply_value(f.scalars, sha256, take_data(bytes, f.bytes[7])))
}

#[cfg(test)]
#[path = "log_protobuf_decode_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "log_protobuf_owned_tests.rs"]
mod owned_tests;
