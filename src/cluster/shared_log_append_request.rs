// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Canonical append frames with per-member metadata and one immutable payload.
use super::log_op::{LogOpRequest, MAX_OP_DATA, OP_APPEND};
use crate::ObjectError;
use prost::Message;
use std::io::{self, IoSlice, Write};
use std::sync::Arc;

pub(super) struct SharedLogAppendRequest {
    prefix: Vec<u8>,
    data: Arc<Vec<u8>>,
    suffix: Vec<u8>,
}
impl SharedLogAppendRequest {
    pub(super) fn new(request: LogOpRequest, data: Arc<Vec<u8>>) -> Result<Self, ObjectError> {
        if request.op != OP_APPEND || !request.data.is_empty() || data.len() > MAX_OP_DATA {
            return Err(ObjectError::InvalidArgument(
                "invalid shared append frame".into(),
            ));
        }
        // Complete destructuring and construction make schema additions a compile obligation.
        // Prost encodes the scalar fields; only the existing bytes-field10 boundary is split.
        let LogOpRequest {
            version,
            op,
            volume,
            chunk,
            generation,
            epoch,
            offset,
            length,
            flags,
            data: _,
            attempt_hi,
            attempt_lo,
            published_length,
            initialization_hi,
            initialization_lo,
            storage_hi,
            storage_lo,
        } = request;
        let header = LogOpRequest {
            version,
            op,
            volume,
            chunk,
            generation,
            epoch,
            offset,
            length,
            flags,
            data: Vec::new(),
            attempt_hi: 0,
            attempt_lo: 0,
            published_length: 0,
            initialization_hi: 0,
            initialization_lo: 0,
            storage_hi: 0,
            storage_lo: 0,
        };
        let suffix = LogOpRequest {
            version: 0,
            op: 0,
            volume: 0,
            chunk: 0,
            generation: 0,
            epoch: 0,
            offset: 0,
            length: 0,
            flags: 0,
            data: Vec::new(),
            attempt_hi,
            attempt_lo,
            published_length,
            initialization_hi,
            initialization_lo,
            storage_hi,
            storage_lo,
        }
        .encode_to_vec();
        let mut prefix =
            super::message::encode_request(&super::message::ReplicaRequest::LogOp(header));
        if !data.is_empty() {
            prefix.push(0x52);
            let mut size = data.len() as u64;
            while size >= 128 {
                prefix.push((size as u8 & 127) | 128);
                size >>= 7;
            }
            prefix.push(size as u8);
        }
        let body = prefix
            .len()
            .checked_sub(super::wire::HEADER_BYTES)
            .and_then(|n| n.checked_add(data.len()))
            .and_then(|n| n.checked_add(suffix.len()))
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| {
                ObjectError::InvalidArgument("shared append frame exceeds wire length".into())
            })?;
        prefix[2..6].copy_from_slice(&body.to_be_bytes());
        Ok(Self {
            prefix,
            data,
            suffix,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.prefix.len() + self.data.len() + self.suffix.len()
    }
    pub(super) fn write_from(
        &self,
        writer: &mut impl Write,
        position: usize,
        budget: usize,
    ) -> io::Result<usize> {
        let mut skip = position;
        let mut left = budget;
        let mut count = 0;
        let mut slices = [IoSlice::new(&[]); 3];
        for bytes in [
            self.prefix.as_slice(),
            self.data.as_slice(),
            self.suffix.as_slice(),
        ] {
            if skip >= bytes.len() {
                skip -= bytes.len();
                continue;
            }
            let size = (bytes.len() - skip).min(left);
            if size == 0 {
                break;
            }
            slices[count] = IoSlice::new(&bytes[skip..skip + size]);
            count += 1;
            left -= size;
            skip = 0;
        }
        writer.write_vectored(&slices[..count])
    }
}

pub(super) enum PendingRequestFrame {
    Contiguous(Arc<Vec<u8>>),
    Shared(SharedLogAppendRequest),
}
impl PendingRequestFrame {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Contiguous(frame) => frame.len(),
            Self::Shared(frame) => frame.len(),
        }
    }
    pub(super) fn write_from(
        &self,
        writer: &mut impl Write,
        position: usize,
        budget: usize,
    ) -> io::Result<usize> {
        match self {
            Self::Contiguous(frame) => {
                writer.write(&frame[position..frame.len().min(position + budget)])
            }
            Self::Shared(frame) => frame.write_from(writer, position, budget),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn flatten(frame: &SharedLogAppendRequest) -> Vec<u8> {
        [
            frame.prefix.as_slice(),
            frame.data.as_slice(),
            frame.suffix.as_slice(),
        ]
        .concat()
    }
    #[test]
    fn split_append_frames_preserve_every_existing_wire_byte() {
        for size in [0, 1, 127, 128, 16383, 16384, MAX_OP_DATA] {
            let data = Arc::new((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>());
            for version in 1..=5 {
                for wide in [false, true] {
                    let value = if wide { u64::MAX } else { 0 };
                    let mut request = LogOpRequest {
                        version,
                        op: OP_APPEND,
                        volume: value,
                        chunk: value,
                        generation: if wide { u32::MAX } else { 0 },
                        epoch: value,
                        offset: value,
                        length: 0,
                        flags: 0,
                        data: Vec::new(),
                        attempt_hi: value,
                        attempt_lo: 0,
                        published_length: value,
                        initialization_hi: 0,
                        initialization_lo: value,
                        storage_hi: value,
                        storage_lo: 0,
                    };
                    let frame =
                        SharedLogAppendRequest::new(request.clone(), Arc::clone(&data)).unwrap();
                    request.data = data.as_ref().clone();
                    let expected = super::super::message::encode_request(
                        &super::super::message::ReplicaRequest::LogOp(request),
                    );
                    assert_eq!(
                        flatten(&frame),
                        expected,
                        "size{size} version{version} wide{wide}"
                    );
                    assert_eq!(frame.len(), expected.len());
                    assert!(Arc::ptr_eq(&frame.data, &data));
                }
            }
        }
    }
    struct ShortWriter {
        bytes: Vec<u8>,
        limit: usize,
        calls: usize,
    }
    impl Write for ShortWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.write_vectored(&[IoSlice::new(bytes)])
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls % 4 == 0 {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let mut left = self.limit;
            let before = self.bytes.len();
            for slice in slices {
                let count = slice.len().min(left);
                self.bytes.extend_from_slice(&slice[..count]);
                left -= count;
                if left == 0 {
                    break;
                }
            }
            Ok(self.bytes.len() - before)
        }
    }
    #[test]
    fn segmented_requests_resume_across_every_boundary_and_interrupted_write() {
        let request =
            LogOpRequest::append_published(3, 11, 7, 9, 13, Vec::new(), [3; 16], 4096, [4; 16])
                .for_storage([5; 16], [4; 16]);
        let data = Arc::new(vec![99; 4096]);
        let frame = SharedLogAppendRequest::new(request, data).unwrap();
        for limit in [1, 2, 7, 31, 4096, 8192] {
            let mut writer = ShortWriter {
                bytes: Vec::new(),
                limit,
                calls: 0,
            };
            let mut at = 0;
            while at < frame.len() {
                match frame.write_from(&mut writer, at, 64 * 1024) {
                    Ok(n) => {
                        assert!(n > 0);
                        at += n;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => panic!("{e}"),
                }
            }
            assert_eq!(writer.bytes, flatten(&frame));
        }
    }
    #[test]
    fn personalized_members_share_one_payload_and_reject_an_owned_second_copy() {
        let data = Arc::new(vec![7; 4096]);
        let frames: Vec<_> = (1..=3)
            .map(|member| {
                SharedLogAppendRequest::new(
                    LogOpRequest::append_published(
                        3,
                        11,
                        1,
                        2,
                        0,
                        Vec::new(),
                        [8; 16],
                        4096,
                        [member; 16],
                    )
                    .for_storage([member + 3; 16], [member; 16]),
                    Arc::clone(&data),
                )
                .unwrap()
            })
            .collect();
        for frame in &frames {
            assert!(Arc::ptr_eq(&frame.data, &data));
            assert!(frame.prefix.len() + frame.suffix.len() < 256);
        }
        assert_ne!(flatten(&frames[0]), flatten(&frames[1]));
        let request = LogOpRequest::append(3, 11, 1, 2, 0, vec![1]);
        assert!(SharedLogAppendRequest::new(request, data).is_err());
    }
}
