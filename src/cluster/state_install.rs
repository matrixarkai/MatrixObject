// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded staging for standalone page installation.
#[cfg(feature = "replica-server")]
use crate::PageState;
use crate::{ObjectError, PageId, ReplicaHead};
use prost::Message;

#[derive(Clone, Eq, PartialEq, Message)]
pub struct InstallRange {
    #[prost(uint64, tag = "1")]
    pub offset: u64,
    #[prost(uint64, tag = "2")]
    pub length: u64,
}
#[derive(Clone, Eq, PartialEq, Message)]
pub struct InstallHead {
    #[prost(uint64, tag = "1")]
    pub logical_length: u64,
    #[prost(uint64, tag = "2")]
    pub write_sequence: u64,
    #[prost(string, tag = "3")]
    pub checksum: String,
    #[prost(string, tag = "4")]
    pub state_checksum: String,
    #[prost(message, repeated, tag = "5")]
    pub discarded_ranges: Vec<InstallRange>,
    #[prost(uint64, tag = "6")]
    pub data_length: u64,
}
/// Code 11. Operations: 1 begin, 2 append, 3 commit, 4 abort.
/// Empty expected checksum on begin means the destination page must be absent.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct StateInstall {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub operation: u32,
    #[prost(string, tag = "3")]
    pub token: String,
    #[prost(uint64, tag = "4")]
    pub volume: u64,
    #[prost(uint64, tag = "5")]
    pub chunk: u64,
    #[prost(uint64, tag = "6")]
    pub page: u64,
    #[prost(string, tag = "7")]
    pub expected_checksum: String,
    #[prost(message, optional, tag = "8")]
    pub head: Option<InstallHead>,
    #[prost(uint64, tag = "9")]
    pub offset: u64,
    #[prost(bytes = "vec", tag = "10")]
    pub data: Vec<u8>,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::InvalidArgument(message.into())
}
#[cfg(feature = "replica-server")]
fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
impl StateInstall {
    pub(crate) fn decode_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > super::state_transfer::MAX_STATE_RANGE_BYTES + 512 {
            return Err(invalid("install frame exceeds bound"));
        }
        let value: Self = super::conditional_requests::decode(bytes)?;
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.version != 1 || self.token.is_empty() || self.token.len() > 128 {
            return Err(invalid("invalid install version or token"));
        }
        match self.operation {
            1 => {
                self.page_id()?;
                let head = self
                    .head
                    .as_ref()
                    .ok_or_else(|| invalid("missing install head"))?;
                if self.expected_checksum.len() > 128
                    || self.offset != 0
                    || !self.data.is_empty()
                    || head.data_length > super::state_transfer::MAX_STATE_BYTES
                    || head.logical_length > head.data_length
                    || head.write_sequence == 0
                    || head.checksum.is_empty()
                    || head.checksum.len() > 128
                    || head.state_checksum.is_empty()
                    || head.state_checksum.len() > 128
                    || head.discarded_ranges.len() > 4096
                {
                    return Err(invalid("invalid install head bounds"));
                }
                let mut end = 0;
                for (index, range) in head.discarded_ranges.iter().enumerate() {
                    let next = range
                        .offset
                        .checked_add(range.length)
                        .ok_or_else(|| invalid("install discard overflow"))?;
                    if range.length == 0
                        || next > head.logical_length
                        || (index > 0 && range.offset <= end)
                    {
                        return Err(invalid("invalid install discarded ranges"));
                    }
                    end = next;
                }
            }
            2..=4 => {
                if self.volume != 0
                    || self.chunk != 0
                    || self.page != 0
                    || !self.expected_checksum.is_empty()
                    || self.head.is_some()
                {
                    return Err(invalid("unexpected install fields"));
                }
                if self.operation == 2 {
                    if self.data.is_empty()
                        || self.data.len() > super::state_transfer::MAX_STATE_RANGE_BYTES
                        || self.offset > super::state_transfer::MAX_STATE_BYTES
                    {
                        return Err(invalid("invalid install append bounds"));
                    }
                } else if self.offset != 0 || !self.data.is_empty() {
                    return Err(invalid("unexpected install completion data"));
                }
            }
            _ => return Err(invalid("unknown install operation")),
        }
        Ok(())
    }
    pub(crate) fn page_id(&self) -> Result<PageId, ObjectError> {
        Ok(PageId {
            chunk_id: crate::ChunkId::new(self.volume, self.chunk)?,
            page_index: self.page,
        })
    }
    pub fn begin(
        token: String,
        page: &PageId,
        expected: Option<&str>,
        head: &ReplicaHead,
        data_length: u64,
    ) -> Self {
        Self {
            version: 1,
            operation: 1,
            token,
            volume: page.chunk_id.volume_id(),
            chunk: page.chunk_id.id(),
            page: page.page_index,
            expected_checksum: expected.unwrap_or("").into(),
            head: Some(InstallHead {
                logical_length: head.logical_length,
                write_sequence: head.write_sequence,
                checksum: head.checksum.clone(),
                state_checksum: head.state_checksum.clone(),
                discarded_ranges: head
                    .discarded_ranges
                    .iter()
                    .map(|r| InstallRange {
                        offset: r.offset,
                        length: r.length,
                    })
                    .collect(),
                data_length,
            }),
            ..Default::default()
        }
    }
    pub fn command(operation: u32, token: String) -> Self {
        Self {
            version: 1,
            operation,
            token,
            ..Default::default()
        }
    }
}

#[cfg(feature = "replica-server")]
pub(super) struct StagedState {
    pub begin: StateInstall,
    data: Vec<u8>,
    touched: std::time::Instant,
}
#[cfg(feature = "replica-server")]
impl StagedState {
    pub fn expired(&self) -> bool {
        self.touched.elapsed() >= std::time::Duration::from_secs(60)
    }
    pub fn new(begin: StateInstall) -> Result<Self, ObjectError> {
        begin.validate()?;
        if begin.operation != 1 {
            return Err(invalid("staging requires begin"));
        }
        let mut data = Vec::new();
        data.try_reserve_exact(begin.head.as_ref().unwrap().data_length as usize)
            .map_err(|_| refused("cannot reserve install buffer"))?;
        Ok(Self {
            begin,
            data,
            touched: std::time::Instant::now(),
        })
    }
    pub fn append(&mut self, request: &StateInstall) -> Result<(), ObjectError> {
        request.validate()?;
        if request.operation != 2 {
            return Err(invalid("staging requires append"));
        }
        let total = self.begin.head.as_ref().unwrap().data_length as usize;
        if request.token != self.begin.token
            || request.offset != self.data.len() as u64
            || request.data.len() > total.saturating_sub(self.data.len())
        {
            return Err(refused("install append differs from staged position"));
        }
        self.data.extend_from_slice(&request.data);
        self.touched = std::time::Instant::now();
        Ok(())
    }
    pub fn finish(self) -> Result<(PageId, PageState, String), ObjectError> {
        let page = self.begin.page_id()?;
        let head = self.begin.head.unwrap();
        if self.data.len() as u64 != head.data_length {
            return Err(refused("install payload is incomplete"));
        }
        let state = PageState {
            logical_length: head.logical_length,
            write_sequence: head.write_sequence,
            checksum_fnv1a64: crate::fnv1a64(&self.data),
            data: std::sync::Arc::new(self.data),
            ec_parity_fragment: None,
            ec_fragment_digests: None,
            discarded_ranges: head
                .discarded_ranges
                .into_iter()
                .map(|r| crate::DataRange {
                    offset: r.offset,
                    length: r.length,
                })
                .collect(),
        };
        crate::validate_chunk_state(&state)?;
        if state.checksum() != head.checksum
            || crate::chunk_state_handoff_checksum(&page, &state, &state.checksum())
                != head.state_checksum
        {
            return Err(ObjectError::Corruption(
                "install payload checksum mismatch".into(),
            ));
        }
        Ok((page, state, self.begin.expected_checksum))
    }
}

#[cfg(all(test, feature = "replica-server"))]
mod tests {
    use super::*;
    #[test]
    fn staged_install_codec_limits_offsets_and_expiry() {
        let page = PageId {
            chunk_id: crate::ChunkId::new(1, 2).unwrap(),
            page_index: 0,
        };
        let mut source = crate::MatrixReplicaService::new();
        source.write_chunk(page.clone(), 0, b"hello").unwrap();
        let head = source.head_chunk(&page).unwrap();
        let begin = StateInstall::begin("test".into(), &page, None, &head, 5);
        assert_eq!(
            StateInstall::decode_payload(&begin.encode_to_vec()).unwrap(),
            begin
        );
        let mut bytes = begin.encode_to_vec();
        bytes.extend_from_slice(&[8, 1]);
        assert!(StateInstall::decode_payload(&bytes).is_err());
        let mut bytes = begin.encode_to_vec();
        bytes.extend_from_slice(&[88, 1]);
        assert!(StateInstall::decode_payload(&bytes).is_err());
        for field in 0..6 {
            let mut bad = begin.clone();
            match field {
                0 => bad.version = 2,
                1 => bad.token.clear(),
                2 => {
                    bad.head.as_mut().unwrap().data_length =
                        super::super::state_transfer::MAX_STATE_BYTES + 1
                }
                3 => bad.head.as_mut().unwrap().logical_length = 6,
                4 => bad
                    .head
                    .as_mut()
                    .unwrap()
                    .discarded_ranges
                    .push(InstallRange {
                        offset: 4,
                        length: 2,
                    }),
                _ => bad.data.push(1),
            }
            assert!(bad.validate().is_err());
        }
        let mut stage = StagedState::new(begin.clone()).unwrap();
        let mut append = StateInstall::command(2, "test".into());
        append.data = b"hello".to_vec();
        append.offset = 1;
        assert!(stage.append(&append).is_err());
        append.offset = 0;
        stage.append(&append).unwrap();
        assert!(stage.append(&append).is_err());
        assert!(!stage.expired());
        stage.touched = std::time::Instant::now() - std::time::Duration::from_secs(61);
        assert!(stage.expired());
        let (_, state, _) = stage.finish().unwrap();
        assert_eq!(state.data.as_slice(), b"hello");
        assert!(StagedState::new(begin.clone()).unwrap().finish().is_err());
        let mut stage = StagedState::new(begin).unwrap();
        append.data = b"wrong".to_vec();
        stage.append(&append).unwrap();
        assert!(stage.finish().is_err());
    }
}
