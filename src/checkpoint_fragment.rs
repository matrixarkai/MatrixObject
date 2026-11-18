// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded binary storage fragments for checkpoint catalogs and bulk installs.
//! These are storage records, not state transitions. A checkpoint-aware journal
//! publishes their completed catalog before installing state. Ordinary state
//! replay refuses them.
use crate::ObjectError;
use prost::Message;
const MAGIC: &[u8; 4] = b"MOCB";

#[derive(Clone, PartialEq, Message)]
pub struct CheckpointFragment {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub checkpoint_id: Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub kind: u32,
    #[prost(uint64, tag = "4")]
    pub ordinal: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub data: Vec<u8>,
}
impl CheckpointFragment {
    pub const PAYLOAD: u32 = 1;
    pub const CATALOG: u32 = 2;
    pub const COMMIT: u32 = 3;
    pub const MAX_DATA_BYTES: usize = 1024 * 1024;

    fn validate(&self, length: usize, external: bool) -> Result<(), ObjectError> {
        if self.version != 1
            || self.checkpoint_id.len() != 16
            || !matches!(self.kind, Self::PAYLOAD | Self::CATALOG | Self::COMMIT)
            || length == 0
            || length > Self::MAX_DATA_BYTES
            || (external && !self.data.is_empty())
            || (!external && self.data.len() != length)
        {
            return Err(ObjectError::Corruption(
                "invalid checkpoint fragment".into(),
            ));
        }
        Ok(())
    }
    pub fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate(self.data.len(), false)?;
        let mut out = Vec::with_capacity(4 + self.encoded_len());
        out.extend_from_slice(MAGIC);
        self.encode(&mut out)
            .map_err(|e| ObjectError::Corruption(e.to_string()))?;
        Ok(out)
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > Self::MAX_DATA_BYTES + 128 || !bytes.starts_with(MAGIC) {
            return Err(ObjectError::Corruption(
                "invalid checkpoint fragment envelope".into(),
            ));
        }
        let value =
            Self::decode(&bytes[4..]).map_err(|e| ObjectError::Corruption(e.to_string()))?;
        if value.to_payload()? != bytes {
            return Err(ObjectError::Corruption(
                "noncanonical checkpoint fragment".into(),
            ));
        }
        Ok(value)
    }
    pub(crate) fn validate_external(&self, length: u32) -> Result<(), ObjectError> {
        self.validate(length as usize, true)
    }
    pub(crate) fn without_data(&self) -> Self {
        Self {
            version: self.version,
            checkpoint_id: self.checkpoint_id.clone(),
            kind: self.kind,
            ordinal: self.ordinal,
            data: Vec::new(),
        }
    }
}
