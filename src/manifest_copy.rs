// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Metadata-only copy with a source checked at the publication position.
use crate::ObjectError;
use prost::Message;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct LiveCopySource {
    #[prost(string, tag = "1")]
    pub bucket: String,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(uint64, tag = "3")]
    pub generation: u64,
}
impl LiveCopySource {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.generation == 0 {
            return Err(invalid("copy source generation is zero"));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
