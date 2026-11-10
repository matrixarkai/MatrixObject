// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Metadata-only copy with a source checked at the publication position.
use crate::{object_manifest::ObjectManifest, ObjectError};
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

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestCopyRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub source_bucket: String,
    #[prost(string, tag = "3")]
    pub source_key: String,
    #[prost(string, tag = "4")]
    pub bucket: String,
    #[prost(string, tag = "5")]
    pub key: String,
    #[prost(message, optional, tag = "6")]
    pub expected: Option<ObjectManifest>,
    #[prost(uint64, tag = "7")]
    pub max_bytes: u64,
    #[prost(uint64, tag = "8")]
    pub update_time_ms: u64,
}
impl ManifestCopyRequest {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.source_bucket)?;
        crate::validate_key(&self.source_key)?;
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.version != 1 {
            return Err(invalid("unsupported manifest copy version"));
        }
        if let Some(expected) = &self.expected {
            expected.validate()?;
            if expected.bucket != self.bucket || expected.key != self.key {
                return Err(invalid("copy expectation names another destination"));
            }
        }
        Ok(())
    }
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 1024 * 1024 + 16384 {
            return Err(invalid("copy request exceeds bound"));
        }
        let value = Self::decode(bytes).map_err(|_| invalid("invalid manifest copy protobuf"))?;
        value.validate()?;
        if value.encode_to_vec() != bytes {
            return Err(invalid("noncanonical manifest copy protobuf"));
        }
        Ok(value)
    }
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

#[derive(Debug)]
pub enum AtomicCopyFailure {
    Refused(ObjectError),
    Unconfirmed(ObjectError),
}
impl std::fmt::Display for AtomicCopyFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(formatter, "atomic copy was not admitted: {error}"),
            Self::Unconfirmed(error) => {
                write!(formatter, "atomic copy publication is unconfirmed: {error}")
            }
        }
    }
}
impl std::error::Error for AtomicCopyFailure {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn atomic_copy_codec_rejects_truncation_unknown_fields_and_wrong_destination() {
        let request = ManifestCopyRequest {
            version: 1,
            source_bucket: "source".into(),
            source_key: "key".into(),
            bucket: "target".into(),
            key: "key".into(),
            ..Default::default()
        };
        let bytes = request.encode_to_vec();
        assert_eq!(ManifestCopyRequest::from_payload(&bytes).unwrap(), request);
        for cut in 0..bytes.len() {
            assert!(ManifestCopyRequest::from_payload(&bytes[..cut]).is_err());
        }
        let mut unknown = bytes;
        unknown.extend_from_slice(&[0x48, 1]);
        assert!(ManifestCopyRequest::from_payload(&unknown).is_err());
        let mut wrong = request;
        wrong.expected = Some(ObjectManifest {
            format_version: 2,
            bucket: "other".into(),
            key: "key".into(),
            generation: 1,
            length: 0,
            extents: Vec::new(),
            deleted: false,
        });
        assert!(wrong.validate().is_err());
    }
}
