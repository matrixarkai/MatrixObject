// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObjectId {
    pub bucket: SharedText,
    pub key: SharedText,
}

impl ObjectId {
    pub fn new(bucket: impl Into<SharedText>, key: impl Into<SharedText>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.to_string());
        fields.insert(format!("{field_prefix}key"), self.key.to_string());
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ChunkId {
    pub volume_id: u64,
    pub id: u64,
}

impl ChunkId {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.volume_id.to_string(),
        );
        fields.insert(format!("{field_prefix}segment_id"), self.id.to_string());
        fields
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataRange {
    pub offset: u64,
    pub length: u64,
}

impl DataRange {
    pub fn end(&self) -> Result<u64, ObjectError> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| ObjectError::InvalidArgument("range end overflows u64".to_string()))
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}offset"), self.offset.to_string());
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields
    }
}

pub(crate) fn validate_pipeline_range(
    operation: &str,
    offset: u64,
    length: u64,
) -> Result<u64, ObjectError> {
    if offset > PIPELINE_OFFSET_MAX {
        return Err(ObjectError::InvalidArgument(format!(
            "{operation} offset exceeds pipeline offset limit"
        )));
    }
    let end = offset.checked_add(length).ok_or_else(|| {
        ObjectError::InvalidArgument(format!("{operation} range offset plus length overflows"))
    })?;
    if length > 0 && end - 1 > PIPELINE_OFFSET_MAX {
        return Err(ObjectError::InvalidArgument(format!(
            "{operation} range end exceeds pipeline offset limit"
        )));
    }
    Ok(end)
}

pub(crate) fn validate_pipeline_data_range(
    operation: &str,
    range: &DataRange,
) -> Result<u64, ObjectError> {
    validate_pipeline_range(operation, range.offset, range.length)
}

