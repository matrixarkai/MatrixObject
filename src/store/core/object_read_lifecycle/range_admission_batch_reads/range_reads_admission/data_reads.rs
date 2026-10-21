// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn get_object_metadata_range_with_bounded_read_admission(
        &self,
        metadata: ObjectMetadata,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<AdmittedObjectData, ObjectError> {
        check_payload_read_allowed(&metadata, now_millis())?;
        let admission = self.can_accept_object_metadata_range_read_bounded(
            &metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        if !admission.can_accept {
            return Ok(AdmittedObjectData {
                object: None,
                admission,
            });
        }
        let data = self.read_extents_bounded_by_chunks(
            &metadata.extents,
            offset,
            admission.selected_bytes,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok(AdmittedObjectData {
            object: Some(ObjectData { metadata, data }),
            admission,
        })
    }

    fn get_object_metadata_range_conditional_with_bounded_read_admission(
        &self,
        metadata: ObjectMetadata,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        if not_modified {
            return Ok(AdmittedConditionalObjectData {
                admission: self.not_modified_object_range_read_admission(
                    &metadata,
                    offset,
                    length,
                    max_payload_bytes,
                ),
                object: Some(ConditionalObjectData {
                    metadata,
                    data: None,
                    not_modified: true,
                }),
            });
        }
        check_payload_read_allowed(&metadata, now_millis())?;
        let admission = self.can_accept_object_metadata_range_read_bounded(
            &metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        if !admission.can_accept {
            return Ok(AdmittedConditionalObjectData {
                object: None,
                admission,
            });
        }
        let data = self.read_extents_bounded_by_chunks(
            &metadata.extents,
            offset,
            admission.selected_bytes,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok(AdmittedConditionalObjectData {
            object: Some(ConditionalObjectData {
                metadata,
                data: Some(data),
                not_modified: false,
            }),
            admission,
        })
    }

    pub fn get_object_range_with_read_stats(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ObjectData, ChunkReadStats), ObjectError> {
        self.get_object_range_with_read_stats_bounded(bucket, key, offset, length, 0)
    }

    pub fn get_object_range_with_read_stats_bounded(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<(ObjectData, ChunkReadStats), ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let (data, stats) = self.read_extents_with_stats_bounded(
            &metadata.extents,
            offset,
            bounded,
            max_payload_bytes,
        )?;
        Ok((ObjectData { metadata, data }, stats))
    }

}
