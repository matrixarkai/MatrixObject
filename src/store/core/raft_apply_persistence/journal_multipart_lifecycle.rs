// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn encoded_journal_optional_string_len(value: Option<&str>) -> usize {
        std::mem::size_of::<u64>() + value.map(encoded_string_len).unwrap_or_default()
    }

    fn encoded_journal_optional_u64_len(value: Option<u64>) -> usize {
        std::mem::size_of::<u64>() * if value.is_some() { 2 } else { 1 }
    }

    fn encoded_journal_optional_usize_len(value: Option<usize>) -> usize {
        std::mem::size_of::<u64>() * if value.is_some() { 2 } else { 1 }
    }

    fn append_multipart_create_journal_record(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        options: &PutOptions,
        create_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(upload_id)
                + encoded_string_len(bucket)
                + encoded_string_len(key)
                + encoded_string_len(content_type)
                + encoded_journal_put_options_len(options),
        );
        append_u64(&mut payload, 12);
        append_string(&mut payload, upload_id);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_string(&mut payload, content_type);
        append_u64(&mut payload, create_time_ms);
        append_journal_put_options(&mut payload, options);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_multipart_upload_part_journal_record(
        &self,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4 + encoded_string_len(upload_id) + data.len(),
        );
        append_u64(&mut payload, 13);
        append_string(&mut payload, upload_id);
        append_u64(&mut payload, part_number as u64);
        append_u64(&mut payload, update_time_ms);
        append_u64(&mut payload, data.len() as u64);
        payload.extend_from_slice(data);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_multipart_complete_journal_record(
        &self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<&str>,
        expected_checksum: Option<&str>,
        replica_count: Option<usize>,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(upload_id)
                + Self::encoded_journal_optional_u64_len(expected_size)
                + Self::encoded_journal_optional_string_len(expected_etag)
                + Self::encoded_journal_optional_string_len(expected_checksum)
                + Self::encoded_journal_optional_usize_len(replica_count),
        );
        append_u64(&mut payload, 14);
        append_string(&mut payload, upload_id);
        append_u64(&mut payload, update_time_ms);
        match expected_size {
            Some(expected_size) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, expected_size);
            }
            None => append_u64(&mut payload, 0),
        }
        match expected_etag {
            Some(expected_etag) => {
                append_u64(&mut payload, 1);
                append_string(&mut payload, expected_etag);
            }
            None => append_u64(&mut payload, 0),
        }
        match expected_checksum {
            Some(expected_checksum) => {
                append_u64(&mut payload, 1);
                append_string(&mut payload, expected_checksum);
            }
            None => append_u64(&mut payload, 0),
        }
        match replica_count {
            Some(replica_count) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, replica_count as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_multipart_abort_journal_record(
        &self,
        upload_id: &str,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2 + encoded_string_len(upload_id),
        );
        append_u64(&mut payload, 15);
        append_string(&mut payload, upload_id);
        append_u64(&mut payload, update_time_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_bucket_versioning_journal_record(
        &self,
        bucket: &str,
        enabled: bool,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() * 2 + encoded_string_len(bucket));
        append_u64(&mut payload, 16);
        append_string(&mut payload, bucket);
        append_u64(&mut payload, enabled as u64);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_lifecycle_apply_journal_record(
        &self,
        max_actions: usize,
        now_ms: u64,
        transitions_only: bool,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(std::mem::size_of::<u64>() * 4);
        append_u64(&mut payload, 22);
        append_u64(&mut payload, max_actions as u64);
        append_u64(&mut payload, now_ms);
        append_u64(&mut payload, transitions_only as u64);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_legacy_lifecycle_journal_record(
        &self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() * 3 + encoded_string_len(bucket));
        append_u64(&mut payload, 23);
        append_string(&mut payload, bucket);
        append_u64(&mut payload, noncurrent_versions_to_keep as u64);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_delete_object_version_journal_record(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + encoded_string_len(bucket)
                + encoded_string_len(key),
        );
        append_u64(&mut payload, 17);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_u64(&mut payload, version);
        append_u64(&mut payload, update_time_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

}
