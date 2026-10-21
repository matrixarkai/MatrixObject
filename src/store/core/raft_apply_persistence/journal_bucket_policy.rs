// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn encoded_bucket_journal_optional_string_len(value: Option<&str>) -> usize {
        std::mem::size_of::<u64>() + value.map(encoded_string_len).unwrap_or_default()
    }

    fn encoded_bucket_journal_optional_u64_len(value: Option<u64>) -> usize {
        std::mem::size_of::<u64>() * if value.is_some() { 2 } else { 1 }
    }

    fn encoded_bucket_journal_optional_bool_len(value: Option<bool>) -> usize {
        std::mem::size_of::<u64>() * if value.is_some() { 2 } else { 1 }
    }

    fn encoded_bucket_journal_optional_usize_len(value: Option<usize>) -> usize {
        std::mem::size_of::<u64>() * if value.is_some() { 2 } else { 1 }
    }

    fn encoded_bucket_journal_string_map_len(values: &BTreeMap<String, String>) -> usize {
        std::mem::size_of::<u64>()
            + values
                .iter()
                .map(|(key, value)| encoded_string_len(key) + encoded_string_len(value))
                .sum::<usize>()
    }

    fn encoded_bucket_journal_string_list_len(values: &[String]) -> usize {
        std::mem::size_of::<u64>()
            + values.iter().map(|value| encoded_string_len(value)).sum::<usize>()
    }

    fn encoded_bucket_journal_delete_request_len(request: &ObjectDeleteRequest) -> usize {
        encoded_string_len(&request.bucket)
            + encoded_string_len(&request.key)
            + Self::encoded_bucket_journal_optional_string_len(request.if_match_etag.as_deref())
            + Self::encoded_bucket_journal_optional_u64_len(request.version)
    }

    fn encoded_bucket_journal_lifecycle_policy_len(policy: &BucketLifecyclePolicy) -> usize {
        std::mem::size_of::<u64>() * 6
            + encoded_string_len(policy.transition_storage_class.as_deref().unwrap_or(""))
            + encoded_string_len(policy.filter_tag_key.as_deref().unwrap_or(""))
            + encoded_string_len(policy.filter_tag_value.as_deref().unwrap_or(""))
            + encoded_string_len(policy.filter_prefix.as_deref().unwrap_or(""))
    }

    fn encoded_bucket_journal_cors_policy_len(policy: &BucketCorsPolicy) -> usize {
        Self::encoded_bucket_journal_string_list_len(&policy.allowed_origins)
            + Self::encoded_bucket_journal_string_list_len(&policy.allowed_methods)
            + Self::encoded_bucket_journal_string_list_len(&policy.allowed_headers)
            + Self::encoded_bucket_journal_string_list_len(&policy.expose_headers)
            + std::mem::size_of::<u64>()
    }

    fn append_create_bucket_journal_record(&self, bucket: &str) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() + encoded_string_len(bucket));
        append_u64(&mut payload, 1);
        append_string(&mut payload, bucket);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_delete_bucket_journal_record(
        &self,
        bucket: &str,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() * 2 + encoded_string_len(bucket));
        append_u64(&mut payload, 3);
        append_string(&mut payload, bucket);
        append_u64(&mut payload, update_time_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_delete_object_journal_record(
        &self,
        bucket: &str,
        key: &str,
        update_time_ms: u64,
        if_match_etag: Option<&str>,
        release_root_placement: bool,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + encoded_string_len(bucket)
                + encoded_string_len(key)
                + Self::encoded_bucket_journal_optional_string_len(if_match_etag),
        );
        append_u64(&mut payload, 5);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_u64(&mut payload, update_time_ms);
        match if_match_etag {
            Some(etag) => {
                append_u64(&mut payload, 1);
                append_string(&mut payload, etag);
            }
            None => append_u64(&mut payload, 0),
        }
        append_u64(&mut payload, release_root_placement as u64);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_batch_delete_journal_record(
        &self,
        requests: &[&ObjectDeleteRequest],
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() || requests.is_empty() {
            return Ok(());
        }
        let request_bytes = requests
            .iter()
            .map(|request| Self::encoded_bucket_journal_delete_request_len(request))
            .sum::<usize>();
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() * 5 + request_bytes);
        append_u64(&mut payload, 21);
        append_u64(&mut payload, max_items as u64);
        append_u64(&mut payload, release_root_placements as u64);
        append_u64(&mut payload, update_time_ms);
        append_u64(&mut payload, requests.len() as u64);
        for request in requests {
            append_object_delete_request(&mut payload, request);
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_set_object_tags_journal_record(
        &self,
        bucket: &str,
        key: &str,
        tags: &BTreeMap<String, String>,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(bucket)
                + encoded_string_len(key)
                + Self::encoded_bucket_journal_string_map_len(tags),
        );
        append_u64(&mut payload, 6);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_u64(&mut payload, update_time_ms);
        append_string_map(&mut payload, tags);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_object_lock_journal_record(
        &self,
        bucket: &str,
        key: &str,
        update_time_ms: u64,
        legal_hold: Option<bool>,
        retention_until_ms: Option<u64>,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(bucket)
                + encoded_string_len(key)
                + Self::encoded_bucket_journal_optional_bool_len(legal_hold)
                + Self::encoded_bucket_journal_optional_u64_len(retention_until_ms),
        );
        append_u64(&mut payload, 7);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_u64(&mut payload, update_time_ms);
        match legal_hold {
            Some(enabled) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, enabled as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        match retention_until_ms {
            Some(retention_until_ms) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, retention_until_ms);
            }
            None => append_u64(&mut payload, 0),
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_object_transition_journal_record(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        update_time_ms: u64,
        replica_count: Option<usize>,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(bucket)
                + encoded_string_len(key)
                + encoded_string_len(storage_class)
                + Self::encoded_bucket_journal_optional_usize_len(replica_count),
        );
        append_u64(&mut payload, 9);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_string(&mut payload, storage_class);
        append_u64(&mut payload, update_time_ms);
        match replica_count {
            Some(replica_count) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, replica_count as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_object_restore_journal_record(
        &self,
        bucket: &str,
        key: &str,
        restore_until_ms: u64,
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
        append_u64(&mut payload, 10);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_u64(&mut payload, restore_until_ms);
        append_u64(&mut payload, update_time_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_object_version_restore_journal_record(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
        replica_count: Option<usize>,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + encoded_string_len(bucket)
                + encoded_string_len(key)
                + Self::encoded_bucket_journal_optional_usize_len(replica_count),
        );
        append_u64(&mut payload, 11);
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_u64(&mut payload, version);
        append_u64(&mut payload, update_time_ms);
        match replica_count {
            Some(replica_count) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, replica_count as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_bucket_policy_journal_record(
        &self,
        bucket: &str,
        update_time_ms: u64,
        retain_for_ms: Option<u64>,
        quota: Option<&BucketQuota>,
        lifecycle: Option<&BucketLifecyclePolicy>,
        cors: Option<&BucketCorsPolicy>,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut estimated_size =
            std::mem::size_of::<u64>() * 6 + encoded_string_len(bucket);
        if retain_for_ms.is_some() {
            estimated_size += std::mem::size_of::<u64>();
        }
        if quota.is_some() {
            estimated_size += std::mem::size_of::<u64>() * 2;
        }
        if let Some(policy) = lifecycle {
            estimated_size += Self::encoded_bucket_journal_lifecycle_policy_len(policy);
        }
        if let Some(policy) = cors {
            estimated_size += Self::encoded_bucket_journal_cors_policy_len(policy);
        }
        let mut payload = Vec::with_capacity(estimated_size);
        append_u64(&mut payload, 8);
        append_string(&mut payload, bucket);
        append_u64(&mut payload, update_time_ms);
        match retain_for_ms {
            Some(retain_for_ms) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, retain_for_ms);
            }
            None => append_u64(&mut payload, 0),
        }
        match quota {
            Some(quota) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, quota.max_live_bytes);
                append_u64(&mut payload, quota.max_live_objects);
            }
            None => append_u64(&mut payload, 0),
        }
        match lifecycle {
            Some(policy) => {
                append_u64(&mut payload, 1);
                append_lifecycle_policy(&mut payload, policy);
            }
            None => append_u64(&mut payload, 0),
        }
        match cors {
            Some(policy) => {
                append_u64(&mut payload, 1);
                append_cors_policy(&mut payload, policy);
            }
            None => append_u64(&mut payload, 0),
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_bucket_policy_document_journal_record(
        &self,
        bucket: &str,
        update_time_ms: u64,
        policy: &BucketPolicyDocument,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(bucket)
                + encoded_string_len(&policy.document),
        );
        append_u64(&mut payload, 24);
        append_string(&mut payload, bucket);
        append_u64(&mut payload, update_time_ms);
        append_bucket_policy_document(&mut payload, policy);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

}
