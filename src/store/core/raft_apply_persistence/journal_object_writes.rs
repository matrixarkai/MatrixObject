// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn encoded_journal_string_map_len(values: &BTreeMap<String, String>) -> usize {
    std::mem::size_of::<u64>()
        + values
            .iter()
            .map(|(key, value)| encoded_string_len(key) + encoded_string_len(value))
            .sum::<usize>()
}

fn encoded_journal_put_options_len(options: &PutOptions) -> usize {
    let storage_class_len = std::mem::size_of::<u64>()
        + options
            .storage_class
            .as_deref()
            .map(encoded_string_len)
            .unwrap_or_default();
    std::mem::size_of::<u64>()
        + storage_class_len
        + encoded_journal_string_map_len(&options.user_metadata)
        + encoded_journal_string_map_len(&options.tags)
}

fn encoded_journal_optional_replica_count_len() -> usize {
    std::mem::size_of::<u64>() * 2
}

fn encoded_object_put_request_len(request: &ObjectPutRequest) -> usize {
    encoded_string_len(&request.bucket)
        + encoded_string_len(&request.key)
        + encoded_string_len(&request.content_type)
        + std::mem::size_of::<u64>()
        + request.data.len()
        + encoded_journal_put_options_len(&request.options)
}

fn encoded_object_copy_request_len(request: &ObjectCopyRequest) -> usize {
    encoded_string_len(&request.source_bucket)
        + encoded_string_len(&request.source_key)
        + std::mem::size_of::<u64>() * if request.source_version.is_some() { 2 } else { 1 }
        + encoded_string_len(&request.destination_bucket)
        + encoded_string_len(&request.destination_key)
        + std::mem::size_of::<u64>()
        + request
            .content_type
            .as_deref()
            .map(encoded_string_len)
            .unwrap_or_default()
        + encoded_journal_put_options_len(&request.options)
}

impl MatrixObjectStore {
    fn build_object_journal_payload(
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        update_time_ms: u64,
        options: &PutOptions,
        append: bool,
        root_replica_count: Option<usize>,
    ) -> Vec<u8> {
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4
                + encoded_string_len(bucket)
                + encoded_string_len(key)
                + encoded_string_len(content_type)
                + data.len()
                + encoded_journal_put_options_len(options)
                + encoded_journal_optional_replica_count_len(),
        );
        append_u64(&mut payload, if append { 2 } else { 4 });
        append_string(&mut payload, bucket);
        append_string(&mut payload, key);
        append_string(&mut payload, content_type);
        append_u64(&mut payload, update_time_ms);
        append_u64(&mut payload, data.len() as u64);
        payload.extend_from_slice(data);
        append_journal_put_options(&mut payload, options);
        match root_replica_count {
            Some(replica_count) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, replica_count as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        payload
    }

    fn append_object_journal_payload(&self, payload: &[u8]) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_batch_put_journal_record(
        &self,
        requests: &[ObjectPutRequest],
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        root_replica_count: Option<usize>,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() || requests.is_empty() {
            return Ok(());
        }
        let request_bytes = requests
            .iter()
            .map(encoded_object_put_request_len)
            .sum::<usize>();
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 7
                + request_bytes
                + encoded_journal_optional_replica_count_len(),
        );
        append_u64(&mut payload, 18);
        append_u64(&mut payload, max_items as u64);
        append_u64(&mut payload, max_payload_bytes);
        append_u64(&mut payload, update_time_ms);
        match root_replica_count {
            Some(replica_count) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, replica_count as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        append_u64(&mut payload, requests.len() as u64);
        for request in requests {
            append_object_put_request(&mut payload, request);
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_copy_journal_record(
        &self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<&str>,
        update_time_ms: u64,
        options: &PutOptions,
        root_replica_count: Option<usize>,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 7
                + encoded_string_len(source_bucket)
                + encoded_string_len(source_key)
                + encoded_string_len(destination_bucket)
                + encoded_string_len(destination_key)
                + content_type.map(encoded_string_len).unwrap_or_default()
                + encoded_journal_put_options_len(options)
                + encoded_journal_optional_replica_count_len(),
        );
        append_u64(&mut payload, 19);
        append_string(&mut payload, source_bucket);
        append_string(&mut payload, source_key);
        match source_version {
            Some(version) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, version);
            }
            None => append_u64(&mut payload, 0),
        }
        append_string(&mut payload, destination_bucket);
        append_string(&mut payload, destination_key);
        match content_type {
            Some(content_type) => {
                append_u64(&mut payload, 1);
                append_string(&mut payload, content_type);
            }
            None => append_u64(&mut payload, 0),
        }
        append_u64(&mut payload, update_time_ms);
        append_journal_put_options(&mut payload, options);
        match root_replica_count {
            Some(replica_count) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, replica_count as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_batch_copy_journal_record(
        &self,
        requests: &[&ObjectCopyRequest],
        max_items: usize,
        max_copied_bytes: u64,
        update_time_ms: u64,
        root_replica_count: Option<usize>,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() || requests.is_empty() {
            return Ok(());
        }
        let request_bytes = requests
            .iter()
            .map(|request| encoded_object_copy_request_len(request))
            .sum::<usize>();
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 7
                + request_bytes
                + encoded_journal_optional_replica_count_len(),
        );
        append_u64(&mut payload, 20);
        append_u64(&mut payload, max_items as u64);
        append_u64(&mut payload, max_copied_bytes);
        append_u64(&mut payload, update_time_ms);
        match root_replica_count {
            Some(replica_count) => {
                append_u64(&mut payload, 1);
                append_u64(&mut payload, replica_count as u64);
            }
            None => append_u64(&mut payload, 0),
        }
        append_u64(&mut payload, requests.len() as u64);
        for request in requests {
            append_object_copy_request(&mut payload, request);
        }
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

}
