// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_optional_string(material: &mut Vec<u8>, value: &Option<String>) {
    append_u64(material, value.is_some() as u64);
    append_string(material, value.as_deref().unwrap_or_default());
}

fn listing_optional_string_encoded_len(value: &Option<String>) -> usize {
    std::mem::size_of::<u64>() + encoded_string_len(value.as_deref().unwrap_or_default())
}

fn listing_string_map_encoded_len(values: &BTreeMap<String, String>) -> usize {
    std::mem::size_of::<u64>()
        + values
            .iter()
            .map(|(key, value)| encoded_string_len(key) + encoded_string_len(value))
            .sum::<usize>()
}

fn listing_object_id_encoded_len(object_id: &ObjectId) -> usize {
    encoded_string_len(&object_id.bucket) + encoded_string_len(&object_id.key)
}

fn listing_object_metadata_encoded_len(metadata: &ObjectMetadata) -> usize {
    listing_object_id_encoded_len(&metadata.object_id)
        + encoded_string_len(&metadata.etag)
        + encoded_string_len(&metadata.content_type)
        + listing_string_map_encoded_len(&metadata.user_metadata)
        + encoded_string_len(&metadata.storage_class)
        + listing_string_map_encoded_len(&metadata.tags)
        + std::mem::size_of::<u64>() * (12 + metadata.extents.len() * 4)
}

fn listing_object_change_record_encoded_len(record: &ObjectChangeRecord) -> usize {
    encoded_string_len(&record.operation)
        + listing_object_id_encoded_len(&record.object_id)
        + encoded_string_len(&record.etag())
        + std::mem::size_of::<u64>() * 6
}

pub(crate) fn object_change_page_proof_checksum(
    after_sequence: u64,
    limit: usize,
    operation: &str,
    page: &ObjectChangePage,
) -> String {
    let records_len = page
        .records
        .iter()
        .map(listing_object_change_record_encoded_len)
        .sum::<usize>();
    let mut material = Vec::with_capacity(
        encoded_string_len(OBJECT_CHANGE_PAGE_PROOF_FORMAT)
            + encoded_string_len(operation)
            + std::mem::size_of::<u64>() * 18
            + records_len,
    );
    append_string(&mut material, OBJECT_CHANGE_PAGE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, after_sequence);
    append_u64(&mut material, limit as u64);
    append_u64(&mut material, page.next_sequence);
    append_u64(&mut material, page.retained_floor_sequence);
    append_u64(&mut material, page.cursor_compacted as u64);
    append_u64(&mut material, page.resync_required as u64);
    append_u64(&mut material, page.resync_from_sequence);
    append_u64(&mut material, page.truncated as u64);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.budget_limited as u64);
    append_object_read_proof(&mut material, &page.read_proof);
    append_u64(&mut material, page.records.len() as u64);
    for record in &page.records {
        append_object_change_record(&mut material, record);
    }
    etag(&material)
}

pub(crate) fn stamp_object_change_page_proof(
    after_sequence: u64,
    limit: usize,
    operation: &str,
    page: &mut ObjectChangePage,
    expected_object_change_page_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    page.object_change_page_proof_format = OBJECT_CHANGE_PAGE_PROOF_FORMAT.to_string();
    page.object_change_page_proof_checksum =
        object_change_page_proof_checksum(after_sequence, limit, operation, page);
    if let Some(expected) = expected_object_change_page_proof_checksum {
        if !expected.is_empty() && page.object_change_page_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} object change page proof checksum mismatch: expected {} got {}",
                operation, expected, page.object_change_page_proof_checksum
            )));
        }
        page.expected_object_change_page_proof_checksum = expected.to_string();
    }
    Ok(())
}

pub(crate) fn object_list_page_proof_checksum(operation: &str, page: &ObjectListPage) -> String {
    let common_prefixes_len = page
        .common_prefixes
        .iter()
        .map(|prefix| encoded_string_len(prefix))
        .sum::<usize>();
    let objects_len = page
        .objects
        .iter()
        .map(listing_object_metadata_encoded_len)
        .sum::<usize>();
    let mut material = Vec::with_capacity(
        encoded_string_len(OBJECT_LIST_PAGE_PROOF_FORMAT)
            + encoded_string_len(operation)
            + encoded_string_len(&page.bucket)
            + encoded_string_len(&page.prefix)
            + listing_optional_string_encoded_len(&page.delimiter)
            + listing_optional_string_encoded_len(&page.marker)
            + listing_optional_string_encoded_len(&page.next_marker)
            + std::mem::size_of::<u64>() * 16
            + common_prefixes_len
            + objects_len,
    );
    append_string(&mut material, OBJECT_LIST_PAGE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_string(&mut material, &page.bucket);
    append_string(&mut material, &page.prefix);
    append_optional_string(&mut material, &page.delimiter);
    append_optional_string(&mut material, &page.marker);
    append_optional_string(&mut material, &page.next_marker);
    append_u64(&mut material, page.scanned_objects);
    append_u64(&mut material, page.emitted_objects);
    append_u64(&mut material, page.emitted_common_prefixes);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.has_more as u64);
    append_object_read_proof(&mut material, &page.read_proof);
    append_u64(&mut material, page.common_prefixes.len() as u64);
    for common_prefix in &page.common_prefixes {
        append_string(&mut material, common_prefix);
    }
    append_u64(&mut material, page.objects.len() as u64);
    for object in &page.objects {
        append_metadata(&mut material, object);
    }
    etag(&material)
}

pub(crate) fn stamp_object_list_page_proof(
    operation: &str,
    page: &mut ObjectListPage,
    expected_object_list_page_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    page.object_list_page_proof_format = OBJECT_LIST_PAGE_PROOF_FORMAT.to_string();
    page.object_list_page_proof_checksum = object_list_page_proof_checksum(operation, page);
    if let Some(expected) = expected_object_list_page_proof_checksum {
        if !expected.is_empty() && page.object_list_page_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} object list page proof checksum mismatch: expected {} got {}",
                operation, expected, page.object_list_page_proof_checksum
            )));
        }
        page.expected_object_list_page_proof_checksum = expected.to_string();
    }
    Ok(())
}

pub(crate) fn object_version_page_proof_checksum(
    operation: &str,
    page: &ObjectVersionPage,
) -> String {
    let versions_len = page
        .versions
        .iter()
        .map(listing_object_metadata_encoded_len)
        .sum::<usize>();
    let mut material = Vec::with_capacity(
        encoded_string_len(OBJECT_LIST_PAGE_PROOF_FORMAT)
            + encoded_string_len(operation)
            + encoded_string_len(&page.bucket)
            + encoded_string_len(&page.key)
            + std::mem::size_of::<u64>() * 14
            + versions_len,
    );
    append_string(&mut material, OBJECT_LIST_PAGE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_string(&mut material, &page.bucket);
    append_string(&mut material, &page.key);
    append_u64(&mut material, page.marker.unwrap_or_default());
    append_u64(&mut material, page.next_version_marker.unwrap_or_default());
    append_u64(&mut material, page.requested_versions);
    append_u64(&mut material, page.emitted_versions);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.has_more as u64);
    append_object_read_proof(&mut material, &page.read_proof);
    append_u64(&mut material, page.versions.len() as u64);
    for version in &page.versions {
        append_metadata(&mut material, version);
    }
    etag(&material)
}

pub(crate) fn stamp_object_version_page_proof(
    operation: &str,
    page: &mut ObjectVersionPage,
    expected_object_list_page_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    page.object_list_page_proof_format = OBJECT_LIST_PAGE_PROOF_FORMAT.to_string();
    page.object_list_page_proof_checksum = object_version_page_proof_checksum(operation, page);
    if let Some(expected) = expected_object_list_page_proof_checksum {
        if !expected.is_empty() && page.object_list_page_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} object version page proof checksum mismatch: expected {} got {}",
                operation, expected, page.object_list_page_proof_checksum
            )));
        }
        page.expected_object_list_page_proof_checksum = expected.to_string();
    }
    Ok(())
}

pub(crate) fn bucket_object_version_page_proof_checksum(
    operation: &str,
    page: &BucketObjectVersionPage,
) -> String {
    let common_prefixes_len = page
        .common_prefixes
        .iter()
        .map(|prefix| encoded_string_len(prefix))
        .sum::<usize>();
    let versions_len = page
        .versions
        .iter()
        .map(listing_object_metadata_encoded_len)
        .sum::<usize>();
    let mut material = Vec::with_capacity(
        encoded_string_len(OBJECT_LIST_PAGE_PROOF_FORMAT)
            + encoded_string_len(operation)
            + encoded_string_len(&page.bucket)
            + encoded_string_len(&page.prefix)
            + listing_optional_string_encoded_len(&page.delimiter)
            + listing_optional_string_encoded_len(&page.key_marker)
            + listing_optional_string_encoded_len(&page.next_key_marker)
            + std::mem::size_of::<u64>() * 18
            + common_prefixes_len
            + versions_len,
    );
    append_string(&mut material, OBJECT_LIST_PAGE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_string(&mut material, &page.bucket);
    append_string(&mut material, &page.prefix);
    append_optional_string(&mut material, &page.delimiter);
    append_optional_string(&mut material, &page.key_marker);
    append_u64(&mut material, page.version_marker.unwrap_or_default());
    append_optional_string(&mut material, &page.next_key_marker);
    append_u64(&mut material, page.next_version_marker.unwrap_or_default());
    append_u64(&mut material, page.scanned_keys);
    append_u64(&mut material, page.emitted_versions);
    append_u64(&mut material, page.emitted_common_prefixes);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.has_more as u64);
    append_object_read_proof(&mut material, &page.read_proof);
    append_u64(&mut material, page.common_prefixes.len() as u64);
    for common_prefix in &page.common_prefixes {
        append_string(&mut material, common_prefix);
    }
    append_u64(&mut material, page.versions.len() as u64);
    for version in &page.versions {
        append_metadata(&mut material, version);
    }
    etag(&material)
}

pub(crate) fn stamp_bucket_object_version_page_proof(
    operation: &str,
    page: &mut BucketObjectVersionPage,
    expected_object_list_page_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    page.object_list_page_proof_format = OBJECT_LIST_PAGE_PROOF_FORMAT.to_string();
    page.object_list_page_proof_checksum =
        bucket_object_version_page_proof_checksum(operation, page);
    if let Some(expected) = expected_object_list_page_proof_checksum {
        if !expected.is_empty() && page.object_list_page_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} bucket object version page proof checksum mismatch: expected {} got {}",
                operation, expected, page.object_list_page_proof_checksum
            )));
        }
        page.expected_object_list_page_proof_checksum = expected.to_string();
    }
    Ok(())
}

pub(crate) fn object_tag_list_page_proof_checksum(operation: &str, page: &ObjectTagPage) -> String {
    let objects_len = page
        .objects
        .iter()
        .map(listing_object_metadata_encoded_len)
        .sum::<usize>();
    let mut material = Vec::with_capacity(
        encoded_string_len(OBJECT_LIST_PAGE_PROOF_FORMAT)
            + encoded_string_len(operation)
            + encoded_string_len(&page.bucket)
            + encoded_string_len(&page.tag_key)
            + encoded_string_len(&page.tag_value)
            + listing_optional_string_encoded_len(&page.marker)
            + listing_optional_string_encoded_len(&page.next_marker)
            + std::mem::size_of::<u64>() * 13
            + objects_len,
    );
    append_string(&mut material, OBJECT_LIST_PAGE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_string(&mut material, &page.bucket);
    append_string(&mut material, &page.tag_key);
    append_string(&mut material, &page.tag_value);
    append_optional_string(&mut material, &page.marker);
    append_optional_string(&mut material, &page.next_marker);
    append_u64(&mut material, page.scanned_objects);
    append_u64(&mut material, page.emitted_objects);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.has_more as u64);
    append_object_read_proof(&mut material, &page.read_proof);
    append_u64(&mut material, page.objects.len() as u64);
    for object in &page.objects {
        append_metadata(&mut material, object);
    }
    etag(&material)
}

pub(crate) fn stamp_object_tag_list_page_proof(
    operation: &str,
    page: &mut ObjectTagPage,
    expected_object_list_page_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    page.object_list_page_proof_format = OBJECT_LIST_PAGE_PROOF_FORMAT.to_string();
    page.object_list_page_proof_checksum = object_tag_list_page_proof_checksum(operation, page);
    if let Some(expected) = expected_object_list_page_proof_checksum {
        if !expected.is_empty() && page.object_list_page_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} tagged object list page proof checksum mismatch: expected {} got {}",
                operation, expected, page.object_list_page_proof_checksum
            )));
        }
        page.expected_object_list_page_proof_checksum = expected.to_string();
    }
    Ok(())
}
