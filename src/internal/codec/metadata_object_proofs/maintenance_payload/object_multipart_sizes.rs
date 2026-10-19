// SPDX-License-Identifier: Apache-2.0
pub(crate) fn sort_metadata_maintenance_actions(actions: &mut [MetadataMaintenanceAction]) {
    actions.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| b.estimated_bytes.cmp(&a.estimated_bytes))
            .then_with(|| b.estimated_items.cmp(&a.estimated_items))
            .then_with(|| a.action.cmp(&b.action))
            .then_with(|| a.key.cmp(&b.key))
            .then_with(|| a.marker.cmp(&b.marker))
    });
}

pub(crate) fn object_metadata_payload_bytes(metadata: &ObjectMetadata) -> u64 {
    let user_metadata_bytes: usize = metadata
        .user_metadata
        .iter()
        .map(|(key, value)| key.len().saturating_add(value.len()))
        .sum();
    let tag_bytes: usize = metadata
        .tags
        .iter()
        .map(|(key, value)| key.len().saturating_add(value.len()))
        .sum();
    metadata
        .object_id
        .bucket
        .len()
        .saturating_add(metadata.object_id.key.len())
        .saturating_add(metadata.etag.len())
        .saturating_add(metadata.content_type.len())
        .saturating_add(user_metadata_bytes)
        .saturating_add(metadata.storage_class.len())
        .saturating_add(tag_bytes)
        .saturating_add(metadata.extents.len().saturating_mul(32))
        .saturating_add(96) as u64
}

pub(crate) fn multipart_upload_payload_bytes(upload: &MultipartUpload) -> u64 {
    let option_tag_bytes: usize = upload
        .options
        .tags
        .iter()
        .map(|(key, value)| key.len().saturating_add(value.len()))
        .sum();
    let part_metadata_bytes = upload.parts.len().saturating_mul(32);
    let staged_payload_bytes: usize = upload
        .part_payloads
        .values()
        .map(|payload| payload.len())
        .sum();
    upload
        .upload_id
        .len()
        .saturating_add(upload.bucket.len())
        .saturating_add(upload.key.len())
        .saturating_add(upload.content_type.len())
        .saturating_add(option_tag_bytes)
        .saturating_add(part_metadata_bytes)
        .saturating_add(staged_payload_bytes)
        .saturating_add(96) as u64
}

pub(crate) fn multipart_part_payload_bytes(upload: &MultipartUpload, part: &MultipartPart) -> u64 {
    let staged_payload_bytes = upload
        .part_payloads
        .get(&part.part_number)
        .map(|payload| payload.len())
        .unwrap_or(0);
    part.etag
        .len()
        .saturating_add(staged_payload_bytes)
        .saturating_add(32) as u64
}
