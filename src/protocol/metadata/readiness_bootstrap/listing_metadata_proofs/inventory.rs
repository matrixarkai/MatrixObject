// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_object_placement(material: &mut Vec<u8>, placement: &ObjectPlacement) {
    append_object_id(material, &placement.object_id);
    append_u64(material, placement.version);
    append_u64(material, placement.length);
    append_u64(material, placement.update_time_ms);
    append_u64(material, placement.node_ids.len() as u64);
    for node_id in &placement.node_ids {
        append_string(material, node_id);
    }
}

fn listing_object_placement_encoded_len(placement: &ObjectPlacement) -> usize {
    listing_object_id_encoded_len(&placement.object_id)
        + std::mem::size_of::<u64>() * 4
        + placement
            .node_ids
            .iter()
            .map(|node_id| encoded_string_len(node_id))
            .sum::<usize>()
}

fn listing_object_inventory_record_encoded_len(record: &ObjectInventoryRecord) -> usize {
    listing_object_metadata_encoded_len(&record.metadata)
        + std::mem::size_of::<u64>()
        + record
            .placement
            .as_ref()
            .map(listing_object_placement_encoded_len)
            .unwrap_or(0)
}

pub(crate) fn append_object_inventory_record(
    material: &mut Vec<u8>,
    record: &ObjectInventoryRecord,
) {
    append_metadata(material, &record.metadata);
    append_u64(material, record.placement.is_some() as u64);
    if let Some(placement) = &record.placement {
        append_object_placement(material, placement);
    }
}

pub(crate) fn object_inventory_page_proof_checksum(
    operation: &str,
    page: &ObjectInventoryPage,
) -> String {
    let records_len = page
        .records
        .iter()
        .map(listing_object_inventory_record_encoded_len)
        .sum::<usize>();
    let mut material = Vec::with_capacity(
        encoded_string_len(OBJECT_LIST_PAGE_PROOF_FORMAT)
            + encoded_string_len(operation)
            + encoded_string_len(&page.bucket)
            + encoded_string_len(&page.prefix)
            + listing_optional_string_encoded_len(&page.marker)
            + listing_optional_string_encoded_len(&page.next_marker)
            + std::mem::size_of::<u64>() * 11
            + records_len,
    );
    append_string(&mut material, OBJECT_LIST_PAGE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_string(&mut material, &page.bucket);
    append_string(&mut material, &page.prefix);
    append_optional_string(&mut material, &page.marker);
    append_optional_string(&mut material, &page.next_marker);
    append_u64(&mut material, page.listed_objects);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.has_more as u64);
    append_object_read_proof(&mut material, &page.read_proof);
    append_u64(&mut material, page.records.len() as u64);
    for record in &page.records {
        append_object_inventory_record(&mut material, record);
    }
    etag(&material)
}

pub(crate) fn stamp_object_inventory_page_proof(
    operation: &str,
    page: &mut ObjectInventoryPage,
    expected_object_list_page_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    page.object_list_page_proof_format = OBJECT_LIST_PAGE_PROOF_FORMAT.to_string();
    page.object_list_page_proof_checksum = object_inventory_page_proof_checksum(operation, page);
    if let Some(expected) = expected_object_list_page_proof_checksum {
        if !expected.is_empty() && page.object_list_page_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} object inventory page proof checksum mismatch: expected {} got {}",
                operation, expected, page.object_list_page_proof_checksum
            )));
        }
        page.expected_object_list_page_proof_checksum = expected.to_string();
    }
    Ok(())
}
