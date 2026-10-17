pub(crate) fn object_metadata_batch_read_proof_checksum(
    operation: &str,
    batch: &ObjectMetadataBatchRead,
    proof: &ObjectReadProof,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, METADATA_BATCH_READ_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, batch.requested_objects);
    append_u64(&mut material, batch.found_objects);
    append_u64(&mut material, batch.missing_objects);
    append_u64(&mut material, batch.payload_bytes);
    append_u64(
        &mut material,
        batch
            .expected_metadata_shard_topology_epoch
            .unwrap_or_default(),
    );
    append_u64(
        &mut material,
        batch.expected_metadata_shard_topology_epoch.is_some() as u64,
    );
    append_string(
        &mut material,
        &batch.expected_metadata_shard_topology_checksum,
    );
    append_object_read_proof(&mut material, proof);
    append_u64(&mut material, batch.objects.len() as u64);
    for metadata in &batch.objects {
        append_metadata(&mut material, metadata);
    }
    append_u64(&mut material, batch.missing.len() as u64);
    for object_id in &batch.missing {
        append_object_id(&mut material, object_id);
    }
    etag(&material)
}

pub(crate) fn conditional_object_metadata_batch_read_proof_checksum(
    operation: &str,
    batch: &ConditionalObjectMetadataBatchRead,
    proof: &ObjectReadProof,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, METADATA_BATCH_READ_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, batch.requested_objects);
    append_u64(&mut material, batch.found_objects);
    append_u64(&mut material, batch.not_modified_objects);
    append_u64(&mut material, batch.missing_objects);
    append_u64(&mut material, batch.failed_objects);
    append_u64(&mut material, batch.payload_bytes);
    append_object_read_proof(&mut material, proof);
    append_u64(&mut material, batch.items.len() as u64);
    for item in &batch.items {
        append_string(&mut material, &item.request.bucket);
        append_string(&mut material, &item.request.key);
        append_u64(&mut material, item.request.version.unwrap_or_default());
        append_u64(&mut material, item.request.version.is_some() as u64);
        append_u64(
            &mut material,
            item.request.preconditions.if_match_etags.len() as u64,
        );
        for etag in &item.request.preconditions.if_match_etags {
            append_string(&mut material, etag);
        }
        append_u64(
            &mut material,
            item.request.preconditions.if_none_match_etags.len() as u64,
        );
        for etag in &item.request.preconditions.if_none_match_etags {
            append_string(&mut material, etag);
        }
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        if let Some(metadata) = item.metadata.as_ref() {
            append_u64(&mut material, 1);
            append_u64(&mut material, metadata.not_modified as u64);
            append_metadata(&mut material, &metadata.metadata);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}
