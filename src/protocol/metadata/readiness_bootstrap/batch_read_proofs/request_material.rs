pub(crate) fn append_object_read_proof(material: &mut Vec<u8>, proof: &ObjectReadProof) {
    append_u64(material, proof.requested_read_index);
    append_u64(material, proof.consensus_read_index);
    append_u64(material, proof.metadata_commit_index);
    append_u64(material, proof.read_fence_read_index_lag);
    append_u64(material, proof.read_fence_applied_lag);
}

pub(crate) fn append_route_control_proof_material(
    material: &mut Vec<u8>,
    proof: &ChunkRouteControlProof,
) {
    append_u64(material, proof.control_receipt as u64);
    append_string(material, &proof.state_format);
    append_string(material, &proof.state_checksum);
    append_u64(material, proof.chunk_route_control_commit_index);
    append_u64(material, proof.chunk_route_control_next_log_index);
    append_u64(material, proof.write_quorum);
    append_u64(material, proof.read_quorum);
}

pub(crate) fn append_object_read_request_material(
    material: &mut Vec<u8>,
    request: &ObjectReadRequest,
) {
    append_string(material, &request.bucket);
    append_string(material, &request.key);
    match request.range.as_ref() {
        Some(range) => {
            append_u64(material, 1);
            append_u64(material, range.offset);
            append_u64(material, range.length);
        }
        None => append_u64(material, 0),
    }
    append_u64(material, request.version.unwrap_or_default());
    append_u64(material, request.version.is_some() as u64);
}

pub(crate) fn append_object_read_preconditions_material(
    material: &mut Vec<u8>,
    preconditions: &ObjectReadPreconditions,
) {
    append_string_list(material, &preconditions.if_match_etags);
    append_string_list(material, &preconditions.if_none_match_etags);
}

pub(crate) fn append_conditional_object_read_request_material(
    material: &mut Vec<u8>,
    request: &ConditionalObjectReadRequest,
) {
    append_string(material, &request.bucket);
    append_string(material, &request.key);
    match request.range.as_ref() {
        Some(range) => {
            append_u64(material, 1);
            append_u64(material, range.offset);
            append_u64(material, range.length);
        }
        None => append_u64(material, 0),
    }
    append_object_read_preconditions_material(material, &request.preconditions);
}

pub(crate) fn append_versioned_conditional_object_read_request_material(
    material: &mut Vec<u8>,
    request: &VersionedConditionalObjectReadRequest,
) {
    append_string(material, &request.bucket);
    append_string(material, &request.key);
    append_u64(material, request.version);
    match request.range.as_ref() {
        Some(range) => {
            append_u64(material, 1);
            append_u64(material, range.offset);
            append_u64(material, range.length);
        }
        None => append_u64(material, 0),
    }
    append_object_read_preconditions_material(material, &request.preconditions);
}

