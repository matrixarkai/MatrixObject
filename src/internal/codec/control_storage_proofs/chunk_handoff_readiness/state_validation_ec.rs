pub(crate) fn validate_chunk_state(state: &ChunkState) -> Result<(), ObjectError> {
    if state.logical_length as usize > state.data.len() {
        return Err(ObjectError::Corruption(
            "chunk state logical length exceeds payload".to_string(),
        ));
    }
    if state.checksum != etag(&state.data) {
        return Err(ObjectError::Corruption(
            "chunk state checksum mismatch".to_string(),
        ));
    }
    validate_chunk_ec_sidecars(state)?;
    if merge_range_slice(&state.discarded_ranges) != state.discarded_ranges {
        return Err(ObjectError::Corruption(
            "chunk state discard ranges are not normalized".to_string(),
        ));
    }
    for range in &state.discarded_ranges {
        if range.end()? > state.logical_length {
            return Err(ObjectError::Corruption(
                "chunk state discard range exceeds logical length".to_string(),
            ));
        }
    }
    Ok(())
}

/// How many data shards a payload of `len` bytes is split into.
///
/// The XOR sidecar costs `len / k`, so a larger `k` is cheaper: at `k = 8` it is
/// 12.5% of the payload instead of 50%. What a larger `k` gives up is the extent
/// of damage a single shard can absorb, so a shard is never allowed below
/// `CHUNK_EC_MIN_FRAGMENT_BYTES`. A small payload keeps `k = 2` and exactly the
/// recovery it has always had; a ~1 MiB chunk reaches `k = 8` with 128 KiB
/// shards, which is far above any page- or sector-sized corruption.
pub(crate) fn chunk_ec_fragment_count(len: usize) -> usize {
    if len == 0 {
        return CHUNK_EC_MIN_DATA_FRAGMENTS;
    }
    let by_size = len / CHUNK_EC_MIN_FRAGMENT_BYTES;
    if by_size < CHUNK_EC_MIN_DATA_FRAGMENTS {
        CHUNK_EC_MIN_DATA_FRAGMENTS
    } else if by_size > CHUNK_EC_MAX_DATA_FRAGMENTS {
        CHUNK_EC_MAX_DATA_FRAGMENTS
    } else {
        by_size
    }
}

/// Length of one shard, which is also the length of the sidecar.
pub(crate) fn chunk_ec_fragment_len(len: usize) -> usize {
    let shards = chunk_ec_fragment_count(len);
    len.div_ceil(shards)
}

/// The payload's shards, zero-padded to equal length.
///
/// Derived wherever they are needed rather than stored: keeping them cost a whole
/// extra copy of every chunk, per replica, for bytes the payload already holds.
/// Only the sidecar below is kept.
pub(crate) fn chunk_ec_data_fragments(data: &[u8]) -> Vec<std::sync::Arc<Vec<u8>>> {
    let shards = chunk_ec_fragment_count(data.len());
    let shard_len = chunk_ec_fragment_len(data.len());
    (0..shards)
        .map(|index| {
            let start = (index * shard_len).min(data.len());
            let end = ((index + 1) * shard_len).min(data.len());
            let mut shard = data[start..end].to_vec();
            shard.resize(shard_len, 0);
            std::sync::Arc::new(shard)
        })
        .collect()
}

/// XOR of every shard, computed straight off the payload.
///
/// This is the only part of the sidecar set a damaged payload cannot yield, so it
/// is the only part stored.
pub(crate) fn chunk_ec_parity_from_data(data: &[u8]) -> Vec<u8> {
    let shards = chunk_ec_fragment_count(data.len());
    let shard_len = chunk_ec_fragment_len(data.len());
    let mut sidecar = vec![0u8; shard_len];
    for index in 0..shards {
        let base = index * shard_len;
        for offset in 0..shard_len {
            if let Some(byte) = data.get(base + offset) {
                sidecar[offset] ^= byte;
            }
        }
    }
    sidecar
}

/// Drop the sidecar, for a replica that keeps none.
pub(crate) fn clear_chunk_ec_sidecars(state: &mut ChunkState) {
    if !state.ec_parity_fragment.is_empty() {
        state.ec_parity_fragment = std::sync::Arc::new(Vec::new());
    }
}

/// Whether this chunk carries a sidecar at all.
///
/// An empty sidecar beside a non-empty payload is how absence is spelled, which
/// lets the free functions here tell "not kept" from "wrong" without reaching for
/// the service's policy. A payload that is itself empty legitimately has an empty
/// sidecar, hence the second arm.
pub(crate) fn chunk_keeps_ec_sidecar(state: &ChunkState) -> bool {
    !state.ec_parity_fragment.is_empty() || state.data.is_empty()
}

pub(crate) fn validate_chunk_ec_sidecars(state: &ChunkState) -> Result<(), ObjectError> {
    if !chunk_keeps_ec_sidecar(state) {
        return Ok(());
    }
    if *state.ec_parity_fragment != chunk_ec_parity_from_data(&state.data) {
        return Err(ObjectError::Corruption(
            "chunk EC sidecar does not match payload".to_string(),
        ));
    }
    Ok(())
}

/// Rebuild a payload whose own checksum fails, from the sidecar plus the shards
/// that survived.
///
/// Which shard is damaged is unknown, so each is tried in turn and a candidate is
/// accepted only when it reproduces the recorded checksum. That recovers
/// single-shard damage from `len / k` bytes of sidecar instead of a whole mirror.
pub(crate) fn chunk_ec_recover_data(
    data: &[u8],
    ec_parity_fragment: &[u8],
    expected_len: usize,
    checksum: &str,
) -> Result<Vec<u8>, ObjectError> {
    if expected_len == 0 {
        return Ok(Vec::new());
    }
    let shards = chunk_ec_fragment_count(data.len());
    let shard_len = chunk_ec_fragment_len(data.len());
    if ec_parity_fragment.len() != shard_len {
        return Err(ObjectError::Corruption(
            "chunk EC sidecar length mismatch".to_string(),
        ));
    }

    for missing in 0..shards {
        let mut candidate = data.to_vec();
        for offset in 0..shard_len {
            let mut byte = ec_parity_fragment[offset];
            for index in 0..shards {
                if index == missing {
                    continue;
                }
                if let Some(surviving) = data.get(index * shard_len + offset) {
                    byte ^= surviving;
                }
            }
            let position = missing * shard_len + offset;
            if position < candidate.len() {
                candidate[position] = byte;
            }
        }
        candidate.truncate(expected_len);
        if etag_matches(&candidate, checksum) {
            return Ok(candidate);
        }
    }

    Err(ObjectError::Corruption(
        "chunk payload and EC sidecar cannot reproduce the recorded checksum".to_string(),
    ))
}

pub(crate) fn refresh_chunk_ec_sidecars(state: &mut ChunkState) {
    state.ec_parity_fragment = std::sync::Arc::new(chunk_ec_parity_from_data(&state.data));
}
