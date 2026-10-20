// SPDX-License-Identifier: Apache-2.0
pub(crate) fn validate_chunk_state(state: &PageState) -> Result<(), ObjectError> {
    if state.logical_length as usize > state.data.len() {
        return Err(ObjectError::Corruption(
            "chunk state logical length exceeds payload".to_string(),
        ));
    }
    if state.checksum() != etag(&state.data) {
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

/// A digest of every data fragment, in fragment order.
///
/// The fragments themselves are derived wherever they are needed; these are not, because a
/// digest recomputed from damaged bytes agrees with the damage. See `PageState`.
pub(crate) fn chunk_ec_fragment_digests_from_data(data: &[u8]) -> Vec<u64> {
    let shards = chunk_ec_fragment_count(data.len());
    let shard_len = chunk_ec_fragment_len(data.len());
    (0..shards)
        .map(|index| {
            let start = (index * shard_len).min(data.len());
            let end = ((index + 1) * shard_len).min(data.len());
            // ZERO-PADDED TO THE SHARD LENGTH, exactly as `chunk_ec_data_fragments` pads
            // and as the sidecar's XOR treats a short tail.
            //
            // NO INPUT DISTINGUISHES THIS, and that is worth saying rather than leaving it
            // to look load-bearing. The only reader of these digests compares a stored
            // table against one it computes with this same function, so padding or not
            // padding changes both sides together -- a mutation that removed the resize
            // went uncaught against a payload with a short tail, which is the case the
            // padding exists for. It stays for consistency with the two functions above,
            // so a future reader who does cross-compare a digest with a rebuilt fragment
            // finds them agreeing.
            let mut shard = data[start..end].to_vec();
            shard.resize(shard_len, 0);
            fnv1a64(&shard)
        })
        .collect()
}

/// Which fragments disagree with their stored digests, in fragment order.
pub(crate) fn chunk_ec_damaged_fragments(data: &[u8], digests: &[u64]) -> Vec<usize> {
    let measured = chunk_ec_fragment_digests_from_data(data);
    if measured.len() != digests.len() {
        // A digest table of the wrong width says nothing about which fragment is damaged,
        // so it names none rather than guessing -- the caller falls back to the search.
        return Vec::new();
    }
    measured
        .iter()
        .zip(digests.iter())
        .enumerate()
        .filter(|(_, (measured, stored))| measured != stored)
        .map(|(index, _)| index)
        .collect()
}

/// Drop the sidecar, for a replica that keeps none.
pub(crate) fn clear_chunk_ec_sidecars(state: &mut PageState) {
    if !state.fragment_bytes().is_empty() {
        state.ec_parity_fragment = None;
    }
    // The digests go with it. They are only useful beside the sidecar -- on their own they
    // can say a fragment is damaged and do nothing about it -- and a replica that keeps no
    // sidecar should pay for neither.
    if state.ec_fragment_digests.is_some() {
        state.ec_fragment_digests = None;
    }
}

/// Whether this chunk carries a sidecar at all.
///
/// An empty sidecar beside a non-empty payload is how absence is spelled, which
/// lets the free functions here tell "not kept" from "wrong" without reaching for
/// the service's policy. A payload that is itself empty legitimately has an empty
/// sidecar, hence the second arm.
pub(crate) fn chunk_keeps_ec_sidecar(state: &PageState) -> bool {
    !state.fragment_bytes().is_empty() || state.data.is_empty()
}

pub(crate) fn validate_chunk_ec_sidecars(state: &PageState) -> Result<(), ObjectError> {
    if !chunk_keeps_ec_sidecar(state) {
        return Ok(());
    }
    if state.fragment_bytes() != chunk_ec_parity_from_data(&state.data).as_slice() {
        return Err(ObjectError::Corruption(
            "chunk EC sidecar does not match payload".to_string(),
        ));
    }
    // The digests are checked only for SHAPE here, not against the payload. Validation runs
    // on a page whose own checksum has already matched, so the fragments necessarily agree
    // with digests taken from the same bytes, and comparing them would be a second way of
    // asking a question already answered. A table of the wrong width is a different thing:
    // it cannot locate anything, and a reader that trusted it would mis-name the damage.
    let digests = state.fragment_digests();
    if !digests.is_empty() && digests.len() != chunk_ec_fragment_count(state.data.len()) {
        return Err(ObjectError::Corruption(format!(
            "chunk EC digest table holds {} entries for a payload of {} fragments",
            digests.len(),
            chunk_ec_fragment_count(state.data.len())
        )));
    }
    Ok(())
}

/// Rebuild one fragment of `data` from the sidecar and the fragments that survived.
///
/// The caller says which fragment to rebuild. Nothing here checks the result; that is the
/// caller's job, and the two callers check it differently.
fn rebuild_fragment(
    data: &[u8],
    ec_parity_fragment: &[u8],
    damaged: usize,
    shards: usize,
    shard_len: usize,
    expected_len: usize,
) -> Vec<u8> {
    let mut candidate = data.to_vec();
    for offset in 0..shard_len {
        let mut byte = ec_parity_fragment[offset];
        for index in 0..shards {
            if index == damaged {
                continue;
            }
            if let Some(surviving) = data.get(index * shard_len + offset) {
                byte ^= surviving;
            }
        }
        let position = damaged * shard_len + offset;
        if position < candidate.len() {
            candidate[position] = byte;
        }
    }
    candidate.truncate(expected_len);
    candidate
}

/// Rebuild a payload whose own checksum fails, from the sidecar plus the fragments that
/// survived.
///
/// # THE DIGESTS TURN A SEARCH INTO A LOOKUP
///
/// `digests` holds one digest per fragment, taken when the page was written. With it the
/// damaged fragment is identified: one pass over `k` digests, one rebuild, one checksum.
/// Without it -- an older snapshot, or a replica that stored none -- this falls back to
/// what it has always done, assuming each fragment in turn is the damaged one and
/// accepting the first candidate that reproduces the page checksum: `k` rebuilds and `k`
/// page checksums at worst.
///
/// The fallback is kept rather than replaced because a missing digest table must not make
/// a page unrecoverable that was recoverable before.
///
/// # WHAT THE DIGESTS SAY THAT THE SEARCH CANNOT
///
/// The search can only report that nothing worked. The digests distinguish the cases, and
/// the refusals say which:
///
/// - **no fragment damaged** -- the payload is whole, so the damage is in the sidecar or in
///   the recorded checksum, and rebuilding a fragment would replace good bytes with bytes
///   derived from a bad sidecar;
/// - **one damaged** -- rebuild it;
/// - **several damaged** -- one unit of redundancy recovers one, and the refusal says how
///   many were found rather than that something unspecified failed.
pub(crate) fn chunk_ec_recover_data(
    data: &[u8],
    ec_parity_fragment: &[u8],
    digests: &[u64],
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

    if digests.len() == shards {
        let damaged = chunk_ec_damaged_fragments(data, digests);
        match damaged.as_slice() {
            [] => {
                return Err(ObjectError::Corruption(
                    "every chunk fragment matches its digest, so the payload is whole and \
                     the damage is in the sidecar or the recorded checksum"
                        .to_string(),
                ));
            }
            [one] => {
                let candidate = rebuild_fragment(
                    data,
                    ec_parity_fragment,
                    *one,
                    shards,
                    shard_len,
                    expected_len,
                );
                if etag_matches(&candidate, checksum) {
                    return Ok(candidate);
                }
                return Err(ObjectError::Corruption(format!(
                    "chunk fragment {one} was rebuilt from the sidecar and the page still \
                     does not match its checksum, so the sidecar is damaged too"
                )));
            }
            several => {
                return Err(ObjectError::Corruption(format!(
                    "{} chunk fragments are damaged and one unit of redundancy recovers one",
                    several.len()
                )));
            }
        }
    }

    for missing in 0..shards {
        let candidate = rebuild_fragment(
            data,
            ec_parity_fragment,
            missing,
            shards,
            shard_len,
            expected_len,
        );
        if etag_matches(&candidate, checksum) {
            return Ok(candidate);
        }
    }

    Err(ObjectError::Corruption(
        "chunk payload and EC sidecar cannot reproduce the recorded checksum".to_string(),
    ))
}

pub(crate) fn refresh_chunk_ec_sidecars(state: &mut PageState) {
    state.ec_parity_fragment =
        Some(std::sync::Arc::new(chunk_ec_parity_from_data(&state.data)));
    state.ec_fragment_digests = Some(std::sync::Arc::new(
        chunk_ec_fragment_digests_from_data(&state.data),
    ));
}
