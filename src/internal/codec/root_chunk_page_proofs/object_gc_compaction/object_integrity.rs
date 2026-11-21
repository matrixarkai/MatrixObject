// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn etag(data: &[u8]) -> String {
    let mut checksum = String::with_capacity(24);
    checksum.push_str("fnv1a64:");
    append_hex64(&mut checksum, fnv1a64(data));
    checksum
}

pub(crate) fn append_etag(out: &mut String, data: &[u8]) {
    out.push_str("fnv1a64:");
    append_hex64(out, fnv1a64(data));
}

/// The same spelling as [`append_etag`], for a digest already in hand.
pub(crate) fn append_etag_of_digest(out: &mut String, digest: u64) {
    out.push_str("fnv1a64:");
    append_hex64(out, digest);
}

pub(crate) fn etag_matches(data: &[u8], expected: &str) -> bool {
    const PREFIX: &str = "fnv1a64:";
    let Some(hex) = expected.strip_prefix(PREFIX) else {
        return false;
    };
    if hex.len() != 16 {
        return false;
    }
    let mut value = 0u64;
    for byte in hex.bytes() {
        let Some(digit) = byte.to_ascii_lowercase().checked_sub(b'0') else {
            return false;
        };
        let nibble = match byte {
            b'0'..=b'9' => u64::from(digit),
            b'a'..=b'f' => u64::from(byte - b'a' + 10),
            b'A'..=b'F' => u64::from(byte - b'A' + 10),
            _ => return false,
        };
        value = (value << 4) | nibble;
    }
    fnv1a64(data) == value
}

fn append_hex64(out: &mut String, mut value: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut digits = [b'0'; 16];
    for index in (0..16).rev() {
        digits[index] = HEX[(value & 0xf) as usize];
        value >>= 4;
    }
    out.push_str(std::str::from_utf8(&digits).expect("hex digits are valid utf-8"));
}

pub(crate) const FNV1A64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A64_PRIME: u64 = 0x100000001b3;

/// Read back what [`append_etag_of_digest`] spelled.
///
/// `None` for anything else, which is how a durable decoder tells a corrupt
/// value from a digest rather than silently reading one as zero. A spelling
/// with a writer and no reader is a one-way door, and this one now opens both
/// ways.
pub(crate) fn parse_etag_digest(spelled: &str) -> Option<u64> {
    let hex = spelled.strip_prefix("fnv1a64:")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

pub(crate) fn fnv1a64(data: &[u8]) -> u64 {
    fnv1a64_resumed(FNV1A64_OFFSET_BASIS, data)
}

/// FNV-1a over `data`, starting from a digest already computed over the bytes
/// that precede it.
///
/// # WHY A DIGEST IS A RESUMABLE STATE HERE
///
/// FNV-1a keeps no state beyond the 64 bits it returns: each byte is
/// `hash ^= byte; hash *= prime`. So the digest of a prefix *is* the
/// hash state after that prefix, and
/// `fnv1a64_resumed(fnv1a64(a), b) == fnv1a64(a ++ b)` exactly -- not an
/// approximation of it. That identity is what lets an append produce the whole
/// object's digest without reading the whole object back, and it is asserted
/// directly rather than left as a comment, because it is the only thing
/// standing between the append path and a silently wrong etag.
pub(crate) fn fnv1a64_resumed(mut hash: u64, data: &[u8]) -> u64 {
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV1A64_PRIME);
    }
    hash
}

/// FNV-1a over `count` zero bytes, starting from a digest already computed, in O(log count).
///
/// A zero byte leaves the hash unchanged under the xor and then multiplies it by the prime, so
/// `count` of them multiply it by `prime^count` modulo 2^64. The result is exactly what hashing
/// the zeros one at a time gives, and a test pins that identity.
pub(crate) fn fnv1a64_zero_run(mut hash: u64, count: usize) -> u64 {
    let mut left = count;
    while left > 0 {
        let step = left.min(u32::MAX as usize);
        hash = hash.wrapping_mul(FNV1A64_PRIME.wrapping_pow(step as u32));
        left -= step;
    }
    hash
}
