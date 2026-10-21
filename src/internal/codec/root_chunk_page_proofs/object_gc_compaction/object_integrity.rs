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

pub(crate) fn normalize_expected_object_checksum(checksum: &str) -> Result<String, ObjectError> {
    let mut value = checksum.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Ok(String::new());
    }
    const PREFIX: &str = "fnv1a64:";
    if let Some(stripped) = value.strip_prefix(PREFIX) {
        value = stripped.to_string();
    }
    if value.len() != 16 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(ObjectError::InvalidArgument(
            "expected checksum must be fnv1a64 with 16 hex digits".to_string(),
        ));
    }
    Ok(format!("{PREFIX}{value}"))
}

pub(crate) fn verify_expected_object_checksum(
    expected_checksum: Option<&str>,
    data: &[u8],
) -> Result<(), ObjectError> {
    let Some(expected_checksum) = expected_checksum else {
        return Ok(());
    };
    let expected_checksum = normalize_expected_object_checksum(expected_checksum)?;
    if expected_checksum.is_empty() {
        return Ok(());
    }
    let actual_checksum = etag(data);
    if expected_checksum != actual_checksum {
        return Err(ObjectError::PreconditionFailed(
            "expected checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn normalize_entity_tag(tag: &str) -> String {
    let mut value = tag.trim();
    if value
        .as_bytes()
        .get(0..2)
        .map_or(false, |prefix| prefix.eq_ignore_ascii_case(b"w/"))
    {
        value = value.get(2..).unwrap_or_default().trim();
    }
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

pub(crate) fn entity_tag_list_matches(tags: &[String], etag: &str) -> bool {
    tags.iter().any(|tag| {
        let token = tag.trim();
        token == "*" || (!token.is_empty() && normalize_entity_tag(token) == etag)
    })
}

pub(crate) fn put_options_have_if_match(options: &PutOptions) -> bool {
    options.if_match_etag.is_some()
        || options
            .if_match_etags
            .iter()
            .any(|etag| !etag.trim().is_empty())
}

pub(crate) fn put_options_if_match_satisfied(options: &PutOptions, etag: &str) -> bool {
    if let Some(expected) = options.if_match_etag.as_deref() {
        if expected == "*" || normalize_entity_tag(expected) == etag {
            return true;
        }
    }
    entity_tag_list_matches(&options.if_match_etags, etag)
}

pub(crate) fn evaluate_object_read_preconditions(
    metadata: &ObjectMetadata,
    preconditions: &ObjectReadPreconditions,
) -> Result<bool, ObjectError> {
    if !preconditions.if_match_etags.is_empty()
        && !entity_tag_list_matches(&preconditions.if_match_etags, &metadata.etag())
    {
        return Err(ObjectError::PreconditionFailed(
            "if-match etag does not match current object".to_string(),
        ));
    }
    Ok(!preconditions.if_none_match_etags.is_empty()
        && entity_tag_list_matches(&preconditions.if_none_match_etags, &metadata.etag()))
}

pub(crate) fn fnv1a64(data: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(crate) fn stable_string_u64(value: &str) -> u64 {
    fnv1a64(value.as_bytes())
}
