// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The wire contract of the S3 door's append and resolve verbs.
//!
//! Everything here is spelled once and used by both ends of the connection. A gateway that
//! answered a different word than the client reads, or a client that sent a header the gateway
//! does not look for, is a defect the compiler cannot see when each side owns its own copy of
//! the string; with one copy it is a change to this file and to nothing else.

use std::fmt;

/// Bumped on any change a peer built against the previous value would misread.
pub const WIRE_REVISION: u32 = 1;

/// Offset a `PutObject` body is expected to land at, which turns the put into an append.
///
/// Spelled as S3 spells it: S3 Express One Zone takes this header on `PutObject` and appends when
/// it is present.
pub const WRITE_OFFSET_HEADER: &str = "x-amz-write-offset-bytes";

/// Names one attempt of an append: 32 hex characters (16 random bytes) chosen by the client once
/// per attempt, the same on every retry of that attempt, and echoed in the response.
pub const ATTEMPT_HEADER: &str = "x-amz-attempt-id";

/// The object's length after the request, or on a `HEAD` its length now.
pub const OBJECT_SIZE_HEADER: &str = "x-amz-object-size";

/// Carries the answer of `?append-resolve`: one of the [`Resolution`] words.
pub const RESOLUTION_HEADER: &str = "x-append-resolution";

/// On a 409 for an append whose outcome cannot be decided: the value is [`APPEND_STATE_AMBIGUOUS`].
pub const APPEND_STATE_HEADER: &str = "x-amz-append-state";

/// The value of [`APPEND_STATE_HEADER`] when equal bytes may be at the offset but nothing proves
/// they are this attempt's.
pub const APPEND_STATE_AMBIGUOUS: &str = "ambiguous";

/// The query parameter that turns a `POST` on an object into a resolve.
pub const RESOLVE_PARAM: &str = "append-resolve";

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// `text` as exactly `N` bytes of hex (upper or lower case), or `None`.
fn decode_hex<const N: usize>(text: &str) -> Option<[u8; N]> {
    let bytes = text.as_bytes();
    if bytes.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (index, pair) in bytes.chunks(2).enumerate() {
        out[index] = hex_digit(pair[0])? * 16 + hex_digit(pair[1])?;
    }
    Some(out)
}

/// Lower-case hex of `bytes`.
pub fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

/// The identity of one append attempt: 16 bytes, written as 32 hex characters.
///
/// A client makes one per attempt and reuses it on every retry of that attempt, so the gateway
/// can answer "did my append land" by the attempt and never by comparing bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AttemptId([u8; 16]);

impl AttemptId {
    /// An id from 16 bytes the caller drew from a source of randomness.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        AttemptId(bytes)
    }

    /// The id written as 32 hex characters, or `None` for anything else.
    pub fn parse(text: &str) -> Option<Self> {
        decode_hex::<16>(text).map(AttemptId)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// The 32 lower-case hex characters that travel in [`ATTEMPT_HEADER`].
    pub fn hex(&self) -> String {
        encode_hex(&self.0)
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex())
    }
}

/// What an append whose outcome was not known turned out to be.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Resolution {
    /// The append's bytes are at the offset it named.
    Landed,
    /// They are not, and the append can never land: retry it as a new append.
    Absent,
    /// Bytes equal to the append's are at that offset, but nothing proves they are THIS attempt's:
    /// another writer's identical append at the same offset looks the same. The caller decides
    /// from its own record of what it acknowledged; a gateway never answers "landed" for it.
    Ambiguous,
}

impl Resolution {
    /// The word carried in [`RESOLUTION_HEADER`] and in the response body.
    pub const fn word(self) -> &'static str {
        match self {
            Resolution::Landed => "landed",
            Resolution::Absent => "absent",
            Resolution::Ambiguous => "ambiguous",
        }
    }

    /// The resolution a word names, ignoring surrounding whitespace.
    pub fn from_word(word: &str) -> Option<Self> {
        match word.trim() {
            "landed" => Some(Resolution::Landed),
            "absent" => Some(Resolution::Absent),
            "ambiguous" => Some(Resolution::Ambiguous),
            _ => None,
        }
    }
}

/// `POST /bucket/key?append-resolve&attempt=<32 hex>&offset=O&length=N[&sha256=<hex>]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolveRequest {
    /// The attempt the append carried in [`ATTEMPT_HEADER`].
    pub attempt: AttemptId,
    /// The offset the append named.
    pub offset: u64,
    /// The number of bytes the append carried.
    pub length: u64,
    /// SHA-256 of the append's bytes: optional evidence, never the identity.
    pub sha256: Option<[u8; 32]>,
}

impl ResolveRequest {
    /// The query string, without a leading `?`.
    pub fn encode_query(&self) -> String {
        let mut query = format!(
            "{RESOLVE_PARAM}&attempt={}&offset={}&length={}",
            self.attempt, self.offset, self.length
        );
        if let Some(digest) = &self.sha256 {
            query.push_str("&sha256=");
            query.push_str(&encode_hex(digest));
        }
        query
    }

    /// Read the request from a query lookup (`name` to its decoded value). The error is the text
    /// a gateway answers a 400 with.
    pub fn parse(param: impl Fn(&str) -> Option<String>) -> Result<Self, &'static str> {
        let number = |name: &str| param(name).and_then(|value| value.parse::<u64>().ok());
        let (Some(offset), Some(length)) = (number("offset"), number("length")) else {
            return Err("append-resolve needs numeric offset and length parameters");
        };
        let Some(attempt) = param("attempt").and_then(|value| AttemptId::parse(&value)) else {
            return Err("append-resolve needs the attempt parameter: the 32 hex characters the append sent in x-amz-attempt-id");
        };
        let sha256 = match param("sha256") {
            None => None,
            Some(value) => match decode_hex::<32>(&value) {
                Some(digest) => Some(digest),
                None => return Err("append-resolve's sha256 parameter must be 64 hex characters"),
            },
        };
        Ok(ResolveRequest { attempt, offset, length, sha256 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(query: &'a str) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            query
                .split('&')
                .filter_map(|pair| pair.split_once('='))
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn the_header_and_query_names_are_the_ones_clients_already_send() {
        // Golden: a peer built before this crate spells these exactly so. Changing one is a wire
        // change and must bump WIRE_REVISION.
        assert_eq!(WRITE_OFFSET_HEADER, "x-amz-write-offset-bytes");
        assert_eq!(ATTEMPT_HEADER, "x-amz-attempt-id");
        assert_eq!(OBJECT_SIZE_HEADER, "x-amz-object-size");
        assert_eq!(RESOLUTION_HEADER, "x-append-resolution");
        assert_eq!(APPEND_STATE_HEADER, "x-amz-append-state");
        assert_eq!(APPEND_STATE_AMBIGUOUS, "ambiguous");
        assert_eq!(RESOLVE_PARAM, "append-resolve");
        assert_eq!(WIRE_REVISION, 1);
    }

    #[test]
    fn an_attempt_id_is_32_hex_characters_and_round_trips() {
        let id = AttemptId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0xab, 0xff]);
        assert_eq!(id.hex(), "000102030405060708090a0b0c0dabff");
        assert_eq!(id.to_string(), id.hex());
        assert_eq!(AttemptId::parse(&id.hex()), Some(id));
        assert_eq!(AttemptId::parse(&id.hex().to_uppercase()), Some(id));
    }

    #[test]
    fn an_attempt_id_of_any_other_shape_is_refused() {
        for bad in [
            "",
            "00",
            "000102030405060708090a0b0c0dabf",
            "000102030405060708090a0b0c0dabfff",
            "000102030405060708090a0b0c0dabgg",
            "000102030405060708090a0b0c0d+bff",
            // 32 BYTES long but not 32 characters: a multi-byte character must not slip through.
            "000102030405060708090a0b0c0dabé",
        ] {
            assert_eq!(AttemptId::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn each_resolution_word_round_trips_and_nothing_else_parses() {
        for resolution in [Resolution::Landed, Resolution::Absent, Resolution::Ambiguous] {
            assert_eq!(Resolution::from_word(resolution.word()), Some(resolution));
            assert_eq!(Resolution::from_word(&format!(" {}\n", resolution.word())), Some(resolution));
        }
        assert_eq!(Resolution::from_word("Landed"), None);
        assert_eq!(Resolution::from_word(""), None);
        assert_eq!(Resolution::Landed.word(), "landed");
        assert_eq!(Resolution::Absent.word(), "absent");
        assert_eq!(Resolution::Ambiguous.word(), "ambiguous");
    }

    #[test]
    fn a_resolve_request_is_written_in_the_form_the_gateway_documents() {
        let attempt = AttemptId::from_bytes([0x11; 16]);
        let request = ResolveRequest { attempt, offset: 7, length: 3, sha256: None };
        assert_eq!(
            request.encode_query(),
            "append-resolve&attempt=11111111111111111111111111111111&offset=7&length=3"
        );
        let with_digest = ResolveRequest { sha256: Some([0xcd; 32]), ..request };
        assert_eq!(
            with_digest.encode_query(),
            format!(
                "append-resolve&attempt=11111111111111111111111111111111&offset=7&length=3&sha256={}",
                "cd".repeat(32)
            )
        );
    }

    #[test]
    fn a_resolve_request_is_read_back_exactly_as_it_was_written() {
        let request = ResolveRequest {
            attempt: AttemptId::from_bytes([9; 16]),
            offset: u64::MAX,
            length: 0,
            sha256: Some([7; 32]),
        };
        assert_eq!(ResolveRequest::parse(lookup(&request.encode_query())), Ok(request));
        let bare = ResolveRequest { sha256: None, ..request };
        assert_eq!(ResolveRequest::parse(lookup(&bare.encode_query())), Ok(bare));
    }

    #[test]
    fn a_malformed_resolve_request_is_refused_with_a_reason() {
        let id = "11111111111111111111111111111111";
        let numbers = "append-resolve&attempt=11111111111111111111111111111111";
        assert!(ResolveRequest::parse(lookup("append-resolve")).unwrap_err().contains("offset and length"));
        assert!(ResolveRequest::parse(lookup(&format!("{numbers}&offset=x&length=3"))).unwrap_err().contains("offset and length"));
        assert!(ResolveRequest::parse(lookup(&format!("{numbers}&offset=4"))).unwrap_err().contains("offset and length"));
        assert!(ResolveRequest::parse(lookup("append-resolve&offset=4&length=3")).unwrap_err().contains("attempt"));
        assert!(ResolveRequest::parse(lookup("append-resolve&attempt=zz&offset=4&length=3")).unwrap_err().contains("attempt"));
        assert!(ResolveRequest::parse(lookup(&format!("append-resolve&attempt={id}&offset=4&length=3&sha256=zz")))
            .unwrap_err()
            .contains("sha256"));
    }
}
