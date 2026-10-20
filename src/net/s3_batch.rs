// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `DeleteObjects` -- the batch delete, and the one S3 answer where failure is not a status.
//!
//! # WHY THIS VERB IS SHAPED UNLIKE EVERY OTHER ONE HERE
//!
//! Every other request on this surface either worked or did not, and the status says which.
//! `DeleteObjects` names up to a thousand keys and each one can fail on its own, so S3 answers
//! **200 with a per-key report**: a `<Deleted>` for each key that went and an `<Error>` carrying
//! that key's own code for each that did not.
//!
//! That is not a quirk to work around, it is the contract a client depends on. A tool deleting a
//! prefix sends a batch, reads which keys remain, and retries those. Answering a single 4xx for a
//! batch where one key failed would make it retry the whole batch forever, and answering a flat
//! 200 would make it believe keys are gone that are not.
//!
//! # A KEY THAT WAS NOT THERE IS A SUCCESS
//!
//! Same reasoning as the single-object delete, and the same answer, because a client that retries
//! a batch it already completed must see its keys as gone rather than as failures. S3 agrees.
//!
//! # THE PARSER
//!
//! Strict, for the reasons given at the top of [`super::s3_multipart`]: one document, everything
//! else refused, no entity resolution, no document type declaration.

use super::s3::{xml_escape, S3ErrorCode};
use super::s3_multipart::xml_unescape;

/// What a `DeleteObjects` body asked for.
pub struct DeleteRequest {
    pub keys: Vec<String>,
    /// `<Quiet>true</Quiet>`: report only the failures.
    ///
    /// A batch of a thousand successful deletes is a thousand `<Deleted>` elements the client has
    /// no use for, which is what the flag exists to drop.
    pub quiet: bool,
}

/// Most keys one batch may name, which is S3's own limit.
pub const MAX_DELETE_KEYS: usize = 1_000;

/// Read a `DeleteObjects` body.
pub fn parse_delete_objects(body: &[u8]) -> Result<DeleteRequest, String> {
    let text = std::str::from_utf8(body)
        .map_err(|_| "the request body is not UTF-8 text".to_string())?;
    if text.contains("<!DOCTYPE") || text.contains("<!ENTITY") {
        return Err("the request body carries a document type declaration, which this endpoint \
                    does not accept"
            .to_string());
    }
    if !text.contains("<Delete") {
        return Err("the request body is not a Delete document".to_string());
    }
    // `<Quiet>` is read before the keys so a malformed value is refused rather than defaulting to
    // the loud form: a client that asked for quiet and got every success back would be handed a
    // body it is not prepared to read.
    let quiet = match element(text, "Quiet") {
        None => false,
        Some(raw) => match raw.trim() {
            "true" => true,
            "false" => false,
            other => return Err(format!("Quiet must be true or false, and is {other:?}")),
        },
    };

    let mut keys = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find("<Object>") {
        let after_open = &rest[open + "<Object>".len()..];
        let close = after_open
            .find("</Object>")
            .ok_or_else(|| "an <Object> element is not closed".to_string())?;
        let block = &after_open[..close];
        let key = element(block, "Key")
            .ok_or_else(|| "an <Object> element carries no <Key>".to_string())?;
        let key = xml_unescape(key);
        if key.is_empty() {
            return Err("an <Object> names an empty key".to_string());
        }
        if keys.len() == MAX_DELETE_KEYS {
            return Err(format!(
                "a batch may name at most {MAX_DELETE_KEYS} keys"
            ));
        }
        keys.push(key);
        rest = &after_open[close + "</Object>".len()..];
    }
    if keys.is_empty() {
        return Err("the delete list is empty".to_string());
    }
    Ok(DeleteRequest { keys, quiet })
}

fn element<'a>(block: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    Some(&block[start..end])
}

/// What happened to one key.
pub enum DeleteOutcome {
    Deleted(String),
    Failed {
        key: String,
        code: S3ErrorCode,
        message: String,
    },
}

/// A `DeleteResult` body.
///
/// `quiet` drops the successes and never the failures: a quiet batch that hid an error would
/// leave a client believing a key is gone when it is not, which is the one thing this answer
/// exists to tell it.
pub fn delete_result_xml(outcomes: &[DeleteOutcome], quiet: bool) -> String {
    let mut body = String::with_capacity(128 + outcomes.len() * 64);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<DeleteResult xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\">");
    for outcome in outcomes {
        match outcome {
            DeleteOutcome::Deleted(key) => {
                if !quiet {
                    body.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(key)));
                }
            }
            DeleteOutcome::Failed { key, code, message } => {
                body.push_str(&format!(
                    "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
                    xml_escape(key),
                    code.as_str(),
                    xml_escape(message)
                ));
            }
        }
    }
    body.push_str("</DeleteResult>\n");
    body
}

/// A `CopyObjectResult` body.
pub fn copy_object_result_xml(last_modified_ms: u64, etag: &str) -> String {
    let mut body = String::with_capacity(256);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<CopyObjectResult xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\">");
    body.push_str(&format!(
        "<LastModified>{}</LastModified>",
        super::s3::iso8601(last_modified_ms)
    ));
    let quoted = format!("\"{etag}\"");
    body.push_str(&format!("<ETag>{}</ETag>", xml_escape(&quoted)));
    body.push_str("</CopyObjectResult>\n");
    body
}

/// Split an `x-amz-copy-source` header into its bucket and key.
///
/// The header is `/bucket/key` or `bucket/key` -- both forms are sent in practice, and the leading
/// slash is optional in S3's own definition -- and the key may be percent-encoded, since it
/// travels in something shaped like a path.
pub fn parse_copy_source(header: &str) -> Result<(String, String), String> {
    let trimmed = header.trim();
    // A versionId suffix names a version this endpoint does not serve. Refused rather than
    // ignored: ignoring it would copy the CURRENT object while the client believes it asked for
    // an older one.
    if let Some(at) = trimmed.find("?versionId=") {
        return Err(format!(
            "x-amz-copy-source names a version ({}), and this endpoint copies the current object \
             only",
            &trimmed[at + "?versionId=".len()..]
        ));
    }
    let path = trimmed.strip_prefix('/').unwrap_or(trimmed);
    let (bucket, key) = path
        .split_once('/')
        .ok_or_else(|| "x-amz-copy-source must be /bucket/key".to_string())?;
    // DEFENCE IN DEPTH, and recorded as such because a mutation run proved it. The store's own
    // `validate_bucket` and `validate_key` refuse an empty half as well, so removing this changes
    // the message and not the outcome: `/b/` and `//k` are the only inputs that reach it, and
    // both are refused downstream with the same status and code.
    //
    // It stays for the message -- a client told its `x-amz-copy-source` is wrong looks at the
    // header, where one told "key must be 1..4096 bytes" looks at its destination path -- and
    // because refusing here costs no lock.
    if bucket.is_empty() || key.is_empty() {
        return Err("x-amz-copy-source must name both a bucket and a key".to_string());
    }
    // THE SAME DECODER THE REQUEST PATH GOES THROUGH, deliberately rather than a second one of
    // my own. A copy source names a key that was written through the path, so a decoder here that
    // differed from `http::percent_decode` by one edge case would copy a key the client never
    // asked for -- and the two would have no reason to be compared.
    Ok((
        super::http::percent_decode(bucket),
        super::http::percent_decode(key),
    ))
}

/// A `LocationConstraint` body.
///
/// # WHY IT IS EMPTY
///
/// S3 answers an empty `<LocationConstraint/>` for its own default region, and every client reads
/// that as "the endpoint you already have". This store has no notion of a region, so an empty
/// constraint is the only answer that is TRUE -- naming a region would send a client that honours
/// it to an endpoint that does not exist.
///
/// It is answered at all because SDKs call `GetBucketLocation` before their first request and
/// treat a failure as the bucket being unreachable.
pub fn location_constraint_xml() -> String {
    let mut body = String::with_capacity(128);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<LocationConstraint xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\"></LocationConstraint>\n");
    body
}

/// A `VersioningConfiguration` body.
///
/// # S3 HAS THREE STATES HERE AND THIS STORE HAS A BOOLEAN
///
/// S3 distinguishes `Enabled`, `Suspended`, and never-configured -- the last by omitting `<Status>`
/// altogether. This store keeps one flag, so `false` cannot say which of the other two it is.
///
/// Omitting `<Status>` is the honest answer for `false`: it is what S3 sends for a bucket whose
/// versioning was never configured, and it is the answer a client acts on identically either way
/// (neither state versions anything). What is lost is a client that specifically wants to know
/// whether versioning was once on and then suspended, and closing that needs the store to carry
/// the third state rather than this function to invent it.
pub fn versioning_configuration_xml(enabled: bool) -> String {
    let mut body = String::with_capacity(160);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<VersioningConfiguration xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\">");
    if enabled {
        body.push_str("<Status>Enabled</Status>");
    }
    body.push_str("</VersioningConfiguration>\n");
    body
}

/// Read a `VersioningConfiguration` body.
///
/// `Enabled` turns it on and `Suspended` turns it off. Anything else is refused rather than read
/// as off: a client that sent a status this store does not understand has asked for something,
/// and silently disabling versioning is the worst available interpretation of an unknown request.
pub fn parse_versioning_configuration(body: &[u8]) -> Result<bool, String> {
    let text = std::str::from_utf8(body)
        .map_err(|_| "the request body is not UTF-8 text".to_string())?;
    if text.contains("<!DOCTYPE") || text.contains("<!ENTITY") {
        return Err("the request body carries a document type declaration, which this endpoint \
                    does not accept"
            .to_string());
    }
    if !text.contains("<VersioningConfiguration") {
        return Err("the request body is not a VersioningConfiguration document".to_string());
    }
    match element(text, "Status").map(str::trim) {
        Some("Enabled") => Ok(true),
        Some("Suspended") => Ok(false),
        Some(other) => Err(format!(
            "Status must be Enabled or Suspended, and is {other:?}"
        )),
        None => Err("the configuration carries no <Status>".to_string()),
    }
}
