// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The S3 wire format: error codes, XML bodies and timestamps.
//!
//! # WHY THIS IS A SEPARATE MODULE FROM THE ROUTING
//!
//! What makes a surface S3-compatible is not the verbs -- this store already had PUT, GET, HEAD
//! and DELETE on `/{bucket}/{key}` -- it is what a client can PARSE. An S3 client reads an
//! `<Error>` document to learn whether to retry, reads `<ListBucketResult>` to page a listing,
//! and reads `<Code>` to tell a missing bucket from a missing key. A surface that answers the
//! right status with a plain-text body is one an S3 client reports as an unknown failure.
//!
//! So the wire format lives here, on its own, and [`super::service`] routes onto it. The two
//! halves fail separately: a routing mistake is a wrong status, a format mistake is a body no
//! client can read, and keeping them apart lets a test say which happened.
//!
//! # WHAT IS DELIBERATELY NOT HERE
//!
//! Request signing. SigV4 is a separate concern from the wire format and is not implemented, so
//! this surface stays unauthenticated and the module serving it says so at its top. An
//! `Authorization` header is accepted and IGNORED rather than refused, because refusing it would
//! fail every signing client at the door for a reason unrelated to what it asked for.

use super::http::{civil_parts, Response};
use crate::ObjectError;

/// The XML namespace every S3 response body carries.
pub const S3_XML_NAMESPACE: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// The storage class this store reports for an object that names none.
pub const DEFAULT_STORAGE_CLASS: &str = "STANDARD";

/// An S3 error code, which is the part of an error body a client branches on.
///
/// The names are S3's, not this store's, because a client matches them literally: `NoSuchKey` is
/// how a client learns to stop retrying, and anything else -- including a perfectly clear English
/// sentence -- reads as an unknown failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3ErrorCode {
    NoSuchBucket,
    NoSuchKey,
    InvalidArgument,
    InvalidRange,
    PreconditionFailed,
    BucketNotEmpty,
    NoSuchUpload,
    InvalidPart,
    InvalidPartOrder,
    MalformedXml,
    MethodNotAllowed,
    NotImplemented,
    InternalError,
    ServiceUnavailable,
}

impl S3ErrorCode {
    /// The code as a client reads it.
    pub fn as_str(self) -> &'static str {
        match self {
            S3ErrorCode::NoSuchBucket => "NoSuchBucket",
            S3ErrorCode::NoSuchKey => "NoSuchKey",
            S3ErrorCode::InvalidArgument => "InvalidArgument",
            S3ErrorCode::InvalidRange => "InvalidRange",
            S3ErrorCode::PreconditionFailed => "PreconditionFailed",
            S3ErrorCode::BucketNotEmpty => "BucketNotEmpty",
            S3ErrorCode::NoSuchUpload => "NoSuchUpload",
            S3ErrorCode::InvalidPart => "InvalidPart",
            S3ErrorCode::InvalidPartOrder => "InvalidPartOrder",
            // S3 spells it MalformedXML, with the acronym in capitals. The variant is Rust
            // cased and the WIRE spelling is what a client matches, so the two differ here
            // on purpose.
            S3ErrorCode::MalformedXml => "MalformedXML",
            S3ErrorCode::MethodNotAllowed => "MethodNotAllowed",
            S3ErrorCode::NotImplemented => "NotImplemented",
            S3ErrorCode::InternalError => "InternalError",
            S3ErrorCode::ServiceUnavailable => "ServiceUnavailable",
        }
    }

    /// The status S3 answers this code with.
    ///
    /// Pinned to the code rather than chosen at each call site: a client that sees a `NoSuchKey`
    /// body under a 500 has been told two different things about whether to retry.
    pub fn status(self) -> u16 {
        match self {
            S3ErrorCode::NoSuchBucket | S3ErrorCode::NoSuchKey => 404,
            S3ErrorCode::InvalidArgument => 400,
            S3ErrorCode::InvalidRange => 416,
            S3ErrorCode::PreconditionFailed => 412,
            S3ErrorCode::BucketNotEmpty => 409,
            S3ErrorCode::NoSuchUpload => 404,
            S3ErrorCode::InvalidPart
            | S3ErrorCode::InvalidPartOrder
            | S3ErrorCode::MalformedXml => 400,
            S3ErrorCode::MethodNotAllowed => 405,
            S3ErrorCode::NotImplemented => 501,
            S3ErrorCode::InternalError => 500,
            S3ErrorCode::ServiceUnavailable => 503,
        }
    }
}

/// Escape text for an XML body.
///
/// Object keys are caller-supplied and may hold any of these, so a listing that does not escape
/// answers a document the client cannot parse -- and an ampersand alone is enough to do it.
pub fn xml_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Format epoch milliseconds the way S3 dates an object in a listing.
///
/// ISO 8601 with milliseconds and a literal Z. `Last-Modified` on a GET is the HTTP date instead;
/// the two are not interchangeable and a client parses each in its own place.
pub fn iso8601(epoch_ms: u64) -> String {
    let parts = civil_parts(epoch_ms);
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z",
        year = parts.year,
        month = parts.month,
        day = parts.day,
        hour = parts.hour,
        minute = parts.minute,
        second = parts.second,
        millis = epoch_ms % 1_000
    )
}

/// An S3 `<Error>` body, with the status its code pins.
///
/// `resource` is the path the request named, which is what S3 reports and what a client logs.
pub fn error_response(code: S3ErrorCode, message: &str, resource: &str) -> Response {
    let mut body = String::with_capacity(256);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<Error><Code>");
    body.push_str(code.as_str());
    body.push_str("</Code><Message>");
    body.push_str(&xml_escape(message));
    body.push_str("</Message><Resource>");
    body.push_str(&xml_escape(resource));
    body.push_str("</Resource><RequestId></RequestId></Error>\n");
    Response {
        status: code.status(),
        headers: vec![("content-type".to_string(), "application/xml".to_string())],
        body: body.into_bytes(),
    }
}

/// One object in a listing.
pub struct ListEntry {
    pub key: String,
    pub last_modified_ms: u64,
    pub etag: String,
    pub size: u64,
    pub storage_class: String,
}

/// What a listing answered, before it becomes XML.
pub struct ListResult {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub max_keys: usize,
    pub is_truncated: bool,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
    pub start_after: Option<String>,
    pub entries: Vec<ListEntry>,
    pub common_prefixes: Vec<String>,
}

/// A `ListObjectsV2` body.
///
/// `KeyCount` is the entries plus the common prefixes, which is what S3 reports and NOT the same
/// as the entry count: a listing rolled up by a delimiter can answer prefixes and no keys at all,
/// and a client paging on the entry count would stop early.
pub fn list_objects_v2_xml(result: &ListResult) -> String {
    let mut body = String::with_capacity(512 + result.entries.len() * 200);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<ListBucketResult xmlns=\"");
    body.push_str(S3_XML_NAMESPACE);
    body.push_str("\">");
    body.push_str(&format!("<Name>{}</Name>", xml_escape(&result.bucket)));
    body.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(&result.prefix)));
    if let Some(delimiter) = &result.delimiter {
        body.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(delimiter)));
    }
    body.push_str(&format!("<MaxKeys>{}</MaxKeys>", result.max_keys));
    body.push_str(&format!(
        "<KeyCount>{}</KeyCount>",
        result.entries.len() + result.common_prefixes.len()
    ));
    body.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    if let Some(token) = &result.continuation_token {
        body.push_str(&format!(
            "<ContinuationToken>{}</ContinuationToken>",
            xml_escape(token)
        ));
    }
    if let Some(token) = &result.next_continuation_token {
        body.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(token)
        ));
    }
    if let Some(after) = &result.start_after {
        body.push_str(&format!("<StartAfter>{}</StartAfter>", xml_escape(after)));
    }
    for entry in &result.entries {
        body.push_str("<Contents>");
        body.push_str(&format!("<Key>{}</Key>", xml_escape(&entry.key)));
        body.push_str(&format!(
            "<LastModified>{}</LastModified>",
            iso8601(entry.last_modified_ms)
        ));
        // The quotes are part of an ETag, and in XML they are escaped.
        let mut quoted = String::with_capacity(entry.etag.len() + 2);
        quoted.push('"');
        quoted.push_str(&entry.etag);
        quoted.push('"');
        body.push_str(&format!("<ETag>{}</ETag>", xml_escape(&quoted)));
        body.push_str(&format!("<Size>{}</Size>", entry.size));
        body.push_str(&format!(
            "<StorageClass>{}</StorageClass>",
            xml_escape(&entry.storage_class)
        ));
        body.push_str("</Contents>");
    }
    for prefix in &result.common_prefixes {
        body.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(prefix)
        ));
    }
    body.push_str("</ListBucketResult>\n");
    body
}

/// A `ListAllMyBuckets` body, which is what `GET /` answers.
///
/// # WHY THERE IS NO `CreationDate`
///
/// This store keeps its buckets as a set of names and records no creation time for them, so there
/// is no value to put in the element. It is OMITTED rather than filled with the current time or
/// with zero: a client that sorts or ages buckets by that date would be reading an answer this
/// store never knew, and a wrong date is worse than an absent one. Every major S3 client models
/// the field as optional, so omitting it costs a client nothing it was relying on.
pub fn list_all_my_buckets_xml(buckets: &[String]) -> String {
    let mut body = String::with_capacity(256 + buckets.len() * 48);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<ListAllMyBucketsResult xmlns=\"");
    body.push_str(S3_XML_NAMESPACE);
    body.push_str("\"><Owner><ID></ID><DisplayName></DisplayName></Owner><Buckets>");
    for bucket in buckets {
        body.push_str(&format!(
            "<Bucket><Name>{}</Name></Bucket>",
            xml_escape(bucket)
        ));
    }
    body.push_str("</Buckets></ListAllMyBucketsResult>\n");
    body
}

/// An XML response that is not an error.
pub fn xml_response(status: u16, body: String) -> Response {
    Response {
        status,
        headers: vec![("content-type".to_string(), "application/xml".to_string())],
        body: body.into_bytes(),
    }
}

/// Which S3 code one of this store's errors is.
///
/// `NotFound` is the one that needs to know where it came from: S3 answers `NoSuchBucket` for a
/// bucket that is not there and `NoSuchKey` for a key that is not, and a client tells them apart
/// to decide whether to create the bucket or give up on the object. The rest map without context.
pub fn code_for(err: &ObjectError, missing_is_bucket: bool) -> S3ErrorCode {
    match err {
        ObjectError::NotFound(_) => {
            if missing_is_bucket {
                S3ErrorCode::NoSuchBucket
            } else {
                S3ErrorCode::NoSuchKey
            }
        }
        ObjectError::InvalidArgument(_) => S3ErrorCode::InvalidArgument,
        ObjectError::PreconditionFailed(_) => S3ErrorCode::PreconditionFailed,
        // THE SERVER'S FAULT, because it is: corruption is a statement about stored state and
        // nothing the client can change about its request will fix it.
        ObjectError::Corruption(_) => S3ErrorCode::InternalError,
    }
}
