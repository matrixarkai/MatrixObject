// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The S3 multipart-upload wire format.
//!
//! # WHY THIS IS ITS OWN MODULE
//!
//! Every other document this surface produces travels one way: the store knows something and
//! writes it out. `CompleteMultipartUpload` is the first that travels the other way -- the client
//! sends a list of the parts it believes it uploaded, and the server has to READ it.
//!
//! Reading is where a wire format gets dangerous, and it deserves a module whose doc comment can
//! say so rather than a section inside one about writing.
//!
//! # THE PARSER IS DELIBERATELY NOT AN XML PARSER
//!
//! It recognises exactly one document -- `<Part>` blocks each holding a `<PartNumber>` and an
//! `<ETag>` -- and refuses everything else. It does not resolve entities, it does not accept
//! attributes it has no use for, it does not follow a DTD, and it does not try to be forgiving.
//!
//! That is the safe direction and it is also the compatible one: every S3 client sends this
//! document in this shape, so there is nothing to gain from accepting more of XML and a great deal
//! to lose. Accepting a `<!DOCTYPE>` would be accepting an external entity; accepting an
//! unrecognised element would be accepting a part list the client did not mean.
//!
//! # WHAT S3 CHECKS THAT THE STORE DOES NOT
//!
//! The store's `complete_multipart_upload` completes whatever parts it holds; it takes no part
//! list. S3 takes one and validates it, and a client depends on that validation -- it is how a
//! client learns that a part it thought it uploaded is missing or was overwritten, rather than
//! getting an object silently assembled from something else.
//!
//! So the validation is done here, before the store is asked to complete, and it answers S3's own
//! codes: `InvalidPart` when a part the client named is not the part the store holds, and
//! `InvalidPartOrder` when the numbers do not ascend.
//!
//! NOT checked: S3's 5 MiB minimum for a non-final part. This store accepts any part size, and
//! enforcing a minimum it does not have would refuse uploads it can serve perfectly well. Every
//! real S3 client already sends parts far above it, so the check would only ever fire on a client
//! this store could have satisfied.

use super::s3::{xml_escape, S3ErrorCode};

/// One part a client claims to have uploaded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedPart {
    pub part_number: u32,
    /// The etag with its quoting and XML escaping removed, which is the form the store holds.
    pub etag: String,
}

/// Read a `CompleteMultipartUpload` body.
///
/// Returns the parts in the order they appeared, which is the order the caller must then check:
/// this reports what the document SAID and leaves whether it is acceptable to the validation,
/// because "the document is malformed" and "the parts are wrong" are different answers to a client.
pub fn parse_complete_multipart_upload(body: &[u8]) -> Result<Vec<ClaimedPart>, String> {
    let text = std::str::from_utf8(body)
        .map_err(|_| "the request body is not UTF-8 text".to_string())?;
    // A DOCTYPE is how an XML parser is talked into fetching something. There is no use for one in
    // this document, so its presence means the body is not the document this understands.
    if text.contains("<!DOCTYPE") || text.contains("<!ENTITY") {
        return Err("the request body carries a document type declaration, which this endpoint \
                    does not accept"
            .to_string());
    }
    if !text.contains("<CompleteMultipartUpload") {
        return Err("the request body is not a CompleteMultipartUpload document".to_string());
    }

    let mut parts = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find("<Part>") {
        let after_open = &rest[open + "<Part>".len()..];
        let close = after_open
            .find("</Part>")
            .ok_or_else(|| "a <Part> element is not closed".to_string())?;
        let block = &after_open[..close];
        let part_number = element(block, "PartNumber")
            .ok_or_else(|| "a <Part> element carries no <PartNumber>".to_string())?;
        let etag = element(block, "ETag")
            .ok_or_else(|| "a <Part> element carries no <ETag>".to_string())?;
        let part_number: u32 = part_number
            .trim()
            .parse()
            .map_err(|_| format!("part number {part_number:?} is not a number"))?;
        if part_number == 0 {
            return Err("part numbers start at 1".to_string());
        }
        parts.push(ClaimedPart {
            part_number,
            etag: unquote(&xml_unescape(etag.trim())),
        });
        rest = &after_open[close + "</Part>".len()..];
    }
    if parts.is_empty() {
        return Err("the part list is empty, and an upload completed with no parts would be an \
                    object the client did not send"
            .to_string());
    }
    Ok(parts)
}

/// The text between `<name>` and `</name>`, or nothing.
fn element<'a>(block: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    Some(&block[start..end])
}

/// Undo the five escapes [`xml_escape`] produces.
///
/// `&amp;` is replaced LAST, which is not a style choice: replacing it first would turn
/// `&amp;quot;` -- an escaped literal `&quot;` -- into `&quot;` and then into a quote character,
/// inventing a quote the client never sent.
pub fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Strip one layer of quoting and any weak-validator marker from an etag.
fn unquote(value: &str) -> String {
    value
        .trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_string()
}

/// What the store holds for one part, for validating a claim against it.
pub struct StoredPart {
    pub part_number: u32,
    pub etag: String,
}

/// Check a client's part list against what the store holds.
///
/// Reports the S3 code and a message, or nothing when the list is acceptable.
pub fn validate_claimed_parts(
    claimed: &[ClaimedPart],
    stored: &[StoredPart],
) -> Option<(S3ErrorCode, String)> {
    // ASCENDING AND DISTINCT, CHECKED FIRST. A client that sent its parts out of order has a bug
    // that `InvalidPart` would misreport as a missing part, sending it to look for the wrong thing.
    for pair in claimed.windows(2) {
        if pair[1].part_number <= pair[0].part_number {
            return Some((
                S3ErrorCode::InvalidPartOrder,
                format!(
                    "part {} follows part {}: the part list must ascend and name each part once",
                    pair[1].part_number, pair[0].part_number
                ),
            ));
        }
    }
    for part in claimed {
        match stored.iter().find(|held| held.part_number == part.part_number) {
            None => {
                return Some((
                    S3ErrorCode::InvalidPart,
                    format!(
                        "part {} was named in the request and this upload holds no such part",
                        part.part_number
                    ),
                ))
            }
            Some(held) if held.etag != part.etag => {
                return Some((
                    S3ErrorCode::InvalidPart,
                    format!(
                        "part {} was uploaded with a different etag, so the part the request \
                         names is not the part this upload holds",
                        part.part_number
                    ),
                ))
            }
            Some(_) => {}
        }
    }
    None
}

/// An `InitiateMultipartUploadResult` body.
pub fn initiate_multipart_upload_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    let mut body = String::with_capacity(256);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<InitiateMultipartUploadResult xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\">");
    body.push_str(&format!("<Bucket>{}</Bucket>", xml_escape(bucket)));
    body.push_str(&format!("<Key>{}</Key>", xml_escape(key)));
    body.push_str(&format!("<UploadId>{}</UploadId>", xml_escape(upload_id)));
    body.push_str("</InitiateMultipartUploadResult>\n");
    body
}

/// A `CompleteMultipartUploadResult` body.
pub fn complete_multipart_upload_xml(bucket: &str, key: &str, etag: &str) -> String {
    let mut body = String::with_capacity(320);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<CompleteMultipartUploadResult xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\">");
    // RELATIVE, because this store does not know what it is reached as. S3 answers an absolute URL
    // built from its own endpoint; a value invented from a Host header would be wrong behind any
    // proxy, and the path is the part a client uses.
    body.push_str(&format!(
        "<Location>/{}/{}</Location>",
        xml_escape(bucket),
        xml_escape(key)
    ));
    body.push_str(&format!("<Bucket>{}</Bucket>", xml_escape(bucket)));
    body.push_str(&format!("<Key>{}</Key>", xml_escape(key)));
    let quoted = format!("\"{etag}\"");
    body.push_str(&format!("<ETag>{}</ETag>", xml_escape(&quoted)));
    body.push_str("</CompleteMultipartUploadResult>\n");
    body
}

/// One part in a `ListParts` answer.
pub struct PartEntry {
    pub part_number: u32,
    pub last_modified_ms: u64,
    pub etag: String,
    pub size: u64,
}

/// What a `ListParts` answered.
pub struct ListPartsResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number_marker: u32,
    pub next_part_number_marker: Option<u32>,
    pub max_parts: usize,
    pub is_truncated: bool,
    pub storage_class: String,
    pub parts: Vec<PartEntry>,
}

/// A `ListPartsResult` body.
pub fn list_parts_xml(result: &ListPartsResult) -> String {
    let mut body = String::with_capacity(512 + result.parts.len() * 160);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<ListPartsResult xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\">");
    body.push_str(&format!("<Bucket>{}</Bucket>", xml_escape(&result.bucket)));
    body.push_str(&format!("<Key>{}</Key>", xml_escape(&result.key)));
    body.push_str(&format!(
        "<UploadId>{}</UploadId>",
        xml_escape(&result.upload_id)
    ));
    body.push_str(&format!(
        "<PartNumberMarker>{}</PartNumberMarker>",
        result.part_number_marker
    ));
    if let Some(next) = result.next_part_number_marker {
        body.push_str(&format!(
            "<NextPartNumberMarker>{next}</NextPartNumberMarker>"
        ));
    }
    body.push_str(&format!("<MaxParts>{}</MaxParts>", result.max_parts));
    body.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    body.push_str(&format!(
        "<StorageClass>{}</StorageClass>",
        xml_escape(&result.storage_class)
    ));
    for part in &result.parts {
        body.push_str("<Part>");
        body.push_str(&format!("<PartNumber>{}</PartNumber>", part.part_number));
        body.push_str(&format!(
            "<LastModified>{}</LastModified>",
            super::s3::iso8601(part.last_modified_ms)
        ));
        let quoted = format!("\"{}\"", part.etag);
        body.push_str(&format!("<ETag>{}</ETag>", xml_escape(&quoted)));
        body.push_str(&format!("<Size>{}</Size>", part.size));
        body.push_str("</Part>");
    }
    body.push_str("</ListPartsResult>\n");
    body
}

/// One upload in a `ListMultipartUploads` answer.
pub struct UploadEntry {
    pub key: String,
    pub upload_id: String,
    pub initiated_ms: u64,
    pub storage_class: String,
}

/// What a `ListMultipartUploads` answered.
pub struct ListUploadsResult {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: String,
    pub upload_id_marker: String,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
    pub max_uploads: usize,
    pub is_truncated: bool,
    pub uploads: Vec<UploadEntry>,
    pub common_prefixes: Vec<String>,
}

/// A `ListMultipartUploadsResult` body.
pub fn list_multipart_uploads_xml(result: &ListUploadsResult) -> String {
    let mut body = String::with_capacity(512 + result.uploads.len() * 180);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<ListMultipartUploadsResult xmlns=\"");
    body.push_str(super::s3::S3_XML_NAMESPACE);
    body.push_str("\">");
    body.push_str(&format!("<Bucket>{}</Bucket>", xml_escape(&result.bucket)));
    body.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(&result.prefix)));
    if let Some(delimiter) = &result.delimiter {
        body.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(delimiter)));
    }
    body.push_str(&format!(
        "<KeyMarker>{}</KeyMarker>",
        xml_escape(&result.key_marker)
    ));
    body.push_str(&format!(
        "<UploadIdMarker>{}</UploadIdMarker>",
        xml_escape(&result.upload_id_marker)
    ));
    if let Some(next) = &result.next_key_marker {
        body.push_str(&format!(
            "<NextKeyMarker>{}</NextKeyMarker>",
            xml_escape(next)
        ));
    }
    if let Some(next) = &result.next_upload_id_marker {
        body.push_str(&format!(
            "<NextUploadIdMarker>{}</NextUploadIdMarker>",
            xml_escape(next)
        ));
    }
    body.push_str(&format!("<MaxUploads>{}</MaxUploads>", result.max_uploads));
    body.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    for upload in &result.uploads {
        body.push_str("<Upload>");
        body.push_str(&format!("<Key>{}</Key>", xml_escape(&upload.key)));
        body.push_str(&format!(
            "<UploadId>{}</UploadId>",
            xml_escape(&upload.upload_id)
        ));
        body.push_str(&format!(
            "<Initiated>{}</Initiated>",
            super::s3::iso8601(upload.initiated_ms)
        ));
        body.push_str(&format!(
            "<StorageClass>{}</StorageClass>",
            xml_escape(&upload.storage_class)
        ));
        body.push_str("</Upload>");
    }
    for prefix in &result.common_prefixes {
        body.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(prefix)
        ));
    }
    body.push_str("</ListMultipartUploadsResult>\n");
    body
}
