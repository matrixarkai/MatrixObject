// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HTTP/1.1 request reading and response writing, on the standard library alone.
//!
//! # WHY THIS IS HAND-WRITTEN RATHER THAN A DEPENDENCY
//!
//! This crate has one optional dependency. A store is where other people's data sits, so what it
//! links is part of what they are trusting; pulling in an async runtime and an HTTP stack to
//! answer six verbs would be the largest single addition to that surface, and it would apply to
//! everyone who builds the crate rather than only to those who want a server. The verb set is
//! small and the framing rules it needs are few, so the whole of it fits in one reviewable file.
//!
//! # WHAT IT REFUSES, AND WHY REFUSING IS THE POINT
//!
//! A hand-written parser earns its place only by being strict. The failure that matters for an
//! object store is not an unsupported feature -- it is a body read as the wrong number of bytes
//! and stored anyway, because the object is then silently corrupt and nothing downstream can tell.
//! So every framing this file does not implement is answered with a status rather than guessed at:
//!
//!   * `Transfer-Encoding` of any kind      -> 501, because only `Content-Length` framing is read
//!   * a body-carrying method with no length -> 411, rather than assuming it is empty
//!   * `Content-Length` that does not parse  -> 400
//!   * two different `Content-Length` values -> 400, a framing disagreement, not a choice
//!   * a body over the configured maximum    -> 413, before any of it is buffered
//!   * a head over the configured maximum    -> 431, before the request line is trusted
//!   * `Expect: 100-continue`                -> answered, because a client that waits would hang
//!
//! None of those is an opinion about HTTP. Each is a case where continuing would mean writing
//! bytes whose length this file is not sure of.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;

/// Largest request head (request line plus headers) this server will read.
///
/// Bounded so a client cannot make the server buffer without limit by never sending the blank
/// line that ends the head.
pub const MAX_HEAD_BYTES: usize = 64 * 1024;

/// Largest single header line.
pub const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;

/// One parsed request: what was asked, of what, with what bytes.
#[derive(Debug)]
pub struct Request {
    /// Uppercase method, as sent.
    pub method: String,
    /// Path with percent-escapes decoded and the query string removed.
    pub path: String,
    /// Raw query string without the `?`; empty when there was none.
    pub query: String,
    /// Header names lowercased, values trimmed, in the order sent.
    pub headers: Vec<(String, String)>,
    /// Request body, exactly `Content-Length` bytes.
    pub body: Vec<u8>,
    /// Whether the connection may carry another request.
    pub keep_alive: bool,
}

impl Request {
    /// One header's value, by lowercase name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Every header whose name starts with `prefix`, as (suffix, value).
    pub fn headers_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.headers.iter().filter_map(move |(name, value)| {
            name.strip_prefix(prefix)
                .filter(|suffix| !suffix.is_empty())
                .map(|suffix| (suffix, value.as_str()))
        })
    }

    /// One query parameter's value, percent-decoded.
    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key == name).then(|| percent_decode(value))
        })
    }
}

/// Why a request could not be read, carrying the status that says so.
#[derive(Debug)]
pub struct RequestError {
    /// HTTP status to answer with.
    pub status: u16,
    /// Short reason, sent as the body.
    pub reason: String,
}

impl RequestError {
    fn new(status: u16, reason: impl Into<String>) -> Self {
        Self {
            status,
            reason: reason.into(),
        }
    }
}

/// Read one request. `Ok(None)` means the peer closed cleanly before sending anything, which on a
/// kept-alive connection is the ordinary way a client goes away and is not a failure.
pub fn read_request(
    reader: &mut BufReader<TcpStream>,
    max_body_bytes: usize,
) -> Result<Option<Request>, RequestError> {
    let mut head_bytes = 0usize;
    let mut line = String::new();

    loop {
        line.clear();
        let read = read_line(reader, &mut line, &mut head_bytes)?;
        if read == 0 {
            return Ok(None);
        }
        if !line.trim().is_empty() {
            break;
        }
        // A blank line before the request line is permitted leftover from an earlier request.
    }

    let mut parts = line.trim().split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| RequestError::new(400, "no method in request line"))?
        .to_ascii_uppercase();
    let target = parts
        .next()
        .ok_or_else(|| RequestError::new(400, "no target in request line"))?
        .to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();

    // HTTP/1.0 defaults to closing and HTTP/1.1 to keeping alive; an explicit `Connection` header
    // below overrides whichever default applies.
    let mut keep_alive = !version.eq_ignore_ascii_case("HTTP/1.0");

    let (raw_path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_string(), query.to_string()),
        None => (target, String::new()),
    };

    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length: Option<usize> = None;
    let mut transfer_encoding: Option<String> = None;
    let mut expect_continue = false;

    loop {
        line.clear();
        let read = read_line(reader, &mut line, &mut head_bytes)?;
        if read == 0 {
            return Err(RequestError::new(400, "headers ended without a blank line"));
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        let (name, value) = trimmed
            .split_once(':')
            .ok_or_else(|| RequestError::new(400, "header line has no colon"))?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        match name.as_str() {
            "content-length" => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| RequestError::new(400, "content-length is not a number"))?;
                if content_length.is_some_and(|first| first != parsed) {
                    return Err(RequestError::new(400, "two different content-length values"));
                }
                content_length = Some(parsed);
            }
            "transfer-encoding" => transfer_encoding = Some(value.clone()),
            "connection" => {
                if value.eq_ignore_ascii_case("close") {
                    keep_alive = false;
                } else if value.eq_ignore_ascii_case("keep-alive") {
                    keep_alive = true;
                }
            }
            "expect" => expect_continue = value.eq_ignore_ascii_case("100-continue"),
            _ => {}
        }
        headers.push((name, value));
    }

    // REFUSED RATHER THAN GUESSED. `chunked` needs its own framing reader; without one the bytes
    // after the head are chunk headers, and reading them as a body stores a corrupt object.
    if let Some(encoding) = transfer_encoding {
        return Err(RequestError::new(
            501,
            format!("transfer-encoding is not supported (got {encoding}); use content-length"),
        ));
    }

    let body_expected = matches!(method.as_str(), "PUT" | "POST" | "PATCH");
    let length = match (content_length, body_expected) {
        (Some(length), _) => length,
        (None, true) => {
            return Err(RequestError::new(
                411,
                "a body-carrying method needs content-length",
            ))
        }
        (None, false) => 0,
    };
    if length > max_body_bytes {
        return Err(RequestError::new(
            413,
            format!("body of {length} bytes exceeds the {max_body_bytes}-byte maximum"),
        ));
    }

    // ANSWERED BEFORE THE BODY IS READ, because a client that sent `Expect: 100-continue` is
    // waiting for this line and will not send the body until it arrives.
    if expect_continue {
        let stream = reader.get_mut();
        stream
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .map_err(|err| RequestError::new(400, format!("writing 100-continue failed: {err}")))?;
        stream
            .flush()
            .map_err(|err| RequestError::new(400, format!("flushing 100-continue failed: {err}")))?;
    }

    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).map_err(|err| {
        RequestError::new(400, format!("body shorter than content-length: {err}"))
    })?;

    Ok(Some(Request {
        method,
        path: percent_decode(&raw_path),
        query,
        headers,
        body,
        keep_alive,
    }))
}

/// Read one line, counting it against the head budget.
fn read_line(
    reader: &mut BufReader<TcpStream>,
    line: &mut String,
    head_bytes: &mut usize,
) -> Result<usize, RequestError> {
    let read = reader
        .read_line(line)
        .map_err(|err| RequestError::new(400, format!("reading the request head failed: {err}")))?;
    if read > MAX_HEADER_LINE_BYTES {
        return Err(RequestError::new(431, "a header line is too long"));
    }
    *head_bytes += read;
    if *head_bytes > MAX_HEAD_BYTES {
        return Err(RequestError::new(431, "the request head is too long"));
    }
    Ok(read)
}

/// One response: status, headers, body.
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Headers to send, in order. `Content-Length` is added by the writer.
    pub headers: Vec<(String, String)>,
    /// Body bytes; empty for a HEAD answer or a status that carries none.
    pub body: Vec<u8>,
}

impl Response {
    /// A response with a status and no body.
    pub fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// A `text/plain` response.
    pub fn text(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: vec![(
                "content-type".to_string(),
                "text/plain; charset=utf-8".to_string(),
            )],
            body: body.into().into_bytes(),
        }
    }

    /// Add one header.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// The reason phrase for a status.
///
/// Only the codes this server sends. An unknown code gets a generic phrase rather than a panic:
/// the phrase is decoration, and clients read the number.
///
/// # ONE CODE THAT IS DELIBERATELY ABSENT
///
/// An earlier sketch of this surface answered a write whose bytes were already stored with **208
/// Already Reported**, as a dedup signal. That is not implementable here and claiming it would
/// have been worse than omitting it. An object's `etag` in this store is derived from its
/// IDENTITY -- bucket, key, version, length and content type -- and not from a hash of its
/// content, so two different payloads of the same length under the same key carry the same etag
/// at the same version. A server comparing etags therefore cannot tell "the same bytes" from "the
/// same shape", and the only way to answer honestly would be to read the stored object back and
/// compare it on every write. So this surface does not pretend to deduplicate; what it does
/// support is the store's real conditional writes, `If-None-Match` and `If-Match`, which are
/// enforced inside the store rather than guessed at out here.
fn reason_phrase(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Content Too Large",
        416 => "Range Not Satisfiable",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Statuses that must not carry a body, per HTTP.
fn status_forbids_body(status: u16) -> bool {
    matches!(status, 204 | 304) || (100..200).contains(&status)
}

/// Write one response.
///
/// `head_only` suppresses the body for a HEAD request while keeping the `Content-Length` the
/// matching GET would have reported, so a client can size a read before making it.
pub fn write_response(
    stream: &mut TcpStream,
    response: &Response,
    keep_alive: bool,
    head_only: bool,
) -> std::io::Result<()> {
    let mut out = Vec::with_capacity(256 + response.body.len());
    out.extend_from_slice(
        format!(
            "HTTP/1.1 {} {}\r\n",
            response.status,
            reason_phrase(response.status)
        )
        .as_bytes(),
    );
    for (name, value) in &response.headers {
        // A header value carrying CR or LF would end the head early and let a caller append
        // headers or a body of their choosing. Object keys and client-supplied metadata reach
        // these values, so this is filtered rather than trusted.
        if value.contains('\r') || value.contains('\n') {
            continue;
        }
        out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    if !status_forbids_body(response.status) {
        out.extend_from_slice(format!("content-length: {}\r\n", response.body.len()).as_bytes());
    }
    out.extend_from_slice(
        if keep_alive {
            "connection: keep-alive\r\n"
        } else {
            "connection: close\r\n"
        }
        .as_bytes(),
    );
    out.extend_from_slice(b"\r\n");
    if !head_only && !status_forbids_body(response.status) && !response.body.is_empty() {
        out.extend_from_slice(&response.body);
    }
    stream.write_all(&out)?;
    stream.flush()
}

/// Decode `%XX` escapes and `+`-as-space, leaving anything malformed as written.
///
/// Malformed escapes are left alone rather than dropped: an object key is an identity, and a key
/// that silently loses a character addresses a different object than the caller meant.
pub fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let high = (bytes[index + 1] as char).to_digit(16);
                let low = (bytes[index + 2] as char).to_digit(16);
                match (high, low) {
                    (Some(high), Some(low)) => {
                        out.push((high * 16 + low) as u8);
                        index += 3;
                    }
                    _ => {
                        out.push(b'%');
                        index += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One `Range` header, resolved against a known object length.
#[derive(Debug, PartialEq, Eq)]
pub enum ResolvedRange {
    /// Serve these bytes as 206, with this `Content-Range`.
    Partial { offset: u64, length: u64 },
    /// The header named nothing inside the object: answer 416.
    Unsatisfiable,
    /// No range header, or a unit this server does not implement: serve the whole object as 200.
    Whole,
}

/// Resolve `bytes=` ranges against `total`.
///
/// Only a single range is implemented. A multi-range request asks for a `multipart/byteranges`
/// body, which this server does not build -- so rather than silently serving the first range
/// under a 206 (which a client would read as the whole of what it asked for) it falls back to
/// [`ResolvedRange::Whole`], which is a legal answer to any range request and cannot be
/// misread.
pub fn resolve_range(header: Option<&str>, total: u64) -> ResolvedRange {
    let Some(header) = header else {
        return ResolvedRange::Whole;
    };
    let Some(spec) = header.trim().strip_prefix("bytes=") else {
        return ResolvedRange::Whole;
    };
    if spec.contains(',') {
        return ResolvedRange::Whole;
    }
    let Some((start, end)) = spec.split_once('-') else {
        return ResolvedRange::Whole;
    };
    let start = start.trim();
    let end = end.trim();
    match (start.is_empty(), end.is_empty()) {
        // `bytes=-N`: the last N bytes.
        (true, false) => match end.parse::<u64>() {
            Ok(0) => ResolvedRange::Unsatisfiable,
            Ok(suffix) => {
                let length = suffix.min(total);
                if total == 0 {
                    ResolvedRange::Unsatisfiable
                } else {
                    ResolvedRange::Partial {
                        offset: total - length,
                        length,
                    }
                }
            }
            Err(_) => ResolvedRange::Whole,
        },
        // `bytes=N-`: from N to the end.
        (false, true) => match start.parse::<u64>() {
            Ok(offset) if offset < total => ResolvedRange::Partial {
                offset,
                length: total - offset,
            },
            Ok(_) => ResolvedRange::Unsatisfiable,
            Err(_) => ResolvedRange::Whole,
        },
        // `bytes=A-B`, inclusive of B, as HTTP specifies.
        (false, false) => match (start.parse::<u64>(), end.parse::<u64>()) {
            (Ok(offset), Ok(last)) if offset < total && offset <= last => {
                let last = last.min(total.saturating_sub(1));
                ResolvedRange::Partial {
                    offset,
                    length: last - offset + 1,
                }
            }
            (Ok(_), Ok(_)) => ResolvedRange::Unsatisfiable,
            _ => ResolvedRange::Whole,
        },
        (true, true) => ResolvedRange::Whole,
    }
}

/// Format epoch milliseconds as an HTTP date (RFC 9110 IMF-fixdate).
///
/// Written out rather than taken from a dependency for the reason the rest of this file is: it is
/// civil-date arithmetic over a fixed epoch, it has no configuration, and `Last-Modified` is in
/// this store's own list of CORS-exposed headers, so omitting it would leave a header the policy
/// already advertises unsent.
/// The civil date and time one epoch-millisecond timestamp falls on.
///
/// Shared by the two formatters that need it -- the HTTP date on a GET and the ISO 8601 date in
/// an S3 listing -- because the shift-to-March arithmetic below is the part that is easy to get
/// subtly wrong, and two copies of it can disagree about a leap day.
pub struct CivilParts {
    pub year: i64,
    pub month: i64,
    pub day: i64,
    pub hour: u64,
    pub minute: u64,
    pub second: u64,
    /// 0 is Monday, matching the order of the day names an HTTP date uses.
    pub weekday: usize,
}

/// Split epoch milliseconds into a civil date and time, in UTC.
pub fn civil_parts(epoch_ms: u64) -> CivilParts {
    let seconds = epoch_ms / 1_000;
    let days = (seconds / 86_400) as i64;
    let second_of_day = seconds % 86_400;
    let (hour, minute, second) = (
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60,
        second_of_day % 60,
    );
    // 1970-01-01 was a Thursday, which is index 3 when Monday is 0.
    let weekday = (((days % 7) + 7 + 3) % 7) as usize;

    // Civil-from-days, the standard shift-to-March algorithm: March-based years have a
    // 146,097-day 400-year cycle with no leap-day special case inside it.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    if month <= 2 {
        year += 1;
    }
    CivilParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        weekday,
    }
}

/// Format epoch milliseconds as an HTTP date (RFC 9110 IMF-fixdate).
///
/// Written out rather than taken from a dependency for the reason the rest of this file is: it is
/// civil-date arithmetic over a fixed epoch, it has no configuration, and `Last-Modified` is in
/// this store's own list of CORS-exposed headers, so omitting it would leave a header the policy
/// already advertises unsent.
pub fn http_date(epoch_ms: u64) -> String {
    const DAY_NAMES: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTH_NAMES: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let parts = civil_parts(epoch_ms);
    format!(
        "{weekday}, {day:02} {month_name} {year} {hour:02}:{minute:02}:{second:02} GMT",
        weekday = DAY_NAMES[parts.weekday],
        day = parts.day,
        month_name = MONTH_NAMES[(parts.month - 1) as usize],
        year = parts.year,
        hour = parts.hour,
        minute = parts.minute,
        second = parts.second,
    )
}
