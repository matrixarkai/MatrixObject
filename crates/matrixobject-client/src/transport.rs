// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! How a request reaches the door: over a socket, or into a door served in this process.
//!
//! [`S3Client`](crate::S3Client) speaks the S3 contract and does not care how a request travels.
//! A transport carries one request and returns the door's answer. A transport that cannot get an
//! answer at all returns an error, and the client treats that as an outcome it cannot know: the
//! request may have been served before the connection failed.

use std::fmt;
use std::sync::Arc;

/// One reply from the door.
#[derive(Clone, Debug)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Reply {
    /// The first header named `name`, matched without regard to case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// The door could not answer: the request may or may not have been served.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportError(pub String);

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(error: std::io::Error) -> Self {
        TransportError(error.to_string())
    }
}

/// What a transport moved, for callers that account for cost. A transport that sends nothing over
/// a wire (a door in this process) reports zeros.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WireStats {
    /// Bytes written to the wire, request heads included.
    pub bytes_sent: u64,
    /// Bytes read from the wire, reply heads included.
    pub bytes_received: u64,
    /// Connections opened.
    pub connections_opened: u64,
}

/// Carries one request to the door and returns its answer.
///
/// `target` is the path and query as they appear on the wire, percent-encoded. `headers` are the
/// extra headers the client sets; the transport adds the ones its own framing needs.
pub trait Transport: Send + Sync + fmt::Debug {
    fn send(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply, TransportError>;

    fn wire_stats(&self) -> WireStats {
        WireStats::default()
    }
}

impl<T: Transport + ?Sized> Transport for Arc<T> {
    fn send(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply, TransportError> {
        (**self).send(method, target, headers, body)
    }

    fn wire_stats(&self) -> WireStats {
        (**self).wire_stats()
    }
}
