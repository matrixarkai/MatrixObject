// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A request handler reached as function calls: the client SDK's [`Transport`] over a handler in
//! this process.
//!
//! A node that runs the gateway's request handling itself has no gateway process and no hop to
//! it. Everything above the transport is the code a client of a gateway runs -- the append rules,
//! the settlement of an append whose outcome is unknown, ranged reads, listing -- so the two ways
//! of reaching a cluster cannot disagree about a request, only about how it travels.

use std::sync::Arc;

use matrixobject_client::{Reply, Transport, TransportError};

use super::http::percent_decode;
use super::{Request, RequestHandler};

/// The request a handler would have parsed off a socket: the target split into its decoded path and
/// its raw query, the method upper-cased, and header names lower-cased.
pub fn request_from_target(
    method: &str,
    target: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> Request {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    Request {
        method: method.to_ascii_uppercase(),
        path: percent_decode(path),
        query: query.to_string(),
        headers: headers
            .iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .collect(),
        body: body.to_vec(),
        keep_alive: false,
    }
}

/// A [`Transport`] whose door is a [`RequestHandler`] in this process.
pub struct InProcessDoor {
    handler: Arc<dyn RequestHandler>,
}

impl InProcessDoor {
    pub fn new(handler: Arc<dyn RequestHandler>) -> Self {
        Self { handler }
    }
}

impl std::fmt::Debug for InProcessDoor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessDoor").finish_non_exhaustive()
    }
}

impl Transport for InProcessDoor {
    fn send(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply, TransportError> {
        let response = self.handler.handle(&request_from_target(method, target, headers, body));
        Ok(Reply { status: response.status, headers: response.headers, body: response.body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_target_is_split_into_its_decoded_path_and_its_raw_query() {
        let request = request_from_target(
            "put",
            "/b/dir%2Fkey%20x?append-resolve&attempt=ab%2Fcd",
            &[("X-Amz-Attempt-Id", " abc ".to_string())],
            b"body",
        );
        assert_eq!(request.method, "PUT");
        assert_eq!(request.path, "/b/dir/key x");
        assert_eq!(request.query, "append-resolve&attempt=ab%2Fcd", "the query stays raw: the handler decodes each value");
        assert_eq!(request.headers, vec![("x-amz-attempt-id".to_string(), "abc".to_string())]);
        assert_eq!(request.body, b"body");
        assert!(!request.keep_alive);
        let bare = request_from_target("GET", "/b", &[], b"");
        assert_eq!((bare.path.as_str(), bare.query.as_str()), ("/b", ""));
    }

    struct Echo;
    impl RequestHandler for Echo {
        fn handle(&self, request: &Request) -> super::super::Response {
            super::super::Response::text(207, format!("{} {} {}", request.method, request.path, request.body.len()))
                .with_header("x-seen", request.query.clone())
        }
    }

    #[test]
    fn the_handler_is_called_and_its_answer_comes_back_whole() {
        let door = InProcessDoor::new(Arc::new(Echo));
        let reply = door.send("POST", "/b/k%2F1?q=1", &[], b"abc").unwrap();
        assert_eq!(reply.status, 207);
        assert_eq!(reply.header("x-seen"), Some("q=1"));
        assert_eq!(String::from_utf8(reply.body).unwrap(), "POST /b/k/1 3");
    }
}
