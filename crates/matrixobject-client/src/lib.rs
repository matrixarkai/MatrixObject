// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Client SDK for the MatrixObject S3 door.
//!
//! The gateway and every client speak one contract: the header and query names, the shape of an
//! attempt id, the words a resolve answers with. It is written down once, in [`wire`], and both
//! sides use that code instead of each spelling the same strings. The crate depends on nothing
//! but the standard library, `sha2` for the digest a resolve carries and `getrandom` for attempt ids.

#![forbid(unsafe_code)]

mod client;
mod tcp;
mod transport;
pub mod wire;

pub use client::{AppendReceipt, AppendResolutions, ClientError, S3Client, MAX_KEY_BYTES};
pub use tcp::{TcpOptions, TcpTransport};
pub use transport::{Reply, Transport, TransportError, WireStats};

#[cfg(test)]
mod client_tests;

#[cfg(test)]
mod tcp_tests;
