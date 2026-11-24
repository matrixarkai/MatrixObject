// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The limits and header names of the S3 door that more than one front end answers with.

/// Default `Content-Type` recorded for a body that arrives without one.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Offset a `PutObject` body is expected to land at, which turns the put into an append.
///
/// Spelled as S3 spells it. S3 Express One Zone takes this header on `PutObject` and appends
/// when it is present, so answering it widens what this door covers of S3 rather than adding
/// a verb of its own -- which matters here, because there is no HTTP mode of this server that
/// answers anything other than S3.
pub use matrixobject_client::wire::WRITE_OFFSET_HEADER;

/// Most keys one bucket listing will answer with, when the caller names no limit.
pub const DEFAULT_LIST_LIMIT: usize = 1_000;

/// Most keys one bucket listing will answer with, whatever the caller asks for.
pub const MAX_LIST_LIMIT: usize = 10_000;
