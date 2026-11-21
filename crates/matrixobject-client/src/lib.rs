// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Client SDK for the MatrixObject S3 door.
//!
//! The gateway and every client speak one contract: the header and query names, the shape of an
//! attempt id, the words a resolve answers with. It is written down once, in [`wire`], and both
//! sides use that code instead of each spelling the same strings. The crate depends on nothing
//! but the standard library.

pub mod wire;
