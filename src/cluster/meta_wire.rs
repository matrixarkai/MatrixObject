// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The request codes a metaserver node answers, and the bytes a boolean takes.
//!
//! # THE FRAMING IS THE REPLICA'S, DELIBERATELY
//!
//! The six-byte header, the protocol stamp, the payload bound, the status codes and the
//! [`crate::cluster::wire::Cursor`] that refuses to read past the end of a payload are all
//! reused from [`crate::cluster::wire`] rather than written again. A second framing layer
//! would be a second place for a length field to be read from the wrong offset, and this
//! service has no requirement the replica service does not: one small request, one answer,
//! both ends shipped from this repository at the same version.
//!
//! # THE CODES ARE IN A DISJOINT RANGE, AND THAT IS NOT TIDINESS
//!
//! The header carries a stamp and a code and nothing that says which service the frame is
//! for. Two services numbering their verbs from 1 would therefore agree about every byte of a
//! misdirected frame: a replica's `WriteChunk` is code 1, and arriving at a metaserver's port
//! it would decode as whatever that service called 1 -- and the metaserver would *apply* it,
//! a page address read as a heartbeat, every field taken from the wrong place.
//!
//! An operator points a client at the wrong port roughly as often as they get it right the
//! first time, so this is a case worth making impossible rather than unlikely. The
//! metaserver's verbs are numbered from [`META_CODE_BASE`], past every replica code, so a
//! frame for the wrong service is refused as an unknown verb by whichever node receives it.
//! The disjointness is asserted by a test rather than left to two files agreeing.
//!
//! # WHAT IS NOT HERE, AND WHY REGISTRATION IS NOT A VERB
//!
//! The metaserver owns far more than these verbs reach: metadata records, leases, shards,
//! transactions, a leader lease, a change feed and the snapshot machinery around all of them.
//!
//! Registration is absent for a specific reason rather than by omission. A node in the roster
//! is a `PlacementNode` of nineteen fields, three of which are health structures carrying about a
//! hundred fields between them, and the only existing byte encoding for one is inlined in
//! `PlacementSnapshot::to_bytes` -- a durable format with thirty-two readable versions behind it.
//! A second, hand-written encoding would be a hundred and ten chances to drop a field
//! silently. So a node joins this metaserver's roster in the metaserver's own process, and
//! these verbs are the ones that keep it current and place objects on it. See
//! [`crate::cluster::meta_message`].

use crate::ObjectError;

/// The first request code belonging to the metaserver service.
///
/// Chosen so that `RequestCode` (the replica service, 1..=5) and [`MetaRequestCode`] cannot
/// collide even after both grow. A frame whose code falls in neither range is an unknown verb
/// to both services, which is the answer that lets a misdirected client find out.
pub const META_CODE_BASE: u8 = 64;

/// What a request asks the metaserver to do.
///
/// Every name is a method the control plane already has. The discriminants are the wire
/// values and may not be renumbered without bumping
/// [`crate::cluster::wire::PROTOCOL_STAMP`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum MetaRequestCode {
    /// Refresh one node's liveness and usage. Payload: node id, bytes, flags, clock.
    HeartbeatRootNode = 64,
    /// Mark every node whose last heartbeat is older than a time-to-live offline.
    ExpireRootNodeHeartbeats = 65,
    /// The whole roster, as summaries. Empty payload.
    ListRootNodes = 66,
    /// Choose nodes for an object without committing to them.
    PlanObjectPlacement = 67,
    /// Commit a plan, charging the chosen nodes for the bytes.
    CommitObjectPlacement = 68,
    /// The placement committed for an object, if there is one.
    ReadObjectRootPlacement = 69,
    /// Ask whether this metaserver is answering, and what it holds. Empty payload.
    Ping = 70,
    /// Refresh a chunk only if its configured endpoint matches the sender.
    HeartbeatChunkNode = 71,
    /// Read one persisted chunk-group catalog entry.
    ReadChunkGroup = 72,
}

impl MetaRequestCode {
    /// The code this byte names, or `None` for one this build does not know.
    ///
    /// `None` is a status on the wire, not a panic. It is also the answer a replica's frame
    /// gets here, and a metaserver frame gets at a replica, which is the whole reason for
    /// [`META_CODE_BASE`].
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            64 => Some(MetaRequestCode::HeartbeatRootNode),
            65 => Some(MetaRequestCode::ExpireRootNodeHeartbeats),
            66 => Some(MetaRequestCode::ListRootNodes),
            67 => Some(MetaRequestCode::PlanObjectPlacement),
            68 => Some(MetaRequestCode::CommitObjectPlacement),
            69 => Some(MetaRequestCode::ReadObjectRootPlacement),
            70 => Some(MetaRequestCode::Ping),
            71 => Some(MetaRequestCode::HeartbeatChunkNode),
            72 => Some(MetaRequestCode::ReadChunkGroup),
            _ => None,
        }
    }

    /// Every code this build answers, for a test that asserts the ranges do not overlap.
    pub fn all() -> &'static [MetaRequestCode] {
        &[
            MetaRequestCode::HeartbeatRootNode,
            MetaRequestCode::ExpireRootNodeHeartbeats,
            MetaRequestCode::ListRootNodes,
            MetaRequestCode::PlanObjectPlacement,
            MetaRequestCode::CommitObjectPlacement,
            MetaRequestCode::ReadObjectRootPlacement,
            MetaRequestCode::Ping,
            MetaRequestCode::HeartbeatChunkNode,
            MetaRequestCode::ReadChunkGroup,
        ]
    }
}

/// Write a boolean as eight bytes.
///
/// # WHY EIGHT BYTES FOR ONE BIT
///
/// A control-plane frame carries a node's identity and a handful of flags once per heartbeat,
/// so the seven bytes a packed boolean would save are not worth a second integer width in the
/// reader. [`crate::cluster::wire::Cursor`] already refuses a truncated `u64`, and reusing it
/// means a flag cannot be the one field read with a hand-written bounds check.
pub fn put_bool(out: &mut Vec<u8>, value: bool) {
    crate::cluster::wire::put_u64(out, u64::from(value));
}

/// Read a boolean, REFUSING any value that is not 0 or 1.
///
/// The refusal is the point. `value != 0` is the obvious reading and it maps 2, and every
/// other number a misaligned frame produces, onto `true` -- so a frame whose fields are read
/// one position out yields a plausible set of flags instead of an error. A boolean that can
/// only be two things on the wire is a field that detects the misalignment the leftover-bytes
/// check at the end of a decode would otherwise have to catch alone.
pub fn take_bool(cursor: &mut crate::cluster::wire::Cursor<'_>) -> Result<bool, ObjectError> {
    match cursor.take_u64()? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(ObjectError::Corruption(format!(
            "a metaserver frame carries {other} where a boolean belongs, so its fields are \
             not where this reader expects them"
        ))),
    }
}
