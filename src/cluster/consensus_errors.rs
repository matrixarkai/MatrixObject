// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! How a consensus group that did not answer in time is told apart from a bad request.
//!
//! A metadata or chunk Raft group that cannot admit a request, or does not complete one within
//! its request timeout, answers with `ObjectError::InvalidArgument` and a message beginning
//! `metadata consensus ` or `chunk consensus ` (see `consensus_inbox::wait_observed` and the
//! admission refusal in `meta_consensus`). The error type has no variant of its own for that, so
//! a caller that must not treat it as a malformed request recognises it here, in one place,
//! next to the text that produces it. A typed variant would be better and would touch every
//! exhaustive match on `ObjectError`; that is left as a follow-up.
use crate::ObjectError;

/// The group could not take the request in time: its queue was full, no leader was reachable,
/// or the proposal did not complete before the request timeout. Retrying later is reasonable.
pub fn consensus_unavailable(error: &ObjectError) -> bool {
    matches!(
        error,
        ObjectError::InvalidArgument(message)
            if message.starts_with("metadata consensus ") || message.starts_with("chunk consensus ")
    )
}

/// Same as [`consensus_unavailable`], and the group says the request may nevertheless have
/// committed. A caller must resolve that by reading, never by replaying the write blindly.
pub fn outcome_may_be_committed(error: &ObjectError) -> bool {
    consensus_unavailable(error)
        && matches!(error, ObjectError::InvalidArgument(message) if message.contains("write outcome may be committed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_messages_the_groups_produce_are_recognised() {
        let deadline = ObjectError::InvalidArgument(
            "metadata consensus deadline; last observed phase=proposal completion; write outcome may be committed; do not replay".into(),
        );
        assert!(consensus_unavailable(&deadline) && outcome_may_be_committed(&deadline));
        let read = ObjectError::InvalidArgument(
            "chunk consensus deadline; last observed phase=queued; read request; no mutation submitted".into(),
        );
        assert!(consensus_unavailable(&read) && !outcome_may_be_committed(&read));
        let full = ObjectError::InvalidArgument("metadata consensus unavailable or request queue full".into());
        assert!(consensus_unavailable(&full) && !outcome_may_be_committed(&full));
        assert!(!consensus_unavailable(&ObjectError::InvalidArgument("invalid bucket name".into())));
        assert!(!consensus_unavailable(&ObjectError::PreconditionFailed("metadata consensus deadline".into())));
    }
}
