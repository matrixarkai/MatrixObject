// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! How a consensus group that did not answer in time is told apart from a bad request.
//!
//! A metadata or chunk Raft group that cannot admit a request, does not complete one within its
//! request timeout, lets one expire while it waits in the queue, or loses leadership under it,
//! answers with `ObjectError::InvalidArgument` and one of the messages listed in `RETRYABLE`
//! (see `consensus_inbox::wait_observed` and the refusals in `meta_consensus` and
//! `chunk_consensus`). The error type has no variant of its own for that, so
//! a caller that must not treat it as a malformed request recognises it here, in one place,
//! next to the text that produces it. A typed variant would be better and would touch every
//! exhaustive match on `ObjectError`; that is left as a follow-up.
use crate::ObjectError;

/// The start of every message a group uses to say "not now" rather than "your request is wrong".
/// A request that waited in the queue past its deadline, and one whose leader changed under it, are
/// as retryable as one that timed out; a worker that adds a message of this kind must add its start
/// here (the guard test below reads the worker sources and fails if one is missing).
const RETRYABLE: [&str; 9] = [
    "metadata consensus ",
    "chunk consensus ",
    "metadata request expired ",
    "metadata expiry request expired ",
    "catalog request expired ",
    "membership request expired ",
    "chunk request expired ",
    "metadata leadership changed",
    "chunk leadership changed",
];

/// The group could not take the request in time: its queue was full, no leader was reachable, the
/// proposal did not complete before the request timeout, the request expired while queued, or
/// leadership changed under it. Retrying later is reasonable.
///
/// THE MESSAGE IS RECOGNISED WITH ITS `Display` PREFIXES REMOVED. The metaserver (and a chunk server)
/// answers a failure with `error.to_string()` (`meta_server.rs`, `frame_response`), and `ObjectError`'s
/// `Display` is `"invalid argument: <message>"`; the client rebuilds the error with
/// `StatusCode::into_error(<that text>)`. So an error that crossed the wire is
/// `InvalidArgument("invalid argument: metadata consensus deadline; ...")`, which a plain `starts_with`
/// on the bare message never matched: a refusal built in process was a 503 and the same refusal from a
/// real metaserver was a 400. Every wrapper that formats the error again adds the prefix once more.
pub fn consensus_unavailable(error: &ObjectError) -> bool {
    matches!(
        error,
        ObjectError::InvalidArgument(message)
            if {
                let bare = without_display_prefixes(message);
                RETRYABLE.iter().any(|start| bare.starts_with(start))
            }
    )
}

/// `message` without any number of leading "invalid argument: " (the `Display` prefix of the variant).
fn without_display_prefixes(message: &str) -> &str {
    let mut bare = message;
    while let Some(rest) = bare.strip_prefix("invalid argument: ") {
        bare = rest;
    }
    bare
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

    /// Every message of this kind that the workers can send, verbatim. Queue expiries never reached
    /// the request's work, so they are retryable and NOT possibly committed; a leadership change
    /// under a write says the write may have committed.
    #[test]
    fn queue_expiries_and_leadership_changes_are_retryable_and_say_whether_they_may_have_committed() {
        let not_committed = [
            "metadata consensus unavailable or request queue full",
            "metadata consensus stopped",
            "chunk consensus unavailable",
            "metadata request expired before proposal",
            "metadata request expired before read confirmation",
            "metadata expiry request expired before read confirmation",
            "catalog request expired before proposal",
            "catalog request expired during preparation",
            "membership request expired before proposal",
            "chunk request expired before proposal",
            "chunk request expired before read confirmation",
            "metadata leadership changed; membership outcome may be committed",
            "chunk leadership changed; membership outcome may be committed",
        ];
        for message in not_committed {
            let error = ObjectError::InvalidArgument(message.into());
            assert!(consensus_unavailable(&error), "{message}");
            assert!(!outcome_may_be_committed(&error), "{message}");
        }
        let may_be_committed = [
            "metadata leadership changed; write outcome may be committed; last observed phase=proposal completion",
            "chunk leadership changed; write outcome may be committed; last observed phase=queued",
        ];
        for message in may_be_committed {
            let error = ObjectError::InvalidArgument(message.into());
            assert!(consensus_unavailable(&error) && outcome_may_be_committed(&error), "{message}");
        }
        // A request that is simply wrong stays a client error.
        for message in [
            "metadata proposal exceeds batch byte limit",
            "catalog proposal exceeds byte limit",
            "membership mode or group identity mismatch",
            "invalid conditional write batch",
        ] {
            assert!(!consensus_unavailable(&ObjectError::InvalidArgument(message.into())), "{message}");
        }
    }

    /// What actually reaches the gateway. The metaserver sends `error.to_string()` and the client rebuilds the
    /// error with `StatusCode::into_error`, so the message carries the `Display` prefix (and again for every
    /// wrapper that formats it). This is the shape PR 654's check never matched.
    #[test]
    fn a_refusal_that_crossed_the_metadata_wire_is_still_recognised() {
        use crate::cluster::StatusCode;
        let deadline_write = "metadata consensus deadline; last observed phase=proposal completion; write outcome may be committed; do not replay";
        for inner in [
            "metadata consensus deadline; last observed phase=queued; read request; no mutation submitted",
            deadline_write,
            "metadata consensus unavailable or request queue full",
            "metadata request expired before proposal",
            "metadata leadership changed; write outcome may be committed",
            "chunk consensus deadline; last observed phase=queued; read request; no mutation submitted",
        ] {
            let sent = ObjectError::InvalidArgument(inner.into());
            let received = StatusCode::for_error(&sent).into_error(sent.to_string());
            assert!(matches!(&received, ObjectError::InvalidArgument(m) if m.starts_with("invalid argument: ")), "{received:?}");
            assert!(consensus_unavailable(&received), "over the wire: {received:?}");
            let twice = StatusCode::for_error(&received).into_error(received.to_string());
            assert!(consensus_unavailable(&twice), "through a second hop: {twice:?}");
        }
        let sent = ObjectError::InvalidArgument(deadline_write.into());
        let received = StatusCode::for_error(&sent).into_error(sent.to_string());
        assert!(outcome_may_be_committed(&received), "a write that may have committed must say so after the wire");
        // A genuine client error stays one across the wire, and only the variant's own prefix is removed.
        let bad = ObjectError::InvalidArgument("invalid bucket name".into());
        assert!(!consensus_unavailable(&StatusCode::for_error(&bad).into_error(bad.to_string())));
        let mentioned = ObjectError::InvalidArgument("bad key near metadata consensus ".into());
        assert!(!consensus_unavailable(&mentioned));
    }

    /// The guard: read the worker sources, take every string literal outside the test modules that
    /// says a request expired or leadership changed, and require that it is recognised. A new message
    /// of this kind that is not added to `RETRYABLE` reaches S3 as a 400.
    #[test]
    fn every_expiry_and_leadership_message_in_the_workers_is_recognised() {
        for (name, source, at_least) in [
            ("meta_consensus.rs", include_str!("meta_consensus.rs"), 6),
            ("chunk_consensus.rs", include_str!("chunk_consensus.rs"), 4),
        ] {
            let production = source.split("\n#[cfg(test)]\nmod tests").next().unwrap();
            let mut checked = 0;
            for line in production.lines() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                for literal in line.split('"').skip(1).step_by(2) {
                    if literal.contains(" expired before ")
                        || literal.contains(" expired during ")
                        || literal.contains("leadership changed")
                    {
                        checked += 1;
                        assert!(
                            consensus_unavailable(&ObjectError::InvalidArgument(literal.into())),
                            "{name}: {literal:?} is not recognised as retryable"
                        );
                    }
                }
            }
            assert!(checked >= at_least, "{name}: the scan found only {checked} messages; did the code move?");
        }
    }
}
