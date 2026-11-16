# Appending through the loss and return of one data node

This note covers the remote append path of a fixed three-member Star group when one
chunkserver stops, and when it comes back. It records measured behaviour from real
processes on one machine. It does not establish multi-host latency.

## Behaviour before this change

A routed Star group needs every member to acknowledge a write (`N=3, W=3, R=1`).
Measured with one metaserver and two Star groups, one chunkserver stopped by SIGKILL
after 15 acknowledged appends:

* Metadata considered a three-member Star group available while two members reported
  online (the majority rule written for Raft groups). Placement therefore kept handing
  out the broken group. About every second append failed in 5 to 22 ms with
  `short of a write quorum of 3`, and an immediate retry succeeded on the other group.
* With a single group every append failed for the whole observation window (8 s) and
  nothing recovered.
* In both cases the manifest was unchanged after each failure and every acknowledged byte
  read back exactly.
* Heartbeat expiry defaulted to off, so a stopped node stayed online in the roster
  indefinitely.

## Changes

1. **Placement availability of a Star group.** A fixed Star group is available for
   placement only while all of its members report online and not draining. Its heartbeat
   is its oldest member report. Raft groups keep the majority rule.
   (`project_star_health`, `src/cluster/chunk_groups.rs`.)
2. **Heartbeat expiry is on by default.** `MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS`
   defaults to 15000 ms (five of the chunkserver's default report intervals); `0` is the
   explicit off switch; the value in effect is printed on the startup line as
   `heartbeat_ttl_ms=`. A node expired by silence returns on its next report.
3. **Append with failover.** `RemoteMetaClient::append_object_with_failover` wraps
   `append_object`. After an attempt is refused before publication and the manifest is
   still the one the caller passed, it probes the members of the groups the next attempt
   would use, reports unreachable members offline, waits and plans again. Placement then
   chooses another group and the whole attempt is rewritten there. It reports success
   only when the full write quorum of that group acknowledged it and the conditional
   publication succeeded.
   * The wait before the first switch is 1 s and doubles per switch to 64 s
     (`AppendFailoverPolicy`; tests shorten it).
   * `PublicationUnconfirmed` is never retried here.
   * Pages of refused attempts are returned in `AbandonedAttempt` and are never deleted;
     physical reclamation remains disabled.
   * A manifest that changed since the caller read it is a conflict and is returned to the
     caller unchanged, not retried.

## Why a retry cannot duplicate or lose an acknowledged append

Publication is conditional on the exact manifest the caller supplied, a refused attempt
has not published, and each attempt takes fresh page identities. At most one attempt can
publish. A page that exists on some members of a refused attempt belongs to no manifest.

## What is not done

* A distinct read-only or draining mode is not carried by the chunkserver's own report.
  An administrator's draining flag is preserved by reports, and maintenance mode already
  reports the node offline.
* A returning node is not held out of placement until it has caught up. What was measured
  instead is in the tests: a returning member with no data never made an acknowledged read
  return wrong bytes, and the repair worker restores its pages. Repair order by remaining
  replica count is not implemented; the worker sweeps groups in catalog order.
* The abandoned pages are returned to the caller, not recorded durably in metadata.
* The member switch reports an offline suspicion to metadata on the client's own evidence;
  the member's next report restores it.
* Everything above was exercised against a single metadata server, not a metadata quorum.

## Tests

`tests/member_failure_append.rs` (run one test at a time):

| Test | Shows |
|---|---|
| `appends_continue_without_loss_or_duplicates_when_a_member_is_killed` | 30 appends through a SIGKILL, every one acknowledged once, object equals the acknowledged bytes, post-kill pages held by all three members of their group |
| `with_no_healthy_group_the_append_is_refused_cleanly_and_heals_on_return` | one group: refused before publication, manifest unchanged, works after the node returns |
| `a_stopped_node_leaves_the_roster_within_the_ttl_and_returns_on_its_next_report` | roster leave and return; placement never chooses the broken group |
| `the_metaserver_expires_silent_nodes_by_default_and_has_an_off_switch` | default and off switch |
| `a_returning_member_with_no_data_never_serves_wrong_bytes_and_is_resynced` | empty returning member, reads stay exact, repair restores every page |
| `measure_append_through_member_kill` (ignored) | the measurement script for before and after |
