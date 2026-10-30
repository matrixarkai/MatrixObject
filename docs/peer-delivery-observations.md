# Peer delivery observations

Metadata and chunk consensus timeout diagnostics include per-worker outbound delivery counters. These observations do not participate in admission, quorum, retry or replay decisions. The existing absolute deadline covers queue time, connection setup, transport handshake and frame writes.

- `peer_attempted`: frames offered to a known peer queue.
- `peer_queue_rejected`: offers rejected because a queue is full or disconnected.
- `peer_expired_before_connect`: dequeued frames whose absolute deadline was already exhausted. No connection is opened for them.
- `peer_connect_failed`: connection setup failures, including connection deadlines.
- `peer_write_failed`: failures after connecting, including transport setup, handshake or frame writes.
- `peer_sent`: complete local sends. This does not prove reception, acceptance, durable replication or acknowledgment.
- `peer_max_queue_ms`: largest observed age at dequeue, including expired frames.

Counters belong to one worker instance and reset on process restart. Relaxed atomic snapshots are diagnostic samples, not a simultaneous accounting identity. Values can include earlier elections, stopped members and shutdown; correlate changes with workload phase before assigning a cause. Receive-side drops and kernel scheduling delays require separate evidence.

An uncertain write remains uncertain regardless of these counters. In particular, zero sends observed in a timeout message does not authorize replay: the worker may progress immediately after the observation.
