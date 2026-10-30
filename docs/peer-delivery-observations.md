# Peer delivery observations

Metadata and chunk consensus timeout diagnostics include per-worker outbound delivery counters. These observations do not participate in admission, quorum, retry or replay decisions. The existing absolute deadline covers queue time, connection setup, transport handshake and frame writes.

- `peer_attempted`: frames offered to a known peer queue.
- `peer_queue_rejected`: offers rejected because a queue is full or disconnected.
- `peer_expired_before_connect`: dequeued frames whose absolute deadline was already exhausted. No connection is opened for them.
- `peer_connect_failed`: connection setup failures, including connection deadlines.
- `peer_write_failed`: failures after connecting, including transport setup, handshake or frame writes.
- `peer_sent`: complete local sends. This does not prove reception, acceptance, durable replication or acknowledgment.
- `peer_max_queue_ms`: largest observed age at dequeue, including expired frames.

Counters belong to one worker instance and reset on process restart. Relaxed atomic snapshots are diagnostic samples, not a simultaneous accounting identity. Values can include earlier elections, stopped members and shutdown; correlate changes with workload phase before assigning a cause. Kernel scheduling delays and failures before a connection is accepted require separate evidence.

An uncertain write remains uncertain regardless of these counters. In particular, zero sends observed in a timeout message does not authorize replay: the worker may progress immediately after the observation.

## Receiver observations

The same per-worker diagnostics report the inbound side:

- `peer_receive_accepted`: sockets accepted by this receiver.
- `peer_receive_expired`: frames discarded after their absolute receive deadline.
- `peer_receive_failed`: setup, transport, read or frame-validation failures other than observed expiry.
- `peer_inbox_rejected`: complete, unexpired frames rejected by the bounded worker inbox because it is full or disconnected.
- `peer_enqueued`: frames placed in the worker inbox. This does not prove that Raft accepted them or that a durable outcome committed.

Counters preserve the existing eight-frame inbox, sixteen active receive slots, frame/buffer limits and absolute deadlines. Accepted connections can remain in flight or be closed at shutdown, so the counters are not an accounting equality. A locally successful sender can still correspond to a receiver expiry or inbox rejection.

A local 67-process repetition failed during initial writes despite the affected chunk leader reporting 324 local sends, zero outgoing failures, a 2 ms maximum queue age and a 43 ms maximum completed worker phase. Two later repetitions passed. These observations justify inspecting the receive side; they do not identify a root cause or establish stable availability.
