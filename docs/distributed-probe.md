# External quorum workload probe

`distributed_probe` connects to an existing metadata Raft group and uses its
data-group catalog to write and read complete objects. It starts no servers and
does not change membership or stop processes. The current transports require a
trusted isolated network; group identity checks are not authentication. The first
connection pins one metadata configuration for every worker and both phases of
that invocation. The next invocation discovers its configuration again.

Build with `cargo build --release --locked --all-features --bin distributed_probe`.
For example, against a test deployment with metadata group ID 1:

```sh
target/release/distributed_probe write \
  --metadata 10.0.1.10:17400,10.0.2.10:17400,10.0.3.10:17400 \
  --group 1 --bucket probe-bucket --run run-001 \
  --objects 1024 --bytes 1048576 --workers 8 --groups 2 --page-bytes 262144
```

`write` creates `probe/run-001/00000000` through `probe/run-001/00001023` and
then reads every object, comparing its complete deterministic payload and first
generation. It refuses existing objects instead of replacing them. Choose a new
run name for every write workload. A failure can leave committed objects or
unpublished candidate pages; the probe does not delete data or replay failed
mutations. Inspect uncertain outcomes before deciding what to do next.

After operator-controlled leader loss, recovery, or restart, run the same command
with `verify` instead of `write`. This mode only reads. Keep the bucket, run name,
object count and byte size unchanged. Payload generation is versioned as workload
version 1 in the output. Verification does not require the original worker count.
Use the same page and group settings when recording comparable workloads; those
settings affect writes, not the read-only byte comparison.

Each phase prints completed/requested counts, elapsed seconds, operations per
second and p50/p95/p99 latency in microseconds. Aggregate elapsed time includes
worker connections and payload generation, but excludes the initial identity
discovery. Individual operation latency excludes payload
generation and connection setup but includes read verification. Failed operations
are excluded from latency samples; a failed or incomplete phase exits nonzero and
must not be reported as a successful throughput result. Workers stop admitting new
objects after a failure, then join outstanding work. There is no automatic retry
loop, warmup, cleanup, or overall workload deadline.

Bounds are 1..100000 objects, 1..64 MiB per object, 1..64 workers (no more than the
object count), and at most 256 MiB of generated payloads across workers. Reads and
client internals allocate additional buffers, so this is not a process-memory cap.
Pages are 1..1 MiB and an object may use at most 4096 pages. `--groups` is the
maximum number of data groups selected per object, in 1..4096. The default page
size is 1 MiB. `--timeout-ms` defaults to 10000 and accepts 1..300000; it applies
to individual connect/read/write operations, not the complete object operation.
Metadata requires 1..7 concrete IP sockets and a nonzero group ID. Run names are
1..64 ASCII letters, digits, hyphens or underscores. Bucket names are 3..63
lowercase letters, digits or hyphens, beginning with a letter and ending with a
letter or digit. Invalid settings are rejected before connecting.

The CI process test uses two three-voter data groups and three metadata voters.
It checks writes, refused overwrite attempts, failed verification with a wrong
byte size, leader loss and full restart. That is single-host functional evidence.
Multi-host qualification still requires running this workload on the intended
deployment, recording host resources, replication settings, data size, machine
load, sustained duration and fault schedule alongside its output.

## WSL packet-partition simulation

Run `bash tools/network_partition_test.sh` from the local WSL checkout. The
wrapper builds the test executable, creates a private network namespace, enables
its loopback interface and runs the named test there. It needs Linux networking
tools and root access (or sudo). No host firewall rules or external resources are
used. The test refuses to install rules in PID 1's network namespace.

The nine-process fixture drops inbound Raft peer traffic to the current leader of
each of two data groups and the metadata group. This is a one-way partition:
client API ports remain reachable and the isolated voters may still send packets.
Kernel rule counters must show actual drops. All server processes must remain
alive while the isolated voters refuse quorum reads and the majorities verify
existing objects and publish new ones.

After removing the rules, the fixture stops the replacement leaders, forcing the
healed voters to participate in new metadata and data write quorums. It then
restarts every process and verifies all 36 objects written before, during and
after the partition. The ordinary leader-kill test remains separate. Neither
test establishes behavior under every network failure pattern or sustained load.


### Per-object network budget

`--timeout-ms` bounds each connection attempt and request/response frame.
`--operation-timeout-ms` bounds the combined metadata and data network work for
each object, including placement, allocation, page writes, verification, and
publication. It defaults to `--timeout-ms` and accepts 1..300000 milliseconds.
Each object receives a fresh absolute deadline; one object's elapsed time does
not consume the next object's budget. The selected value is printed as
`operation_timeout_ms` in the workload header.

Initial metadata catalog connection and identity discovery use the connection
and frame limits separately. Payload generation happens before the object timer.
This is not a deadline for the complete workload or a cancellation request to
servers. A timed-out write is never replayed; inspect any uncertain publication
before rerunning a write workload. Data catalog endpoints must be numeric IP
sockets when operation deadlines are enabled.
