// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Whether a replica of this store is really a separate operating-system process.
//!
//! # WHY THIS IS AN INTEGRATION TEST AND NOT A UNIT TEST
//!
//! The claim under test is about processes, so it cannot be made from inside one. These tests
//! run the `replica_server` binary -- the same one an operator starts -- and the only way a
//! test gets the path to a built binary is `env!("CARGO_BIN_EXE_...")`, which cargo defines
//! for integration tests and not for unit tests.
//!
//! # THE REGIME IS ASSERTED BEFORE ANYTHING ELSE IS
//!
//! A test that wrote bytes, killed something and read the bytes back would pass identically
//! against three replicas on one heap. So the first thing
//! [`three_replica_processes_keep_the_data_when_one_is_killed`] does is establish that it is
//! in the regime it is arguing about: three live PIDs that are not this test's, and an
//! ESTABLISHED socket per replica attributed to that replica's PID by the operating system.
//! It reads that twice from two independent instruments -- `ss -tnp` and `/proc` -- and
//! requires them to agree, because one instrument reporting what the test wants to hear is
//! how a comfortable lie gets written down.
//!
//! # LINUX ONLY, AND SAID SO RATHER THAN SKIPPED QUIETLY
//!
//! The regime assertions read `/proc` and the kernel socket table. There is no portable form
//! of that, so the kill test is compiled only on Linux. The protocol tests below it are
//! portable and are not gated.

#![cfg(feature = "replica-server")]

// The crate's own name, written once. Every other item below comes through this alias, the
// same way `src/bin/object_server.rs` names the crate once to reach `net`.
use matrixobjectstore_rs as store;

use store::cluster::{
    ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer, ReplicaNodeShutdown,
    ReplicaPlacement, RemoteReplicaSet, PROTOCOL_STAMP,
};
use store::{ChunkId, ObjectError, PageId};

use std::time::Duration;

// ---------------------------------------------------------------------------------------
// The corpus
// ---------------------------------------------------------------------------------------

/// How many pages the kill test writes and then demands back exactly.
const CORPUS_PAGES: u64 = 24;

/// Bytes per page.
const PAGE_BYTES: usize = 4096;

/// The page this key names.
fn corpus_page_id(index: u64) -> PageId {
    PageId {
        // Spread over volumes and chunks rather than one chunk's pages, so that a bug which
        // confused two fields of the address -- all three are `u64` on the wire -- cannot
        // land on the right page by coincidence.
        chunk_id: ChunkId::new(index % 3 + 1, index + 7).expect("a corpus chunk id fits"),
        page_index: index * 5 + 1,
    }
}

/// The bytes this key holds, distinct per key and not a constant fill.
///
/// A constant payload makes "zero wrong" unfalsifiable: every page would read back correctly
/// even if the store served the wrong one. The index is mixed into every byte.
fn corpus_payload(index: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PAGE_BYTES);
    for at in 0..PAGE_BYTES {
        payload.push(((index as usize)
            .wrapping_mul(31)
            .wrapping_add(at.wrapping_mul(7))
            % 251) as u8);
    }
    payload
}

/// A page the corpus never writes, for the negative control.
fn never_written_page_id() -> PageId {
    PageId {
        chunk_id: ChunkId::new(9, 999_999).expect("the control chunk id fits"),
        page_index: 424_242,
    }
}

// ---------------------------------------------------------------------------------------
// Standing replica processes up
// ---------------------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod processes {
    use std::io::{BufRead, BufReader};
    use std::process::{Child, Command, Stdio};

    /// A replica process this test started.
    pub struct ReplicaProcess {
        pub child: Child,
        pub pid: u32,
        pub endpoint: String,
    }

    /// Start one replica process on a kernel-chosen port and wait for it to say where.
    ///
    /// `token` is put in the environment so that the test can later read
    /// `/proc/<pid>/environ` and confirm the process it is about to kill is one it started.
    /// Killing by name would be the easy way to do this and would also kill a replica
    /// belonging to a run happening alongside it.
    pub fn start(binary: &str, token: &str) -> ReplicaProcess {
        let mut child = Command::new(binary)
            // PORT 0: the kernel picks, so parallel runs of this test cannot collide on a
            // number either of them chose.
            .env("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0")
            .env("MATRIXOBJECT_REPLICA_THREADS", "2")
            .env("MATRIXOBJECT_REPLICA_TEST_TOKEN", token)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the replica_server binary could be started");
        let stdout = child.stdout.take().expect("the child's stdout is a pipe");
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("the replica's first line could be read");
        assert!(
            !line.trim().is_empty(),
            "the replica process produced no startup line, so it died before it listened"
        );
        let endpoint = field(&line, "listening on ")
            .unwrap_or_else(|| panic!("no bound address in the startup line {line:?}"));
        let reported_pid: u32 = field(&line, "pid=")
            .unwrap_or_else(|| panic!("no pid in the startup line {line:?}"))
            .parse()
            .expect("the reported pid is a number");
        // THE PROCESS'S OWN PID AND THE ONE CARGO HANDED US MUST AGREE. They are read from
        // two places -- the child handle and the child's own `std::process::id()` -- and a
        // test that killed the wrong one of those would report a survival it never tested.
        assert_eq!(
            reported_pid,
            child.id(),
            "the replica printed pid {reported_pid} and the spawned child is {}",
            child.id()
        );
        ReplicaProcess {
            pid: child.id(),
            child,
            endpoint,
        }
    }

    /// The whitespace-delimited word that follows `key` in `line`.
    fn field(line: &str, key: &str) -> Option<String> {
        let at = line.find(key)? + key.len();
        Some(
            line[at..]
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string(),
        )
    }

    /// Whether `/proc/<pid>` names a live process.
    pub fn is_live(pid: u32) -> bool {
        std::path::Path::new(&format!("/proc/{pid}")).exists()
    }

    /// Whether this process's environment carries `token`.
    ///
    /// Read from `/proc/<pid>/environ`, which is NUL-separated. This is the confirmation
    /// that stands between "kill a pid" and "kill a pid that is mine".
    pub fn environ_carries(pid: u32, token: &str) -> bool {
        let Ok(bytes) = std::fs::read(format!("/proc/{pid}/environ")) else {
            return false;
        };
        bytes
            .split(|byte| *byte == 0)
            .filter_map(|entry| std::str::from_utf8(entry).ok())
            .any(|entry| entry == format!("MATRIXOBJECT_REPLICA_TEST_TOKEN={token}"))
    }
}

// ---------------------------------------------------------------------------------------
// Reading the operating system's socket table, twice, two ways
// ---------------------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod sockets {
    use std::collections::BTreeSet;
    use std::process::Command;

    /// PIDs that `ss -tnp` reports as holding an ESTABLISHED TCP socket.
    ///
    /// The external tool the task of checking a regime is normally done with. It is run and
    /// its output parsed rather than trusted to exist: a missing `ss` fails this test, which
    /// is the right answer, because the test's whole claim rests on reading the kernel's own
    /// socket table and there is no honest way to proceed without one.
    pub fn established_pids_from_ss() -> BTreeSet<u32> {
        let output = Command::new("ss")
            .args(["-tnp"])
            .output()
            .expect("ss is available; the regime assertion reads the kernel socket table");
        assert!(
            output.status.success(),
            "ss -tnp exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8_lossy(&output.stdout).to_string();
        let mut pids = BTreeSet::new();
        for line in text.lines() {
            if !line.starts_with("ESTAB") {
                continue;
            }
            // `users:(("replica_server",pid=12345,fd=9))`
            let mut rest = line;
            while let Some(at) = rest.find("pid=") {
                rest = &rest[at + 4..];
                let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                if let Ok(pid) = digits.parse::<u32>() {
                    pids.insert(pid);
                }
            }
        }
        pids
    }

    /// The whole `ss -tnp` listing, for a failure message that shows what was actually read.
    pub fn ss_listing() -> String {
        Command::new("ss")
            .args(["-tnp"])
            .output()
            .map(|out| String::from_utf8_lossy(&out.stdout).to_string())
            .unwrap_or_else(|err| format!("<ss could not be run: {err}>"))
    }

    /// Socket inodes in state ESTABLISHED, from `/proc/net/tcp` and `/proc/net/tcp6`.
    fn established_inodes() -> BTreeSet<u64> {
        let mut inodes = BTreeSet::new();
        for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
            let Ok(text) = std::fs::read_to_string(path) else {
                continue;
            };
            for line in text.lines().skip(1) {
                let columns: Vec<&str> = line.split_whitespace().collect();
                // sl local_address rem_address st ... uid timeout inode
                if columns.len() < 10 {
                    continue;
                }
                // `01` is TCP_ESTABLISHED.
                if columns[3] != "01" {
                    continue;
                }
                if let Ok(inode) = columns[9].parse::<u64>() {
                    inodes.insert(inode);
                }
            }
        }
        inodes
    }

    /// Whether `pid` holds a file descriptor for an ESTABLISHED socket.
    ///
    /// Derived from `/proc/<pid>/fd` and `/proc/net/tcp` with no external tool. This is the
    /// second, independent reading of the same fact, and the kill test requires it to agree
    /// with `ss`. Two instruments that agree is evidence; one instrument is a report.
    pub fn has_established_socket(pid: u32) -> bool {
        let established = established_inodes();
        let Ok(entries) = std::fs::read_dir(format!("/proc/{pid}/fd")) else {
            return false;
        };
        for entry in entries.flatten() {
            let Ok(target) = std::fs::read_link(entry.path()) else {
                continue;
            };
            let target = target.to_string_lossy().to_string();
            let Some(inner) = target
                .strip_prefix("socket:[")
                .and_then(|rest| rest.strip_suffix(']'))
            else {
                continue;
            };
            if let Ok(inode) = inner.parse::<u64>() {
                if established.contains(&inode) {
                    return true;
                }
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------------------
// The test that decides it
// ---------------------------------------------------------------------------------------

/// Three replica processes; kill one; every acknowledged byte is still readable.
///
/// The order of the assertions is the argument, so it is written out in the order it is
/// made:
///
/// 1. **the regime** -- three distinct live PIDs, none of them this test's, and an
///    ESTABLISHED socket per replica attributed to that replica's PID, read from `ss -tnp`
///    and from `/proc` and required to agree;
/// 2. **every write acknowledged** -- by all three, not merely by a quorum, so that what the
///    kill removes is known to have been present;
/// 3. **a kill by PID**, after confirming from `/proc/<pid>/environ` that the process belongs
///    to this test;
/// 4. **the data** -- all 24 pages read back from the survivors, exact count, zero missing,
///    zero wrong byte;
/// 5. **the negative control** -- a page never written reads absent, on the same survivors at
///    the same moment;
/// 6. **the quorum was real** -- a write needing all three is now refused.
#[cfg(target_os = "linux")]
#[test]
fn three_replica_processes_keep_the_data_when_one_is_killed() {
    let binary = env!("CARGO_BIN_EXE_replica_server");
    let token = format!(
        "kill-test-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default()
    );

    let mut nodes: Vec<processes::ReplicaProcess> = (0..3)
        .map(|_| processes::start(binary, &token))
        .collect();

    // ---------------------------------------------------------------- 1. THE REGIME
    let pids: Vec<u32> = nodes.iter().map(|node| node.pid).collect();
    let distinct: std::collections::BTreeSet<u32> = pids.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        3,
        "three replica processes must have three distinct PIDs, got {pids:?}"
    );
    assert!(
        !distinct.contains(&std::process::id()),
        "a replica PID is this test's own PID {}, so a replica is not a separate process",
        std::process::id()
    );
    for pid in &pids {
        assert!(
            processes::is_live(*pid),
            "/proc/{pid} does not exist, so that replica is not running"
        );
    }

    let endpoints: Vec<String> = nodes.iter().map(|node| node.endpoint.clone()).collect();
    let mut set = RemoteReplicaSet::connect(
        &endpoints,
        // A WRITE QUORUM OF THREE. Every write is acknowledged by every replica, so after
        // one is killed there is no page whose presence on the survivors is a matter of
        // which replicas the quorum happened to include.
        3,
        2,
        &ReplicaClientOptions {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(20),
            write_timeout: Duration::from_secs(20),
            ..ReplicaClientOptions::default()
        },
    )
    .expect("three replica processes could be connected");

    // Every replica answers before anything is claimed about the set.
    let pongs = set.ping_all();
    assert_eq!(
        pongs,
        vec![Some(0), Some(0), Some(0)],
        "all three replicas must answer and start empty"
    );

    // The sockets, from `ss -tnp`, with PID attribution.
    let ss_pids = sockets::established_pids_from_ss();
    for pid in &pids {
        assert!(
            ss_pids.contains(pid),
            "ss -tnp reports no ESTABLISHED socket for replica PID {pid}; without a socket \
             between these processes the rest of this test proves nothing.\n\
             ss -tnp said:\n{}",
            sockets::ss_listing()
        );
    }
    assert!(
        ss_pids.contains(&std::process::id()),
        "ss -tnp reports no ESTABLISHED socket for this test's own PID {}, so the \
         connections it thinks it holds are not in the kernel's table.\nss -tnp said:\n{}",
        std::process::id(),
        sockets::ss_listing()
    );

    // The same fact again, from /proc, with no external tool. The two readings must agree.
    for pid in &pids {
        assert!(
            sockets::has_established_socket(*pid),
            "/proc/{pid}/fd holds no ESTABLISHED socket, although ss -tnp said it does; the \
             two readings of the socket table disagree and neither can be trusted"
        );
    }

    // Each connection's four-tuple names a replica's bound address as its peer.
    let connections = set.connections().expect("connection addresses are readable");
    assert_eq!(connections.len(), 3, "one connection per replica");
    for (index, (local, peer)) in connections.iter().enumerate() {
        assert_eq!(
            peer, &endpoints[index],
            "connection {index} has peer {peer}, not the replica's bound address {}",
            endpoints[index]
        );
        assert_ne!(
            local, peer,
            "connection {index} has the same address on both ends, which is not a connection \
             between two processes"
        );
    }

    // ------------------------------------------------- 2. EVERY WRITE ACKNOWLEDGED BY THREE
    for index in 0..CORPUS_PAGES {
        let acknowledged = set
            .write_chunk(&corpus_page_id(index), 0, &corpus_payload(index))
            .unwrap_or_else(|err| panic!("corpus page {index} was refused: {err}"));
        assert_eq!(
            acknowledged, 3,
            "corpus page {index} was acknowledged by {acknowledged} replica(s), not all three"
        );
    }
    let pongs = set.ping_all();
    assert_eq!(
        pongs,
        vec![
            Some(CORPUS_PAGES),
            Some(CORPUS_PAGES),
            Some(CORPUS_PAGES)
        ],
        "each replica must hold all {CORPUS_PAGES} pages before one of them is killed"
    );

    // ------------------------------------------------------------------- 3. KILL ONE, BY PID
    let doomed_pid = nodes[0].pid;
    assert!(
        processes::environ_carries(doomed_pid, &token),
        "/proc/{doomed_pid}/environ does not carry this run's token, so that process was not \
         started by this test and must not be killed"
    );
    assert_eq!(
        nodes[0].child.id(),
        doomed_pid,
        "the child handle about to be killed is not the PID that was confirmed"
    );
    nodes[0].child.kill().expect("the replica could be killed");
    nodes[0]
        .child
        .wait()
        .expect("the killed replica could be reaped");

    assert!(
        !processes::is_live(doomed_pid),
        "/proc/{doomed_pid} still exists after the kill, so the replica is still running and \
         a read from 'the survivors' would include it"
    );
    for pid in &pids[1..] {
        assert!(
            processes::is_live(*pid),
            "surviving replica {pid} is gone too; the kill was not confined to one process"
        );
    }
    assert!(
        !sockets::established_pids_from_ss().contains(&doomed_pid),
        "ss -tnp still attributes an ESTABLISHED socket to the killed PID {doomed_pid}"
    );

    // -------------------------------------------------- 4. THE DATA, FROM THE SURVIVORS ONLY
    let mut recovered = 0usize;
    let mut missing: Vec<u64> = Vec::new();
    let mut wrong: Vec<u64> = Vec::new();
    for index in 0..CORPUS_PAGES {
        match set.read_chunk(&corpus_page_id(index), 0, PAGE_BYTES as u64) {
            Ok(read) => {
                if read.data == corpus_payload(index) {
                    recovered += 1;
                } else {
                    wrong.push(index);
                }
            }
            Err(_) => missing.push(index),
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {CORPUS_PAGES} acknowledged page(s) could not be read from the survivors: {missing:?}",
        missing.len()
    );
    assert!(
        wrong.is_empty(),
        "{} page(s) read back with the wrong bytes: {wrong:?}",
        wrong.len()
    );
    assert_eq!(
        recovered as u64, CORPUS_PAGES,
        "expected exactly {CORPUS_PAGES} pages back from the survivors, got {recovered}"
    );

    // ------------------------------------------------------------- 5. THE NEGATIVE CONTROL
    // Same set, same moment, same survivors. Without this the 24 hits above are consistent
    // with a read path that returns something for every address it is given.
    let control = set.read_chunk(&never_written_page_id(), 0, PAGE_BYTES as u64);
    match control {
        Err(ObjectError::NotFound(_)) => {}
        Err(other) => panic!(
            "a page that was never written must read as absent, and this read failed for a \
             different reason instead: {other}"
        ),
        Ok(read) => panic!(
            "a page that was never written returned {} byte(s), so the hits above are not \
             evidence of anything",
            read.data.len()
        ),
    }
    let control_head = set.head_chunk(&never_written_page_id());
    assert!(
        matches!(control_head, Err(ObjectError::NotFound(_))),
        "a page that was never written must have no head either, got {control_head:?}"
    );

    // ---------------------------------------------------------- 6. THE QUORUM WAS NOT A SHAM
    // A write needing all three cannot be met now. If this succeeded, the acknowledgement
    // count in step 2 was not counting replicas.
    let after = set.write_chunk(&corpus_page_id(CORPUS_PAGES + 1), 0, b"after the kill");
    assert!(
        after.is_err(),
        "a write needing all three replicas succeeded with one replica dead, so the write \
         quorum is not being enforced: {after:?}"
    );

    for node in nodes.iter_mut().skip(1) {
        let _ = node.child.kill();
        let _ = node.child.wait();
    }
}

// ---------------------------------------------------------------------------------------
// Protocol tests. These do NOT make a claim about processes and are named so.
// ---------------------------------------------------------------------------------------

/// Stand a replica node up on a thread in this process, for testing the wire format only.
///
/// Named `in_this_process` because it is not a distribution test and must never be mistaken
/// for one: the node here shares this test's address space, which is exactly the arrangement
/// the kill test above exists to distinguish itself from.
fn node_in_this_process() -> (String, ReplicaNodeShutdown) {
    let server = ReplicaNodeServer::bind(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            worker_threads: 2,
            ..ReplicaNodeOptions::default()
        },
    )
    .expect("a replica node binds loopback on a kernel-chosen port");
    let endpoint = server.local_addr().expect("the bound address").to_string();
    let shutdown = server.shutdown_handle();
    std::thread::spawn(move || {
        let _ = server.serve();
    });
    (endpoint, shutdown)
}

/// The two placements read the same bytes back from the same corpus.
///
/// The quorum rule is written twice -- once over method calls, once over sockets -- so this
/// is the guard that the second one is the same rule. A comment saying they match would not
/// fail when they stop matching.
#[test]
fn the_two_placements_read_back_the_same_bytes() {
    let (endpoint_a, stop_a) = node_in_this_process();
    let (endpoint_b, stop_b) = node_in_this_process();
    let (endpoint_c, stop_c) = node_in_this_process();

    let in_process = ReplicaPlacement::InProcess {
        replica_count: 3,
        write_quorum: 3,
        read_quorum: 2,
    };
    let remote = ReplicaPlacement::Remote {
        endpoints: vec![endpoint_a, endpoint_b, endpoint_c],
        write_quorum: 3,
        read_quorum: 2,
    };
    assert!(!in_process.is_remote(), "the in-process arm is not remote");
    assert!(remote.is_remote(), "the remote arm is remote");
    assert_eq!(
        ReplicaPlacement::default(),
        ReplicaPlacement::InProcess {
            replica_count: 3,
            write_quorum: 2,
            read_quorum: 2,
        },
        "the default placement must be the in-process arrangement this store has always had"
    );

    let options = ReplicaClientOptions::default();
    let mut local_fanout = in_process.open(&options).expect("the in-process arm opens");
    let mut remote_fanout = remote.open(&options).expect("the remote arm opens");
    assert!(!local_fanout.is_remote());
    assert!(remote_fanout.is_remote());

    for index in 0..8u64 {
        let page_id = corpus_page_id(index);
        let payload = corpus_payload(index);
        assert_eq!(
            local_fanout
                .write_chunk(&page_id, 0, &payload)
                .expect("the in-process arm accepts the write"),
            3
        );
        assert_eq!(
            remote_fanout
                .write_chunk(&page_id, 0, &payload)
                .expect("the remote arm accepts the write"),
            3
        );
    }
    for index in 0..8u64 {
        let page_id = corpus_page_id(index);
        let local = local_fanout
            .read_chunk(&page_id, 0, PAGE_BYTES as u64)
            .expect("the in-process arm reads back");
        let remote_read = remote_fanout
            .read_chunk(&page_id, 0, PAGE_BYTES as u64)
            .expect("the remote arm reads back");
        assert_eq!(
            local.data, remote_read.data,
            "page {index} read back differently through the two placements"
        );
        assert_eq!(
            local.write_sequence, remote_read.write_sequence,
            "page {index} has a different write sequence through the two placements"
        );
        assert_eq!(
            local.checksum, remote_read.checksum,
            "page {index} has a different checksum through the two placements"
        );
        assert_eq!(
            local_fanout
                .head_chunk(&page_id)
                .expect("the in-process arm heads the page"),
            remote_fanout
                .head_chunk(&page_id)
                .expect("the remote arm heads the page"),
            "page {index} has a different head through the two placements"
        );
    }

    // The absent case, through both arms, so that the two agree about a miss as well as a hit.
    let control = never_written_page_id();
    assert!(
        matches!(
            local_fanout.read_chunk(&control, 0, 16),
            Err(ObjectError::NotFound(_))
        ),
        "the in-process arm must report an unwritten page absent"
    );
    assert!(
        matches!(
            remote_fanout.read_chunk(&control, 0, 16),
            Err(ObjectError::NotFound(_))
        ),
        "the remote arm must report an unwritten page absent"
    );

    stop_a.stop();
    stop_b.stop();
    stop_c.stop();
}

/// A frame carrying a protocol stamp this build does not speak is refused by name.
///
/// The case this protects is a rolling restart, where a new coordinator reaches an old
/// replica. A reader that parsed the frame anyway would take its length field from the wrong
/// offset and allocate whatever it found there.
#[test]
fn a_frame_with_an_unknown_protocol_stamp_is_refused_by_name() {
    use std::io::{Read, Write};

    let (endpoint, stop) = node_in_this_process();
    let mut stream =
        std::net::TcpStream::connect(&endpoint).expect("a raw connection to the replica");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    // A stamp that is not this build's, a `Ping` code, and an empty payload. Everything
    // except the stamp is valid, so a refusal can only be about the stamp.
    let wrong_stamp = PROTOCOL_STAMP.wrapping_add(7);
    stream
        .write_all(&[wrong_stamp, 5, 0, 0, 0, 0])
        .expect("the frame was sent");
    stream.flush().expect("the frame was flushed");

    let mut header = [0u8; 6];
    stream
        .read_exact(&mut header)
        .expect("the replica answered rather than dropping the connection");
    assert_eq!(
        header[0], PROTOCOL_STAMP,
        "the refusal must itself carry this build's stamp"
    );
    assert_eq!(
        header[1], 3,
        "an unknown stamp is a precondition failure (status 3), got status {}",
        header[1]
    );
    let payload_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).expect("the message");
    let message = String::from_utf8_lossy(&payload).to_string();
    assert!(
        message.contains(&wrong_stamp.to_string())
            && message.contains(&PROTOCOL_STAMP.to_string()),
        "the refusal must name both stamps so an operator can see which two versions are \
         talking, got {message:?}"
    );

    stop.stop();
}

/// A frame declaring more payload than the bound is refused before the bytes are read.
///
/// The length field arrives before the payload, so without this bound those four bytes choose
/// how much memory the replica process asks the allocator for.
#[test]
fn a_payload_past_the_declared_bound_is_refused_before_it_is_allocated() {
    use std::io::{Read, Write};

    let server = ReplicaNodeServer::bind(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            // A small bound, so the test can declare a payload past it without asking for a
            // real allocation of any size.
            max_payload_bytes: 1024,
            worker_threads: 1,
            ..ReplicaNodeOptions::default()
        },
    )
    .expect("a replica node binds");
    let endpoint = server.local_addr().expect("the bound address").to_string();
    let stop = server.shutdown_handle();
    std::thread::spawn(move || {
        let _ = server.serve();
    });

    let mut stream =
        std::net::TcpStream::connect(&endpoint).expect("a raw connection to the replica");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    let mut frame = vec![PROTOCOL_STAMP, 1];
    frame.extend_from_slice(&(4_000_000_000u32).to_be_bytes());
    // The declared payload is NOT sent. If the replica were reading before checking, it
    // would block here for its read timeout rather than answering.
    stream.write_all(&frame).expect("the header was sent");
    stream.flush().expect("the header was flushed");

    let mut header = [0u8; 6];
    stream
        .read_exact(&mut header)
        .expect("the replica refused promptly rather than waiting for four billion bytes");
    assert_eq!(
        header[1], 2,
        "an over-long payload is an invalid argument (status 2), got status {}",
        header[1]
    );
    let payload_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).expect("the message");
    let message = String::from_utf8_lossy(&payload).to_string();
    assert!(
        message.contains("4000000000") && message.contains("1024"),
        "the refusal must name both the declared size and the bound, got {message:?}"
    );

    stop.stop();
}
