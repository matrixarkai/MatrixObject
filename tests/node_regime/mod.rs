// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Reading whether a test is really addressing separate operating-system processes.
//!
//! # WHY THESE INSTRUMENTS ARE SHARED AND NOT COPIED
//!
//! Every test that claims to be distributed opens with the same six readings: distinct live
//! PIDs, none of them the test's own; an ESTABLISHED socket per node attributed to that
//! node's PID, read two independent ways and required to agree; and `/proc/<pid>/environ`
//! carrying this run's token before anything is killed.
//!
//! Those readings are the only thing standing between a distribution test and a test that
//! would pass identically against threads on one heap. Copied into each test file they would
//! be several instruments that agree today: a fix to the `ss -tnp` parser lands in one copy,
//! the other copy keeps reporting what it always reported, and the test with the stale copy
//! goes on passing -- which is the failure mode that matters, because a regime assertion that
//! has quietly stopped checking anything looks exactly like one that passes.
//!
//! So there is one copy, in a directory rather than a file so that cargo does not build it as
//! a test target of its own, and every node test includes it.
//!
//! # EVERY READING IS A REFUSAL, NOT A SKIP
//!
//! A missing `ss`, an unreadable `/proc`, a node that printed no startup line: each fails the
//! calling test. The alternative -- carrying on with one instrument, or skipping the regime
//! assertion when it cannot be made -- produces a green test whose claim was never checked.

// The two node tests use different subsets of this module, and the build denies warnings, so
// a helper used by one target and not the other would fail the build rather than the test it
// was written for.
#![allow(dead_code)]

/// The environment variable a replica node carries this run's token in.
pub const REPLICA_TOKEN_VAR: &str = "MATRIXOBJECT_REPLICA_TEST_TOKEN";

/// The environment variable a metaserver node carries this run's token in.
///
/// A DIFFERENT VARIABLE FROM THE REPLICA'S, on purpose. A test standing both kinds up has to
/// be able to say which process it is about to kill, and one shared variable would let a
/// confusion between the two pass the `/proc/<pid>/environ` check that exists to catch it.
pub const METASERVER_TOKEN_VAR: &str = "MATRIXOBJECT_METASERVER_TEST_TOKEN";

/// A token unique to this run, for confirming a PID belongs to this test before killing it.
///
/// Process id and a nanosecond clock. Killing by process NAME would be the easy way to end a
/// node and would also end a node belonging to a run happening alongside this one.
pub fn run_token(label: &str) -> String {
    format!(
        "{label}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default()
    )
}

pub mod processes {
    use std::io::{BufRead, BufReader, Read};
    use std::process::{Child, Command, Stdio};

    /// A node process a test started.
    pub struct NodeProcess {
        pub child: Child,
        pub pid: u32,
        pub endpoint: String,
        /// The whole startup line, so a test can assert the rest of the contract and not
        /// only the address. These binaries print what they bound, their pid, the protocol
        /// stamp, their bounds and what they are holding; a test that read only the port
        /// would be trusting the other six fields it never looked at.
        pub startup_line: String,
    }

    /// Reap the process when this goes out of scope, INCLUDING ON A PANIC.
    ///
    /// Found by running a node test against a deliberately broken store rather than by
    /// reading it: an assertion fires partway through, the cleanup at the end of the test
    /// body never runs, and the node processes are left holding their ports. Nine such runs
    /// left twenty-seven of them competing with the next run for the machine. A panic unwinds
    /// and runs `Drop`, so this is the cleanup that happens on the path that needs it.
    ///
    /// Errors are ignored on purpose: a test's own kill step is meant to have killed some of
    /// these already, and a destructor that panicked about an already-reaped child would
    /// replace the real failure with its own.
    impl Drop for NodeProcess {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// Wait for `child` to exit, for at most `within`. `None` means it was still running.
    ///
    /// Bounded, and not with `wait()`. `Child::wait` blocks until the process exits, which is
    /// correct after a signal that cannot be caught and an unbounded hang when the signal did
    /// not land. A test that hangs is worse in continuous integration than a test that fails:
    /// the job spends its whole budget and the log says nothing about why. This was measured,
    /// not imagined -- during development a deliberately removed `kill` turned the `wait()`
    /// that used to be here into an eighteen-minute hang with no output.
    pub fn reap_within(child: &mut Child, within: std::time::Duration) -> Option<i32> {
        let deadline = std::time::Instant::now() + within;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => return Some(status.code().unwrap_or(-1)),
                Ok(None) => {
                    if std::time::Instant::now() >= deadline {
                        return None;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Err(err) => panic!("a node process could not be reaped: {err}"),
            }
        }
    }

    /// Start one replica process on a kernel-chosen port and wait for it to say where.
    pub fn start_replica(binary: &str, token: &str) -> NodeProcess {
        start(
            binary,
            &[
                // PORT 0: the kernel picks, so parallel runs of a node test cannot collide
                // on a number either of them chose.
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_THREADS", "2"),
                (super::REPLICA_TOKEN_VAR, token),
            ],
        )
    }

    /// Start one metaserver process on a kernel-chosen port, with `seed_data_nodes` in its
    /// roster, and wait for it to say where.
    ///
    /// The seed is a bootstrap: placement needs a roster and registration is deliberately not
    /// one of the metaserver's verbs. The seeded nodes differ from each other -- one failure
    /// domain and one allocator shard index each -- so that a placement policy which spreads
    /// across domains is testable rather than trivially satisfied.
    pub fn start_metaserver(binary: &str, token: &str, seed_data_nodes: usize) -> NodeProcess {
        let seeds = seed_data_nodes.to_string();
        start(
            binary,
            &[
                ("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_METASERVER_THREADS", "2"),
                ("MATRIXOBJECT_METASERVER_SEED_DATA_NODES", seeds.as_str()),
                (super::METASERVER_TOKEN_VAR, token),
            ],
        )
    }

    /// Start a node binary with `env`, read its first line, and check the PID it printed.
    ///
    /// The startup line is a contract: these binaries print their bound address before they
    /// serve and flush it, because a test standing several of them up on port `0` has no
    /// other way to learn which ports the kernel chose.
    pub fn start(binary: &str, env: &[(&str, &str)]) -> NodeProcess {
        let mut command = Command::new(binary);
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|err| panic!("the node binary {binary} could be started: {err}"));
        let stdout = child.stdout.take().expect("the child's stdout is a pipe");
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("the node's first line could be read");
        if line.trim().is_empty() {
            let _ = child.kill();
            let status = reap_within(&mut child, std::time::Duration::from_secs(3));
            let mut diagnostic = String::new();
            if status.is_some() {
                if let Some(stderr) = child.stderr.take() {
                    let _ = stderr.take(8192).read_to_string(&mut diagnostic);
                }
            }
            panic!("node produced no startup line; exit={status:?}; stderr={diagnostic}");
        }
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
            "the node printed pid {reported_pid} and the spawned child is {}",
            child.id()
        );
        NodeProcess {
            pid: child.id(),
            child,
            endpoint,
            startup_line: line,
        }
    }

    /// The whitespace-delimited word that follows `key` in `line`.
    pub fn field(line: &str, key: &str) -> Option<String> {
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

    /// Whether this process's environment carries `token` under `variable`.
    ///
    /// Read from `/proc/<pid>/environ`, which is NUL-separated. This is the confirmation
    /// that stands between "kill a pid" and "kill a pid that is mine".
    pub fn environ_carries(pid: u32, variable: &str, token: &str) -> bool {
        let Ok(bytes) = std::fs::read(format!("/proc/{pid}/environ")) else {
            return false;
        };
        bytes
            .split(|byte| *byte == 0)
            .filter_map(|entry| std::str::from_utf8(entry).ok())
            .any(|entry| entry == format!("{variable}={token}"))
    }

    /// Confirm `pid` is this run's and then kill it, bounded.
    ///
    /// Both halves in one place because the order is the safety property: the environ check
    /// is useless after the signal.
    pub fn confirm_and_kill(
        node: &mut NodeProcess,
        variable: &str,
        token: &str,
        within: std::time::Duration,
    ) {
        let pid = node.pid;
        assert!(
            environ_carries(pid, variable, token),
            "/proc/{pid}/environ does not carry this run's token, so that process was not \
             started by this test and must not be killed"
        );
        assert_eq!(
            node.child.id(),
            pid,
            "the child handle about to be killed is not the PID that was confirmed"
        );
        node.child.kill().expect("the node could be killed");
        assert!(
            reap_within(&mut node.child, within).is_some(),
            "node {pid} was still running {:?} after it was killed, so any survivor claim \
             made after this point is a claim about a process that is still answering",
            within
        );
        assert!(
            !is_live(pid),
            "/proc/{pid} still exists after the kill, so the node is still running and a \
             read from 'the survivors' would include it"
        );
    }
}

pub mod sockets {
    use std::collections::BTreeSet;
    use std::process::Command;

    /// PIDs that `ss -tnp` reports as holding an ESTABLISHED TCP socket.
    ///
    /// The external tool the task of checking a regime is normally done with. It is run and
    /// its output parsed rather than trusted to exist: a missing `ss` fails the calling test,
    /// which is the right answer, because the claim rests on reading the kernel's own socket
    /// table and there is no honest way to proceed without one.
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
            // `users:(("chunkserver",pid=12345,fd=9))`
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
    /// second, independent reading of the same fact, and a node test requires it to agree
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

    /// Assert the full regime for `pids`: live, not the test's, socketed, read two ways.
    ///
    /// The whole point of the module, in one call, so that a node test cannot accidentally
    /// make four of the five readings.
    pub fn assert_established_two_ways(pids: &[u32]) {
        let ss_pids = established_pids_from_ss();
        for pid in pids {
            assert!(
                ss_pids.contains(pid),
                "ss -tnp reports no ESTABLISHED socket for node PID {pid}; without a socket \
                 between these processes the rest of this test proves nothing.\n\
                 ss -tnp said:\n{}",
                ss_listing()
            );
        }
        assert!(
            ss_pids.contains(&std::process::id()),
            "ss -tnp reports no ESTABLISHED socket for this test's own PID {}, so the \
             connections it thinks it holds are not in the kernel's table.\n\
             ss -tnp said:\n{}",
            std::process::id(),
            ss_listing()
        );
        // The same fact again, from /proc, with no external tool. The two readings must agree.
        for pid in pids {
            assert!(
                has_established_socket(*pid),
                "/proc/{pid}/fd holds no ESTABLISHED socket, although ss -tnp said it does; \
                 the two readings of the socket table disagree and neither can be trusted"
            );
        }
    }
}

#[path = "node_regime_checkpoints.rs"]
pub mod checkpoints;

#[cfg(target_os = "linux")]
#[path = "isolated_network_partitions.rs"]
pub mod network;

#[cfg(feature = "peer-tls")]
#[allow(dead_code)]
#[path = "node_regime_tls.rs"]
pub mod tls;
