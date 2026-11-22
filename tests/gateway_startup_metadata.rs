// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(
    target_os = "linux",
    feature = "metadata-consensus",
    feature = "http-server"
))]
mod node_regime;
use node_regime::processes::{self, NodeProcess};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};

fn startup_case(restore_quorum: bool) {
    let token = node_regime::run_token("gateway-startup");
    let root = std::env::temp_dir().join(&token);
    std::fs::create_dir(&root).unwrap();
    let mut reserved: Vec<_> = (0..3)
        .map(|_| {
            Some((
                TcpListener::bind("127.0.0.1:0").unwrap(),
                TcpListener::bind("127.0.0.1:0").unwrap(),
            ))
        })
        .collect();
    let apis: Vec<_> = reserved
        .iter()
        .map(|p| p.as_ref().unwrap().0.local_addr().unwrap().to_string())
        .collect();
    let peers = reserved
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{}@{}", i + 1, p.as_ref().unwrap().1.local_addr().unwrap()))
        .collect::<Vec<_>>()
        .join(",");
    // Unstarted voters must refuse TCP rather than accept into an unserved backlog.
    drop(reserved[1].take());
    drop(reserved[2].take());
    let mut start = |i: usize| {
        std::fs::create_dir_all(root.join(i.to_string())).unwrap();
        drop(reserved[i].take());
        processes::start(
            env!("CARGO_BIN_EXE_metaserver"),
            &[
                ("MATRIXOBJECT_METASERVER_ADDR", &apis[i]),
                ("MATRIXOBJECT_METASERVER_THREADS", "2"),
                ("MATRIXOBJECT_METASERVER_RAFT_PEERS", &peers),
                ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", &(i + 1).to_string()),
                (
                    "MATRIXOBJECT_METASERVER_RAFT_STATE_DIR",
                    root.join(i.to_string()).to_str().unwrap(),
                ),
                ("MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS", "200"),
                (node_regime::METASERVER_TOKEN_VAR, &token),
            ],
        )
    };
    let mut nodes = vec![start(0)];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let mut command = Command::new(env!("CARGO_BIN_EXE_object_server"));
    command
        .env_clear()
        .envs(std::env::vars().filter(|(k, _)| !k.starts_with("MATRIXOBJECT_")));
    let child = command
        .env("MATRIXOBJECT_HTTP_ADDR", addr.to_string())
        .env("MATRIXOBJECT_CLUSTER_META", apis.join(","))
        .env("MATRIXOBJECT_CLUSTER_META_GROUP", "1")
        .env(
            "MATRIXOBJECT_CLUSTER_TIMEOUT_MS",
            if restore_quorum { "10000" } else { "1500" },
        )
        .env("MATRIXOBJECT_THREADS", "2")
        .env("MATRIXOBJECT_BUCKET", "temporalstore")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut door = NodeProcess {
        pid: child.id(),
        child,
        endpoint: addr.to_string(),
        startup_line: String::new(),
    };
    let (send, receive) = mpsc::channel();
    let stdout = door.child.stdout.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        let mut output = BufReader::new(stdout);
        output.read_line(&mut line).unwrap();
        let _ = send.send(line);
        std::io::copy(&mut output, &mut std::io::sink()).unwrap();
    });
    // A voter is listening, but no majority can answer the startup read.
    if restore_quorum {
        std::thread::sleep(Duration::from_secs(2));
        assert!(
            door.child.try_wait().unwrap().is_none(),
            "door abandoned the metadata election before its operation deadline"
        );
        assert!(
            TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err(),
            "door listened without a metadata quorum"
        );
        nodes.push(start(1));
        nodes.push(start(2));
        let line = receive.recv_timeout(Duration::from_secs(12)).unwrap();
        assert!(line.contains("object_server listening on"), "{line:?}");
        assert!(line.contains(&format!("pid={}", door.pid)), "{line}");
        let startup_observed = std::time::Instant::now();
        // The startup deadline must not become a permanent deadline on runtime connections.
        for after_startup_deadline in [false, true] {
            if after_startup_deadline {
                std::thread::sleep(
                    Duration::from_millis(10_200).saturating_sub(startup_observed.elapsed()),
                );
            }
            let mut stream = TcpStream::connect(addr).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            stream
                .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        }
    } else {
        assert_eq!(
            processes::reap_within(&mut door.child, Duration::from_secs(5)),
            Some(1)
        );
        assert!(receive
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_empty());
        let mut message = String::new();
        door.child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut message)
            .unwrap();
        assert!(message.contains("metadata"), "{message}");
        assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err());
    }
    drop(door);
    reader.join().unwrap();
    drop(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_gateway_waits_for_metadata_election_before_listening() {
    startup_case(true);
}

#[test]
fn a_gateway_without_metadata_quorum_stops_at_its_startup_deadline() {
    startup_case(false);
}
