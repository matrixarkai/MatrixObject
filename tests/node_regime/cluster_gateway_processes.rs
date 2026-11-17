// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The S3 gateway over a real cluster: three metadata voters and three chunk servers, each its
//! own process, and `object_server` in cluster mode in front of them. Everything goes over HTTP.
use super::*;
use matrixobjectstore_rs::cluster::{
    ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity,
};
use std::io::Read;

struct Cluster {
    metadata: Group,
    _chunks: Vec<NodeProcess>,
    gateway: NodeProcess,
    directory: PathBuf,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn http(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("no response head from {addr} for {method} {path}"));
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    (status, headers, raw[split + 4..].to_vec())
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate == name)
        .map(|(_, value)| value.as_str())
}

impl Cluster {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(node_regime::run_token("cluster-gateway"));
        std::fs::create_dir(&directory).unwrap();
        let chunks: Vec<NodeProcess> = (0..3)
            .map(|id| {
                processes::start(
                    env!("CARGO_BIN_EXE_chunkserver"),
                    &[
                        ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                        (
                            "MATRIXOBJECT_REPLICA_JOURNAL",
                            directory.join(format!("{id}.journal")).to_str().unwrap(),
                        ),
                        ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "268435456"),
                        ("MATRIXOBJECT_REPLICA_MAX_PAGES", "65536"),
                    ],
                )
            })
            .collect();
        let members: Vec<String> = (0..3).map(|id| format!("gateway-member-{id}")).collect();
        let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
            placement_id: "gateway-group".into(),
            failure_domain: "local-domain".into(),
            identity: Some(ChunkGroupIdentity {
                version: 3,
                group_id: 1,
                node_id: 1,
                capacity_bytes: 1 << 28,
                max_pages: 65536,
                voters: vec![1, 2, 3],
                peer_endpoints: vec![],
                bootstrap_voters: vec![],
                bootstrap_id: 0,
            }),
            api_endpoints: chunks.iter().map(|node| node.endpoint.clone()).collect(),
            member_ids: members.clone(),
        }])
        .unwrap();
        let metadata = Group::configured_with_timeout(
            None,
            None,
            Some(catalog.to_bytes().unwrap()),
            0,
            false,
            5000,
        );
        let group_id = metadata
            .client(metadata.leader())
            .metadata_group_identity()
            .unwrap()
            .group_id;
        let endpoints: Vec<String> = metadata
            .nodes
            .iter()
            .map(|node| node.as_ref().unwrap().endpoint.clone())
            .collect();
        let mut client = RemoteMetaClient::connect_raft_catalog(
            &endpoints,
            group_id,
            &MetaClientOptions {
                connect_timeout: Duration::from_secs(3),
                read_timeout: Duration::from_secs(30),
                write_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        )
        .unwrap();
        for member in &members {
            client
                .heartbeat_placement_node(member, 0, 1 << 28, false, true, 100)
                .unwrap();
        }
        let gateway = processes::start(
            env!("CARGO_BIN_EXE_object_server"),
            &[
                ("MATRIXOBJECT_HTTP_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_BUCKET", "gateway"),
                ("MATRIXOBJECT_THREADS", "4"),
                ("MATRIXOBJECT_CLUSTER_META", &endpoints.join(",")),
                ("MATRIXOBJECT_CLUSTER_META_GROUP", &group_id.to_string()),
                ("MATRIXOBJECT_CLUSTER_TIMEOUT_MS", "20000"),
            ],
        );
        Self {
            metadata,
            _chunks: chunks,
            gateway,
            directory,
        }
    }
    fn addr(&self) -> &str {
        &self.gateway.endpoint
    }
    /// Retry a request while the cluster is between leaders; a refusal is only final once the
    /// deadline passes.
    fn until_ok(&self, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> u16 {
        let until = Instant::now() + Duration::from_secs(40);
        loop {
            let (status, _, reply) = http(self.addr(), method, path, headers, body);
            if status < 300 || Instant::now() > until {
                assert!(status < 300, "{method} {path} ended at {status}: {}", String::from_utf8_lossy(&reply));
                return status;
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    }
}

#[test]
fn the_gateway_serves_its_verbs_over_real_metadata_and_chunk_processes() {
    let cluster = Cluster::new();
    let addr = cluster.addr();
    let health = http(addr, "GET", "/healthz", &[], b"");
    assert_eq!(health.0, 200);

    // whole objects
    let payload: Vec<u8> = (0..300_000u32).map(|i| (i * 7) as u8).collect();
    assert_eq!(cluster.until_ok("PUT", "/gateway/blob", &[], &payload), 200);
    let (status, headers, body) = http(addr, "GET", "/gateway/blob", &[], b"");
    assert_eq!((status, body == payload), (200, true));
    assert_eq!(header(&headers, "x-amz-object-size"), Some("300000"));
    let (status, headers, body) =
        http(addr, "GET", "/gateway/blob", &[("range", "bytes=1000-1999")], b"");
    assert_eq!(status, 206);
    assert_eq!(body, payload[1000..2000]);
    assert_eq!(header(&headers, "content-range"), Some("bytes 1000-1999/300000"));
    let (status, headers, _) = http(addr, "HEAD", "/gateway/blob", &[], b"");
    assert_eq!((status, header(&headers, "x-amz-object-size")), (200, Some("300000")));
    assert_eq!(http(addr, "GET", "/gateway/blob", &[("range", "bytes=300000-300010")], b"").0, 416);

    // appends land at the end or are refused
    for (offset, text) in [(0u64, "alpha"), (5, "beta"), (9, "gamma")] {
        let (status, headers, _) = http(
            addr,
            "PUT",
            "/gateway/log",
            &[("x-amz-write-offset-bytes", &offset.to_string())],
            text.as_bytes(),
        );
        assert_eq!(status, 200, "append at {offset}");
        assert_eq!(
            header(&headers, "x-amz-object-size"),
            Some((offset + text.len() as u64).to_string().as_str())
        );
    }
    assert_eq!(
        http(addr, "PUT", "/gateway/log", &[("x-amz-write-offset-bytes", "3")], b"x").0,
        412
    );
    assert_eq!(http(addr, "GET", "/gateway/log", &[], b"").2, b"alphabetagamma");

    // listing, delete, and the verbs that are not served
    let (_, _, listing) = http(addr, "GET", "/gateway?list-type=2", &[], b"");
    let listing = String::from_utf8_lossy(&listing).to_string();
    assert!(listing.contains("<Key>blob</Key>") && listing.contains("<Key>log</Key>"), "{listing}");
    assert_eq!(http(addr, "DELETE", "/gateway/blob", &[], b"").0, 204);
    assert_eq!(http(addr, "GET", "/gateway/blob", &[], b"").0, 404);
    let (_, _, listing) = http(addr, "GET", "/gateway?list-type=2", &[], b"");
    assert!(!String::from_utf8_lossy(&listing).contains("<Key>blob</Key>"));
    assert_eq!(http(addr, "POST", "/gateway/log?uploads", &[], b"").0, 501);
    assert_eq!(http(addr, "GET", "/gateway?versioning", &[], b"").0, 501);
}

#[test]
fn the_gateway_keeps_serving_after_the_metadata_leader_is_lost() {
    let mut cluster = Cluster::new();
    assert_eq!(cluster.until_ok("PUT", "/gateway/survivor", &[], b"before"), 200);
    let leader = cluster.metadata.leader();
    cluster.metadata.kill(leader);
    // A new leader is elected; the gateway's pooled connection to the old one fails and it
    // reconnects through its metadata catalog.
    assert_eq!(cluster.until_ok("PUT", "/gateway/after", &[], b"after"), 200);
    let (status, _, body) = http(cluster.addr(), "GET", "/gateway/survivor", &[], b"");
    assert_eq!((status, body.as_slice()), (200, b"before".as_slice()));
    assert_eq!(
        cluster.until_ok(
            "PUT",
            "/gateway/survivor",
            &[("x-amz-write-offset-bytes", "6")],
            b"+more"
        ),
        200
    );
    assert_eq!(
        http(cluster.addr(), "GET", "/gateway/survivor", &[], b"").2,
        b"before+more"
    );
}
