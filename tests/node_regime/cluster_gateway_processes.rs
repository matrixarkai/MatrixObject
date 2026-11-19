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
    endpoints: Vec<String>,
    group_id: u64,
}

/// One more `object_server` in cluster mode over the same metadata group.
fn gateway_process(endpoints: &[String], group_id: u64, extra: &[(&str, &str)]) -> NodeProcess {
    let joined = endpoints.join(",");
    let group = group_id.to_string();
    let mut environment = vec![
        ("MATRIXOBJECT_HTTP_ADDR", "127.0.0.1:0"),
        ("MATRIXOBJECT_BUCKET", "gateway"),
        ("MATRIXOBJECT_THREADS", "4"),
        ("MATRIXOBJECT_CLUSTER_META", joined.as_str()),
        ("MATRIXOBJECT_CLUSTER_META_GROUP", group.as_str()),
        ("MATRIXOBJECT_CLUSTER_TIMEOUT_MS", "20000"),
    ];
    environment.extend_from_slice(extra);
    processes::start(env!("CARGO_BIN_EXE_object_server"), &environment)
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
        Self::with_env(&[])
    }
    fn meta_client(&self) -> RemoteMetaClient {
        RemoteMetaClient::connect_raft_catalog(
            &self.endpoints,
            self.group_id,
            &MetaClientOptions {
                connect_timeout: Duration::from_secs(3),
                read_timeout: Duration::from_secs(30),
                write_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn with_env(extra: &[(&str, &str)]) -> Self {
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
        let gateway = gateway_process(&endpoints, group_id, extra);
        Self {
            metadata,
            _chunks: chunks,
            gateway,
            directory,
            endpoints,
            group_id,
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

/// A metadata group configured to name at most eight objects, a gateway in front of it, real
/// processes and real HTTP. The ninth CREATE is a 507 that says the catalog is full (not a 503, not
/// "unconfirmed", not "corruption") and it is answered, not retried until a deadline; everything that
/// does not add a name still works. A delete keeps its slot (the catalog keeps tombstones so that a
/// recreation must name the deleted generation), so a delete does NOT make room for a different key.
#[test]
fn a_full_catalog_refuses_a_new_object_with_507_and_still_serves_what_exists() {
    OBJECT_CAP.with(|cap| cap.set(Some(8)));
    let cluster = Cluster::new();
    OBJECT_CAP.with(|cap| cap.set(None));
    let addr = cluster.addr();
    // Retry only while the cluster is between leaders (503); any other answer is final.
    let decided = |method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]| -> (u16, String) {
        let until = Instant::now() + Duration::from_secs(40);
        loop {
            let (status, _, reply) = http(addr, method, path, headers, body);
            if status != 503 || Instant::now() > until {
                return (status, String::from_utf8_lossy(&reply).to_string());
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    };
    let listed = || {
        let (_, _, listing) = http(addr, "GET", "/gateway?list-type=2", &[], b"");
        String::from_utf8_lossy(&listing).matches("<Key>").count()
    };
    let baseline = listed();
    let mut created = Vec::new();
    let refusal = loop {
        let key = format!("o{}", created.len());
        let started = Instant::now();
        let (status, text) = decided("PUT", &format!("/gateway/{key}"), &[("x-amz-write-offset-bytes", "0")], b"x");
        if status == 200 {
            created.push(key);
            assert!(created.len() <= 8, "the configured limit of 8 was not enforced");
        } else {
            assert!(started.elapsed() < Duration::from_secs(10), "the refusal took {:?}: it was retried, not answered", started.elapsed());
            break (status, text);
        }
    };
    assert_eq!(refusal.0, 507, "{}", refusal.1);
    assert!(refusal.1.contains("catalog full"), "{}", refusal.1);
    assert!(!refusal.1.contains("corruption") && !refusal.1.contains("unconfirmed"), "{}", refusal.1);
    assert_eq!(created.len() + baseline, 8, "{} created over {} that were there", created.len(), baseline);
    assert_eq!(listed(), 8, "the refused create left nothing behind");

    // What does not add a name is still served.
    assert_eq!(decided("PUT", "/gateway/o0", &[], b"overwritten").0, 200, "overwrite");
    assert_eq!(decided("PUT", "/gateway/o1", &[("x-amz-write-offset-bytes", "1")], b"y").0, 200, "append");
    assert_eq!(decided("GET", "/gateway/o0", &[], b"").1, "overwritten");
    assert_eq!(decided("DELETE", "/gateway/o2", &[], b"").0, 204, "delete");
    // ... and a delete keeps its slot: a different new key is still refused, the deleted key can come back.
    assert_eq!(decided("PUT", "/gateway/another", &[], b"z").0, 507, "a delete does not free a slot for another key");
    assert_eq!(decided("PUT", "/gateway/o2", &[], b"back").0, 200, "the deleted key is re-created");
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

fn sha256_hex(data: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, data)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn resolve_target(offset: u64, data: &[u8]) -> String {
    format!(
        "/gateway/log?append-resolve&offset={offset}&length={}&sha256={}",
        data.len(),
        sha256_hex(data)
    )
}

/// The resolve of an ambiguous append over a real cluster: bytes found, a lost attempt fenced and
/// refused when it finally arrives, and the retry as a new append stored once.
fn resolve_over_real_processes(cluster: &Cluster) {
    let addr = cluster.addr();
    for (offset, text) in [(0u64, "alpha"), (5, "beta")] {
        assert_eq!(
            cluster.until_ok("PUT", "/gateway/log", &[("x-amz-write-offset-bytes", &offset.to_string())], text.as_bytes()),
            200
        );
    }
    // The bytes are there: landed, and nothing is fenced.
    let (status, headers, _) = http(addr, "POST", &resolve_target(5, b"beta"), &[], b"");
    assert_eq!((status, header(&headers, "x-append-resolution")), (200, Some("landed")));
    // Nothing at offset 9: the resolve fences. Only the generation moves.
    let mut client = cluster.meta_client();
    let before = client.read_manifest("gateway", "log").unwrap();
    let (status, headers, body) = http(addr, "POST", &resolve_target(9, b"gamma"), &[], b"");
    assert_eq!(
        (status, header(&headers, "x-append-resolution")),
        (200, Some("absent")),
        "{}",
        String::from_utf8_lossy(&body)
    );
    let after = client.read_manifest("gateway", "log").unwrap();
    assert_eq!((after.generation, after.length), (before.generation + 1, 9));
    assert!(after.extents == before.extents, "the fence keeps the extents exactly");
    assert_eq!(http(addr, "GET", "/gateway/log", &[], b"").2, b"alphabeta");
    // The lost attempt, built on the manifest it read before the fence, is refused when it arrives.
    let options = matrixobjectstore_rs::cluster::ObjectWriteOptions::default();
    assert!(client.append_object(before, b"gamma", &options).is_err(), "a fenced attempt must not publish");
    assert_eq!(http(addr, "GET", "/gateway/log", &[], b"").2, b"alphabeta");
    // The caller retries as a new append: stored exactly once.
    assert_eq!(
        cluster.until_ok("PUT", "/gateway/log", &[("x-amz-write-offset-bytes", "9")], b"gamma"),
        200
    );
    assert_eq!(http(addr, "GET", "/gateway/log", &[], b"").2, b"alphabetagamma");
    let (_, headers, _) = http(addr, "POST", &resolve_target(9, b"gamma"), &[], b"");
    assert_eq!(header(&headers, "x-append-resolution"), Some("landed"));
}

#[test]
fn an_append_resolve_finds_landed_bytes_and_fences_a_lost_attempt_over_real_processes() {
    resolve_over_real_processes(&Cluster::new());
}

#[test]
fn an_append_resolve_fences_under_the_publication_fence_of_compact_appends() {
    resolve_over_real_processes(&Cluster::with_env(&[
        ("MATRIXOBJECT_CLUSTER_COMPACT_APPEND", "1"),
        ("MATRIXOBJECT_CLUSTER_RESERVE_NEXT", "1"),
    ]));
}

#[test]
fn killing_the_gateway_mid_append_then_resolving_through_another_stores_every_append_exactly_once() {
    let cluster = Cluster::new();
    let survivor = cluster.addr().to_string();
    let (mut offset, mut expected) = (0u64, Vec::<u8>::new());
    let mut random = 0x9e37_79b9_7f4a_7c15u64;
    for round in 0..8u64 {
        let data: Vec<u8> = (0..200_000u64).map(|i| (i ^ (round * 31)) as u8).collect();
        let mut victim = gateway_process(&cluster.endpoints, cluster.group_id, &[]);
        let victim_addr = victim.endpoint.clone();
        let (at, body) = (offset, data.clone());
        let sender = std::thread::spawn(move || {
            // The victim dies mid-request, so this panics on the missing response; that is the point.
            let _ = http(&victim_addr, "PUT", "/gateway/log", &[("x-amz-write-offset-bytes", &at.to_string())], &body);
        });
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        std::thread::sleep(Duration::from_millis((random >> 33) % 150));
        victim.child.kill().ok();
        let _ = sender.join();
        // Resolve through the survivor until it decides.
        let until = Instant::now() + Duration::from_secs(60);
        let resolution = loop {
            let (status, headers, reply) = http(&survivor, "POST", &resolve_target(offset, &data), &[], b"");
            if status == 200 {
                break header(&headers, "x-append-resolution").unwrap().to_string();
            }
            assert!(Instant::now() < until, "resolve never decided: {status} {}", String::from_utf8_lossy(&reply));
            std::thread::sleep(Duration::from_millis(300));
        };
        if resolution == "absent" {
            assert_eq!(
                cluster.until_ok("PUT", "/gateway/log", &[("x-amz-write-offset-bytes", &offset.to_string())], &data),
                200,
                "round {round}: the retry as a new append"
            );
        } else {
            assert_eq!(resolution, "landed", "round {round}");
        }
        expected.extend_from_slice(&data);
        offset += data.len() as u64;
        let (_, _, stored) = http(&survivor, "GET", "/gateway/log", &[], b"");
        assert!(stored == expected, "round {round}: {} bytes stored, {} expected ({resolution})", stored.len(), expected.len());
    }
}

