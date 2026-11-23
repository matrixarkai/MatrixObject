// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Storage protocol integration probe with three separate replica processes.
//! Uses synthetic allocation/epoch context. It does not validate metadata activation,
//! object APIs, metadata pin acquisition, production readiness or performance.
use matrixobjectstore_rs::cluster::{
    AppendIdentity, ChunkLogError, ChunkLogReader, LogOpReply, LogOpRequest, RemoteReplicaClient,
    ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer,
};
use std::{
    error::Error,
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
const INITIALIZATION: [u8; 16] = [7; 16];
const VOLUME: u64 = 3;
const CHUNK: u64 = 11;
const GENERATION: u32 = 7;
fn require(value: bool, message: &str) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}
struct Peer {
    child: Child,
    addr: String,
    dir: PathBuf,
    limit: usize,
    stopped: bool,
}
impl Peer {
    fn start(dir: PathBuf, limit: usize) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let child = Command::new(std::env::current_exe()?)
            .arg("--node")
            .arg(&dir)
            .arg(limit.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut peer = Self {
            child,
            addr: String::new(),
            dir,
            limit,
            stopped: false,
        };
        std::fs::write(peer.dir.join("peer.pid"), peer.child.id().to_string())?;
        let output = peer
            .child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("missing child output"))?;
        let mut line = String::new();
        io::BufReader::new(output).read_line(&mut line)?;
        let addr = line
            .strip_prefix("NODE ")
            .ok_or_else(|| io::Error::other("replica did not report its address"))?
            .trim();
        let _: std::net::SocketAddr = addr.parse()?;
        peer.addr = addr.to_owned();
        Ok(peer)
    }
    fn call(&self, request: LogOpRequest) -> Result<LogOpReply> {
        let mut c = RemoteReplicaClient::connect(&self.addr, &ReplicaClientOptions::default())?;
        Ok(c.log_call(request).map_err(ChunkLogError::from)?)
    }
    fn probe(&self) -> Result<LogOpReply> {
        self.call(LogOpRequest::publication_probe(VOLUME, CHUNK, GENERATION))
    }
    fn stop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
            self.stopped = true;
        }
    }
    fn restart(&mut self) -> Result<()> {
        self.stop();
        *self = Self::start(self.dir.clone(), self.limit)?;
        Ok(())
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop();
    }
}
fn read_all(peer: &Peer, length: usize, capacity: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(length);
    while out.len() < length {
        let count = capacity.min(length - out.len());
        let r = peer.call(LogOpRequest::read_initialized_publication(
            VOLUME,
            CHUNK,
            GENERATION,
            out.len() as u64,
            count as u32,
            INITIALIZATION,
        ))?;
        require(
            r.publication_initialization() == Some(INITIALIZATION),
            "read initialization differs",
        )?;
        require(r.data.len() == count, "short published read")?;
        out.extend_from_slice(&r.data);
    }
    Ok(out)
}
fn run(root: &Path) -> Result<()> {
    require(!root.exists(), "probe requires a fresh directory")?;
    std::fs::create_dir_all(root)?;
    let mut peers: Vec<Peer> = [512, 1024, 4096]
        .iter()
        .enumerate()
        .map(|(i, n)| Peer::start(root.join(format!("node{i}")), *n))
        .collect::<Result<_>>()?;
    let pids: std::collections::BTreeSet<_> = peers.iter().map(|p| p.child.id()).collect();
    require(pids.len() == 3, "replicas are not distinct processes")?;
    let mut capacity = usize::MAX;
    for peer in &peers {
        let r = peer.probe()?;
        require(
            r.has_publication_write_protocol(),
            "writable version4 unsupported",
        )?;
        require(!r.chunk_present, "new member unexpectedly exists")?;
        require(
            r.publication_payload_limit == peer.limit as u64,
            "frame advertisement differs",
        )?;
        capacity = capacity.min(
            r.publication_record_capacity()
                .ok_or_else(|| io::Error::other("no record capacity"))?,
        );
    }
    require(capacity == 256, "unexpected common record capacity")?;
    for peer in &peers {
        let r = peer.call(LogOpRequest::initialize_publication(
            VOLUME,
            CHUNK,
            GENERATION,
            1,
            INITIALIZATION,
        ))?;
        require(
            r.publication_initialization() == Some(INITIALIZATION),
            "initialization mismatch",
        )?;
        require(
            r.initialized_publication_prefix(INITIALIZATION)
                .is_some_and(|p| p.length == 0),
            "genesis prefix differs",
        )?;
    }
    let data: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let attempt = AppendIdentity {
        object_incarnation: [1; 16],
        attempt_id: [2; 16],
        request_offset: 0,
        request_length: data.len() as u64,
    };
    let pieces: Vec<&[u8]> = data.chunks(capacity).collect();
    for (index, piece) in pieces.iter().enumerate() {
        let offset = (index * capacity) as u64;
        let tag = attempt.record_tag(
            VOLUME,
            CHUNK,
            GENERATION,
            offset,
            offset,
            piece.len() as u32,
        )?;
        let last = index + 1 == pieces.len();
        let published = if last { data.len() as u64 } else { 0 };
        for peer in &peers[..if last { 1 } else { 3 }] {
            let r = peer.call(LogOpRequest::append_published(
                VOLUME,
                CHUNK,
                GENERATION,
                1,
                offset,
                piece.to_vec(),
                tag,
                published,
                INITIALIZATION,
            ))?;
            require(
                r.published_length == published,
                "append publication differs",
            )?;
        }
    }
    let before: Vec<u64> = peers
        .iter()
        .map(|p| p.probe().map(|r| r.published_length))
        .collect::<Result<_>>()?;
    require(
        before == vec![4096, 0, 0],
        "lost-final-record setup differs",
    )?;
    for (index, peer) in peers.iter().enumerate() {
        let r = peer.call(LogOpRequest::fence_publication(
            VOLUME,
            CHUNK,
            GENERATION,
            2,
            INITIALIZATION,
        ))?;
        require(
            r.published_length == before[index] && r.length == before[index] && r.epoch == 2,
            "fence discarded complete history",
        )?;
    }
    let endpoints: Vec<_> = peers.iter().map(|p| p.addr.clone()).collect();
    let mut reader = ChunkLogReader::new(
        &endpoints,
        VOLUME,
        CHUNK,
        GENERATION,
        ReplicaClientOptions::default(),
    );
    require(
        reader.read_published(2, &[INITIALIZATION; 3], 0, data.len(), || Ok(()))? == data,
        "automatic stabilized read differs",
    )?;
    require(
        peers
            .iter()
            .map(|p| p.probe())
            .collect::<Result<Vec<_>>>()?
            .iter()
            .filter(|r| r.published_length == 4096)
            .count()
            >= 2,
        "read escaped before a second durable copy",
    )?;
    let stale = LogOpRequest::append_published(
        VOLUME,
        CHUNK,
        GENERATION,
        1,
        4096,
        b"x".to_vec(),
        [3; 16],
        4097,
        INITIALIZATION,
    );
    require(peers[1].call(stale).is_err(), "stale epoch was accepted")?;
    peers[0].stop();
    let mut reader = ChunkLogReader::new(
        &endpoints,
        VOLUME,
        CHUNK,
        GENERATION,
        ReplicaClientOptions::default(),
    );
    require(
        reader.read_published(2, &[INITIALIZATION; 3], 0, data.len(), || Ok(()))? == data,
        "fresh read after source loss differs",
    )?;
    for peer in &peers[1..] {
        require(
            read_all(peer, data.len(), capacity)? == data,
            "surviving replica read differs",
        )?;
    }
    for peer in &mut peers {
        peer.restart()?;
        let r = peer.probe()?;
        require(
            r.epoch == 2 && r.published_length == 4096,
            "restart lost prefix/epoch",
        )?;
        require(
            r.publication_initialization() == Some(INITIALIZATION),
            "restart lost initialization",
        )?;
        require(
            read_all(peer, data.len(), capacity)? == data,
            "restart changed data",
        )?;
    }
    println!("storage protocol probe passed: three replica processes, bounded records, preserving fences, automatic prefix stabilization, one unavailable member and full restart; metadata/object integration not exercised");
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.first().is_some_and(|s| s == "--node") {
        require(args.len() == 3, "node arguments")?;
        let dir = PathBuf::from(&args[1]);
        let limit: usize = args[2]
            .to_str()
            .ok_or_else(|| io::Error::other("frame limit is not text"))?
            .parse()?;
        let server = ReplicaNodeServer::bind_with_journal(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                publication_writes: true,
                max_payload_bytes: limit,
                ..Default::default()
            },
            &dir.join("journal"),
        )?;
        println!("NODE {}", server.local_addr()?);
        io::stdout().flush()?;
        server.serve()?;
        return Ok(());
    }
    require(
        args.len() == 1,
        "usage: guarded_publication_process_probe <fresh-directory>",
    )?;
    run(Path::new(&args[0]))
}
