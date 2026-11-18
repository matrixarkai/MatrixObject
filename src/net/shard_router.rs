// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Spreading one S3 door over several independent clusters by hash slot.
//!
//! One metadata group holds at most 100,000 objects and carries every chunk node's heartbeat, so
//! one cluster does not scale out. The least invasive way to use more than one is a door that
//! owns a static map from hash slot to cluster and sends each request to the cluster that owns
//! its slot. Nothing in a cluster changes and nothing is shared between clusters: this is a
//! client-side router, and the map is a file the operator edits.
//!
//! # The map file
//!
//! ```text
//! # comments and blank lines are ignored
//! slots 4096                                   # optional, 1..=65536, default 4096
//! shard s0 0-2047    10.0.0.1:17200,10.0.0.2:17200,10.0.0.3:17200
//! shard s1 2048-4095 10.0.1.1:17200,10.0.1.2:17200,10.0.1.3:17200
//! bucket wal bucket                            # optional per bucket: `bucket` (default) or `key`
//! ```
//!
//! A shard line names the slot ranges it owns (comma separated, both ends inclusive) and the
//! metadata endpoints of its cluster. Every slot must be owned by exactly one shard.
//!
//! # Routing
//!
//! `slot = fnv1a64(route_key) % slots`. The route key is the bucket name, or for a bucket set to
//! `key` the bucket, a zero byte and the key. **The hash and the slot count are part of where
//! data lives: changing either moves objects, so neither may ever change for a map in use.** The
//! tests pin the hash.
//!
//! With the default `bucket` mode a whole bucket lives on one shard, so listing, multipart
//! uploads and copies stay inside one cluster. A `key` bucket spreads over all shards; this router
//! serves its object requests but refuses its bucket-level requests (a listing would need a merge
//! of every shard's pages), so it is for write-heavy buckets that are never listed.

use super::{Request, RequestHandler, Response};
use std::collections::BTreeMap;
use std::sync::Arc;

pub const DEFAULT_SLOTS: u32 = 4096;
const MAX_SLOTS: u32 = 65_536;

/// How a bucket's objects are placed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteMode {
    /// The whole bucket on the shard that owns the bucket name's slot.
    Bucket,
    /// Each object on the shard that owns the slot of bucket plus key.
    Key,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardSpec {
    pub name: String,
    /// Inclusive slot ranges this shard owns.
    pub ranges: Vec<(u32, u32)>,
    /// Metadata endpoints of the shard's cluster.
    pub meta: Vec<String>,
}

/// The stable route hash: 64 bit FNV-1a. Pinned by test vectors; never change it.
fn route_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardMap {
    slots: u32,
    shards: Vec<ShardSpec>,
    /// Shard index for every slot.
    owner: Vec<u16>,
    modes: BTreeMap<String, RouteMode>,
}

impl ShardMap {
    /// Parse and validate a map file.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut slots = DEFAULT_SLOTS;
        let mut slots_seen = false;
        let mut shards: Vec<ShardSpec> = Vec::new();
        let mut modes = BTreeMap::new();
        for (number, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let at = |message: String| format!("line {}: {message}", number + 1);
            let fields: Vec<&str> = line.split_whitespace().collect();
            match fields[0] {
                "slots" => {
                    if fields.len() != 2 || slots_seen || !shards.is_empty() {
                        return Err(at("`slots <n>` must appear once, before any shard".into()));
                    }
                    slots = fields[1].parse().map_err(|_| at("slot count is not a number".into()))?;
                    if !(1..=MAX_SLOTS).contains(&slots) {
                        return Err(at(format!("slot count must be 1 to {MAX_SLOTS}")));
                    }
                    slots_seen = true;
                }
                "shard" => {
                    if fields.len() != 4 {
                        return Err(at("expected `shard <name> <ranges> <meta endpoints>`".into()));
                    }
                    let name = fields[1].to_string();
                    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) {
                        return Err(at(format!("invalid shard name {name:?}")));
                    }
                    if shards.iter().any(|s| s.name == name) {
                        return Err(at(format!("shard {name} is declared twice")));
                    }
                    let mut ranges = Vec::new();
                    for part in fields[2].split(',') {
                        let (a, b) = part.split_once('-').ok_or_else(|| at(format!("range {part:?} is not start-end")))?;
                        let (a, b): (u32, u32) = (
                            a.parse().map_err(|_| at(format!("bad range start {a:?}")))?,
                            b.parse().map_err(|_| at(format!("bad range end {b:?}")))?,
                        );
                        if a > b || b >= slots {
                            return Err(at(format!("range {part} is outside 0-{}", slots - 1)));
                        }
                        ranges.push((a, b));
                    }
                    let meta: Vec<String> =
                        fields[3].split(',').filter(|e| !e.is_empty()).map(str::to_string).collect();
                    if meta.is_empty() {
                        return Err(at("a shard needs at least one metadata endpoint".into()));
                    }
                    shards.push(ShardSpec { name, ranges, meta });
                }
                "bucket" => {
                    if fields.len() != 3 {
                        return Err(at("expected `bucket <name> bucket|key`".into()));
                    }
                    let mode = match fields[2] {
                        "bucket" => RouteMode::Bucket,
                        "key" => RouteMode::Key,
                        other => return Err(at(format!("route mode {other:?} is not `bucket` or `key`"))),
                    };
                    if modes.insert(fields[1].to_string(), mode).is_some() {
                        return Err(at(format!("bucket {} is declared twice", fields[1])));
                    }
                }
                other => return Err(at(format!("unknown directive {other:?}"))),
            }
        }
        if shards.is_empty() {
            return Err("the map declares no shard".into());
        }
        if shards.len() > usize::from(u16::MAX) {
            return Err("too many shards".into());
        }
        let mut owner: Vec<Option<u16>> = vec![None; slots as usize];
        for (index, shard) in shards.iter().enumerate() {
            for (a, b) in &shard.ranges {
                for slot in *a..=*b {
                    if let Some(first) = owner[slot as usize] {
                        return Err(format!(
                            "slot {slot} is owned by both {} and {}",
                            shards[first as usize].name, shard.name
                        ));
                    }
                    owner[slot as usize] = Some(index as u16);
                }
            }
        }
        if let Some(slot) = owner.iter().position(|o| o.is_none()) {
            return Err(format!("slot {slot} is owned by no shard"));
        }
        Ok(Self { slots, shards, owner: owner.into_iter().flatten().collect(), modes })
    }

    pub fn slots(&self) -> u32 {
        self.slots
    }

    pub fn shards(&self) -> &[ShardSpec] {
        &self.shards
    }

    pub fn mode_of(&self, bucket: &str) -> RouteMode {
        self.modes.get(bucket).copied().unwrap_or(RouteMode::Bucket)
    }

    /// The slot an object lives in.
    pub fn slot_of(&self, bucket: &str, key: &str) -> u32 {
        let hash = match self.mode_of(bucket) {
            RouteMode::Bucket => route_hash(bucket.as_bytes()),
            RouteMode::Key => {
                let mut route_key = Vec::with_capacity(bucket.len() + 1 + key.len());
                route_key.extend_from_slice(bucket.as_bytes());
                route_key.push(0);
                route_key.extend_from_slice(key.as_bytes());
                route_hash(&route_key)
            }
        };
        (hash % u64::from(self.slots)) as u32
    }

    /// The index (into [`Self::shards`]) of the shard that owns an object.
    pub fn shard_of(&self, bucket: &str, key: &str) -> usize {
        usize::from(self.owner[self.slot_of(bucket, key) as usize])
    }
}

/// A request handler that sends each request to the shard that owns its slot.
pub struct ShardRouter {
    map: ShardMap,
    handlers: Vec<Arc<dyn RequestHandler>>,
}

impl ShardRouter {
    /// One handler per shard, in the map's shard order.
    pub fn new(map: ShardMap, handlers: Vec<Arc<dyn RequestHandler>>) -> Result<Self, String> {
        if handlers.len() != map.shards().len() {
            return Err(format!("{} handlers for {} shards", handlers.len(), map.shards().len()));
        }
        Ok(Self { map, handlers })
    }

    pub fn map(&self) -> &ShardMap {
        &self.map
    }

    /// Healthy when at least one shard answers: a door that still serves part of the key space
    /// is better in rotation than out of it. The body names every shard's state.
    fn health(&self, request: &Request) -> Response {
        let mut healthy = 0;
        let mut lines = String::new();
        for (spec, handler) in self.map.shards().iter().zip(&self.handlers) {
            let probe = Request {
                method: "GET".to_string(),
                path: "/healthz".to_string(),
                query: String::new(),
                headers: Vec::new(),
                body: Vec::new(),
                keep_alive: request.keep_alive,
            };
            let status = handler.handle(&probe).status;
            if status == 200 {
                healthy += 1;
            }
            lines.push_str(&format!("shard={} status={status}\n", spec.name));
        }
        let head = format!("shards={} healthy={healthy}\n", self.handlers.len());
        Response::text(if healthy > 0 { 200 } else { 503 }, format!("{head}{lines}"))
    }
}

impl RequestHandler for ShardRouter {
    fn handle(&self, request: &Request) -> Response {
        if matches!(request.path.as_str(), "/healthz" | "/health") {
            return self.health(request);
        }
        if request.path == "/" {
            return Response::text(501, "listing buckets is not supported by the shard router\n");
        }
        let trimmed = request.path.trim_start_matches('/');
        let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
        if bucket.is_empty() {
            return Response::text(400, "the path names no bucket\n");
        }
        if key.is_empty() && self.map.mode_of(bucket) == RouteMode::Key {
            return Response::text(
                501,
                format!("bucket {bucket} is routed by key; bucket-level requests need a merge across shards\n"),
            );
        }
        self.handlers[self.map.shard_of(bucket, key)].handle(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Mutex;

    const TWO: &str = "shard a 0-2047 h1:1,h2:1\nshard b 2048-4095 h3:1\n";

    #[test]
    fn the_route_hash_and_slots_are_pinned_forever() {
        // Changing these moves data. Known FNV-1a 64 vectors, then the slots they give.
        assert_eq!(route_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(route_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        let map = ShardMap::parse(TWO).unwrap();
        assert_eq!(map.slot_of("wal", "anything"), 1403);
        assert_eq!(map.slot_of("events", "anything"), 3332);
        let keyed = ShardMap::parse(&format!("{TWO}bucket wal key\n")).unwrap();
        assert_eq!(keyed.slot_of("wal", "seg-0001"), 2050);
    }

    #[test]
    fn a_map_must_own_every_slot_exactly_once() {
        let err = |text: &str| ShardMap::parse(text).unwrap_err();
        assert!(err("shard a 0-2046 h:1\nshard b 2048-4095 h:1\n").contains("slot 2047 is owned by no shard"));
        assert!(err("shard a 0-2048 h:1\nshard b 2048-4095 h:1\n").contains("slot 2048 is owned by both a and b"));
        assert!(err("shard a 0-4096 h:1\n").contains("outside"));
        assert!(err("shard a 5-4 h:1\n").contains("outside"));
        assert!(err("shard a 0-2047 h:1\nshard a 2048-4095 h:1\n").contains("declared twice"));
        assert!(err("").contains("no shard"));
        assert!(err("shard a 0-4095\n").contains("expected"));
        assert!(err("shard a 0-4095 \nshard\n").contains("expected"));
        assert!(err("shard a 0-4095 h:1\nbucket x sideways\n").contains("not `bucket` or `key`"));
        assert!(err("shard a 0-4095 h:1\nslots 8\n").contains("before any shard"));
        assert!(err("shard a 0-4095 h:1\nfrobnicate\n").contains("unknown directive"));
        // Several ranges, a custom slot count, comments.
        let ok = ShardMap::parse("slots 8 # tiny\nshard a 0-1,6-7 h:1\nshard b 2-5 h:1 # rest\n").unwrap();
        assert_eq!(ok.slots(), 8);
        assert_eq!(ok.shards().len(), 2);
    }

    #[test]
    fn a_bucket_lives_on_one_shard_and_buckets_spread() {
        let map = ShardMap::parse(TWO).unwrap();
        let first = map.shard_of("wal", "k0");
        assert!((0..1000).all(|i| map.shard_of("wal", &format!("k{i}")) == first));
        let used: std::collections::BTreeSet<usize> =
            (0..200).map(|i| map.shard_of(&format!("bucket-{i}"), "k")).collect();
        assert_eq!(used.len(), 2, "200 buckets must use both shards");
    }

    #[test]
    fn a_key_routed_bucket_spreads_its_keys_evenly() {
        let text = "shard a 0-1023 h:1\nshard b 1024-2047 h:1\nshard c 2048-3071 h:1\nshard d 3072-4095 h:1\nbucket big key\n";
        let map = ShardMap::parse(text).unwrap();
        let mut counts = [0usize; 4];
        for i in 0..100_000 {
            counts[map.shard_of("big", &format!("object-{i}"))] += 1;
        }
        for count in counts {
            assert!((22_500..=27_500).contains(&count), "uneven spread {counts:?}");
        }
    }

    #[test]
    fn giving_a_slot_range_to_a_new_shard_moves_only_those_slots() {
        let before = ShardMap::parse("bucket big key\nshard a 0-2047 h:1\nshard b 2048-4095 h:1\n").unwrap();
        // c takes 1024-2047 from a.
        let after =
            ShardMap::parse("bucket big key\nshard a 0-1023 h:1\nshard b 2048-4095 h:1\nshard c 1024-2047 h:1\n").unwrap();
        let (mut moved, mut kept) = (0, 0);
        for i in 0..50_000 {
            let key = format!("o{i}");
            let slot = before.slot_of("big", &key);
            assert_eq!(slot, after.slot_of("big", &key), "the slot of a key never depends on the shard list");
            let (old, new) = (&before.shards()[before.shard_of("big", &key)].name, &after.shards()[after.shard_of("big", &key)].name);
            if (1024..=2047).contains(&slot) {
                assert_eq!((old.as_str(), new.as_str()), ("a", "c"));
                moved += 1;
            } else {
                assert_eq!(old, new, "slot {slot} must not move");
                kept += 1;
            }
        }
        assert!(moved > 10_000 && kept > 30_000, "moved {moved} kept {kept}");
    }

    struct Fake {
        name: &'static str,
        status: AtomicU16,
        seen: Mutex<Vec<(String, String, Vec<u8>)>>,
    }

    impl Fake {
        fn new(name: &'static str) -> Arc<Self> {
            Arc::new(Self { name, status: AtomicU16::new(200), seen: Mutex::new(Vec::new()) })
        }
        fn paths(&self) -> Vec<String> {
            self.seen.lock().unwrap().iter().map(|(_, p, _)| p.clone()).collect()
        }
    }

    impl RequestHandler for Fake {
        fn handle(&self, request: &Request) -> Response {
            self.seen.lock().unwrap().push((request.method.clone(), request.path.clone(), request.body.clone()));
            Response::text(self.status.load(Ordering::SeqCst), self.name)
        }
    }

    fn req(method: &str, path: &str, body: &[u8]) -> Request {
        Request {
            method: method.into(),
            path: path.into(),
            query: String::new(),
            headers: Vec::new(),
            body: body.to_vec(),
            keep_alive: true,
        }
    }

    fn router(text: &str, fakes: &[&Arc<Fake>]) -> ShardRouter {
        let map = ShardMap::parse(text).unwrap();
        ShardRouter::new(map, fakes.iter().map(|f| Arc::clone(f) as Arc<dyn RequestHandler>).collect()).unwrap()
    }

    #[test]
    fn requests_reach_the_owning_shard_unchanged() {
        let (a, b) = (Fake::new("a"), Fake::new("b"));
        let router = router(TWO, &[&a, &b]);
        // "wal" is slot 1403: shard a. "events" is slot 3332: shard b.
        assert_eq!(router.handle(&req("PUT", "/wal/seg/1", b"payload")).body, b"a");
        assert_eq!(router.handle(&req("GET", "/events/x", b"")).body, b"b");
        assert_eq!(a.seen.lock().unwrap()[0], ("PUT".to_string(), "/wal/seg/1".to_string(), b"payload".to_vec()));
        assert_eq!(b.paths(), ["/events/x"]);
        // A bucket-level request goes to the bucket's shard too.
        assert_eq!(router.handle(&req("GET", "/wal", b"")).body, b"a");
    }

    #[test]
    fn a_failing_shard_fails_only_its_own_buckets() {
        let (a, b) = (Fake::new("a"), Fake::new("b"));
        a.status.store(503, Ordering::SeqCst);
        let router = router(TWO, &[&a, &b]);
        assert_eq!(router.handle(&req("GET", "/wal/k", b"")).status, 503);
        assert_eq!(router.handle(&req("GET", "/events/k", b"")).status, 200);
    }

    #[test]
    fn a_key_routed_bucket_serves_objects_but_refuses_bucket_level_requests() {
        let (a, b) = (Fake::new("a"), Fake::new("b"));
        let router = router(&format!("{TWO}bucket big key\n"), &[&a, &b]);
        assert_eq!(router.handle(&req("GET", "/big", b"")).status, 501);
        assert_eq!(router.handle(&req("PUT", "/big", b"")).status, 501);
        let statuses: std::collections::BTreeSet<Vec<u8>> =
            (0..200).map(|i| router.handle(&req("GET", &format!("/big/o{i}"), b"")).body).collect();
        assert_eq!(statuses.len(), 2, "objects of one key-routed bucket reach both shards");
        assert!(a.paths().iter().chain(b.paths().iter()).all(|p| p.starts_with("/big/o")));
    }

    #[test]
    fn odd_paths_are_answered_without_asking_a_shard() {
        let (a, b) = (Fake::new("a"), Fake::new("b"));
        let router = router(TWO, &[&a, &b]);
        assert_eq!(router.handle(&req("GET", "/", b"")).status, 501);
        assert_eq!(router.handle(&req("GET", "///", b"")).status, 400);
        assert!(a.paths().is_empty() && b.paths().is_empty());
    }

    #[test]
    fn health_is_up_while_any_shard_is_up_and_names_each_shard() {
        let (a, b) = (Fake::new("a"), Fake::new("b"));
        let router = router(TWO, &[&a, &b]);
        let response = router.handle(&req("GET", "/healthz", b""));
        assert_eq!(response.status, 200);
        let body = String::from_utf8(response.body).unwrap();
        assert!(body.contains("shards=2 healthy=2") && body.contains("shard=a status=200"), "{body}");
        a.status.store(503, Ordering::SeqCst);
        let response = router.handle(&req("GET", "/healthz", b""));
        assert_eq!(response.status, 200, "one shard down leaves the door serving the rest");
        assert!(String::from_utf8(response.body).unwrap().contains("shard=a status=503"));
        b.status.store(503, Ordering::SeqCst);
        assert_eq!(router.handle(&req("GET", "/healthz", b"")).status, 503);
    }

    #[test]
    fn the_handler_count_must_match_the_shard_count() {
        let a = Fake::new("a");
        let map = ShardMap::parse(TWO).unwrap();
        assert!(ShardRouter::new(map, vec![a as Arc<dyn RequestHandler>]).is_err());
    }
}
