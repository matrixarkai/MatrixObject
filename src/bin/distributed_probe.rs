// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded workloads against an existing metadata quorum and its data groups.

#[cfg(all(feature = "meta-server", feature = "replica-server"))]
mod probe {
    use matrixobjectstore_rs::cluster::{
        MetaClientOptions, MetaGroupIdentity, ObjectWriteOptions, RemoteMetaClient,
        ReplicaClientOptions,
    };
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    pub const USAGE: &str = "distributed_probe write|verify --metadata IP:PORT,IP:PORT --group ID --bucket NAME --run NAME --objects N --bytes N --workers N --groups N [--page-bytes N] [--timeout-ms N] [--operation-timeout-ms N]\nwrite creates new objects and verifies them; verify only reads. Reuse identical parameters for later verification. No cleanup or mutation replay is performed.";

    struct Config {
        write: bool,
        endpoints: Vec<String>,
        group: u64,
        bucket: String,
        run: String,
        objects: usize,
        bytes: usize,
        workers: usize,
        groups: u32,
        page_bytes: usize,
        timeout: Duration,
        operation_timeout: Duration,
    }
    impl Config {
        fn parse(args: &[String]) -> Result<Self, String> {
            let write = match args.first().map(String::as_str) {
                Some("write") => true,
                Some("verify") => false,
                _ => return Err(USAGE.into()),
            };
            let mut options = BTreeMap::new();
            let mut pairs = args[1..].chunks_exact(2);
            for pair in &mut pairs {
                if ![
                    "--metadata",
                    "--group",
                    "--bucket",
                    "--run",
                    "--objects",
                    "--bytes",
                    "--workers",
                    "--groups",
                    "--page-bytes",
                    "--timeout-ms",
                    "--operation-timeout-ms",
                ]
                .contains(&pair[0].as_str())
                    || options.insert(pair[0].as_str(), pair[1].as_str()).is_some()
                {
                    return Err(format!("unknown or duplicate option: {}", pair[0]));
                }
            }
            if !pairs.remainder().is_empty() {
                return Err("every option needs a value".into());
            }
            let get = |name| {
                options
                    .get(name)
                    .copied()
                    .ok_or_else(|| format!("missing {name}"))
            };
            let number = |name, default, min, max| -> Result<u64, String> {
                let value = options
                    .get(name)
                    .copied()
                    .or(default)
                    .ok_or_else(|| format!("missing {name}"))?;
                let n: u64 = value.parse().map_err(|_| format!("invalid {name}"))?;
                if !(min..=max).contains(&n) {
                    return Err(format!("{name} must be {min}..={max}"));
                }
                Ok(n)
            };
            let endpoints: Vec<String> = get("--metadata")?.split(',').map(str::to_owned).collect();
            if !(1..=7).contains(&endpoints.len()) {
                return Err("metadata needs 1..7 endpoints".into());
            }
            for endpoint in &endpoints {
                let address: SocketAddr = endpoint
                    .parse()
                    .map_err(|_| "metadata endpoints must be IP sockets")?;
                if address.port() == 0
                    || address.ip().is_unspecified()
                    || address.ip().is_multicast()
                {
                    return Err("metadata endpoints must be concrete unicast sockets".into());
                }
            }
            let bucket = get("--bucket")?.to_owned();
            let run = get("--run")?.to_owned();
            if !(3..=63).contains(&bucket.len())
                || !bucket.as_bytes()[0].is_ascii_lowercase()
                || !bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
                || !bucket
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
            {
                return Err("bucket must be 3..63 lowercase letters, digits or hyphens, start with a letter and end with a letter or digit".into());
            }
            if run.is_empty()
                || run.len() > 64
                || !run
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
            {
                return Err(
                    "run must contain 1..64 ASCII letters, digits, hyphens or underscores".into(),
                );
            }
            let timeout_ms = number("--timeout-ms", Some("10000"), 1, 300_000)?;
            let operation_default = timeout_ms.to_string();
            let config = Self {
                write,
                endpoints,
                bucket,
                run,
                group: number("--group", None, 1, u64::MAX)?,
                objects: number("--objects", None, 1, 100_000)? as usize,
                bytes: number("--bytes", None, 1, 64 * 1024 * 1024)? as usize,
                workers: number("--workers", None, 1, 64)? as usize,
                groups: number("--groups", None, 1, 4096)? as u32,
                page_bytes: number("--page-bytes", Some("1048576"), 1, 1024 * 1024)? as usize,
                timeout: Duration::from_millis(timeout_ms),
                operation_timeout: Duration::from_millis(number(
                    "--operation-timeout-ms",
                    Some(operation_default.as_str()),
                    1,
                    300_000,
                )?),
            };
            if config.bytes.div_ceil(config.page_bytes) > 4096
                || config.workers > config.objects
                || config.bytes > (256 * 1024 * 1024) / config.workers
            {
                return Err("workload exceeds page, worker or payload-buffer bounds".into());
            }
            Ok(config)
        }
        fn key(&self, index: usize) -> String {
            format!("probe/{}/{index:08}", self.run)
        }
        fn payload(&self, index: usize) -> Vec<u8> {
            let mut state = 0xcbf29ce484222325u64;
            for byte in self.bucket.bytes().chain(self.key(index).bytes()) {
                state = (state ^ u64::from(byte)).wrapping_mul(0x100000001b3);
            }
            (0..self.bytes)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect()
        }
        fn client(&self, expected: Option<&MetaGroupIdentity>) -> Result<RemoteMetaClient, String> {
            let options = MetaClientOptions {
                connect_timeout: self.timeout,
                read_timeout: self.timeout,
                write_timeout: self.timeout,
                ..Default::default()
            };
            match expected {
                Some(identity) => RemoteMetaClient::connect_verified_raft_catalog(
                    &self.endpoints,
                    identity,
                    &options,
                ),
                None => {
                    RemoteMetaClient::connect_raft_catalog(&self.endpoints, self.group, &options)
                }
            }
            .map_err(|e| e.to_string())
        }
        fn replica(&self, started: Instant) -> ReplicaClientOptions {
            ReplicaClientOptions {
                deadline: Some(started + self.operation_timeout),
                connect_timeout: self.timeout,
                read_timeout: self.timeout,
                write_timeout: self.timeout,
                ..Default::default()
            }
        }
        fn operation(
            &self,
            client: &mut RemoteMetaClient,
            index: usize,
            write: bool,
        ) -> Result<Duration, String> {
            let key = self.key(index);
            let expected = self.payload(index);
            let started = Instant::now();
            if write {
                let options = ObjectWriteOptions {
                    max_object_bytes: self.bytes,
                    page_bytes: self.page_bytes,
                    target_groups: self.groups,
                    replica: self.replica(started),
                    ..Default::default()
                };
                client
                    .write_object(&self.bucket, &key, None, &expected, &options)
                    .map_err(|e| format!("{key}: {e}; outcome must be inspected, not replayed"))?;
            } else {
                let (manifest, actual) = client
                    .read_object(&self.bucket, &key, self.bytes, &self.replica(started))
                    .map_err(|e| format!("{key}: {e}"))?;
                if actual != expected || manifest.generation != 1 {
                    return Err(format!("{key}: recovered bytes or generation do not match"));
                }
            }
            Ok(started.elapsed())
        }
        fn phase(&self, write: bool, identity: &MetaGroupIdentity) -> Result<(), String> {
            let next = AtomicUsize::new(0);
            let stop = AtomicBool::new(false);
            let started = Instant::now();
            let results = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..self.workers)
                    .map(|_| {
                        scope.spawn(|| {
                            let mut samples = Vec::new();
                            let result = (|| {
                                let mut client = self.client(Some(identity))?;
                                while !stop.load(Ordering::Acquire) {
                                    let index = next.fetch_add(1, Ordering::Relaxed);
                                    if index >= self.objects {
                                        break;
                                    }
                                    samples.push(self.operation(&mut client, index, write)?);
                                }
                                Ok::<(), String>(())
                            })();
                            if result.is_err() {
                                stop.store(true, Ordering::Release);
                            }
                            (samples, result)
                        })
                    })
                    .collect();
                workers.into_iter().map(|w| w.join()).collect::<Vec<_>>()
            });
            let mut samples = Vec::new();
            let mut errors = Vec::new();
            for result in results {
                match result {
                    Ok((times, result)) => {
                        samples.extend(times);
                        if let Err(e) = result {
                            errors.push(e);
                        }
                    }
                    Err(_) => errors.push("worker panicked".into()),
                }
            }
            samples.sort_unstable();
            let percentile = |p: usize| {
                samples
                    .get((samples.len() * p).div_ceil(100).saturating_sub(1))
                    .map_or(0, Duration::as_micros)
            };
            let elapsed = started.elapsed().as_secs_f64();
            println!("RESULT phase={} completed={} requested={} elapsed_s={elapsed:.6} ops_s={:.3} p50_us={} p95_us={} p99_us={} failed={}",
                if write { "write" } else { "verify" }, samples.len(), self.objects,
                samples.len() as f64 / elapsed.max(f64::MIN_POSITIVE), percentile(50), percentile(95), percentile(99), !errors.is_empty());
            if !errors.is_empty() {
                return Err(errors.join("\n"));
            }
            if samples.len() != self.objects {
                return Err("workload ended before every object completed".into());
            }
            Ok(())
        }
    }
    pub fn run(args: &[String]) -> Result<(), String> {
        let config = Config::parse(args)?;
        println!("WORKLOAD version=1 group={} bucket={} run={} objects={} bytes={} workers={} groups={} page_bytes={} timeout_ms={} operation_timeout_ms={}", config.group, config.bucket, config.run, config.objects, config.bytes, config.workers, config.groups, config.page_bytes, config.timeout.as_millis(), config.operation_timeout.as_millis());
        let identity = config
            .client(None)?
            .metadata_group_identity()
            .map_err(|e| e.to_string())?;
        if config.write {
            config.phase(true, &identity)?;
        }
        config.phase(false, &identity)
    }
}

fn main() {
    #[cfg(all(feature = "meta-server", feature = "replica-server"))]
    {
        let args: Vec<String> = std::env::args().skip(1).collect();
        if args == ["--help"] {
            println!("{}", probe::USAGE);
            return;
        }
        if let Err(error) = probe::run(&args) {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
    #[cfg(not(all(feature = "meta-server", feature = "replica-server")))]
    {
        eprintln!("distributed_probe requires meta-server and replica-server features");
        std::process::exit(2);
    }
}
