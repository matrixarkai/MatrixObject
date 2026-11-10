// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Periodically repair fixed Star members from quorum-verified page state.
//! The metadata route supplies membership; each batch is bounded and a blocked
//! page is revisited on the next sweep. This worker does not admit maintenance
//! nodes for placement or reclaim abandoned pages.

#[cfg(not(all(feature = "meta-server", feature = "replica-server")))]
fn main() {
    eprintln!("star_repaird requires meta-server and replica-server features");
    std::process::exit(2);
}

#[cfg(all(feature = "meta-server", feature = "replica-server"))]
#[path = "star_repair_worker/connection_cache.rs"]
mod connection_cache;

#[cfg(all(feature = "meta-server", feature = "replica-server"))]
mod worker {
    use super::connection_cache::ConnectionCache;
    use matrixobjectstore_rs::cluster::{
        ApiTlsConfig, MetaClientOptions, PageRepairStatus, RemoteMetaClient, RemoteReplicaSet,
        ReplicaClientOptions,
    };
    use matrixobjectstore_rs::PageId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[derive(Clone)]
    struct Config {
        metadata_endpoints: Vec<String>,
        group: Option<String>,
        all_groups: bool,
        workers: usize,
        active_groups: usize,
        batch_pages: usize,
        interval: Duration,
        sweep_interval: Duration,
        scrub_interval: Duration,
        metadata: MetaClientOptions,
        replica: ReplicaClientOptions,
    }

    fn number(name: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
        let raw = std::env::var(name).unwrap_or_else(|_| default.to_string());
        let value = raw.parse::<u64>().map_err(|_| format!("invalid {name}"))?;
        if !(min..=max).contains(&value) {
            return Err(format!("{name} must be between {min} and {max}"));
        }
        Ok(value)
    }

    impl Config {
        fn from_env() -> Result<Self, String> {
            let raw = std::env::var("MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS")
                .map_err(|_| "missing repair metadata endpoints".to_string())?;
            let metadata_endpoints: Vec<_> =
                raw.split(',').map(str::trim).map(str::to_owned).collect();
            if metadata_endpoints.is_empty()
                || metadata_endpoints.len() > 7
                || metadata_endpoints.iter().any(String::is_empty)
                || metadata_endpoints
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != metadata_endpoints.len()
            {
                return Err(
                    "repair metadata endpoints must be one through seven distinct sockets".into(),
                );
            }
            let group = std::env::var("MATRIXOBJECT_REPAIR_GROUP").ok();
            let all_groups = match std::env::var("MATRIXOBJECT_REPAIR_ALL_GROUPS") {
                Ok(value) if value == "1" => true,
                Ok(_) => return Err("repair all-groups flag must be 1".into()),
                Err(std::env::VarError::NotPresent) => false,
                Err(_) => return Err("invalid repair all-groups flag".into()),
            };
            if all_groups == group.is_some() {
                return Err("configure exactly one repair group mode".into());
            }
            if group.as_ref().is_some_and(|group| {
                group.is_empty()
                    || group.len() > 128
                    || !group
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            }) {
                return Err("invalid repair group".into());
            }
            let workers = number("MATRIXOBJECT_REPAIR_WORKERS", 4, 1, 16)? as usize;
            let active_groups = number("MATRIXOBJECT_REPAIR_ACTIVE_GROUPS", 32, 1, 128)? as usize;
            let batch_pages = number("MATRIXOBJECT_REPAIR_BATCH_PAGES", 16, 1, 64)? as usize;
            let interval =
                Duration::from_millis(number("MATRIXOBJECT_REPAIR_INTERVAL_MS", 500, 100, 60_000)?);
            let sweep_interval = Duration::from_millis(number(
                "MATRIXOBJECT_REPAIR_SWEEP_INTERVAL_MS",
                300_000,
                1_000,
                86_400_000,
            )?);
            let scrub_interval = Duration::from_secs(number(
                "MATRIXOBJECT_REPAIR_SCRUB_INTERVAL_SECS",
                86_400,
                3_600,
                604_800,
            )?);
            let tls = ApiTlsConfig::from_env("MATRIXOBJECT_CLIENT")
                .map_err(|error| format!("invalid repair TLS configuration: {error}"))?;
            let metadata = MetaClientOptions {
                tls: tls.clone(),
                connect_timeout: Duration::from_secs(1),
                read_timeout: Duration::from_secs(5),
                ..Default::default()
            };
            let replica = ReplicaClientOptions {
                tls,
                connect_timeout: Duration::from_secs(1),
                ..Default::default()
            };
            Ok(Self {
                metadata_endpoints,
                group,
                all_groups,
                workers,
                active_groups,
                batch_pages,
                interval,
                sweep_interval,
                scrub_interval,
                metadata,
                replica,
            })
        }
    }

    #[derive(PartialEq, Eq)]
    struct GroupRoute {
        endpoints: Vec<String>,
        quorum: usize,
    }

    enum StepError {
        Retry(String),
        Stop(String),
    }

    struct StepReport {
        scanned: usize,
        installed: usize,
        blocked: bool,
        completed: bool,
    }

    fn step(
        config: &Config,
        group: &str,
        metadata: &mut Option<RemoteMetaClient>,
        active: &mut Option<RemoteReplicaSet>,
        binding: &mut Option<GroupRoute>,
        cursor: &mut Option<PageId>,
        scrub: bool,
    ) -> Result<StepReport, StepError> {
        if metadata.is_none() {
            *metadata = Some(
                RemoteMetaClient::connect_catalog(&config.metadata_endpoints, &config.metadata)
                    .map_err(|error| StepError::Retry(error.to_string()))?,
            );
        }
        let route = match metadata.as_mut().unwrap().read_chunk_route(group) {
            Ok(route) => route,
            Err(error) => {
                *metadata = None;
                return Err(StepError::Retry(error.to_string()));
            }
        };
        let (endpoints, _) = route
            .standalone_star_route()
            .map_err(|error| StepError::Stop(error.to_string()))?
            .ok_or_else(|| StepError::Stop("repair group is not fixed Star membership".into()))?;
        let quorum = endpoints.len() / 2 + 1;
        if binding
            .as_ref()
            .is_none_or(|current| current.endpoints != endpoints || current.quorum != quorum)
        {
            *active = None;
            *cursor = None;
            *binding = Some(GroupRoute {
                endpoints: endpoints.clone(),
                quorum,
            });
        }
        if active.is_none() {
            let replicas = RemoteReplicaSet::connect(&endpoints, quorum, quorum, &config.replica)
                .map_err(|error| StepError::Retry(error.to_string()))?;
            *active = Some(replicas);
        }
        let replicas = active.as_mut().unwrap();
        let batch = if scrub {
            replicas.repair_inventory_batch(cursor.as_ref(), config.batch_pages)
        } else {
            replicas.repair_inventory_batch_head_first(cursor.as_ref(), config.batch_pages)
        }
        .map_err(|error| StepError::Retry(error.to_string()))?;
        if batch.pages.iter().any(|entry| {
            entry.result.as_ref().is_ok_and(|report| {
                report
                    .members
                    .iter()
                    .any(|status| matches!(status, PageRepairStatus::Unconfirmed(_)))
            })
        }) {
            return Err(StepError::Stop(
                "repair commit has an unconfirmed result; inspect the destination before retrying"
                    .into(),
            ));
        }
        let installed = batch
            .pages
            .iter()
            .filter_map(|entry| entry.result.as_ref().ok())
            .flat_map(|report| &report.members)
            .filter(|status| matches!(status, PageRepairStatus::Installed))
            .count();
        let scanned = batch.pages.len();
        let blocked = batch.blocked;
        *cursor = if blocked {
            // Skip this unresolved page only for the current sweep. Resetting at
            // the end revisits it without starving identities that sort after it.
            batch.pages.last().map(|entry| entry.page.clone())
        } else if batch.has_more {
            batch.resume_after
        } else {
            None
        };
        Ok(StepReport {
            scanned,
            installed,
            blocked,
            completed: cursor.is_none(),
        })
    }

    struct GroupState {
        group: String,
        binding: Option<GroupRoute>,
        cursor: Option<PageId>,
        sweep_scanned: usize,
        sweep_installed: usize,
        sweep_blocked: usize,
        retry_delay: Duration,
        next_scrub: Instant,
        scrub: bool,
        next_at: Instant,
        stopped: bool,
    }

    impl GroupState {
        fn new(group: String, config: &Config) -> Self {
            let now = Instant::now();
            Self {
                group,
                binding: None,
                cursor: None,
                sweep_scanned: 0,
                sweep_installed: 0,
                sweep_blocked: 0,
                retry_delay: config.interval,
                next_scrub: now + config.scrub_interval,
                scrub: false,
                next_at: now,
                stopped: false,
            }
        }

        fn advance(
            &mut self,
            config: &Config,
            metadata: &mut Option<RemoteMetaClient>,
            active: &mut Option<RemoteReplicaSet>,
        ) -> Result<(), String> {
            let wait = match step(
                config,
                &self.group,
                metadata,
                active,
                &mut self.binding,
                &mut self.cursor,
                self.scrub,
            ) {
                Ok(report) => {
                    self.retry_delay = config.interval;
                    self.sweep_scanned = self.sweep_scanned.saturating_add(report.scanned);
                    self.sweep_installed = self.sweep_installed.saturating_add(report.installed);
                    self.sweep_blocked = self
                        .sweep_blocked
                        .saturating_add(usize::from(report.blocked));
                    if report.blocked {
                        eprintln!(
                            "star repair group={} blocked one page in this sweep",
                            self.group
                        );
                    }
                    if report.completed {
                        println!(
                            "star repair sweep group={} scrub={} scanned={} installed={} blocked={}",
                            self.group, self.scrub, self.sweep_scanned, self.sweep_installed, self.sweep_blocked
                        );
                        self.sweep_scanned = 0;
                        self.sweep_installed = 0;
                        self.sweep_blocked = 0;
                        if self.scrub {
                            self.next_scrub = Instant::now() + config.scrub_interval;
                            self.scrub = false;
                        } else if Instant::now() >= self.next_scrub {
                            self.scrub = true;
                        }
                        config.sweep_interval
                    } else {
                        config.interval
                    }
                }
                Err(StepError::Retry(error)) => {
                    eprintln!("star repair group={} retry: {error}", self.group);
                    let wait = self.retry_delay;
                    self.retry_delay = self
                        .retry_delay
                        .saturating_mul(2)
                        .min(Duration::from_secs(30));
                    wait
                }
                Err(StepError::Stop(error)) => return Err(format!("group={} {error}", self.group)),
            };
            self.next_at = Instant::now() + wait;
            Ok(())
        }
    }

    fn discover_groups(config: &Config) -> Result<Vec<String>, String> {
        let mut metadata =
            RemoteMetaClient::connect_catalog(&config.metadata_endpoints, &config.metadata)
                .map_err(|error| error.to_string())?;
        let mut after: Option<String> = None;
        let mut groups = Vec::new();
        loop {
            let page = metadata
                .list_chunk_routes(after.as_deref(), 32)
                .map_err(|error| error.to_string())?;
            for route in &page.routes {
                if route
                    .standalone_star_route()
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    groups.push(route.group.as_ref().unwrap().placement_id.clone());
                }
            }
            if !page.has_more {
                break;
            }
            after = page
                .routes
                .last()
                .map(|route| route.group.as_ref().unwrap().placement_id.clone());
        }
        if groups.is_empty() {
            return Err("metadata catalog has no fixed Star groups".into());
        }
        Ok(groups)
    }

    fn run_partition(
        config: Config,
        groups: Vec<String>,
        remaining: Arc<AtomicUsize>,
    ) -> Result<(), String> {
        let mut metadata = None;
        let mut connections = ConnectionCache::new(config.active_groups);
        let mut states: Vec<_> = groups
            .into_iter()
            .map(|group| GroupState::new(group, &config))
            .collect();
        loop {
            let mut next_at = Instant::now() + Duration::from_secs(1);
            // Partition slots are stable, so cache lookup needs no allocated group key.
            for (index, state) in states.iter_mut().enumerate() {
                if state.stopped {
                    continue;
                }
                if Instant::now() >= state.next_at {
                    let mut active = connections.take(index);
                    if let Err(error) = state.advance(&config, &mut metadata, &mut active) {
                        if !config.all_groups {
                            return Err(error);
                        }
                        eprintln!("star repair stopped: {error}");
                        state.stopped = true;
                        if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                            return Err("all repair groups stopped".into());
                        }
                        continue;
                    }
                    if let Some(active) = active {
                        connections.put(index, active);
                    }
                }
                next_at = next_at.min(state.next_at);
            }
            if states.iter().all(|state| state.stopped) {
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
            std::thread::sleep(
                next_at
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100)),
            );
        }
    }

    pub fn run() -> Result<(), String> {
        let config = Config::from_env()?;
        if !config.all_groups {
            return run_partition(
                config.clone(),
                vec![config.group.unwrap()],
                Arc::new(AtomicUsize::new(1)),
            );
        }
        let mut retry_delay = config.interval;
        let groups = loop {
            match discover_groups(&config) {
                Ok(groups) => break groups,
                Err(error)
                    if error == "metadata catalog has no fixed Star groups"
                        || error.contains("does not implement this request") =>
                {
                    return Err(error)
                }
                Err(error) => {
                    eprintln!("star repair catalog retry: {error}");
                    std::thread::sleep(retry_delay);
                    retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(30));
                }
            }
        };
        let remaining = Arc::new(AtomicUsize::new(groups.len()));
        let count = config.workers.min(groups.len());
        let mut partitions = vec![Vec::new(); count];
        for (index, group) in groups.into_iter().enumerate() {
            partitions[index % count].push(group);
        }
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut handles = Vec::with_capacity(count);
        for partition in partitions {
            let config = config.clone();
            let sender = sender.clone();
            let remaining = Arc::clone(&remaining);
            handles.push(std::thread::spawn(move || {
                if let Err(error) = run_partition(config, partition, remaining) {
                    let _ = sender.send(error);
                }
            }));
        }
        drop(sender);
        loop {
            match receiver.recv_timeout(Duration::from_secs(1)) {
                Ok(error) => return Err(error),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err("repair workers stopped".into());
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if handles.iter().any(std::thread::JoinHandle::is_finished) {
                        return Err("repair worker stopped unexpectedly".into());
                    }
                }
            }
        }
    }
}

#[cfg(all(feature = "meta-server", feature = "replica-server"))]
fn main() {
    if let Err(error) = worker::run() {
        eprintln!("star repair stopped: {error}");
        std::process::exit(2);
    }
}
