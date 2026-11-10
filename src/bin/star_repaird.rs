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
mod worker {
    use matrixobjectstore_rs::cluster::{
        ApiTlsConfig, MetaClientOptions, PageRepairStatus, RemoteMetaClient, RemoteReplicaSet,
        ReplicaClientOptions,
    };
    use matrixobjectstore_rs::PageId;
    use std::time::Duration;

    struct Config {
        metadata_endpoints: Vec<String>,
        group: String,
        batch_pages: usize,
        interval: Duration,
        sweep_interval: Duration,
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
            let group = std::env::var("MATRIXOBJECT_REPAIR_GROUP")
                .map_err(|_| "missing repair group".to_string())?;
            if group.is_empty()
                || group.len() > 128
                || !group
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            {
                return Err("invalid repair group".into());
            }
            let batch_pages = number("MATRIXOBJECT_REPAIR_BATCH_PAGES", 16, 1, 64)? as usize;
            let interval =
                Duration::from_millis(number("MATRIXOBJECT_REPAIR_INTERVAL_MS", 500, 100, 60_000)?);
            let sweep_interval = Duration::from_millis(number(
                "MATRIXOBJECT_REPAIR_SWEEP_INTERVAL_MS",
                300_000,
                1_000,
                86_400_000,
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
                batch_pages,
                interval,
                sweep_interval,
                metadata,
                replica,
            })
        }
    }

    struct ActiveGroup {
        endpoints: Vec<String>,
        quorum: usize,
        replicas: RemoteReplicaSet,
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
        metadata: &mut Option<RemoteMetaClient>,
        active: &mut Option<ActiveGroup>,
        cursor: &mut Option<PageId>,
    ) -> Result<StepReport, StepError> {
        if metadata.is_none() {
            *metadata = Some(
                RemoteMetaClient::connect_catalog(&config.metadata_endpoints, &config.metadata)
                    .map_err(|error| StepError::Retry(error.to_string()))?,
            );
        }
        let route = match metadata.as_mut().unwrap().read_chunk_route(&config.group) {
            Ok(route) => route,
            Err(error) => {
                *metadata = None;
                return Err(StepError::Retry(error.to_string()));
            }
        };
        let (endpoints, quorum) = route
            .standalone_star_route()
            .map_err(|error| StepError::Stop(error.to_string()))?
            .ok_or_else(|| StepError::Stop("repair group is not fixed Star membership".into()))?;
        if active
            .as_ref()
            .is_none_or(|current| current.endpoints != endpoints || current.quorum != quorum)
        {
            *active = None;
            *cursor = None;
            let replicas = RemoteReplicaSet::connect(&endpoints, quorum, quorum, &config.replica)
                .map_err(|error| StepError::Retry(error.to_string()))?;
            *active = Some(ActiveGroup {
                endpoints,
                quorum,
                replicas,
            });
        }
        let batch = active
            .as_mut()
            .unwrap()
            .replicas
            .repair_inventory_batch(cursor.as_ref(), config.batch_pages)
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

    pub fn run() -> Result<(), String> {
        let config = Config::from_env()?;
        let mut metadata = None;
        let mut active = None;
        let mut cursor = None;
        let mut sweep_scanned = 0usize;
        let mut sweep_installed = 0usize;
        let mut sweep_blocked = 0usize;
        let mut retry_delay = config.interval;
        loop {
            let wait = match step(&config, &mut metadata, &mut active, &mut cursor) {
                Ok(report) => {
                    retry_delay = config.interval;
                    sweep_scanned = sweep_scanned.saturating_add(report.scanned);
                    sweep_installed = sweep_installed.saturating_add(report.installed);
                    sweep_blocked = sweep_blocked.saturating_add(usize::from(report.blocked));
                    if report.blocked {
                        eprintln!(
                            "star repair group={} blocked one page in this sweep",
                            config.group
                        );
                    }
                    if report.completed {
                        println!(
                            "star repair sweep group={} scanned={} installed={} blocked={}",
                            config.group, sweep_scanned, sweep_installed, sweep_blocked
                        );
                        sweep_scanned = 0;
                        sweep_installed = 0;
                        sweep_blocked = 0;
                        config.sweep_interval
                    } else {
                        config.interval
                    }
                }
                Err(StepError::Retry(error)) => {
                    eprintln!("star repair retry: {error}");
                    let wait = retry_delay;
                    retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(30));
                    wait
                }
                Err(StepError::Stop(error)) => return Err(error),
            };
            std::thread::sleep(wait);
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
