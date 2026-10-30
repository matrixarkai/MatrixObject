// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::{NodeReplica, ServerShutdown};
use crate::cluster::{DataNodeSeed, MetaClientOptions, RemoteMetaClient};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Automatic reports for one chunk process. Metadata membership must already exist.
#[derive(Clone, Debug)]
pub struct ReplicaHeartbeatOptions {
    /// Logical metadata placement target for fenced reports from a dynamic Raft group.
    pub placement_id: Option<String>,
    pub tls: Option<crate::cluster::ApiTlsConfig>,
    pub node_id: String,
    pub advertised_endpoint: SocketAddr,
    pub metadata_endpoints: Vec<SocketAddr>,
    pub interval: Duration,
    /// Pin metadata membership and bootstrap identity on every report connection.
    pub expected_metadata_identity: Option<crate::cluster::MetaGroupIdentity>,
}

impl ReplicaHeartbeatOptions {
    pub(super) fn validate(&self) -> std::io::Result<()> {
        let invalid = |s| std::io::Error::new(std::io::ErrorKind::InvalidInput, s);
        DataNodeSeed::parse_roster(&format!(
            "{}@{}@health",
            self.node_id, self.advertised_endpoint
        ))
        .map_err(|e| invalid(e.to_string()))?;
        if self.metadata_endpoints.is_empty()
            || self.metadata_endpoints.len() > 7
            || !(Duration::from_millis(100)..=Duration::from_secs(60)).contains(&self.interval)
        {
            return Err(invalid(
                "heartbeats require 1..7 metadata endpoints and a 100..60000 ms interval".into(),
            ));
        }
        if let Some(id) = &self.placement_id {
            if id.is_empty()
                || id.len() > 128
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            {
                return Err(invalid("invalid chunk placement identity".into()));
            }
        }
        if let Some(identity) = &self.expected_metadata_identity {
            identity.validate().map_err(|e| invalid(e.to_string()))?;
        }
        let roster = self
            .metadata_endpoints
            .iter()
            .enumerate()
            .map(|(i, endpoint)| format!("meta-{i}@{endpoint}@health"))
            .collect::<Vec<_>>()
            .join(",");
        DataNodeSeed::parse_roster(&roster).map_err(|e| invalid(e.to_string()))?;
        Ok(())
    }
}

#[cfg(feature = "chunk-consensus")]
fn local_membership(
    state: &NodeReplica,
) -> Result<crate::cluster::MembershipStatus, crate::ObjectError> {
    let (status, observed) = state.applied_membership.as_ref().ok_or_else(|| {
        crate::ObjectError::PreconditionFailed("chunk membership has not applied".into())
    })?;
    if observed.elapsed() > Duration::from_secs(1) {
        return Err(crate::ObjectError::PreconditionFailed(
            "chunk membership observation expired".into(),
        ));
    }
    Ok(status.clone())
}

pub(super) fn run(
    options: ReplicaHeartbeatOptions,
    replica: Arc<Mutex<NodeReplica>>,
    stop: Arc<ServerShutdown>,
    #[cfg(feature = "chunk-consensus")] consensus: Option<Arc<super::consensus::ChunkConsensus>>,
) {
    let mut endpoints: Vec<_> = options
        .metadata_endpoints
        .iter()
        .map(ToString::to_string)
        .collect();
    let client_options = MetaClientOptions {
        tls: options.tls.clone(),
        connect_timeout: Duration::from_millis(100),
        read_timeout: Duration::from_millis(500),
        write_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    let advertised = DataNodeSeed::parse_roster(&format!(
        "{}@{}@health",
        options.node_id, options.advertised_endpoint
    ))
    .expect("validated heartbeat options")[0]
        .endpoint
        .to_string();
    let mut reported_failure = false;
    while !stop.is_stopped() {
        let result = (|| -> Result<(), crate::ObjectError> {
            // The client is dropped after each report so idle chunk processes never
            // retain the metadata server's connection workers between intervals.
            let mut client = match &options.expected_metadata_identity {
                Some(identity) => RemoteMetaClient::connect_verified_raft_catalog(
                    &endpoints,
                    identity,
                    &client_options,
                )?,
                None => RemoteMetaClient::connect_catalog(&endpoints, &client_options)?,
            };
            #[cfg(feature = "chunk-consensus")]
            let dynamic = consensus.as_ref().filter(|c| c.identity().version == 2);
            let (used, capacity, online, membership) = {
                let state = replica.try_lock().map_err(|error| match error {
                    std::sync::TryLockError::WouldBlock => crate::ObjectError::PreconditionFailed(
                        "replica health is busy applying state".into(),
                    ),
                    std::sync::TryLockError::Poisoned(_) => {
                        crate::ObjectError::Corruption("replica health lock poisoned".into())
                    }
                })?;
                #[cfg(feature = "chunk-consensus")]
                let membership = if let Some(consensus) = dynamic {
                    if consensus.is_failed() || state.failed {
                        return Err(crate::ObjectError::PreconditionFailed(
                            "chunk consensus unavailable for health report".into(),
                        ));
                    }
                    Some(local_membership(&state)?)
                } else {
                    None
                };
                #[cfg(not(feature = "chunk-consensus"))]
                let membership: Option<crate::cluster::MembershipStatus> = None;
                (
                    state.used_bytes,
                    state.service.capacity_bytes(),
                    !state.failed && !state.maintenance,
                    membership,
                )
            };
            if stop.is_stopped() {
                return Ok(());
            }
            if let Some(status) = membership {
                #[cfg(feature = "chunk-consensus")]
                client.update_chunk_catalog(crate::cluster::ChunkCatalogCommand {
                    version: 1,
                    operation: 2,
                    placement_id: options
                        .placement_id
                        .clone()
                        .expect("validated dynamic report mode"),
                    expected_fence: Vec::new(),
                    bootstrap_id: dynamic.unwrap().identity().bootstrap_id,
                    status: Some(status),
                    member_id: options.node_id.clone(),
                    expected_endpoint: advertised.clone(),
                    used_bytes: used,
                    capacity_bytes: capacity,
                    online,
                })?;
                #[cfg(not(feature = "chunk-consensus"))]
                {
                    let _ = status;
                    unreachable!("standalone replica has no applied membership");
                }
            } else {
                client.report_chunk_node(&options.node_id, &advertised, used, capacity, online)?;
            }
            if let Some(position) = endpoints
                .iter()
                .position(|endpoint| endpoint == client.endpoint())
            {
                endpoints.rotate_left(position);
            }
            Ok(())
        })();
        match result {
            Err(error) if !reported_failure => {
                eprintln!("chunk heartbeat for {} failed: {error}", options.node_id);
                reported_failure = true;
            }
            Ok(()) if reported_failure => {
                eprintln!("chunk heartbeat for {} recovered", options.node_id);
                reported_failure = false;
            }
            _ => {}
        }
        if stop.wait_for(options.interval) {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{
        MetaNodeOptions, MetaNodeServer, RemoteReplicaClient, ReplicaClientOptions,
        ReplicaNodeOptions, ReplicaNodeServer,
    };
    use crate::{ChunkId, PageId};

    #[cfg(feature = "chunk-consensus")]
    #[test]
    fn fenced_report_uses_local_applied_identity_and_refuses_stale_observations() {
        let server = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
        let mut state = server.replica.lock().unwrap();
        assert!(local_membership(&state).is_err());
        let status = crate::cluster::MembershipStatus {
            version: 1,
            node_id: 2,
            term: 3,
            applied_index: 19,
            voters: vec![1, 2, 3],
            outgoing_voters: vec![],
            learners: vec![4],
        };
        state.used_bytes = 123;
        state.applied_membership = Some((status.clone(), std::time::Instant::now()));
        assert_eq!(local_membership(&state).unwrap(), status);
        state.applied_membership.as_mut().unwrap().1 -= Duration::from_secs(2);
        assert!(local_membership(&state).is_err());
    }

    #[test]
    fn busy_replica_state_does_not_block_heartbeat_shutdown() {
        let meta = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
        let mut chunk =
            ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
        let endpoint = chunk.local_addr().unwrap();
        let metadata = meta.local_addr().unwrap();
        meta.seed_data_roster(
            &DataNodeSeed::parse_roster(&format!("chunk-1@{endpoint}@rack")).unwrap(),
            1 << 28,
        )
        .unwrap();
        chunk
            .enable_heartbeats(ReplicaHeartbeatOptions {
                placement_id: None,
                tls: None,
                expected_metadata_identity: None,
                node_id: "chunk-1".into(),
                advertised_endpoint: endpoint,
                metadata_endpoints: vec![metadata],
                interval: Duration::from_millis(100),
            })
            .unwrap();
        let state = chunk.replica.clone();
        let stop = chunk.shutdown_handle();
        let meta_stop = meta.shutdown_handle();
        let metadata_thread = std::thread::spawn(move || meta.serve().unwrap());
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let result = chunk.serve();
            tx.send(()).unwrap();
            result
        });
        let guard = state.lock().unwrap();
        std::thread::sleep(Duration::from_millis(350));
        stop.stop();
        let finished = rx.recv_timeout(Duration::from_secs(2)).is_ok();
        drop(guard);
        worker.join().unwrap().unwrap();
        meta_stop.stop();
        metadata_thread.join().unwrap();
        assert!(
            finished,
            "heartbeat waited for the replica lock during shutdown"
        );
    }

    #[test]
    fn failed_journal_is_reported_offline_with_correct_cached_usage() {
        let meta = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
        let mut chunk =
            ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
        let endpoint = chunk.local_addr().unwrap();
        let metadata = meta.local_addr().unwrap();
        meta.seed_data_roster(
            &DataNodeSeed::parse_roster(&format!("chunk-1@{endpoint}@rack")).unwrap(),
            1 << 28,
        )
        .unwrap();
        chunk
            .enable_heartbeats(ReplicaHeartbeatOptions {
                placement_id: None,
                tls: None,
                expected_metadata_identity: None,
                node_id: "chunk-1".into(),
                advertised_endpoint: endpoint,
                metadata_endpoints: vec![metadata],
                interval: Duration::from_millis(100),
            })
            .unwrap();
        let state = Arc::clone(&chunk.replica);
        let meta_stop = meta.shutdown_handle();
        let chunk_stop = chunk.shutdown_handle();
        let meta_worker = std::thread::spawn(move || meta.serve().unwrap());
        let chunk_worker = std::thread::spawn(move || chunk.serve().unwrap());
        let mut client =
            RemoteMetaClient::connect(&metadata.to_string(), &MetaClientOptions::default())
                .unwrap();
        let wait = |client: &mut RemoteMetaClient, online: bool, used: u64| {
            let until = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let node = &client.list_placement_nodes().unwrap()[0];
                if node.online == online && node.used_bytes == used {
                    break;
                }
                assert!(
                    std::time::Instant::now() < until,
                    "health report did not converge"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        wait(&mut client, true, 0);
        let mut data =
            RemoteReplicaClient::connect(&endpoint.to_string(), &ReplicaClientOptions::default())
                .unwrap();
        let page = PageId {
            chunk_id: ChunkId::new(1, 1).unwrap(),
            page_index: 1,
        };
        data.write_chunk(page.clone(), 1024, b"abc").unwrap();
        wait(&mut client, true, 1027);
        let path =
            std::env::temp_dir().join(format!("health-failed-journal-{}", std::process::id()));
        std::fs::write(&path, []).unwrap();
        state.lock().unwrap().journal =
            Some(crate::cluster::replica_journal::ReplicaJournal::read_only_for_test(&path));
        assert!(data.write_chunk(page, 1027, b"d").is_err());
        wait(&mut client, false, 1028);
        {
            let state = state.lock().unwrap();
            assert!(state.failed);
            assert_eq!(state.used_bytes, state.service.physical_bytes());
        }
        drop(data);
        drop(client);
        chunk_stop.stop();
        chunk_worker.join().unwrap();
        meta_stop.stop();
        meta_worker.join().unwrap();
        drop(state);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn maintenance_reports_offline_and_excludes_placement_while_repair_remains_available() {
        let meta = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
        let mut chunk = ReplicaNodeServer::bind(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                maintenance: true,
                ..Default::default()
            },
        )
        .unwrap();
        let endpoint = chunk.local_addr().unwrap();
        let metadata = meta.local_addr().unwrap();
        meta.seed_data_roster(
            &DataNodeSeed::parse_roster(&format!("maintenance-1@{endpoint}@rack")).unwrap(),
            1 << 28,
        )
        .unwrap();
        chunk
            .enable_heartbeats(ReplicaHeartbeatOptions {
                placement_id: None,
                tls: None,
                node_id: "maintenance-1".into(),
                advertised_endpoint: endpoint,
                metadata_endpoints: vec![metadata],
                interval: Duration::from_millis(100),
                expected_metadata_identity: None,
            })
            .unwrap();
        #[cfg(feature = "chunk-consensus")]
        {
            let directory = std::env::temp_dir()
                .join(format!("maintenance-raft-refusal-{}", std::process::id()));
            let error = chunk
                .enable_consensus(super::super::ChunkRaftOptions {
                    bootstrap_voters: None,
                    peer_tls: None,
                    node_id: 1,
                    group_id: 1,
                    peers: std::collections::BTreeMap::new(),
                    directory: directory.clone(),
                    request_timeout: Duration::from_secs(1),
                    checkpoint_after_bytes: 0,
                })
                .unwrap_err();
            assert!(error.to_string().contains("maintenance mode"));
            assert!(!directory.exists());
        }
        let meta_stop = meta.shutdown_handle();
        let chunk_stop = chunk.shutdown_handle();
        let meta_worker = std::thread::spawn(move || meta.serve().unwrap());
        let chunk_worker = std::thread::spawn(move || chunk.serve().unwrap());
        let mut catalog =
            RemoteMetaClient::connect(&metadata.to_string(), &MetaClientOptions::default())
                .unwrap();
        let wait = |catalog: &mut RemoteMetaClient, used: u64| {
            let until = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let nodes = catalog.list_placement_nodes().unwrap();
                let node = &nodes[0];
                if !node.online && node.used_bytes == used {
                    break;
                }
                assert!(
                    std::time::Instant::now() < until,
                    "maintenance health did not converge"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        wait(&mut catalog, 0);
        assert!(catalog
            .plan_object_placement("bucket", "key", 1, 1)
            .is_err());
        let mut data =
            RemoteReplicaClient::connect(&endpoint.to_string(), &ReplicaClientOptions::default())
                .unwrap();
        let page = PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 0,
        };
        assert!(data.write_chunk(page.clone(), 0, b"ordinary").is_err());
        let mut source = crate::MatrixReplicaService::new();
        source.write_chunk(page.clone(), 0, b"repair-data").unwrap();
        data.install_chunk_state(
            &page,
            &source.read_chunk_state(&page).unwrap(),
            None,
            "health-repair",
        )
        .unwrap();
        wait(&mut catalog, 11);
        assert!(catalog
            .plan_object_placement("bucket", "other", 1, 1)
            .is_err());
        assert_eq!(data.read_chunk(&page, 0, 11).unwrap().data, b"repair-data");
        drop(data);
        drop(catalog);
        chunk_stop.stop();
        chunk_worker.join().unwrap();
        meta_stop.stop();
        meta_worker.join().unwrap();
    }
}
