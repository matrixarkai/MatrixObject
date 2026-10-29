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

pub(super) fn run(
    options: ReplicaHeartbeatOptions,
    replica: Arc<Mutex<NodeReplica>>,
    stop: Arc<ServerShutdown>,
) {
    let mut endpoints: Vec<_> = options
        .metadata_endpoints
        .iter()
        .map(ToString::to_string)
        .collect();
    let client_options = MetaClientOptions {
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
            let (used, capacity, online) = {
                let state = replica.lock().map_err(|_| {
                    crate::ObjectError::Corruption("replica health lock poisoned".into())
                })?;
                (
                    state.used_bytes,
                    state.service.capacity_bytes(),
                    !state.failed,
                )
            };
            if stop.is_stopped() {
                return Ok(());
            }
            client.report_chunk_node(
                &options.node_id,
                &advertised,
                used,
                capacity,
                online,
            )?;
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
}
