// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Write the binary chunk-group catalog for ONE standalone three-member group, for a metadata
//! server to load with `MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE`.
//!
//! ```text
//! star_catalog OUT PLACEMENT_ID CAPACITY_BYTES MAX_PAGES IP:PORT,IP:PORT,IP:PORT MEMBER_ID,MEMBER_ID,MEMBER_ID
//! ```
//!
//! Every member must be configured with the same capacity and page limit. Used by
//! `scripts/cluster_gateway_bringup.sh`.

use matrixobjectstore_rs::cluster::{ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        eprintln!("usage: star_catalog OUT PLACEMENT_ID CAPACITY_BYTES MAX_PAGES EP,EP,EP MEMBER,MEMBER,MEMBER");
        std::process::exit(2);
    }
    let endpoints: Vec<String> = args[5].split(',').map(str::to_string).collect();
    let members: Vec<String> = args[6].split(',').map(str::to_string).collect();
    let voters: Vec<u64> = (1..=endpoints.len() as u64).collect();
    let group = ChunkGroupConfig {
        placement_id: args[2].clone(),
        failure_domain: "local".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: args[3].parse().expect("capacity"),
            max_pages: args[4].parse().expect("max pages"),
            voters,
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints,
        member_ids: members,
    };
    let catalog = ChunkGroupCatalog::new(vec![group]).expect("a valid standalone group");
    std::fs::write(&args[1], catalog.to_bytes().expect("encode")).expect("write catalog");
}
