// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Read, declare and raise the cluster feature bits of a metadata group.
//!
//! ```text
//! cluster_features EP[,EP...] show
//! cluster_features EP[,EP...] declare VOTER BITS
//! cluster_features EP[,EP...] raise BITS VOTER[,VOTER...]
//! ```
//!
//! `BITS` is a number (`0x` for hex). A raise is refused unless every listed voter (the whole
//! membership) has declared every bit. Raised bits are never lowered. Bits: 1 compact append,
//! 2 reserve-next, 4 chunk manifest, 8 read grace, 16 log chunk leases (raised together with 4
//! for log objects: 0x14).
//!
//! A raise is irreversible: raised bits are never lowered, and raising the chunk manifest bits on a
//! cluster that is not a private test cluster is an explicit decision of its owner.

use matrixobjectstore_rs::cluster::{MetaClientOptions, RemoteMetaClient};

fn bits(text: &str) -> u64 {
    match text.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16).expect("bits"),
        None => text.parse().expect("bits"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: cluster_features EP[,EP...] show | declare VOTER BITS | raise BITS VOTER[,VOTER...]");
        std::process::exit(2);
    }
    let endpoints: Vec<String> = args[1].split(',').map(str::to_string).collect();
    let mut client = RemoteMetaClient::connect_catalog(&endpoints, &MetaClientOptions::default())
        .expect("connect to the metadata group");
    let features = match (args[2].as_str(), args.len()) {
        ("show", 3) => client.read_cluster_features(),
        ("declare", 5) => client.declare_voter_features(args[3].parse().expect("voter"), bits(&args[4])),
        ("raise", 5) => {
            let voters: Vec<u64> = args[4].split(',').map(|v| v.parse().expect("voter")).collect();
            client.raise_cluster_features(bits(&args[3]), voters)
        }
        _ => {
            eprintln!("usage: cluster_features EP[,EP...] show | declare VOTER BITS | raise BITS VOTER[,VOTER...]");
            std::process::exit(2);
        }
    };
    match features {
        Ok(features) => {
            println!("raised {:#x}", features.raised);
            for (voter, declared) in &features.declared {
                println!("voter {voter} declares {declared:#x}");
            }
        }
        Err(error) => {
            eprintln!("refused: {error}");
            std::process::exit(1);
        }
    }
}
