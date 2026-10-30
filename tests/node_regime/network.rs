// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Kernel packet loss confined to a disposable test network namespace.
use std::net::SocketAddr;
use std::process::Command;

pub fn require_isolated() {
    assert_ne!(
        std::fs::read_link("/proc/self/ns/net").unwrap(),
        std::fs::read_link("/proc/1/ns/net").unwrap(),
        "run this test through tools/network_partition_test.sh; never modify the host namespace"
    );
}

pub struct BlockPeers(Vec<SocketAddr>);
impl BlockPeers {
    pub fn new(addresses: &[SocketAddr]) -> Self {
        require_isolated();
        let mut blocked = Self(Vec::new());
        for address in addresses {
            assert!(address.is_ipv4() && address.ip().is_loopback());
            assert!(Self::rule("-I", address));
            blocked.0.push(*address);
        }
        blocked
    }
    fn rule(operation: &str, address: &SocketAddr) -> bool {
        Command::new("iptables")
            .args([
                "-w",
                "2",
                operation,
                "OUTPUT",
                "-p",
                "tcp",
                "-d",
                &address.ip().to_string(),
                "--dport",
                &address.port().to_string(),
                "-j",
                "DROP",
            ])
            .status()
            .expect("iptables must be available")
            .success()
    }
    pub fn assert_dropped(&self) {
        let output = Command::new("iptables")
            .args([
                "-w",
                "2",
                "-L",
                "OUTPUT",
                "--numeric",
                "--verbose",
                "--exact",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        let listing = String::from_utf8(output.stdout).unwrap();
        for address in &self.0 {
            let port = format!("dpt:{}", address.port());
            let line = listing
                .lines()
                .find(|line| line.split_whitespace().any(|word| word == port))
                .unwrap_or_else(|| panic!("missing drop rule for {address}: {listing}"));
            let packets: u64 = line.split_whitespace().next().unwrap().parse().unwrap();
            assert!(packets > 0, "no packets dropped for {address}: {listing}");
            println!("PARTITION peer={address} dropped_packets={packets}");
        }
    }
    pub fn heal(&mut self) {
        while let Some(address) = self.0.last() {
            assert!(Self::rule("-D", address));
            self.0.pop();
        }
    }
}
impl Drop for BlockPeers {
    fn drop(&mut self) {
        for address in &self.0 {
            let _ = Self::rule("-D", address);
        }
    }
}
