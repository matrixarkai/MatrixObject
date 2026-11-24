// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn publication_cluster() -> (Directory, Vec<Node>) {
    let dir = Directory::new();
    let nodes = (0..3)
        .map(|i| {
            Node::start_with_options(
                &dir.0.join(format!("n{i}")),
                "127.0.0.1:0",
                ReplicaNodeOptions {
                    publication_writes: true,
                    ..Default::default()
                },
            )
        })
        .collect();
    (dir, nodes)
}
const PUB_INIT: [u8; 16] = [7; 16];
fn pub_call(node: &Node, request: LogOpRequest) -> LogOpReply {
    RemoteReplicaClient::connect(&node.addr, &options())
        .unwrap()
        .log_call(request)
        .unwrap()
}
fn pub_init(nodes: &[Node]) {
    for node in nodes {
        pub_call(
            node,
            LogOpRequest::initialize_publication(V, C, 1, 1, PUB_INIT),
        );
    }
}
fn pub_append(node: &Node, offset: u64, body: &[u8], tag: u8, published: u64) {
    pub_call(
        node,
        LogOpRequest::append_published(
            V,
            C,
            1,
            1,
            offset,
            body.to_vec(),
            [tag; 16],
            published,
            PUB_INIT,
        ),
    );
}
fn pub_length(node: &Node) -> u64 {
    pub_call(node, LogOpRequest::publication_probe(V, C, 1)).published_length
}
#[test]
fn published_read_repairs_lone_complete_prefix_before_surviving_source_loss() {
    let (_dir, mut nodes) = publication_cluster();
    pub_init(&nodes);
    for node in &nodes {
        pub_append(node, 0, b"ab", 1, 0);
    }
    pub_append(&nodes[0], 2, b"cd", 2, 4);
    let eps = endpoints(&nodes);
    assert_eq!(
        reader(&eps)
            .read_published(1, &[PUB_INIT; 3], 0, 4, || Ok(()))
            .unwrap(),
        b"abcd"
    );
    assert!(nodes.iter().filter(|n| pub_length(n) == 4).count() >= 2);
    nodes[0].halt();
    // A fresh client must stabilize again, rather than return the median zero.
    assert_eq!(
        reader(&eps)
            .read_published(1, &[PUB_INIT; 3], 0, 4, || Ok(()))
            .unwrap(),
        b"abcd"
    );
    assert_eq!(pub_length(&nodes[1]), 4);
    assert_eq!(pub_length(&nodes[2]), 4);
}
#[test]
fn published_read_rejects_conflicting_complete_tips_and_prefixes() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    pub_append(&nodes[0], 0, b"abcd", 1, 4);
    pub_append(&nodes[1], 0, b"wxyz", 2, 4);
    assert!(matches!(
        reader(&endpoints(&nodes)).stabilize_publication(1, &[PUB_INIT; 3], || Ok(())),
        Err(ChunkLogError::AttemptMismatch { .. })
    ));
    assert_eq!(pub_length(&nodes[2]), 0);
}
#[test]
fn published_read_rejects_conflicting_shorter_prefix() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    pub_append(&nodes[0], 0, b"ab", 1, 2);
    pub_append(&nodes[0], 2, b"cd", 2, 4);
    pub_append(&nodes[1], 0, b"xy", 3, 2);
    assert!(matches!(
        reader(&endpoints(&nodes)).stabilize_publication(1, &[PUB_INIT; 3], || Ok(())),
        Err(ChunkLogError::AttemptMismatch { .. })
    ));
    assert_eq!(pub_length(&nodes[2]), 0);
}
#[test]
fn published_read_never_counts_or_initializes_missing_members() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes[..1]);
    pub_append(&nodes[0], 0, b"ab", 1, 2);
    assert!(matches!(
        reader(&endpoints(&nodes)).stabilize_publication(1, &[PUB_INIT; 3], || Ok(())),
        Err(ChunkLogError::NoQuorum { .. })
    ));
    for node in &nodes[1..] {
        assert!(!pub_call(node, LogOpRequest::publication_probe(V, C, 1)).chunk_present);
    }
}
#[test]
fn published_read_invalidated_pin_cannot_be_retried_into_success() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    pub_append(&nodes[0], 0, b"ab", 1, 2);
    let mut checks = 0;
    let result = reader(&endpoints(&nodes)).stabilize_publication(1, &[PUB_INIT; 3], || {
        checks += 1;
        if checks == 2 {
            Err(ChunkLogError::Refused(ObjectError::PreconditionFailed(
                "pin expired".into(),
            )))
        } else {
            Ok(())
        }
    });
    assert!(result.is_err());
    assert_eq!(checks, 2);
    assert_eq!(pub_length(&nodes[1]), 0);
    assert_eq!(pub_length(&nodes[2]), 0);
}
#[test]
fn published_read_rejects_new_epoch_wrong_initialization_and_duplicate_routes() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    pub_call(
        &nodes[2],
        LogOpRequest::fence_publication(V, C, 1, 2, PUB_INIT),
    );
    let eps = endpoints(&nodes);
    assert_eq!(
        reader(&eps).stabilize_publication(1, &[PUB_INIT; 3], || Ok(())),
        Err(ChunkLogError::StaleEpoch { current: 2 })
    );
    assert!(reader(&eps)
        .stabilize_publication(2, &[[9; 16]; 3], || Ok(()))
        .is_err());
    assert!(reader(&vec![eps[0].clone(); 3])
        .stabilize_publication(1, &[PUB_INIT; 3], || Ok(()))
        .is_err());
}
#[test]
fn published_read_maintenance_members_cannot_certify_lone_prefix() {
    let (_dir, mut nodes) = publication_cluster();
    pub_init(&nodes);
    pub_append(&nodes[0], 0, b"ab", 1, 2);
    for node in &mut nodes[1..] {
        node.restart();
    } // restart defaults to read-only publication mode
    assert!(matches!(
        reader(&endpoints(&nodes)).stabilize_publication(1, &[PUB_INIT; 3], || Ok(())),
        Err(ChunkLogError::NoQuorum { .. })
    ));
    assert_eq!(pub_length(&nodes[1]), 0);
    assert_eq!(pub_length(&nodes[2]), 0);
}

#[test]
fn published_read_checks_pin_after_fetching_payload() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    for node in &nodes {
        pub_append(node, 0, b"ab", 1, 2);
    }
    let mut checks = 0;
    let result = reader(&endpoints(&nodes)).read_published(1, &[PUB_INIT; 3], 0, 2, || {
        checks += 1;
        if checks == 2 {
            Err(ChunkLogError::Refused(ObjectError::PreconditionFailed(
                "pin expired".into(),
            )))
        } else {
            Ok(())
        }
    });
    assert!(result.is_err());
    assert_eq!(checks, 2);
}
#[test]
fn published_read_never_exposes_a_durable_unpublished_suffix() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    for node in &nodes {
        pub_append(node, 0, b"ab", 1, 2);
        pub_append(node, 2, b"cd", 2, 2);
    }
    let eps = endpoints(&nodes);
    assert_eq!(
        reader(&eps)
            .read_published(1, &[PUB_INIT; 3], 0, 2, || Ok(()))
            .unwrap(),
        b"ab"
    );
    assert!(reader(&eps)
        .read_published(1, &[PUB_INIT; 3], 0, 4, || Ok(()))
        .is_err());
}

#[test]
fn published_read_starts_every_probe_before_waiting_for_any_reply() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    for node in &nodes {
        pub_append(node, 0, b"complete", 1, 8);
    }
    let gate = Arc::new((std::sync::Mutex::new(0usize), std::sync::Condvar::new()));
    let proxies: Vec<_> = nodes
        .iter()
        .map(|node| {
            FaultProxy::with_probe_gate(node.addr.clone(), 0, false, Some(Arc::clone(&gate)))
        })
        .collect();
    let endpoints: Vec<_> = proxies.iter().map(|proxy| proxy.addr.clone()).collect();
    let mut config = options();
    config.read_timeout = Duration::from_secs(5);
    let mut reader = ChunkLogReader::new(&endpoints, V, C, 1, config);
    assert_eq!(
        reader
            .read_published(1, &[PUB_INIT; 3], 0, 8, || Ok(()))
            .unwrap(),
        b"complete"
    );
    assert_eq!(*gate.0.lock().unwrap(), 3);
    let operations: Vec<_> = proxies
        .iter()
        .flat_map(|p| p.operations.lock().unwrap().clone())
        .collect();
    assert_eq!(
        operations
            .iter()
            .filter(|&&op| op == super::super::log_op::OP_TAIL)
            .count(),
        3
    );
    assert_eq!(
        operations
            .iter()
            .filter(|&&op| op == super::super::log_op::OP_READ)
            .count(),
        1
    );
    assert_eq!(operations.len(), 4);
}

#[test]
fn published_probe_retries_only_a_reused_failed_connection() {
    let (_dir, nodes) = publication_cluster();
    pub_init(&nodes);
    for node in &nodes {
        pub_append(node, 0, b"complete", 1, 8);
    }
    let proxy = FaultProxy::new(nodes[2].addr.clone(), super::super::log_op::OP_TAIL, false);
    let mut eps = endpoints(&nodes);
    eps[2] = proxy.addr.clone();
    proxy.armed.store(false, Ordering::SeqCst);
    let mut reader = reader(&eps);
    assert_eq!(
        reader
            .stabilize_publication(1, &[PUB_INIT; 3], || Ok(()))
            .unwrap(),
        8
    );
    proxy.operations.lock().unwrap().clear();
    proxy.armed.store(true, Ordering::SeqCst);
    assert_eq!(
        reader
            .stabilize_publication(1, &[PUB_INIT; 3], || Ok(()))
            .unwrap(),
        8
    );
    assert_eq!(
        *proxy.operations.lock().unwrap(),
        vec![super::super::log_op::OP_TAIL, super::super::log_op::OP_TAIL,]
    );
    assert_eq!(
        reader
            .stabilize_publication(1, &[PUB_INIT; 3], || Ok(()))
            .unwrap(),
        8
    );
}
