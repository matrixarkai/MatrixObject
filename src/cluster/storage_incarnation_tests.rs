// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
#[test]
fn stale_creation_after_physical_loss_cannot_supply_an_empty_native_vote() {
    let directory = Directory::new();
    let start = |path: &Path, addr: &str| {
        Node::start_with_options(
            path,
            addr,
            ReplicaNodeOptions {
                publication_writes: true,
                ..Default::default()
            },
        )
    };
    let mut nodes: Vec<_> = (0..3)
        .map(|i| start(&directory.0.join(format!("n{i}")), "127.0.0.1:0"))
        .collect();
    let call = |node: &Node, request: LogOpRequest| {
        RemoteReplicaClient::connect(&node.addr, &options())
            .unwrap()
            .log_call(request)
    };
    let roots: Vec<_> = nodes
        .iter()
        .map(|node| {
            call(node, LogOpRequest::storage_probe())
                .unwrap()
                .storage_incarnation()
                .unwrap()
        })
        .collect();
    let initializations = [[7; 16], [8; 16], [9; 16]];
    for i in 0..3 {
        call(
            &nodes[i],
            LogOpRequest::initialize_publication(V, C, 1, 1, initializations[i])
                .for_storage(roots[i], initializations[i]),
        )
        .unwrap();
        if i < 2 {
            call(
                &nodes[i],
                LogOpRequest::append_published(
                    V,
                    C,
                    1,
                    1,
                    0,
                    b"abcd".to_vec(),
                    [11; 16],
                    4,
                    initializations[i],
                )
                .for_storage(roots[i], initializations[i]),
            )
            .unwrap();
        }
    }
    for i in 0..2 {
        let reply = call(
            &nodes[i],
            LogOpRequest::publication_tail(V, C, 1).for_storage(roots[i], initializations[i]),
        )
        .unwrap();
        assert_eq!(
            reply
                .initialized_publication_prefix(initializations[i])
                .unwrap()
                .length,
            4
        );
    }
    let read = call(
        &nodes[0],
        LogOpRequest::read_initialized_publication(V, C, 1, 0, 4, initializations[0])
            .for_storage(roots[0], initializations[0]),
    )
    .unwrap();
    assert_eq!(read.data, b"abcd");
    nodes[0].halt();
    nodes[1].halt();
    let path = nodes[0].dir.clone();
    assert!(path.starts_with(&directory.0) && path != directory.0);
    std::fs::remove_dir_all(&path).unwrap();
    nodes[0] = start(&path, &nodes[0].addr);
    let replacement = call(&nodes[0], LogOpRequest::storage_probe())
        .unwrap()
        .storage_incarnation()
        .unwrap();
    assert_ne!(replacement, roots[0]);
    let delayed = LogOpRequest::initialize_publication(V, C, 1, 1, initializations[0])
        .for_storage(roots[0], initializations[0]);
    assert!(call(&nodes[0], delayed).is_err());
    assert!(call(
        &nodes[0],
        LogOpRequest::publication_tail(V, C, 1).for_storage(roots[0], initializations[0])
    )
    .is_err());
    let remaining = call(
        &nodes[2],
        LogOpRequest::publication_tail(V, C, 1).for_storage(roots[2], initializations[2]),
    )
    .unwrap();
    assert_eq!(remaining.published_length, 0);
    // One surviving empty member cannot turn replacement storage into a second qualified vote.
    assert!(!remaining.has_storage_write_protocol(roots[0]));
}
