// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
#[test]
fn distinct_storage_initializations_survive_repair_source_loss_and_restart() {
    let dir = Directory::new();
    let mut nodes: Vec<_> = (0..3)
        .map(|i| {
            Node::start_with_options(
                &dir.0.join(format!("n{i}")),
                "127.0.0.1:0",
                ReplicaNodeOptions {
                    publication_writes: true,
                    max_payload_bytes: 512 << i,
                    ..Default::default()
                },
            )
        })
        .collect();
    let initializations = [[7; 16], [8; 16], [9; 16]];
    let call = |node: &Node, request: LogOpRequest| {
        RemoteReplicaClient::connect(&node.addr, &options())
            .unwrap()
            .log_call(request)
            .unwrap()
    };
    for (i, node) in nodes.iter().enumerate() {
        call(
            node,
            LogOpRequest::initialize_publication(V, C, 1, 1, initializations[i]),
        );
        call(
            node,
            LogOpRequest::append_published(
                V,
                C,
                1,
                1,
                0,
                b"ab".to_vec(),
                [11; 16],
                0,
                initializations[i],
            ),
        );
    }
    call(
        &nodes[0],
        LogOpRequest::append_published(
            V,
            C,
            1,
            1,
            2,
            b"cd".to_vec(),
            [12; 16],
            4,
            initializations[0],
        ),
    );
    let endpoints = endpoints(&nodes);
    assert_eq!(
        reader(&endpoints)
            .read_published(1, &initializations, 0, 4, || Ok(()))
            .unwrap(),
        b"abcd"
    );
    nodes[0].halt();
    assert_eq!(
        reader(&endpoints)
            .read_published(1, &initializations, 0, 4, || Ok(()))
            .unwrap(),
        b"abcd"
    );
    assert!(reader(&endpoints)
        .read_published(1, &[[7; 16]; 3], 0, 4, || Ok(()))
        .is_err());
    for (i, node) in nodes.iter_mut().enumerate().skip(1) {
        node.restart();
        let reply = call(
            node,
            LogOpRequest::identity_initialized_publication(V, C, 1, 2, initializations[i]),
        );
        assert_eq!(reply.publication_initialization(), Some(initializations[i]));
        assert_eq!(reply.record_identity().unwrap().attempt_tag, Some([12; 16]));
        assert_eq!(reply.record_published_length, 4);
    }
    assert_eq!(
        reader(&endpoints)
            .read_published(1, &initializations, 0, 4, || Ok(()))
            .unwrap(),
        b"abcd"
    );
}
