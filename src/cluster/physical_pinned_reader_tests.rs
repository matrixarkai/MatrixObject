// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::native_object_outcome::{native_operation::Operation, *};
use crate::native_object_state_tests::{chunk, hash, publication};
use crate::physical_member_binding_tests::{commit, physical_start, stage_ref};
fn setup() -> (
    Directory,
    Vec<Node>,
    crate::MatrixMetaService,
    crate::initialized_log_manifest::PhysicalMemberBinding,
) {
    let directory = Directory::new();
    let nodes: Vec<_> = (0..3)
        .map(|i| {
            Node::start_with_options(
                &directory.0.join(format!("n{i}")),
                "127.0.0.1:0",
                ReplicaNodeOptions {
                    publication_writes: true,
                    ..Default::default()
                },
            )
        })
        .collect();
    let call = |node: &Node, request: LogOpRequest| {
        RemoteReplicaClient::connect(&node.addr, &options())
            .unwrap()
            .log_call(request)
            .unwrap()
    };
    let roots: Vec<_> = nodes
        .iter()
        .map(|n| {
            call(n, LogOpRequest::storage_probe())
                .storage_incarnation()
                .unwrap()
        })
        .collect();
    let mut service = crate::native_object_outcome_tests::service();
    let start = physical_start(2, None, 4);
    let mut owned = chunk(&start, 3, 0, 4, 0);
    for (i, r) in owned
        .initialization
        .as_mut()
        .unwrap()
        .replicas
        .iter_mut()
        .enumerate()
    {
        r.storage_incarnation = roots[i].to_vec();
    }
    commit(
        &mut service,
        Operation::Begin(NativeBegin {
            start: Some(start.clone()),
            ..Default::default()
        }),
        1000,
    );
    commit(
        &mut service,
        Operation::Extend(NativeExtend {
            stage: Some(stage_ref(2, 2)),
            chunk: Some(owned.clone()),
        }),
        1001,
    );
    for i in 0..3 {
        let init: [u8; 16] = owned.initialization.as_ref().unwrap().replicas[i]
            .initialization
            .as_slice()
            .try_into()
            .unwrap();
        call(
            &nodes[i],
            LogOpRequest::initialize_publication(0, 3, 1, 2, init).for_storage(roots[i], init),
        );
        call(
            &nodes[i],
            LogOpRequest::append_published(0, 3, 1, 2, 0, b"abcd".to_vec(), [11; 16], 4, init)
                .for_storage(roots[i], init),
        );
    }
    let manifest = publication(&start, &[owned]);
    let id = manifest.object_id();
    commit(
        &mut service,
        Operation::Publish(NativePublish {
            stage: Some(stage_ref(2, 3)),
            manifest: Some(manifest.clone()),
            expected_lease: None,
        }),
        1002,
    );
    commit(
        &mut service,
        Operation::OpenPin(crate::native_object_state::NativeReadPin {
            pin_id: 5,
            bucket: id.bucket.to_string(),
            key: id.key.to_string(),
            generation: manifest.generation,
            manifest_digest: hash(&manifest),
            expires_at_ms: 5000,
        }),
        1003,
    );
    let binding = service
        .native_objects
        .physical_read_binding(5, 0, 1003)
        .unwrap();
    (directory, nodes, service, binding)
}
fn call(node: &Node, request: LogOpRequest) -> LogOpReply {
    RemoteReplicaClient::connect(&node.addr, &options())
        .unwrap()
        .log_call(request)
        .unwrap()
}
#[test]
fn committed_physical_pin_reads_and_repairs_after_source_loss_and_restart() {
    let (_d, mut nodes, service, binding) = setup();
    let request = LogOpRequest::append_published(
        0,
        3,
        1,
        2,
        4,
        b"ef".to_vec(),
        [12; 16],
        6,
        binding.initializations[0],
    )
    .for_storage(binding.storage_incarnations[0], binding.initializations[0]);
    call(&nodes[0], request);
    let endpoints = endpoints(&nodes);
    let mut reader = ChunkLogReader::new(&endpoints, 0, 3, 1, options());
    let check = || {
        service
            .native_objects
            .physical_read_binding(5, 0, 1004)
            .map(|_| ())
            .map_err(|e| ChunkLogError::Refused(e))
    };
    assert_eq!(
        reader.read_physical_pinned(&binding, 0, 6, check).unwrap(),
        b"abcdef"
    );
    nodes[0].halt();
    assert_eq!(
        reader.read_physical_pinned(&binding, 0, 6, check).unwrap(),
        b"abcdef"
    );
    for i in 1..3 {
        nodes[i].restart();
        let observed = call(
            &nodes[i],
            LogOpRequest::identity_initialized_publication(0, 3, 1, 4, binding.initializations[i])
                .for_storage(binding.storage_incarnations[i], binding.initializations[i]),
        );
        assert_eq!(
            observed.record_identity().unwrap().attempt_tag,
            Some([12; 16])
        );
    }
    assert_eq!(
        reader.read_physical_pinned(&binding, 0, 6, check).unwrap(),
        b"abcdef"
    );
}
#[test]
fn expired_pins_wrong_physical_members_and_wrong_chunks_refuse_reads() {
    let (_d, nodes, service, binding) = setup();
    let endpoints = endpoints(&nodes);
    let mut reader = ChunkLogReader::new(&endpoints, 0, 3, 1, options());
    assert!(reader
        .read_physical_pinned(&binding, 0, 4, || service
            .native_objects
            .physical_read_binding(5, 0, 5000)
            .map(|_| ())
            .map_err(ChunkLogError::Refused))
        .is_err());
    let mut wrong = binding.clone();
    wrong.storage_incarnations = [[80; 16], [81; 16], [82; 16]];
    assert!(reader
        .read_physical_pinned(&wrong, 0, 4, || Ok(()))
        .is_err());
    let mut wrong = binding.clone();
    wrong.chunk_id += 1;
    assert!(reader
        .read_physical_pinned(&wrong, 0, 4, || Ok(()))
        .is_err());
    assert_eq!(
        reader
            .read_physical_pinned(&binding, 0, 4, || Ok(()))
            .unwrap(),
        b"abcd"
    );
}
#[test]
fn a_higher_storage_epoch_never_becomes_qualified_by_an_old_pin() {
    let (_d, nodes, _service, binding) = setup();
    call(
        &nodes[0],
        LogOpRequest::fence_publication(0, 3, 1, 3, binding.initializations[0])
            .for_storage(binding.storage_incarnations[0], binding.initializations[0]),
    );
    let mut reader = ChunkLogReader::new(&endpoints(&nodes), 0, 3, 1, options());
    assert!(matches!(
        reader.read_physical_pinned(&binding, 0, 4, || Ok(())),
        Err(ChunkLogError::StaleEpoch { current: 3 })
    ));
    for i in 1..3 {
        let observed = call(
            &nodes[i],
            LogOpRequest::publication_tail(0, 3, 1)
                .for_storage(binding.storage_incarnations[i], binding.initializations[i]),
        );
        assert_eq!(observed.epoch, 2);
    }
}
