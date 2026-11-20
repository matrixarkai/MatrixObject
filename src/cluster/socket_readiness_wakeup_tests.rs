// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::io::Read;

fn wake_bytes(readiness: &SocketReadiness) -> usize {
    let mut bytes = [0; 4096];
    (&readiness.receive).read(&mut bytes).unwrap()
}
fn wait(readiness: &SocketReadiness, stop: &ServerShutdown) -> bool {
    readiness.wait(None, std::iter::empty(), Duration::from_millis(1), stop)
}

#[test]
fn completion_bursts_use_one_wake_byte() {
    let stop = ServerShutdown::default();
    let readiness = SocketReadiness::new(&stop).unwrap();
    let notify = readiness.notifier();
    for _ in 0..4096 {
        notify.notify();
    }
    assert_eq!(wake_bytes(&readiness), 1);
}

#[test]
fn a_drained_completion_remains_pending_and_can_rearm() {
    let stop = ServerShutdown::default();
    let readiness = SocketReadiness::new(&stop).unwrap();
    let notify = readiness.notifier();
    for _ in 0..64 {
        notify.notify();
        // A receiver drain can consume a concurrent completion's wake byte.
        // The published completion must still prevent the next blocking poll.
        assert_eq!(wake_bytes(&readiness), 1);
        assert!(wait(&readiness, &stop));
        assert!(!wait(&readiness, &stop));
    }
}

#[test]
fn concurrent_completion_producers_share_one_pending_wake() {
    let stop = ServerShutdown::default();
    let readiness = SocketReadiness::new(&stop).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(5));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let notify = readiness.notifier();
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..1024 {
                    notify.notify();
                }
            });
        }
        barrier.wait();
    });
    assert_eq!(wake_bytes(&readiness), 1);
    assert!(wait(&readiness, &stop));
    assert!(!wait(&readiness, &stop));
    readiness.notifier().notify();
    assert!(wait(&readiness, &stop));
}

#[test]
fn completion_arriving_between_latch_clear_and_drain_is_retained() {
    let stop = ServerShutdown::default();
    let readiness = SocketReadiness::new(&stop).unwrap();
    let notify = readiness.notifier();
    for _ in 0..64 {
        notify.notify();
        assert!(notify.take_pending());
        // Force the producer's publication between the consumer's clear and
        // drain. The drain consumes both bytes, including the newer wake.
        notify.notify();
        readiness.drain_notifications();
        assert!(wait(&readiness, &stop));
        assert!(!wait(&readiness, &stop));
    }
}
