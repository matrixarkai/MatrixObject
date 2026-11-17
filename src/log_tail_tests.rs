// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::log_chunk::*;
use crate::log_tail::*;
use std::path::PathBuf;

const MAX: u64 = 64 * 1024 * 1024;

fn tail(length: u64, crc: u32) -> Option<ReplicaTail> {
    Some(ReplicaTail { length, crc, epoch: 1, sealed: false })
}
/// Replicas whose bytes are a prefix of one stream: the crc is a function of the length.
fn at(length: u64) -> Option<ReplicaTail> {
    tail(length, length as u32 * 31 + 7)
}

#[test]
fn strong_write_takes_the_shortest_replica() {
    let d = decide_tail(WriteMode::Strong, &[at(300), at(200), at(100)], None).unwrap();
    assert_eq!((d.length, d.cut, d.behind), (100, vec![0, 1], vec![]));
    // a lost replica does not stop the open: every acknowledged byte is on every replica
    let d = decide_tail(WriteMode::Strong, &[at(300), None, at(200)], None).unwrap();
    assert_eq!((d.length, d.cut), (200, vec![0]));
    let d = decide_tail(WriteMode::Strong, &[None, None, at(50)], None).unwrap();
    assert_eq!(d.length, 50);
    assert!(decide_tail(WriteMode::Strong, &[None, None, None], None).is_err());
}

#[test]
fn quorum_write_takes_the_length_two_replicas_hold() {
    let d = decide_tail(WriteMode::Quorum, &[at(300), at(200), at(100)], None).unwrap();
    assert_eq!((d.length, d.cut, d.behind), (200, vec![0], vec![2]));
    let d = decide_tail(WriteMode::Quorum, &[at(300), at(300), at(100)], None).unwrap();
    assert_eq!((d.length, d.cut, d.behind), (300, vec![], vec![2]));
    // two answer: the acknowledged bytes may be on only one of them, so the longer is kept
    let d = decide_tail(WriteMode::Quorum, &[at(300), None, at(100)], None).unwrap();
    assert_eq!((d.length, d.behind), (300, vec![2]));
    // one answers: not enough to know
    assert_eq!(
        decide_tail(WriteMode::Quorum, &[None, at(100), None], None),
        Err(TailError::TooFewReplicas { reachable: 1, needed: 2 })
    );
}

#[test]
fn a_recorded_length_is_final() {
    // the chunk was switched away from at 150: replicas holding more are cut, however many
    let d = decide_tail(WriteMode::Strong, &[at(300), at(150), at(150)], Some(150)).unwrap();
    assert_eq!((d.length, d.cut), (150, vec![0]));
    // a replica below it is behind; the others hold it
    let d = decide_tail(WriteMode::Quorum, &[at(300), at(150), at(90)], Some(150)).unwrap();
    assert_eq!((d.length, d.behind), (150, vec![2]));
    // nobody answering holds the recorded length: refused, never silently shortened
    assert_eq!(
        decide_tail(WriteMode::Strong, &[at(100), at(120), None], Some(150)),
        Err(TailError::ShorterThanRecorded { recorded: 150, longest: 120 })
    );
}

#[test]
fn a_sealed_replica_fixes_the_chunk() {
    let sealed = Some(ReplicaTail { length: 200, crc: 9, epoch: 4, sealed: true });
    let d = decide_tail(WriteMode::Strong, &[sealed, at(250), at(120)], None).unwrap();
    assert!(d.sealed);
    assert_eq!((d.length, d.cut, d.behind), (200, vec![1], vec![2]));
    // sealed replicas that disagree, or disagree with the record
    let other = Some(ReplicaTail { length: 200, crc: 10, epoch: 4, sealed: true });
    assert_eq!(decide_tail(WriteMode::Strong, &[sealed, other, None], None), Err(TailError::Divergent));
    assert_eq!(decide_tail(WriteMode::Strong, &[sealed, None, None], Some(199)), Err(TailError::Divergent));
}

#[test]
fn the_same_length_with_different_bytes_is_divergence() {
    assert_eq!(
        decide_tail(WriteMode::Strong, &[tail(100, 1), tail(100, 2), None], None),
        Err(TailError::Divergent)
    );
}

/// Every state three replicas can be in after a writer dies: no acknowledged byte is dropped and
/// nothing is invented, for every choice of the replica that does not answer.
#[test]
fn no_acknowledged_byte_is_dropped_for_any_crash_state_and_any_lost_replica() {
    let mut cases = 0;
    for a in 0..6u64 {
        for b in 0..6u64 {
            for c in 0..6u64 {
                let lengths = [a, b, c];
                let mut sorted = lengths;
                sorted.sort_unstable();
                for (mode, acked) in [(WriteMode::Strong, sorted[0]), (WriteMode::Quorum, sorted[1])] {
                    for lost in 0..4usize {
                        let replicas: Vec<_> =
                            (0..3).map(|i| if i == lost { None } else { at(lengths[i]) }).collect();
                        match decide_tail(mode, &replicas, None) {
                            Ok(d) => {
                                // quorum with a lost replica keeps the longer survivor; with the lost
                                // replica holding the acknowledged bytes alone this is what saves them
                                assert!(d.length >= acked, "{mode:?} {lengths:?} lost {lost}: {} < {acked}", d.length);
                                assert!(d.length <= sorted[2], "invented bytes: {mode:?} {lengths:?}");
                                for index in d.cut {
                                    assert!(lengths[index] > d.length);
                                }
                                for index in d.behind {
                                    assert!(lengths[index] < d.length);
                                }
                            }
                            Err(TailError::TooFewReplicas { .. }) => {
                                assert!(mode == WriteMode::Quorum && lost < 3 && replicas.iter().flatten().count() < 2);
                            }
                            Err(other) => panic!("{other:?}"),
                        }
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 6 * 6 * 6 * 2 * 4);
}

#[test]
fn leases_grant_one_writer_at_a_time_with_rising_epochs() {
    let mut leases = WriterLeases::default();
    assert_eq!(leases.grant("b/k", "gw1", 1000, 60_000, 10), Ok(10));
    assert_eq!(leases.live_epoch("b/k", 2000), Some(10));
    // a second holder is refused while the lease is live
    assert_eq!(
        leases.grant("b/k", "gw2", 2000, 60_000, 11),
        Err(LeaseError::Held { holder: "gw1".into(), expires_at_ms: 61_000 })
    );
    // the holder heartbeats; a stranger cannot
    assert_eq!(leases.renew("b/k", "gw1", 10, 30_000, 60_000), Ok(()));
    assert_eq!(leases.renew("b/k", "gw2", 10, 30_000, 60_000), Err(LeaseError::NotHeld));
    assert_eq!(leases.renew("b/k", "gw1", 9, 30_000, 60_000), Err(LeaseError::NotHeld));
    // it expires without heartbeats and the standby takes over with a higher epoch
    assert_eq!(leases.live_epoch("b/k", 91_000), None);
    assert_eq!(leases.grant("b/k", "gw2", 91_000, 60_000, 12), Ok(12));
    // the old holder can no longer renew or release
    assert_eq!(leases.renew("b/k", "gw1", 10, 91_001, 60_000), Err(LeaseError::NotHeld));
    assert_eq!(leases.release("b/k", "gw1", 10), Err(LeaseError::NotHeld));
    // release lets a standby in at once
    assert_eq!(leases.release("b/k", "gw2", 12), Ok(()));
    assert_eq!(leases.grant("b/k", "gw3", 91_002, 60_000, 13), Ok(13));
    // the same holder reopening gets a new epoch, fencing its own earlier writer
    assert_eq!(leases.grant("b/k", "gw3", 91_003, 60_000, 14), Ok(14));
    assert_eq!(leases.live_epoch("b/k", 91_004), Some(14));
    // an epoch that does not rise is never granted
    assert_eq!(leases.grant("other", "gw1", 91_004, 1000, 14), Err(LeaseError::NotHeld));
    assert_eq!(leases.highest_epoch(), 14);
}

struct Dir(PathBuf);
impl Dir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("logtail-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn stream(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i * 13 + 5) as u8).collect()
}

/// A writer at epoch 1 dies after writing different lengths to its three replicas; the new owner
/// opens the chunk, and the dead writer is refused everywhere from then on.
#[test]
fn opening_cuts_the_long_replicas_and_fences_the_dead_writer() {
    let dir = Dir::new("open");
    let data = stream(900);
    let lengths = [900usize, 600, 300];
    let mut chunks: Vec<LogChunk> = lengths
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let mut c = LogChunk::open(&dir.0.join(format!("r{i}")), 1, MAX).unwrap();
            for start in (0..*n).step_by(100) {
                c.append(1, start as u64, &data[start..start + 100]).unwrap();
            }
            c.commit().unwrap();
            c
        })
        .collect();
    let [r0, r1, r2] = &mut chunks[..] else { unreachable!() };
    let decision = open_for_append(
        WriteMode::Strong,
        &mut [Some(r0), Some(r1), Some(r2)],
        None,
        2,
    )
    .unwrap();
    assert_eq!((decision.length, decision.cut.clone()), (300, vec![0, 1]));
    for c in &mut chunks {
        assert_eq!(c.length(), 300);
        assert_eq!(c.epoch(), 2);
        assert_eq!(c.read(0, 300), &data[..300]);
        // the dead writer's next pipelined write is refused on every replica
        assert!(matches!(c.append(1, 300, &data[300..400]), Err(LogError::Fenced { current: 2 })));
    }
    // the new owner appends at epoch 2 to all three and they agree
    for c in &mut chunks {
        c.append(2, 300, &data[300..350]).unwrap();
        c.commit().unwrap();
    }
    assert_eq!(chunks[0].footer_crc(), chunks[2].footer_crc());
    // a restart replays the cut: the replica that held 900 now ends at 350
    drop(chunks);
    let again = LogChunk::open(&dir.0.join("r0"), 1, MAX).unwrap();
    assert_eq!((again.length(), again.epoch()), (350, 2));
}

#[test]
fn opening_with_a_lost_replica_leaves_it_to_be_fenced_when_it_returns() {
    let dir = Dir::new("lost");
    let data = stream(500);
    let mut a = LogChunk::open(&dir.0.join("a"), 1, MAX).unwrap();
    let mut b = LogChunk::open(&dir.0.join("b"), 1, MAX).unwrap();
    let mut c = LogChunk::open(&dir.0.join("c"), 1, MAX).unwrap();
    a.append(1, 0, &data[..500]).unwrap();
    b.append(1, 0, &data[..400]).unwrap();
    c.append(1, 0, &data[..300]).unwrap();
    for x in [&mut a, &mut b, &mut c] {
        x.commit().unwrap();
    }
    // quorum, replica c does not answer: the longer of a and b is kept
    let d = open_for_append(WriteMode::Quorum, &mut [Some(&mut a), Some(&mut b), None], None, 2).unwrap();
    assert_eq!((d.length, d.cut, d.behind), (500, vec![], vec![1]));
    assert_eq!(c.epoch(), 1, "an unreachable replica is untouched");
    // when c returns it is below the decided length (it must catch up) and the old epoch is
    // refused once the new writer has reached it
    c.truncate(2, c.length()).unwrap();
    assert!(matches!(c.append(1, 300, &data[300..400]), Err(LogError::Fenced { .. })));
}

#[test]
fn a_sealed_replica_is_left_alone_and_a_recorded_length_cuts_the_rest() {
    let dir = Dir::new("sealed");
    let data = stream(400);
    let mut a = LogChunk::open(&dir.0.join("a"), 1, MAX).unwrap();
    let mut b = LogChunk::open(&dir.0.join("b"), 1, MAX).unwrap();
    a.append(1, 0, &data[..300]).unwrap();
    a.seal(1, true).unwrap();
    b.append(1, 0, &data[..400]).unwrap();
    b.commit().unwrap();
    let d = open_for_append(WriteMode::Strong, &mut [Some(&mut a), Some(&mut b)], Some(300), 2).unwrap();
    assert!(d.sealed);
    assert_eq!((d.length, d.cut), (300, vec![1]));
    assert_eq!(b.length(), 300);
    assert_eq!(a.epoch(), 1, "a sealed replica is not rewritten");
}
