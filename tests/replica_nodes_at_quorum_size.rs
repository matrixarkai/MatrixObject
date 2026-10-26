// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Replica nodes at a width where a majority is not the same number as all-but-one.
//!
//! # WHY THREE REPLICAS COULD NOT SETTLE THIS
//!
//! This store's shipped write quorum is 2 of 3. That number is a majority of three AND
//! all-but-one of three at the same time, so no test at three replicas can say which of the
//! two rules the fan-out implements. The two part company at five, where a majority is 3 and
//! all-but-one is 4 -- and five is also the first width at which the interesting question has
//! an answer: what happens when the set is down to exactly a quorum, and what happens one
//! below it.
//!
//! So the width here is five processes, both quorums are a majority of five, and the two
//! regimes are entered deliberately and asserted separately:
//!
//! | live nodes | what this test requires |
//! |---:|---|
//! | 5 | every page acknowledged by all five |
//! | 3 (exactly a quorum) | all 24 pages read back exact; a new write acknowledged by exactly 3; an unwritten page reads ABSENT |
//! | 2 (one below) | reads REFUSED, writes REFUSED, and absence NOT asserted |
//!
//! # THE LAST ROW IS THE POINT
//!
//! At two live nodes of five, a read cannot be answered and it also cannot be denied. The
//! two surviving nodes do not hold a read quorum, so "this page is absent" is a sentence this
//! set is not entitled to say -- it has not heard from enough of itself to know. A store that
//! answered `absent` there would be answering confidently and wrongly, which is worse than
//! refusing: a caller can retry a refusal and cannot retry a wrong answer it has already
//! acted on. So the test requires the below-quorum read of a page that was never written to
//! fail WITHOUT claiming absence, which is the same assertion as the one above it with the
//! opposite expected verdict, and it is the assertion a read path drifts out of first.
//!
//! # LINUX ONLY FOR THE PROCESS TEST, AND SAID SO RATHER THAN SKIPPED QUIETLY
//!
//! The regime assertions read `/proc` and the kernel socket table. There is no portable form
//! of that, so the five-node test is compiled only on Linux. The quorum arithmetic above it
//! is portable and is not gated.

#![cfg(feature = "replica-server")]

use matrixobjectstore_rs as store;

use store::cluster::{check_quorums, majority, ReplicaClientOptions, ReplicaFanout, ReplicaPlacement};
use store::{ChunkId, ObjectError, PageId};

use std::time::Duration;

#[cfg(target_os = "linux")]
mod node_regime;

// ---------------------------------------------------------------------------------------
// The corpus
// ---------------------------------------------------------------------------------------

/// How many replica processes this test stands up.
const NODE_COUNT: usize = 5;

/// How many pages are written and then demanded back exactly.
const CORPUS_PAGES: u64 = 24;

/// Bytes per page.
const PAGE_BYTES: usize = 4096;

/// The page this key names.
///
/// DELIBERATELY NOT THE SAME ADDRESSES as the three-node test's corpus. All three fields of
/// a page address are `u64` on the wire, so a bug that confused two of them can land on the
/// right page by coincidence for one pattern of addresses and not another. Two tests walking
/// two different patterns is a second sample of the same claim rather than the same sample
/// twice.
fn corpus_page_id(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(index % NODE_COUNT as u64 + 2, index * 3 + 11)
            .expect("a corpus chunk id fits"),
        page_index: index * 7 + 2,
    }
}

/// The bytes this key holds, distinct per key and not a constant fill.
///
/// A constant payload makes "zero wrong" unfalsifiable: every page would read back correctly
/// even if the store served the wrong one. The index is mixed into every byte.
fn corpus_payload(index: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PAGE_BYTES);
    for at in 0..PAGE_BYTES {
        payload.push(((index as usize)
            .wrapping_mul(97)
            .wrapping_add(at.wrapping_mul(13))
            % 239) as u8);
    }
    payload
}

/// A page the corpus never writes, for the negative control.
fn never_written_page_id() -> PageId {
    PageId {
        chunk_id: ChunkId::new(31, 777_777).expect("the control chunk id fits"),
        page_index: 313_131,
    }
}

/// The page the at-quorum write lands on, outside the corpus range.
fn at_quorum_page_id() -> PageId {
    PageId {
        chunk_id: ChunkId::new(17, 4_242).expect("the at-quorum chunk id fits"),
        page_index: 99,
    }
}

// ---------------------------------------------------------------------------------------
// The arithmetic, before any process is started
// ---------------------------------------------------------------------------------------

/// A majority is `n / 2 + 1`, it is not all-but-one, and two majorities always overlap.
///
/// Pure arithmetic, and it is a test rather than a comment because the whole five-node test
/// below rests on `majority(5) == 3`: if that returned 4 the process test would still pass
/// and would be testing all-but-one while claiming a majority.
#[test]
fn a_majority_is_not_all_but_one_and_two_majorities_always_overlap() {
    assert_eq!(majority(1), 1);
    assert_eq!(majority(2), 2);
    assert_eq!(majority(3), 2);
    assert_eq!(majority(4), 3);
    assert_eq!(majority(5), 3);
    assert_eq!(majority(6), 4);
    assert_eq!(majority(7), 4);

    // AT THREE THE TWO RULES COINCIDE. This is the reason this file exists: a quorum of 2
    // over 3 replicas satisfies both readings, so the shipped configuration cannot
    // distinguish them.
    assert_eq!(
        majority(3),
        3 - 1,
        "at three replicas a majority and all-but-one are the same number"
    );
    // AT FIVE THEY DO NOT. The process test uses five for exactly this reason.
    assert_ne!(
        majority(NODE_COUNT),
        NODE_COUNT - 1,
        "at {NODE_COUNT} replicas a majority must differ from all-but-one, or this test's \
         width buys nothing over three"
    );

    // Two majorities of one set always share a member, which is what makes a majority write
    // quorum and a majority read quorum a safe pair at every width.
    for count in 1..=128usize {
        assert!(
            2 * majority(count) > count,
            "two majorities of {count} replica(s) do not overlap"
        );
        assert!(
            check_quorums(count, majority(count), majority(count)).is_ok(),
            "a majority/majority pair over {count} replica(s) was refused"
        );
    }
}

/// Each of the three admission clauses refuses, and each refuses by its own name.
///
/// The clauses are planted against individually rather than the condition as a whole: a
/// single case that fails tells you the check is there and not which part of it fired, and
/// a check with a clause that never fires is a check that has silently narrowed.
#[test]
fn a_quorum_pair_that_cannot_read_its_writes_is_refused_by_name() {
    // The pair this store has always shipped, and the majority pair at five, both pass.
    assert!(check_quorums(3, 2, 2).is_ok(), "the shipped 2/2 of 3 is admissible");
    assert!(check_quorums(5, 3, 3).is_ok(), "a majority pair of 5 is admissible");

    // CLAUSE 1: the write quorum.
    let zero_write = check_quorums(5, 0, 3).expect_err("a write quorum of zero is refused");
    assert!(
        zero_write.to_string().contains("a write quorum of 0 cannot be met"),
        "the refusal must name the write quorum clause, got {zero_write}"
    );
    let wide_write = check_quorums(5, 6, 3).expect_err("a write quorum past the set is refused");
    assert!(
        wide_write.to_string().contains("a write quorum of 6 cannot be met"),
        "the refusal must name the write quorum clause, got {wide_write}"
    );

    // CLAUSE 2: the read quorum.
    let zero_read = check_quorums(5, 3, 0).expect_err("a read quorum of zero is refused");
    assert!(
        zero_read.to_string().contains("a read quorum of 0 cannot be met"),
        "the refusal must name the read quorum clause, got {zero_read}"
    );
    let wide_read = check_quorums(5, 3, 6).expect_err("a read quorum past the set is refused");
    assert!(
        wide_read.to_string().contains("a read quorum of 6 cannot be met"),
        "the refusal must name the read quorum clause, got {wide_read}"
    );

    // CLAUSE 3: the overlap. 2/2 over five is reachable, arithmetically honest, and unsafe:
    // replicas 1 and 2 can acknowledge a write while replicas 3 and 4 answer the read.
    let no_overlap = check_quorums(5, 2, 2).expect_err("a non-overlapping pair is refused");
    assert!(
        no_overlap.to_string().contains("need not share a replica"),
        "the refusal must name the overlap clause, got {no_overlap}"
    );
    assert!(
        no_overlap.to_string().contains("sum to 4"),
        "the overlap refusal must show the sum it rejected, got {no_overlap}"
    );
    // The same pair at three replicas is admissible, so the clause is about the width and
    // not about the numbers 2 and 2. Without this the clause could be a blanket ban.
    assert!(
        check_quorums(3, 2, 2).is_ok(),
        "2/2 must stay admissible at three replicas, or the overlap clause is refusing \
         configurations this store already ships"
    );
}

/// The majority constructors produce a majority, and say so about themselves.
#[test]
fn the_majority_constructors_carry_a_majority_at_every_width() {
    let endpoints: Vec<String> = (0..NODE_COUNT)
        .map(|index| format!("127.0.0.1:{}", 17_400 + index))
        .collect();
    let remote = ReplicaPlacement::remote_with_majority(endpoints);
    assert_eq!(remote.replica_count(), NODE_COUNT);
    assert_eq!(remote.quorums(), (3, 3), "a majority of five is three, twice");
    assert!(remote.is_majority_quorum());
    assert!(remote.is_remote());

    let local = ReplicaPlacement::in_process_with_majority(NODE_COUNT);
    assert_eq!(local.replica_count(), NODE_COUNT);
    assert_eq!(local.quorums(), remote.quorums(), "both arms take the same majority");
    assert!(local.is_majority_quorum());
    assert!(!local.is_remote());

    // The default is unchanged by this file. It is 2/2 of 3, which is a majority of three --
    // so `is_majority_quorum` is true of it, and the width is what makes that uninformative.
    let default = ReplicaPlacement::default();
    assert_eq!(default.replica_count(), 3);
    assert_eq!(default.quorums(), (2, 2));
    assert!(default.is_majority_quorum());

    // NOT EVERY PAIR IS A MAJORITY PAIR, so the predicate is not a constant `true`. Without
    // this the three assertions above would pass against `fn is_majority_quorum() { true }`.
    let all_but_one = ReplicaPlacement::Remote {
        endpoints: (0..NODE_COUNT)
            .map(|index| format!("127.0.0.1:{}", 17_500 + index))
            .collect(),
        write_quorum: NODE_COUNT - 1,
        read_quorum: NODE_COUNT - 1,
    };
    assert!(
        !all_but_one.is_majority_quorum(),
        "an all-but-one pair of {NODE_COUNT} must not report itself a majority pair"
    );
}

// ---------------------------------------------------------------------------------------
// The test that decides it
// ---------------------------------------------------------------------------------------

/// Five replica processes; down to exactly a quorum it serves, one below it refuses.
///
/// The order of the assertions is the argument, so it is written out in the order it is made:
///
/// 1. **the regime** -- five distinct live PIDs, none of them this test's, and an ESTABLISHED
///    socket per replica attributed to that replica's PID, read from `ss -tnp` and from
///    `/proc` and required to agree;
/// 2. **the width and the quorums** -- five nodes, both quorums a majority of five, taken
///    from the constructor rather than written as literals;
/// 3. **every write acknowledged by all five**, so that what the kills remove is known to
///    have been present everywhere;
/// 4. **two kills by PID**, each after confirming from `/proc/<pid>/environ` that the process
///    belongs to this test, leaving exactly a quorum;
/// 5. **at exactly a quorum** -- all 24 pages back, exact count, zero missing, zero wrong; a
///    new write acknowledged by exactly three; an unwritten page reads ABSENT;
/// 6. **a third kill**, leaving one below a quorum;
/// 7. **one below a quorum** -- reads refused, writes refused, and absence NOT asserted.
#[cfg(target_os = "linux")]
#[test]
fn five_replica_nodes_serve_at_exactly_a_quorum_and_refuse_one_below_it() {
    use node_regime::{processes, sockets};

    let binary = env!("CARGO_BIN_EXE_chunkserver");
    let token = node_regime::run_token("quorum-size");

    let mut nodes: Vec<processes::NodeProcess> = (0..NODE_COUNT)
        .map(|_| processes::start_replica(binary, &token))
        .collect();

    // ---------------------------------------------------------------- 1. THE REGIME
    let pids: Vec<u32> = nodes.iter().map(|node| node.pid).collect();
    let distinct: std::collections::BTreeSet<u32> = pids.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        NODE_COUNT,
        "{NODE_COUNT} replica processes must have {NODE_COUNT} distinct PIDs, got {pids:?}"
    );
    assert!(
        !distinct.contains(&std::process::id()),
        "a replica PID is this test's own PID {}, so a replica is not a separate process",
        std::process::id()
    );
    for pid in &pids {
        assert!(
            processes::is_live(*pid),
            "/proc/{pid} does not exist, so that replica is not running"
        );
    }

    // ------------------------------------------------- 2. THE WIDTH AND THE QUORUMS
    let endpoints: Vec<String> = nodes.iter().map(|node| node.endpoint.clone()).collect();
    let placement = ReplicaPlacement::remote_with_majority(endpoints.clone());
    let quorum = majority(NODE_COUNT);
    assert_eq!(placement.replica_count(), NODE_COUNT);
    assert_eq!(
        placement.quorums(),
        (quorum, quorum),
        "both quorums must be a majority of {NODE_COUNT}"
    );
    assert!(placement.is_majority_quorum());
    assert_ne!(
        quorum,
        NODE_COUNT - 1,
        "the quorum under test must not be all-but-one, or this is a three-node test with \
         more processes"
    );
    assert!(placement.is_remote(), "the placement under test is the remote arm");

    let options = ReplicaClientOptions {
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(10),
        ..ReplicaClientOptions::default()
    };
    let mut fanout = placement
        .open(&options)
        .expect("five replica processes could be connected");
    assert!(fanout.is_remote(), "the opened fan-out is the remote one");
    assert_eq!(fanout.replica_count(), NODE_COUNT);
    let set = match &mut fanout {
        ReplicaFanout::Remote(set) => set,
        ReplicaFanout::InProcess(_) => {
            panic!("the remote placement opened as the in-process one, so nothing below this \
                    line crosses a socket")
        }
    };
    assert_eq!(set.write_quorum(), quorum);
    assert_eq!(set.read_quorum(), quorum);

    // Every replica answers, and starts empty, before anything is claimed about the set.
    let pongs = set.ping_all();
    assert_eq!(
        pongs,
        vec![Some(0); NODE_COUNT],
        "all {NODE_COUNT} replicas must answer and start empty"
    );

    // The sockets, from `ss -tnp` with PID attribution and then again from `/proc` with no
    // external tool. Both readings, and they are required to agree.
    sockets::assert_established_two_ways(&pids);

    // Each connection's four-tuple names a replica's bound address as its peer.
    let connections = set.connections().expect("connection addresses are readable");
    assert_eq!(connections.len(), NODE_COUNT, "one connection per replica");
    for (index, (local, peer)) in connections.iter().enumerate() {
        assert_eq!(
            peer, &endpoints[index],
            "connection {index} has peer {peer}, not the replica's bound address {}",
            endpoints[index]
        );
        assert_ne!(
            local, peer,
            "connection {index} has the same address on both ends, which is not a connection \
             between two processes"
        );
    }

    // ------------------------------------------- 3. EVERY WRITE ACKNOWLEDGED BY ALL FIVE
    for index in 0..CORPUS_PAGES {
        let acknowledged = set
            .write_chunk(&corpus_page_id(index), 0, &corpus_payload(index))
            .unwrap_or_else(|err| panic!("corpus page {index} was refused: {err}"));
        assert_eq!(
            acknowledged, NODE_COUNT,
            "corpus page {index} was acknowledged by {acknowledged} replica(s), not all \
             {NODE_COUNT}; what the kills below remove would not be known to have been there"
        );
    }
    assert_eq!(
        set.ping_all(),
        vec![Some(CORPUS_PAGES); NODE_COUNT],
        "each replica must hold all {CORPUS_PAGES} pages before any of them is killed"
    );

    // ------------------------------------------------ 4. TWO KILLS, DOWN TO EXACTLY A QUORUM
    let killed_first = pids[0];
    let killed_second = pids[1];
    processes::confirm_and_kill(
        &mut nodes[0],
        node_regime::REPLICA_TOKEN_VAR,
        &token,
        Duration::from_secs(10),
    );
    processes::confirm_and_kill(
        &mut nodes[1],
        node_regime::REPLICA_TOKEN_VAR,
        &token,
        Duration::from_secs(10),
    );
    for pid in &pids[2..] {
        assert!(
            processes::is_live(*pid),
            "surviving replica {pid} is gone too; the kills were not confined to two processes"
        );
    }
    let live_after_two: usize = pids.iter().filter(|pid| processes::is_live(**pid)).count();
    assert_eq!(
        live_after_two, quorum,
        "exactly a quorum of {quorum} replica(s) must be live at this point, {live_after_two} are"
    );
    let ss_after_two = sockets::established_pids_from_ss();
    for pid in [killed_first, killed_second] {
        assert!(
            !ss_after_two.contains(&pid),
            "ss -tnp still attributes an ESTABLISHED socket to the killed PID {pid}"
        );
    }

    // ------------------------------------------------------- 5. AT EXACTLY A QUORUM, IT SERVES
    let mut recovered = 0usize;
    let mut missing: Vec<u64> = Vec::new();
    let mut wrong: Vec<u64> = Vec::new();
    for index in 0..CORPUS_PAGES {
        match set.read_chunk(&corpus_page_id(index), 0, PAGE_BYTES as u64) {
            Ok(read) => {
                if read.data == corpus_payload(index) {
                    recovered += 1;
                } else {
                    wrong.push(index);
                }
            }
            Err(_) => missing.push(index),
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {CORPUS_PAGES} acknowledged page(s) could not be read at exactly a quorum: \
         {missing:?}",
        missing.len()
    );
    assert!(
        wrong.is_empty(),
        "{} page(s) read back with the wrong bytes: {wrong:?}",
        wrong.len()
    );
    assert_eq!(
        recovered as u64, CORPUS_PAGES,
        "expected exactly {CORPUS_PAGES} pages back at a quorum, got {recovered}"
    );

    // A WRITE STILL LANDS, AND BY EXACTLY A QUORUM. The count is the evidence that the
    // acknowledgement figure counts replicas and not loop iterations: three live nodes can
    // return three and cannot return five.
    let at_quorum = set
        .write_chunk(&at_quorum_page_id(), 0, b"written at exactly a quorum")
        .expect("a write meeting exactly its quorum is accepted");
    assert_eq!(
        at_quorum, quorum,
        "a write with {quorum} live replica(s) was acknowledged by {at_quorum}"
    );
    let at_quorum_read = set
        .read_chunk(&at_quorum_page_id(), 0, 27)
        .expect("the page written at exactly a quorum reads back");
    assert_eq!(
        at_quorum_read.data, b"written at exactly a quorum",
        "the page written at exactly a quorum read back different bytes"
    );

    // THE NEGATIVE CONTROL, at a quorum. Same set, same moment, same survivors. Without this
    // the 24 hits above are consistent with a read path that answers every address it is
    // given. At a quorum the set IS entitled to say absent, and must.
    let control = set.read_chunk(&never_written_page_id(), 0, PAGE_BYTES as u64);
    match control {
        Err(ObjectError::NotFound(_)) => {}
        Err(other) => panic!(
            "at a quorum, a page that was never written must read as absent, and this read \
             failed for a different reason instead: {other}"
        ),
        Ok(read) => panic!(
            "a page that was never written returned {} byte(s), so the hits above are not \
             evidence of anything",
            read.data.len()
        ),
    }
    assert!(
        matches!(
            set.head_chunk(&never_written_page_id()),
            Err(ObjectError::NotFound(_))
        ),
        "at a quorum, a page that was never written must have no head either"
    );

    // ------------------------------------------------ 6. A THIRD KILL, ONE BELOW A QUORUM
    let killed_third = pids[2];
    processes::confirm_and_kill(
        &mut nodes[2],
        node_regime::REPLICA_TOKEN_VAR,
        &token,
        Duration::from_secs(10),
    );
    let live_after_three: usize = pids.iter().filter(|pid| processes::is_live(**pid)).count();
    assert_eq!(
        live_after_three,
        quorum - 1,
        "exactly one below a quorum ({}) must be live at this point, {live_after_three} are",
        quorum - 1
    );
    assert!(
        !sockets::established_pids_from_ss().contains(&killed_third),
        "ss -tnp still attributes an ESTABLISHED socket to the killed PID {killed_third}"
    );
    for pid in &pids[3..] {
        assert!(
            processes::is_live(*pid),
            "replica {pid} should still be live one below a quorum; the third kill was not \
             confined to one process"
        );
    }

    // -------------------------------------------- 7. ONE BELOW A QUORUM, IT REFUSES
    // A page every survivor still holds cannot be served, because two of five is not a read
    // quorum. The surviving bytes are not the question: the question is whether this set is
    // entitled to answer, and it is not.
    let below = set.read_chunk(&corpus_page_id(0), 0, PAGE_BYTES as u64);
    match &below {
        Err(ObjectError::NotFound(_)) => panic!(
            "one below a quorum, a read of a page that WAS written reported it ABSENT. The \
             two live replicas hold it; the set simply cannot reach a read quorum. Reporting \
             absence here is a confident wrong answer, and a caller cannot retry one of those."
        ),
        Err(_) => {}
        Ok(read) => panic!(
            "one below a quorum, a read returned {} byte(s) from {} live replica(s) against \
             a read quorum of {quorum}, so the read quorum is not being enforced",
            read.data.len(),
            quorum - 1
        ),
    }

    // THE NEGATIVE CONTROL AGAIN, WITH THE OPPOSITE VERDICT. At a quorum the same read was
    // required to report absence; one below, reporting absence would be a claim this set has
    // not heard enough of itself to make. This pair is what distinguishes a read path that
    // knows what it does not know from one that reports a miss whenever it fails.
    let control_below = set.read_chunk(&never_written_page_id(), 0, PAGE_BYTES as u64);
    match &control_below {
        Err(ObjectError::NotFound(_)) => panic!(
            "one below a quorum, a read of a page that was never written reported it ABSENT. \
             That answer happens to be true and was not established: only {} of {NODE_COUNT} \
             replicas answered, against a read quorum of {quorum}. A set that says 'absent' \
             without a quorum says it in the outage case too.",
            quorum - 1
        ),
        Err(_) => {}
        Ok(read) => panic!(
            "a page that was never written returned {} byte(s) one below a quorum",
            read.data.len()
        ),
    }

    // A write cannot meet its quorum either, and the refusal is the same shape.
    let write_below = set.write_chunk(&at_quorum_page_id(), 0, b"below quorum");
    assert!(
        write_below.is_err(),
        "a write succeeded with {} of {NODE_COUNT} replicas live against a write quorum of \
         {quorum}, so the write quorum is not being enforced: {write_below:?}",
        quorum - 1
    );

    // The survivors are reaped by `NodeProcess::drop`, which also runs when one of the
    // assertions above fires. An explicit cleanup here would only cover the passing path,
    // which is the path that does not need it.
    drop(nodes);
}
