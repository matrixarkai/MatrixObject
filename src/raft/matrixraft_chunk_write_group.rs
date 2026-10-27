// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Driving the chunk replicas from MatrixRaft's own applier.
//
// # WHAT THIS ADDS OVER THE STATE MACHINE ALONE
//
// `ChunkStateMachine` is one replica that can apply a log entry. On its own, something still has
// to hand it the entries, and a test that calls `StateMachine::apply` in a loop is only testing
// the state machine.
//
// MatrixRaft already owns the half that was missing: [`matrixraft::Applier`] is an apply loop with
// worker threads, a queue per (group, node), and `wait_for_applied`, which blocks until a group
// has reached an index or reports that it has not. So this binds a replica to that, rather than
// writing a second apply loop that would have to be kept honest separately.
//
// # THE ONE PLACE THIS GETS TO DECIDE SOMETHING, AND IT IS THE IMPORTANT ONE
//
// `ApplyHandler::apply` returns the index its group has reached. That return value is what
// `wait_for_applied` compares against, so it is the whole of what a caller learns about whether
// its write landed.
//
// A handler that returns the last index in the batch regardless of outcome would tell a waiter
// that a refused entry had been applied. So the handler returns the index of the last entry that
// APPLIED, and stops there: a group that meets an entry it cannot apply stops reporting progress
// at that entry, every waiter past it times out, and a caller learns its write did not land
// instead of being told it did. The refusal is kept, with its index, so an operator has the reason
// rather than only a stall.

use std::sync::Mutex;
use std::time::Duration;

// `LogId` is already in scope: every facade file is include!d into this one module.
use matrixraft::{
    ApplyHandler, ApplyTask, ApplyWait, Applier, ApplierOptions, DriverGroupKey, LogEntry,
    LogIndex, NodeId,
};

/// Why one entry did not apply, and where.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkApplyRefusal {
    pub index: LogIndex,
    pub reason: String,
}

/// One chunk replica, as something MatrixRaft's applier can drive.
///
/// `ApplyHandler::apply` takes `&self` -- the applier shares a handler across its worker threads --
/// so the state machine sits behind a mutex. That is not a throughput compromise being hidden: a
/// group's entries have to be applied in index order, so one replica applying one entry at a time
/// is the required behaviour and the mutex is how it is enforced rather than hoped for.
pub struct ChunkApplyHandler {
    machine: Mutex<ChunkStateMachine>,
    refusals: Mutex<Vec<ChunkApplyRefusal>>,
}

impl ChunkApplyHandler {
    pub fn new(machine: ChunkStateMachine) -> Self {
        Self {
            machine: Mutex::new(machine),
            refusals: Mutex::new(Vec::new()),
        }
    }

    /// Every entry this replica refused, in the order it met them.
    pub fn refusals(&self) -> Vec<ChunkApplyRefusal> {
        self.refusals
            .lock()
            .expect("chunk apply refusals mutex poisoned")
            .clone()
    }

    /// Run something against the replica this handler drives.
    ///
    /// Takes a closure rather than handing out a guard: the applier threads hold this same lock,
    /// so a guard escaping to a caller is a deadlock waiting for a caller that forgets to drop it.
    pub fn with_replica<T>(&self, read: impl FnOnce(&MatrixReplicaService) -> T) -> T {
        let machine = self.machine.lock().expect("chunk state machine poisoned");
        read(machine.replica())
    }

    pub fn applied_index(&self) -> LogIndex {
        self.machine
            .lock()
            .expect("chunk state machine poisoned")
            .last_applied_index()
    }

    /// Install a snapshot into this replica, as a catching-up node would.
    pub fn install_snapshot_bytes(
        &self,
        bytes: &[u8],
        through_index: LogIndex,
    ) -> Result<(), ObjectError> {
        let mut machine = self.machine.lock().expect("chunk state machine poisoned");
        machine.install_snapshot_payload(bytes, through_index)
    }

    /// The bytes a peer would send to catch someone up, and the index they reach.
    pub fn snapshot_bytes(&self) -> Result<(Vec<u8>, LogIndex), ObjectError> {
        let machine = self.machine.lock().expect("chunk state machine poisoned");
        Ok((machine.snapshot_payload()?, machine.last_applied_index()))
    }
}

impl ApplyHandler for ChunkApplyHandler {
    fn apply(&self, tasks: Vec<ApplyTask>) -> LogIndex {
        let mut machine = self.machine.lock().expect("chunk state machine poisoned");
        let mut reached = machine.last_applied_index();
        for task in tasks {
            for entry in task.entries {
                let index = entry.log_id.index;
                let request = matrixraft::ApplyRequest {
                    group_id: machine.group_id(),
                    log_id: entry.log_id,
                    payload: entry.payload,
                };
                match matrixraft::StateMachine::apply(&mut *machine, request) {
                    Ok(response) => reached = response.applied_index,
                    Err(err) => {
                        // STOP HERE, AND DO NOT REPORT PROGRESS PAST IT. Applying the rest of the
                        // batch would put later entries into a replica that is missing an earlier
                        // one, which is divergence; reporting the batch's last index would tell a
                        // waiter this write landed.
                        self.refusals
                            .lock()
                            .expect("chunk apply refusals mutex poisoned")
                            .push(ChunkApplyRefusal {
                                index,
                                reason: err.to_string(),
                            });
                        return reached;
                    }
                }
            }
        }
        reached
    }
}

/// A chunk write proposed once and applied by every replica from the log.
///
/// This is the coordinator-side counterpart of the star write in
/// `write_chunk_ranges_with_chunk_receipts`. There the coordinator calls each replica itself; here
/// it appends one entry and waits for a quorum of replicas to report that they reached it.
pub struct ChunkRaftWriteGroup {
    group_id: u64,
    applier: Applier,
    handlers: Vec<(NodeId, std::sync::Arc<ChunkApplyHandler>)>,
    write_quorum: usize,
    next_index: LogIndex,
    term: u64,
}

impl ChunkRaftWriteGroup {
    /// Build a group over `replicas`, with `write_quorum` of them required to acknowledge a write.
    pub fn new(
        group_id: u64,
        replicas: Vec<MatrixReplicaService>,
        write_quorum: usize,
        options: ApplierOptions,
    ) -> Result<Self, ObjectError> {
        if replicas.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "a chunk raft group needs at least one replica".to_string(),
            ));
        }
        if write_quorum == 0 || write_quorum > replicas.len() {
            return Err(ObjectError::InvalidArgument(format!(
                "write_quorum must be 1..={} and is {write_quorum}",
                replicas.len()
            )));
        }
        let applier = Applier::start(options)
            .map_err(|err| ObjectError::InvalidArgument(format!("applier refused: {err}")))?;
        let mut handlers = Vec::with_capacity(replicas.len());
        for (position, replica) in replicas.into_iter().enumerate() {
            let node_id = position as NodeId + 1;
            let handler = std::sync::Arc::new(ChunkApplyHandler::new(ChunkStateMachine::new(
                group_id, replica,
            )));
            applier
                .register_group(
                    DriverGroupKey::new(group_id, node_id),
                    handler.clone() as std::sync::Arc<dyn ApplyHandler>,
                )
                .map_err(|err| {
                    ObjectError::InvalidArgument(format!("group registration refused: {err}"))
                })?;
            handlers.push((node_id, handler));
        }
        Ok(Self {
            group_id,
            applier,
            handlers,
            write_quorum,
            next_index: 1,
            term: 1,
        })
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.handlers.iter().map(|(node_id, _)| *node_id).collect()
    }

    pub fn handler(&self, node_id: NodeId) -> Option<&std::sync::Arc<ChunkApplyHandler>> {
        self.handlers
            .iter()
            .find(|(id, _)| *id == node_id)
            .map(|(_, handler)| handler)
    }

    /// Propose one write, and wait for a quorum to apply it.
    ///
    /// `to` names the replicas the entry is delivered to, which is how a partition is expressed:
    /// a replica left out of it receives nothing and falls behind, exactly as one that is
    /// unreachable would. The entry's index is allocated whether or not a quorum is reached,
    /// because an index a log has issued is spent -- reusing it would give two different writes
    /// the same position.
    pub fn propose_to(
        &mut self,
        to: &[NodeId],
        page_id: PageId,
        ranges: Vec<(u64, Vec<u8>)>,
        timeout: Duration,
    ) -> Result<ChunkRaftWriteOutcome, ObjectError> {
        let mut seen = std::collections::BTreeSet::new();
        for node in to {
            if !seen.insert(*node) || self.handler(*node).is_none() {
                return Err(ObjectError::InvalidArgument("chunk outcome targets must be distinct group members".to_string()));
            }
        }
        let index = self.next_index;
        let source = self.handlers.iter().find(|(_, h)| h.applied_index() == index - 1)
            .ok_or_else(|| ObjectError::PreconditionFailed("no replica has the preceding outcome".to_string()))?;
        let payload = source.1.with_replica(|replica| -> Result<Vec<u8>, ObjectError> {
            let before = match replica.read_chunk_state(&page_id) {
                Ok(state) => Some(state), Err(ObjectError::NotFound(_)) => None, Err(err) => return Err(err),
            };
            let mut staged = replica.clone();
            staged.write_chunk_ranges(page_id.clone(), ranges)?;
            crate::outcome::PageOutcome::between(&page_id, before.as_ref(), &staged.read_chunk_state(&page_id)?)?.with_counters(crate::outcome::PageOutcome::counters(replica), crate::outcome::PageOutcome::counters(&staged)).to_payload()
        })?;
        self.next_index = self.next_index.checked_add(1).ok_or_else(|| ObjectError::PreconditionFailed("chunk log index exhausted".to_string()))?;
        let entry = LogEntry {
            log_id: LogId {
                term: self.term,
                index,
            },
            payload,
            is_command: true,
        };
        for node_id in to {
            if self.handler(*node_id).is_none() {
                return Err(ObjectError::InvalidArgument(format!(
                    "node {node_id} is not in group {}",
                    self.group_id
                )));
            }
            self.applier
                .submit(
                    DriverGroupKey::new(self.group_id, *node_id),
                    ApplyTask {
                        entries: vec![entry.clone()],
                        snapshot: None,
                    },
                )
                .map_err(|err| {
                    ObjectError::InvalidArgument(format!("node {node_id} refused the entry: {err}"))
                })?;
        }

        let mut acknowledged = Vec::new();
        let mut behind = Vec::new();
        for node_id in to {
            let wait = self
                .applier
                .wait_for_applied(
                    DriverGroupKey::new(self.group_id, *node_id),
                    index,
                    timeout,
                )
                .map_err(|err| {
                    ObjectError::Corruption(format!("waiting on node {node_id} failed: {err}"))
                })?;
            match wait {
                ApplyWait::Reached { .. } => acknowledged.push(*node_id),
                ApplyWait::TimedOut { applied_index } | ApplyWait::GroupGone { applied_index } | ApplyWait::Stalled { applied_index } => {
                    behind.push((*node_id, applied_index))
                }
            }
        }
        Ok(ChunkRaftWriteOutcome {
            index,
            acknowledged,
            behind,
            committed: false,
        }
        .decided(self.write_quorum))
    }

    /// Propose to every replica in the group.
    pub fn propose(
        &mut self,
        page_id: PageId,
        ranges: Vec<(u64, Vec<u8>)>,
        timeout: Duration,
    ) -> Result<ChunkRaftWriteOutcome, ObjectError> {
        let everyone = self.node_ids();
        self.propose_to(&everyone, page_id, ranges, timeout)
    }

    /// Catch one replica up from another's snapshot.
    ///
    /// THIS IS THE THING A STAR WRITE CANNOT DO AT ALL. A replica that missed a coordinator-driven
    /// write has no record of what it missed -- the coordinator's write list is the only place
    /// that existed, and it is gone. A replica behind a log knows its own applied index, and the
    /// index a snapshot is complete to says exactly which entries it no longer needs.
    pub fn catch_up_from(&self, behind: NodeId, from: NodeId) -> Result<LogIndex, ObjectError> {
        let source = self.handler(from).ok_or_else(|| {
            ObjectError::InvalidArgument(format!("node {from} is not in this group"))
        })?;
        let target = self.handler(behind).ok_or_else(|| {
            ObjectError::InvalidArgument(format!("node {behind} is not in this group"))
        })?;
        let (bytes, through_index) = source.snapshot_bytes()?;
        target.install_snapshot_bytes(&bytes, through_index)?;
        Ok(through_index)
    }

    /// Stop the applier's threads.
    ///
    /// Taken by value, because a group whose applier has stopped cannot serve another write and
    /// leaving it reachable invites one.
    pub fn stop(mut self) {
        self.applier.stop();
    }
}

/// What a proposal reached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRaftWriteOutcome {
    /// The index the entry was given, spent whether or not it committed.
    pub index: LogIndex,
    pub acknowledged: Vec<NodeId>,
    /// Replicas that did not reach the index, with where they stopped. A replica listed here with
    /// an index BELOW this entry's is one that refused an earlier entry and has been stuck since.
    pub behind: Vec<(NodeId, LogIndex)>,
    pub committed: bool,
}

impl ChunkRaftWriteOutcome {
    fn decided(mut self, write_quorum: usize) -> Self {
        self.committed = self.acknowledged.len() >= write_quorum;
        self
    }
}
