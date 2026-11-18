// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::TryLockError;
use std::thread::JoinHandle;

const MAX_GROUP_REQUESTS: usize = 64;
const MAX_GROUP_MUTATIONS: usize = 64;
type ResultReply = Result<ReplicaReply, ObjectError>;

struct Job {
    request: Option<ReplicaRequest>,
    result: Option<ResultReply>,
    wire_bytes: usize,
    reply: SyncSender<ResultReply>,
}

pub(super) struct Committer {
    replica: Arc<Mutex<NodeReplica>>,
    offered_requests: AtomicUsize,
    sender: Option<SyncSender<Job>>,
    worker: Option<JoinHandle<()>>,
    max_payload_bytes: usize,
}

struct Offer<'a>(&'a AtomicUsize);
impl<'a> Offer<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(counter)
    }
}
impl Drop for Offer<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

pub(super) fn eligible(request: &ReplicaRequest) -> bool {
    matches!(
        request,
        ReplicaRequest::WriteChunk { .. }
            | ReplicaRequest::WriteChunkIfSequence { .. }
            | ReplicaRequest::WriteChunkIfSequenceDigest { .. }
            | ReplicaRequest::WriteChunkIfState { .. }
            | ReplicaRequest::WriteChunkBatchIfSequence { .. }
            | ReplicaRequest::WriteChunkBatchIfSequenceDigest { .. }
            | ReplicaRequest::DiscardChunk { .. }
            | ReplicaRequest::DiscardChunkIfSequence { .. }
            | ReplicaRequest::FenceNamespace(_)
    )
}

fn mutation_count(request: &ReplicaRequest) -> usize {
    match request {
        ReplicaRequest::WriteChunkBatchIfSequence { writes }
        | ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } => writes.len().max(1),
        _ => 1,
    }
}

impl Committer {
    pub(super) fn start(
        replica: Arc<Mutex<NodeReplica>>,
        max_payload_bytes: usize,
    ) -> std::io::Result<Arc<Self>> {
        // Waiting request executors retain their existing bounded input charges.
        // A rendezvous channel adds no separately buffered queue of payloads.
        let (sender, receiver) = mpsc::sync_channel(0);
        let worker_replica = Arc::clone(&replica);
        let worker = std::thread::Builder::new()
            .name("star-journal-sync".into())
            .spawn(move || run(receiver, worker_replica, max_payload_bytes))?;
        Ok(Arc::new(Self {
            replica,
            offered_requests: AtomicUsize::new(0),
            sender: Some(sender),
            worker: Some(worker),
            max_payload_bytes,
        }))
    }

    pub(super) fn request(&self, request: ReplicaRequest, wire_bytes: usize) -> ResultReply {
        if wire_bytes > self.max_payload_bytes
            || !eligible(&request)
            || mutation_count(&request) > MAX_GROUP_MUTATIONS
        {
            return Err(ObjectError::InvalidArgument(
                "invalid grouped replica request".into(),
            ));
        }
        if self
            .worker
            .as_ref()
            .is_none_or(|worker| worker.is_finished())
        {
            return Err(self.stopped("replica journal worker stopped"));
        }
        // An idle request uses the executor already handling its frame. Once a
        // request is offered, new requests join that backlog rather than barging
        // ahead with repeated individual synchronizations.
        if self.offered_requests.load(Ordering::Acquire) == 0 {
            match self.replica.try_lock() {
                Ok(mut guard) => {
                    if self.offered_requests.load(Ordering::Acquire) == 0 {
                        let before = guard
                            .journal
                            .as_ref()
                            .map(|journal| journal.logical_length());
                        let result = apply_queued(&mut guard, request, self.max_payload_bytes);
                        finish_mutations(&mut guard, before, finish_deferred_journal)?;
                        return result;
                    }
                }
                Err(TryLockError::Poisoned(_)) => {
                    return Err(ObjectError::Corruption(
                        "replica state lock is poisoned".into(),
                    ));
                }
                Err(TryLockError::WouldBlock) => {}
            }
        }
        let _offer = Offer::new(&self.offered_requests);
        let (reply, receive) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .unwrap()
            .send(Job {
                request: Some(request),
                result: None,
                wire_bytes,
                reply,
            })
            .map_err(|_| self.stopped("replica journal worker stopped"))?;
        receive.recv().map_err(|_| {
            self.stopped("replica journal worker stopped before confirming persistence")
        })?
    }

    fn stopped(&self, message: &str) -> ObjectError {
        if let Ok(mut guard) = self.replica.lock() {
            guard.failed = true;
        }
        ObjectError::Corruption(message.into())
    }
}

impl Drop for Committer {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn run(receiver: Receiver<Job>, replica: Arc<Mutex<NodeReplica>>, max_payload_bytes: usize) {
    let mut pending = None;
    let mut jobs = Vec::with_capacity(MAX_GROUP_REQUESTS);
    loop {
        let first = match pending.take().or_else(|| receiver.recv().ok()) {
            Some(job) => job,
            None => return,
        };
        let mut bytes = first.wire_bytes;
        let mut mutations = mutation_count(first.request.as_ref().unwrap());
        jobs.push(first);
        // Drain only requests already offered by executors. No batch-fill delay
        // is imposed on an idle node. Arrivals during sync belong to a later group.
        while jobs.len() < MAX_GROUP_REQUESTS {
            let Ok(job) = receiver.try_recv() else {
                break;
            };
            let next_mutations =
                mutations.saturating_add(mutation_count(job.request.as_ref().unwrap()));
            if next_mutations > MAX_GROUP_MUTATIONS {
                pending = Some(job);
                break;
            }
            let Some(next) = bytes
                .checked_add(job.wire_bytes)
                .filter(|next| *next <= max_payload_bytes)
            else {
                pending = Some(job);
                break;
            };
            bytes = next;
            mutations = next_mutations;
            jobs.push(job);
        }
        commit_jobs(
            &replica,
            &mut jobs,
            max_payload_bytes,
            finish_deferred_journal,
        );
    }
}

fn apply_queued(
    guard: &mut NodeReplica,
    request: ReplicaRequest,
    max_payload_bytes: usize,
) -> ResultReply {
    match request {
        ReplicaRequest::WriteChunkBatchIfSequence { writes } => {
            apply_batch_locked(guard, writes, max_payload_bytes)
        }
        ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } => {
            apply_batch_digests_locked(guard, writes, max_payload_bytes)
        }
        request => apply_locked(guard, request, max_payload_bytes, false),
    }
}

fn commit_jobs(
    replica: &Mutex<NodeReplica>,
    jobs: &mut Vec<Job>,
    max_payload_bytes: usize,
    sync: impl FnOnce(&mut NodeReplica) -> Result<(), ObjectError>,
) {
    let failure = match replica.lock() {
        Ok(mut guard) => {
            let before = guard
                .journal
                .as_ref()
                .map(|journal| journal.logical_length());
            for job in jobs.iter_mut() {
                job.result = Some(apply_queued(
                    &mut guard,
                    job.request.take().unwrap(),
                    max_payload_bytes,
                ));
            }
            let outcome = finish_mutations(&mut guard, before, sync);
            match outcome {
                Ok(()) => None,
                Err(error) => {
                    guard.failed = true;
                    Some(error.to_string())
                }
            }
        }
        Err(_) => Some("replica state lock is poisoned".into()),
    };
    // Keep the service lock across mutation and durable sync. Direct readers and
    // repair cannot observe this group's state before its persistence completes.
    // Even an independent refusal is conservatively uncertain on group failure.
    for mut job in jobs.drain(..) {
        let result = match &failure {
            Some(message) => Err(ObjectError::Corruption(message.clone())),
            None => job.result.take().unwrap(),
        };
        let _ = job.reply.send(result);
    }
}

fn finish_mutations(
    guard: &mut NodeReplica,
    before: Option<u64>,
    sync: impl FnOnce(&mut NodeReplica) -> Result<(), ObjectError>,
) -> Result<(), ObjectError> {
    if guard.failed {
        return Err(ObjectError::Corruption(
            "replica failed before group synchronization".into(),
        ));
    }
    if before
        != guard
            .journal
            .as_ref()
            .map(|journal| journal.logical_length())
    {
        if let Err(error) = sync(guard) {
            guard.failed = true;
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
#[path = "star_group_commit_tests.rs"]
mod tests;
