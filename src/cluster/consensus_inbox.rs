// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded consensus request admission with activity-driven idle waits.
use super::shutdown::ServerShutdown;
use super::socket_readiness::{Notify, SocketReadiness};
use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::time::Duration;

/// Shared worker observations never participate in admission or consensus decisions.
#[derive(Clone)]
pub(super) struct WorkerProgress(std::sync::Arc<WorkerObservation>);
struct WorkerObservation {
    epoch: std::time::Instant,
    phase_and_start: std::sync::atomic::AtomicU64,
    longest_work_phase: std::sync::atomic::AtomicU64,
    deliveries: super::peer_delivery::PeerDeliveryCounters,
    receives: super::peer_receiver::PeerReceiveCounters,
}
#[derive(Clone, Copy)]
pub(super) enum WorkerPhase {
    Starting = 0,
    PeerPackets = 1,
    #[cfg(feature = "chunk-consensus")]
    StateLock = 2,
    ApplyReady = 3,
    RestoreCheckpoint = 4,
    PublishCheckpoint = 5,
    Requests = 6,
    Idle = 7,
    Stopped = 8,
    ApplyOutcome = 9,
}
impl WorkerProgress {
    pub(super) fn new() -> Self {
        Self(std::sync::Arc::new(WorkerObservation {
            epoch: std::time::Instant::now(),
            phase_and_start: std::sync::atomic::AtomicU64::new(WorkerPhase::Starting as u64),
            longest_work_phase: std::sync::atomic::AtomicU64::new(0),
            deliveries: super::peer_delivery::PeerDeliveryCounters::default(),
            receives: super::peer_receiver::PeerReceiveCounters::default(),
        }))
    }
    fn elapsed_ms(&self) -> u64 {
        self.0.epoch.elapsed().as_millis().min(u128::from(u64::MAX >> 8)) as u64
    }
    pub(super) fn mark(&self, phase: WorkerPhase) {
        let now = self.elapsed_ms();
        let previous = self.0.phase_and_start.swap((now << 8) | phase as u64, std::sync::atomic::Ordering::Relaxed);
        let previous_phase = previous & 255;
        // Startup, idle waiting and shutdown must not hide a completed work stall.
        if (1..=6).contains(&previous_phase) || previous_phase == WorkerPhase::ApplyOutcome as u64 {
            let duration = now.saturating_sub(previous >> 8);
            self.0.longest_work_phase.fetch_max((duration << 8) | previous_phase, std::sync::atomic::Ordering::Relaxed);
        }
    }
    pub(super) fn applying_outcome(&self) -> OutcomeObservation<'_> {
        self.mark(WorkerPhase::ApplyOutcome);
        OutcomeObservation(self)
    }
    pub(super) fn peer_receives(&self) -> super::peer_receiver::PeerReceiveCounters { self.0.receives.clone() }
    pub(super) fn peer_deliveries(&self) -> super::peer_delivery::PeerDeliveryCounters { self.0.deliveries.clone() }
    pub(super) fn describe(&self) -> String {
        let observed = self.0.phase_and_start.load(std::sync::atomic::Ordering::Relaxed);
        fn name(phase: u64) -> &'static str { match phase {
            0 => "starting", 1 => "peer packets", 2 => "state lock", 3 => "apply ready",
            4 => "restore checkpoint", 5 => "publish checkpoint", 6 => "requests",
            7 => "idle", 8 => "stopped", 9 => "apply outcome", _ => "unknown",
        } }
        let longest = self.0.longest_work_phase.load(std::sync::atomic::Ordering::Relaxed);
        format!("; observed worker={}; worker_phase_ms={}; max_completed_work_phase={}; max_completed_work_phase_ms={}", name(observed & 255), self.elapsed_ms().saturating_sub(observed >> 8), if longest == 0 { "none" } else { name(longest & 255) }, longest >> 8) + &self.0.deliveries.describe() + &self.0.receives.describe()
    }
}

/// Returns observation to Raft processing even if application fails or unwinds.
pub(super) struct OutcomeObservation<'a>(&'a WorkerProgress);
impl Drop for OutcomeObservation<'_> {
    fn drop(&mut self) {
        self.0.mark(WorkerPhase::ApplyReady);
    }
}

/// Observations are diagnostic only, never evidence that a mutation is safe to replay.
#[derive(Clone)]
pub(super) struct RequestProgress {
    phase: std::sync::Arc<std::sync::atomic::AtomicU8>,
    read_only: bool,
}
#[derive(Clone, Copy)]
pub(super) enum RequestPhase {
    Queued = 0,
    Preparing = 1,
    ReadConfirmation = 2,
    ProposalCompletion = 3,
}
impl RequestProgress {
    pub(super) fn new(read_only: bool) -> Self {
        Self {
            phase: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(
                RequestPhase::Queued as u8,
            )),
            read_only,
        }
    }
    pub(super) fn mark(&self, phase: RequestPhase) {
        self.phase
            .store(phase as u8, std::sync::atomic::Ordering::Relaxed);
    }
    #[cfg(test)]
    pub(super) fn wait<T>(
        &self,
        receiver: &Receiver<Result<T, crate::ObjectError>>,
        timeout: Duration,
        service: &str,
    ) -> Result<T, crate::ObjectError> {
        self.wait_observed(receiver, timeout, service, None)
    }
    pub(super) fn wait_observed<T>(
        &self,
        receiver: &Receiver<Result<T, crate::ObjectError>>,
        timeout: Duration,
        service: &str,
        worker: Option<&WorkerProgress>,
    ) -> Result<T, crate::ObjectError> {
        match receiver.recv_timeout(timeout) {
            Ok(reply) => reply,
            Err(error) => {
                let reason = match error {
                    mpsc::RecvTimeoutError::Timeout => "deadline",
                    mpsc::RecvTimeoutError::Disconnected => "worker disconnected",
                };
                let phase = match self.phase.load(std::sync::atomic::Ordering::Relaxed) {
                    0 => "queued",
                    1 => "preparing",
                    2 => "read confirmation",
                    3 => "proposal completion",
                    _ => "unknown",
                };
                let outcome = if self.read_only {
                    "read request; no mutation submitted"
                } else {
                    "write outcome may be committed; do not replay"
                };
                let worker = worker.map(WorkerProgress::describe).unwrap_or_default();
                Err(crate::ObjectError::InvalidArgument(format!(
                    "{service} consensus {reason}; last observed phase={phase}; {outcome}{worker}"
                )))
            }
        }
    }
}

pub(super) struct ConsensusChannels<T> {
    pub(super) progress: WorkerProgress,
    pub(super) requests: ConsensusInbox<T>,
    pub(super) packets: Receiver<Vec<u8>>,
    pub(super) outgoing: BTreeMap<u64, SyncSender<super::peer_delivery::PeerDelivery>>,
}

pub(super) struct ConsensusInbox<T> {
    receiver: Receiver<T>,
    readiness: Option<SocketReadiness>,
}
impl<T> ConsensusInbox<T> {
    pub(super) fn new(stop: &ServerShutdown) -> (SyncSender<T>, Self) {
        let (sender, receiver) = mpsc::sync_channel(32);
        #[cfg(unix)]
        let readiness = SocketReadiness::new(stop).ok();
        #[cfg(not(unix))]
        let readiness = {
            let _ = stop;
            None
        };
        (
            sender,
            Self {
                receiver,
                readiness,
            },
        )
    }
    pub(super) fn notifier(&self) -> Option<Notify> {
        self.readiness.as_ref().map(SocketReadiness::notifier)
    }
    pub(super) fn receiver(&self) -> &Receiver<T> {
        &self.receiver
    }
    pub(super) fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }
    pub(super) fn wait(&self, stop: &ServerShutdown, until_tick: Duration) -> bool {
        match &self.readiness {
            Some(waiter) => waiter.wait(
                None,
                std::iter::empty::<(&TcpStream, bool)>(),
                until_tick,
                stop,
            ),
            None => stop.wait_for(until_tick.min(Duration::from_millis(2))),
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn consensus_inbox_bounds_admission_and_wakes_for_work_ticks_and_shutdown() {
        let stop = Arc::new(ServerShutdown::default());
        let (sender, inbox) = ConsensusInbox::new(&stop);
        let notify = inbox.notifier().unwrap();
        for value in 0..32 {
            sender.try_send(value).unwrap();
            notify.notify();
        }
        assert!(matches!(
            sender.try_send(32),
            Err(mpsc::TrySendError::Full(32))
        ));
        // Signals queued before waiting must survive the queue-check/wait boundary.
        assert!(inbox.wait(&stop, Duration::from_millis(50)));
        assert_eq!(
            inbox.receiver().try_iter().collect::<Vec<_>>(),
            (0..32).collect::<Vec<_>>()
        );
        let began = Instant::now();
        assert!(!inbox.wait(&stop, Duration::from_millis(5)));
        assert!(began.elapsed() >= Duration::from_millis(5));
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            sender.try_send(99).unwrap();
            notify.notify();
        });
        assert!(inbox.wait(&stop, Duration::from_millis(50)));
        worker.join().unwrap();
        assert_eq!(inbox.try_recv().unwrap(), 99);
        let shutdown = Arc::clone(&stop);
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            shutdown.stop();
        });
        assert!(inbox.wait(&stop, Duration::from_millis(50)));
        worker.join().unwrap();
        assert!(stop.is_stopped());
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;
    use crate::ObjectError;

    #[test]
    fn completed_work_stalls_remain_visible_after_idle_and_shorter_phases() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let worker = WorkerProgress(std::sync::Arc::new(WorkerObservation {
            epoch: std::time::Instant::now() - Duration::from_secs(2),
            phase_and_start: AtomicU64::new(WorkerPhase::ApplyReady as u64),
            longest_work_phase: AtomicU64::new(0),
            deliveries: super::super::peer_delivery::PeerDeliveryCounters::default(),
            receives: super::super::peer_receiver::PeerReceiveCounters::default(),
        }));
        worker.mark(WorkerPhase::Requests);
        let longest = worker.0.longest_work_phase.load(Ordering::Relaxed);
        assert!(longest >> 8 >= 2000);
        assert!(worker.describe().contains("max_completed_work_phase=apply ready"));
        worker.mark(WorkerPhase::Idle);
        // Simulate a long idle interval without sleeping or changing the clock.
        worker.0.phase_and_start.store(WorkerPhase::Idle as u64, Ordering::Relaxed);
        worker.mark(WorkerPhase::Requests);
        assert_eq!(worker.0.longest_work_phase.load(Ordering::Relaxed), longest);
        assert!(worker.describe().contains("observed worker=requests"));
    }

    #[test]
    fn outcome_observation_records_application_time_and_restores_raft_phase() {
        use std::sync::atomic::Ordering;
        let worker = WorkerProgress::new();
        worker.mark(WorkerPhase::ApplyReady);
        let outcome = worker.applying_outcome();
        assert!(worker.describe().contains("observed worker=apply outcome"));
        // Set a deterministic old phase start without sleeping.
        worker.0.phase_and_start.store(WorkerPhase::ApplyOutcome as u64, Ordering::Relaxed);
        drop(outcome);
        assert!(worker.describe().contains("observed worker=apply ready"));
        assert_eq!(worker.0.longest_work_phase.load(Ordering::Relaxed) & 255, WorkerPhase::ApplyOutcome as u64);
        let result = std::panic::catch_unwind(|| {
            let _outcome = worker.applying_outcome();
            panic!("application failed");
        });
        assert!(result.is_err());
        assert!(worker.describe().contains("observed worker=apply ready"));
    }

    #[test]
    fn consensus_diagnostics_preserve_late_replies_and_uncertain_mutations() {
        for read_only in [false, true] {
            let progress = RequestProgress::new(read_only);
            let (tx, rx) = mpsc::sync_channel(1);
            let error = progress
                .wait::<u64>(&rx, Duration::ZERO, "metadata")
                .unwrap_err()
                .to_string();
            assert!(error.contains("deadline; last observed phase=queued"));
            assert_eq!(error.contains("write outcome may be committed"), !read_only);
            assert_eq!(error.contains("no mutation submitted"), read_only);
            // Observation of queued work must never authorize a mutation replay.
            if !read_only {
                assert!(error.contains("do not replay"));
            }
            for (phase, expected) in [
                (RequestPhase::Preparing, "preparing"),
                (RequestPhase::ReadConfirmation, "read confirmation"),
                (RequestPhase::ProposalCompletion, "proposal completion"),
            ] {
                let worker = progress.clone();
                std::thread::spawn(move || worker.mark(phase))
                    .join()
                    .unwrap();
                let error = progress
                    .wait::<u64>(&rx, Duration::ZERO, "chunk")
                    .unwrap_err()
                    .to_string();
                assert!(error.contains(&format!("last observed phase={expected}")));
                assert_eq!(error.contains("write outcome may be committed"), !read_only);
            }
            let worker = WorkerProgress::new();
            for (phase, expected) in [
                (WorkerPhase::Starting, "starting"),
                (WorkerPhase::PeerPackets, "peer packets"),
                (WorkerPhase::ApplyReady, "apply ready"),
                (WorkerPhase::RestoreCheckpoint, "restore checkpoint"),
                (WorkerPhase::PublishCheckpoint, "publish checkpoint"),
                (WorkerPhase::Requests, "requests"),
                (WorkerPhase::Idle, "idle"),
                (WorkerPhase::Stopped, "stopped"),
            ] {
                let observation = worker.clone();
                std::thread::spawn(move || observation.mark(phase)).join().unwrap();
                let error = progress.wait_observed::<u64>(&rx, Duration::ZERO, "chunk", Some(&worker)).unwrap_err().to_string();
                assert!(error.contains(&format!("observed worker={expected}; worker_phase_ms=")));
                assert!(error.contains("peer_attempted=0"));
                assert!(error.contains("peer_enqueued=0"));
                assert_eq!(error.contains("write outcome may be committed"), !read_only);
                assert_eq!(error.contains("no mutation submitted"), read_only);
            }
            worker.mark(WorkerPhase::RestoreCheckpoint);
            std::thread::sleep(Duration::from_millis(10));
            let description = worker.describe();
            let age: u64 = description.split("worker_phase_ms=").nth(1).unwrap().split(';').next().unwrap().parse().unwrap();
            assert!(age >= 10);
            tx.send(Ok(23)).unwrap();
            assert_eq!(progress.wait_observed(&rx, Duration::ZERO, "chunk", Some(&worker)).unwrap(), 23);
            // A timeout observes the channel; it does not cancel accepted work.
            tx.send(Ok(42)).unwrap();
            assert_eq!(progress.wait(&rx, Duration::ZERO, "chunk").unwrap(), 42);
            tx.send(Err(ObjectError::Corruption("stored data rejected".into())))
                .unwrap();
            assert!(matches!(progress.wait(&rx, Duration::ZERO, "chunk"),
                Err(ObjectError::Corruption(message)) if message == "stored data rejected"));
            drop(tx);
            let error = progress
                .wait::<u64>(&rx, Duration::from_secs(1), "metadata")
                .unwrap_err()
                .to_string();
            assert!(error.contains("worker disconnected"));
            assert!(!error.contains("deadline"));
            assert_eq!(error.contains("write outcome may be committed"), !read_only);
        }
    }
}
