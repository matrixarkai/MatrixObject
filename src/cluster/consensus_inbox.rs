// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded consensus request admission with activity-driven idle waits.
use super::shutdown::ServerShutdown;
use super::socket_readiness::{Notify, SocketReadiness};
use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::time::Duration;

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
    pub(super) fn wait<T>(
        &self,
        receiver: &Receiver<Result<T, crate::ObjectError>>,
        timeout: Duration,
        service: &str,
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
                Err(crate::ObjectError::InvalidArgument(format!(
                    "{service} consensus {reason}; last observed phase={phase}; {outcome}"
                )))
            }
        }
    }
}

pub(super) struct ConsensusChannels<T> {
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
