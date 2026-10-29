// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded consensus request admission with activity-driven idle waits.
use super::shutdown::ServerShutdown;
use super::socket_readiness::{Notify, SocketReadiness};
use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::time::Duration;

pub(super) struct ConsensusChannels<T> {
    pub(super) requests: ConsensusInbox<T>,
    pub(super) packets: Receiver<Vec<u8>>,
    pub(super) outgoing: BTreeMap<u64, SyncSender<Vec<u8>>>,
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
