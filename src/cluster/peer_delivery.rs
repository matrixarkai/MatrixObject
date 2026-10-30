// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A single delivery deadline includes queueing, connection and frame writes.
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

/// Per-worker observations only; a successful send is not a peer acknowledgment.
#[derive(Clone, Default)]
pub(super) struct PeerDeliveryCounters(Arc<DeliveryCounts>);
#[derive(Default)]
struct DeliveryCounts {
    attempted: AtomicU64,
    queue_rejected: AtomicU64,
    expired: AtomicU64,
    connect_failed: AtomicU64,
    write_failed: AtomicU64,
    sent: AtomicU64,
    max_queue_ms: AtomicU64,
}
impl PeerDeliveryCounters {
    pub(super) fn queue_rejected(&self) {
        self.0.queue_rejected.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn describe(&self) -> String {
        let c = &self.0;
        format!("; peer_attempted={}; peer_queue_rejected={}; peer_expired_before_connect={}; peer_connect_failed={}; peer_write_failed={}; peer_sent={}; peer_max_queue_ms={}", c.attempted.load(Ordering::Relaxed), c.queue_rejected.load(Ordering::Relaxed), c.expired.load(Ordering::Relaxed), c.connect_failed.load(Ordering::Relaxed), c.write_failed.load(Ordering::Relaxed), c.sent.load(Ordering::Relaxed), c.max_queue_ms.load(Ordering::Relaxed))
    }
}

pub(super) struct PeerDelivery {
    bytes: Vec<u8>,
    deadline: Instant,
    created: Instant,
    counters: PeerDeliveryCounters,
}
impl PeerDelivery {
    #[cfg(test)]
    pub(super) fn new(bytes: Vec<u8>, timeout: Duration) -> Self {
        Self::observed(bytes, timeout, PeerDeliveryCounters::default())
    }
    pub(super) fn observed(
        bytes: Vec<u8>,
        timeout: Duration,
        counters: PeerDeliveryCounters,
    ) -> Self {
        let created = Instant::now();
        counters.0.attempted.fetch_add(1, Ordering::Relaxed);
        Self {
            bytes,
            deadline: created + timeout,
            created,
            counters,
        }
    }
    #[cfg(test)]
    pub(super) fn send(self, address: &SocketAddr) -> io::Result<()> {
        self.send_with_transport(address, &super::peer_tls::PeerTransport::Plain)
    }
    pub(super) fn send_with_transport(
        self,
        address: &SocketAddr,
        transport: &super::peer_tls::PeerTransport,
    ) -> io::Result<()> {
        let c = &self.counters.0;
        c.max_queue_ms.fetch_max(
            self.created.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let budget = remaining(self.deadline).inspect_err(|_| {
            c.expired.fetch_add(1, Ordering::Relaxed);
        })?;
        let mut stream = TcpStream::connect_timeout(address, budget).inspect_err(|_| {
            c.connect_failed.fetch_add(1, Ordering::Relaxed);
        })?;
        let result = stream
            .set_nodelay(true)
            .and_then(|()| transport.send(&mut stream, address, &self.bytes, self.deadline));
        if result.is_ok() {
            c.sent.fetch_add(1, Ordering::Relaxed);
        } else {
            c.write_failed.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}
pub(super) fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "peer packet deadline"))
}
pub(super) fn write_until(
    stream: &mut TcpStream,
    mut bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "incomplete peer packet",
                ))
            }
            Ok(n) => bytes = &bytes[n..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::sync::mpsc;

    #[test]
    fn delivery_observations_distinguish_queue_expiry_connection_and_success() {
        let counters = PeerDeliveryCounters::default();
        let isolated = PeerDeliveryCounters::default();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let mut expired = PeerDelivery::observed(vec![1], Duration::from_secs(1), counters.clone());
        expired.created = Instant::now() - Duration::from_secs(2);
        expired.deadline = Instant::now() - Duration::from_secs(1);
        tx.try_send(expired).ok().unwrap();
        if tx
            .try_send(PeerDelivery::observed(
                vec![2],
                Duration::from_secs(1),
                counters.clone(),
            ))
            .is_err()
        {
            counters.queue_rejected();
        }
        assert_eq!(
            rx.recv().unwrap().send(&address).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        PeerDelivery::observed(b"sent".to_vec(), Duration::from_secs(2), counters.clone())
            .send(&address)
            .unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, [0, 0, 0, 4, b's', b'e', b'n', b't']);
        // Port zero cannot be a listening destination.
        assert!(
            PeerDelivery::observed(vec![3], Duration::from_secs(1), counters.clone())
                .send(&"127.0.0.1:0".parse().unwrap())
                .is_err()
        );
        let c = &counters.0;
        assert_eq!(c.attempted.load(Ordering::Relaxed), 4);
        assert_eq!(c.queue_rejected.load(Ordering::Relaxed), 1);
        assert_eq!(c.expired.load(Ordering::Relaxed), 1);
        assert_eq!(c.connect_failed.load(Ordering::Relaxed), 1);
        assert_eq!(c.write_failed.load(Ordering::Relaxed), 0);
        assert_eq!(c.sent.load(Ordering::Relaxed), 1);
        assert!(c.max_queue_ms.load(Ordering::Relaxed) >= 2000);
        assert_eq!(isolated.0.attempted.load(Ordering::Relaxed), 0);
        assert!(counters.describe().contains("peer_sent=1"));
    }

    #[test]
    fn queued_peer_delivery_expires_before_connect_and_preserves_fresh_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::sync_channel(8);
        // Queue age is part of the budget; dequeuing must not renew it.
        let old = PeerDelivery {
            bytes: b"old".to_vec(),
            deadline: Instant::now() - Duration::from_secs(1),
            created: Instant::now() - Duration::from_secs(2),
            counters: PeerDeliveryCounters::default(),
        };
        tx.send(old).unwrap();
        assert_eq!(
            rx.recv().unwrap().send(&address).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        tx.send(PeerDelivery::new(b"fresh".to_vec(), Duration::from_secs(2)))
            .unwrap();
        rx.recv().unwrap().send(&address).unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut frame = Vec::new();
        stream.read_to_end(&mut frame).unwrap();
        assert_eq!(frame, [0, 0, 0, 5, b'f', b'r', b'e', b's', b'h']);
        // A connection obtained earlier also cannot renew an expired write budget.
        assert_eq!(
            write_until(
                &mut stream,
                b"late",
                Instant::now() - Duration::from_secs(1)
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::TimedOut
        );
    }
}
