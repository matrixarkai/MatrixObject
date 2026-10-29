// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Socket readiness and executor completion wakeups for the bounded API loop.
use super::{Connection, ServerShutdown, CONNECTIONS_PER_WORKER};
pub(super) use crate::cluster::socket_readiness::Notify;
use crate::cluster::socket_readiness::SocketReadiness;
use std::io;
use std::net::TcpListener;
use std::time::{Duration, Instant};

pub(super) struct Waiter {
    inner: SocketReadiness,
}
impl Waiter {
    pub(super) fn new(stop: &ServerShutdown) -> io::Result<Self> {
        Ok(Self {
            inner: SocketReadiness::new(stop)?,
        })
    }
    pub(super) fn notifier(&self) -> Notify {
        self.inner.notifier()
    }
    pub(super) fn wait(
        &self,
        listener: &TcpListener,
        connections: &[Connection],
        stop: &ServerShutdown,
    ) -> bool {
        let now = Instant::now();
        let timeout = connections
            .iter()
            .map(|c| c.deadline.saturating_duration_since(now))
            .min()
            .unwrap_or(Duration::from_millis(50));
        // Completion uses a separate wake socket. Exclude pending clients so
        // a disconnected client cannot cause readiness to spin during execution.
        self.inner.wait(
            (connections.len() < CONNECTIONS_PER_WORKER).then_some(listener),
            connections
                .iter()
                .filter(|c| c.pending.is_none())
                .map(|c| (&c.stream, !c.output.is_empty())),
            timeout,
            stop,
        )
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn api_readiness_waits_when_idle_and_wakes_for_sockets_completion_and_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(ServerShutdown::default());
        let waiter = Waiter::new(&stop).unwrap();
        let start = Instant::now();
        assert!(!waiter.wait(&listener, &[], &stop));
        assert!(start.elapsed() >= Duration::from_millis(20));
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        assert!(waiter.wait(&listener, &[], &stop));
        let (server, _) = listener.accept().unwrap();
        drop((client, server));
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let active = stop.track(&server).unwrap().unwrap();
        let options = super::super::Options {
            max_payload_bytes: 64,
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        };
        let mut connection = Connection::new(server, active, &options, Some(&waiter.notifier()));
        client.write_all(&[7]).unwrap();
        assert!(waiter.wait(&listener, std::slice::from_ref(&connection), &stop));
        let mut byte = [0];
        connection.stream.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [7]);
        connection.output = vec![1];
        assert!(waiter.wait(&listener, std::slice::from_ref(&connection), &stop));
        connection.output.clear();
        let (_reply, pending) = std::sync::mpsc::sync_channel(1);
        connection.pending = Some(pending);
        drop(client);
        // An executor still owns this request: client hangup must not spin poll.
        assert!(!waiter.wait(&listener, std::slice::from_ref(&connection), &stop));
        drop(connection);
        // Saturation is nonblocking; a full notification socket is already readable.
        for _ in 0..4096 {
            waiter.notifier().notify();
        }
        assert!(waiter.wait(&listener, &[], &stop));
        let waiter = Waiter::new(&stop).unwrap();
        let notify = waiter.notifier();
        let worker = std::thread::spawn(move || {
            notify.notify();
        });
        assert!(waiter.wait(&listener, &[], &stop));
        worker.join().unwrap();
        let waiter = Waiter::new(&stop).unwrap();
        let shutdown = Arc::clone(&stop);
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            shutdown.stop();
        });
        assert!(waiter.wait(&listener, &[], &stop));
        worker.join().unwrap();
        assert!(stop.is_stopped());
    }
}
