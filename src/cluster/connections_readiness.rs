// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Socket readiness and executor completion wakeups for the bounded API loop.
#[cfg(unix)]
use super::CONNECTIONS_PER_WORKER;
use super::{Connection, ServerShutdown};
use std::io;
use std::net::TcpListener;
use std::time::Duration;

#[cfg(unix)]
use std::{
    io::{Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::Arc,
};

#[derive(Clone)]
pub(super) struct Notify {
    #[cfg(unix)]
    socket: Arc<UnixStream>,
}
impl Notify {
    pub(super) fn notify(&self) {
        #[cfg(unix)]
        {
            let _ = (&*self.socket).write(&[1]);
        }
    }
}
pub(super) struct Waiter {
    #[cfg(unix)]
    receive: UnixStream,
    notify: Notify,
}
impl Waiter {
    pub(super) fn new(stop: &ServerShutdown) -> io::Result<Self> {
        #[cfg(unix)]
        {
            let (receive, send) = UnixStream::pair()?;
            receive.set_nonblocking(true)?;
            send.set_nonblocking(true)?;
            let socket = Arc::new(send);
            stop.track_wake(&socket);
            Ok(Self {
                receive,
                notify: Notify { socket },
            })
        }
        #[cfg(not(unix))]
        {
            let _ = stop;
            Ok(Self { notify: Notify {} })
        }
    }
    pub(super) fn notifier(&self) -> Notify {
        self.notify.clone()
    }
    pub(super) fn wait(
        &self,
        listener: &TcpListener,
        connections: &[Connection],
        stop: &ServerShutdown,
    ) -> bool {
        if stop.is_stopped() {
            return true;
        }
        #[cfg(unix)]
        {
            let mut descriptors = Vec::with_capacity(connections.len() + 2);
            let mut add = |fd, events| {
                descriptors.push(libc::pollfd {
                    fd,
                    events,
                    revents: 0,
                })
            };
            add(self.receive.as_raw_fd(), libc::POLLIN);
            if connections.len() < CONNECTIONS_PER_WORKER {
                add(listener.as_raw_fd(), libc::POLLIN);
            }
            for connection in connections {
                // Completion wakes through the separate socket. Watching a closed
                // client while its job runs would otherwise create a busy loop.
                if connection.pending.is_none() {
                    add(
                        connection.stream.as_raw_fd(),
                        if connection.output.is_empty() {
                            libc::POLLIN
                        } else {
                            libc::POLLOUT
                        },
                    );
                }
            }
            let timeout = connections
                .iter()
                .map(|c| {
                    c.deadline
                        .saturating_duration_since(std::time::Instant::now())
                })
                .min()
                .unwrap_or(Duration::from_millis(50))
                .min(Duration::from_millis(50));
            let milliseconds = timeout.as_millis().saturating_add(1).min(50) as libc::c_int;
            // SAFETY: descriptors owns this initialized contiguous array for the
            // duration of poll. All descriptors remain owned by this I/O loop.
            let result = unsafe {
                libc::poll(
                    descriptors.as_mut_ptr(),
                    descriptors.len() as libc::nfds_t,
                    milliseconds,
                )
            };
            if result < 0 {
                // A failed readiness facility must not turn into an unbounded spin.
                stop.wait_for(Duration::from_millis(1));
            }
            let mut bytes = [0; 128];
            for _ in 0..8 {
                match (&self.receive).read(&mut bytes) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            result > 0
        }
        #[cfg(not(unix))]
        {
            let _ = (listener, connections);
            stop.wait_for(Duration::from_millis(1));
            false
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::net::TcpStream;
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
