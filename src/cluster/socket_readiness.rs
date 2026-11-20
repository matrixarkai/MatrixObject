// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Shared bounded socket waits with completion and shutdown notification.
use super::shutdown::ServerShutdown;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

#[cfg(unix)]
use std::{
    io::{Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[cfg(unix)]
struct WakeState {
    socket: Arc<UnixStream>,
    pending: AtomicBool,
}
#[derive(Clone)]
pub(super) struct Notify {
    #[cfg(unix)]
    state: Arc<WakeState>,
}
impl Notify {
    pub(super) fn notify(&self) {
        #[cfg(unix)]
        {
            // One unread completion already schedules the I/O loop. Keep the
            // latch set until the consumer takes it, even if a drain consumes
            // the socket byte for a concurrent completion.
            if !self.state.pending.swap(true, Ordering::AcqRel) {
                let _ = (&*self.state.socket).write(&[1]);
            }
        }
    }
    #[cfg(unix)]
    fn take_pending(&self) -> bool {
        self.state.pending.swap(false, Ordering::AcqRel)
    }
}
pub(super) struct SocketReadiness {
    #[cfg(unix)]
    receive: UnixStream,
    notify: Notify,
}
impl SocketReadiness {
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
                notify: Notify {
                    state: Arc::new(WakeState {
                        socket,
                        pending: AtomicBool::new(false),
                    }),
                },
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
    #[cfg(unix)]
    fn drain_notifications(&self) {
        let mut bytes = [0; 128];
        for _ in 0..8 {
            match (&self.receive).read(&mut bytes) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    }
    pub(super) fn wait<'a>(
        &self,
        listener: Option<&TcpListener>,
        sockets: impl IntoIterator<Item = (&'a TcpStream, bool)>,
        timeout: Duration,
        stop: &ServerShutdown,
    ) -> bool {
        if stop.is_stopped() {
            return true;
        }
        #[cfg(unix)]
        {
            if self.notify.take_pending() {
                self.drain_notifications();
                return true;
            }
            let mut descriptors = Vec::new();
            let mut add = |fd, events| {
                descriptors.push(libc::pollfd {
                    fd,
                    events,
                    revents: 0,
                })
            };
            add(self.receive.as_raw_fd(), libc::POLLIN);
            if let Some(listener) = listener {
                add(listener.as_raw_fd(), libc::POLLIN);
            }
            for (stream, writable) in sockets {
                add(
                    stream.as_raw_fd(),
                    if writable {
                        libc::POLLOUT
                    } else {
                        libc::POLLIN
                    },
                );
            }
            let timeout = timeout.min(Duration::from_millis(50));
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
            // Clear before draining. A producer racing with this drain leaves
            // its latch set for the next wait even if its byte is consumed.
            let notified = self.notify.take_pending();
            self.drain_notifications();
            result > 0 || notified
        }
        #[cfg(not(unix))]
        {
            let _ = (listener, sockets, timeout);
            stop.wait_for(Duration::from_millis(1));
            false
        }
    }
}

#[cfg(all(test, unix))]
#[path = "socket_readiness_wakeup_tests.rs"]
mod wakeup_tests;
