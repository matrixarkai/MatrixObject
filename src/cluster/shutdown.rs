// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// Shares cancellation with accept workers and their currently active sockets.
#[derive(Debug, Default)]
pub(super) struct ServerShutdown {
    stopped: AtomicBool,
    wake: Condvar,
    wait_lock: Mutex<()>,
    connections: Mutex<Vec<Option<TcpStream>>>,
    #[cfg(unix)]
    wake_sockets: Mutex<Vec<std::sync::Weak<std::os::unix::net::UnixStream>>>,
}

impl ServerShutdown {
    pub(super) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Wait between maintenance passes, waking immediately when shutdown begins.
    pub(super) fn wait_for(&self, duration: std::time::Duration) -> bool {
        let guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
        let _wait = self
            .wake
            .wait_timeout_while(guard, duration, |_| !self.is_stopped())
            .unwrap_or_else(|e| e.into_inner());
        self.is_stopped()
    }

    pub(super) fn stop(&self) {
        let connections = self.connections.lock().unwrap_or_else(|e| e.into_inner());
        let _wait = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.stopped.store(true, Ordering::SeqCst);
        self.wake.notify_all();
        #[cfg(unix)]
        for socket in self
            .wake_sockets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(|s| s.upgrade())
        {
            use std::io::Write;
            let _ = (&*socket).write(&[1]);
        }
        for stream in connections.iter().flatten() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    #[cfg(unix)]
    pub(super) fn track_wake(&self, socket: &Arc<std::os::unix::net::UnixStream>) {
        use std::io::Write;
        let mut sockets = self.wake_sockets.lock().unwrap_or_else(|e| e.into_inner());
        sockets.retain(|s| s.strong_count() != 0);
        sockets.push(Arc::downgrade(socket));
        if self.is_stopped() {
            let _ = (&**socket).write(&[1]);
        }
    }

    pub(super) fn track(
        self: &Arc<Self>,
        stream: &TcpStream,
    ) -> std::io::Result<Option<ActiveConnection>> {
        let stream = stream.try_clone()?;
        let mut connections = self.connections.lock().unwrap_or_else(|e| e.into_inner());
        // Registration and cancellation share the lock so an accepted socket cannot
        // start blocking after stop has already walked the active sockets.
        if self.is_stopped() {
            let _ = stream.shutdown(Shutdown::Both);
            return Ok(None);
        }
        let slot = connections
            .iter()
            .position(Option::is_none)
            .unwrap_or(connections.len());
        if slot == connections.len() {
            connections.push(Some(stream));
        } else {
            connections[slot] = Some(stream);
        }
        Ok(Some(ActiveConnection {
            shutdown: Arc::clone(self),
            slot,
        }))
    }
}

pub(super) struct ActiveConnection {
    shutdown: Arc<ServerShutdown>,
    slot: usize,
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.shutdown
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())[self.slot] = None;
    }
}
