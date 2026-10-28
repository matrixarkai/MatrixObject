// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded, nonblocking connection sharing for storage node APIs.
use super::message::check_stamp;
use super::shutdown::ServerShutdown;
use super::wire::{StatusCode, HEADER_BYTES};
use crate::ObjectError;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    mpsc::{self, Receiver, SyncSender, TryRecvError},
    Arc,
};
use std::time::Duration;
pub(super) struct Options {
    pub max_payload_bytes: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}
use crate::cluster::shutdown::ActiveConnection;
use std::{io::ErrorKind, time::Instant};

const CONNECTIONS_PER_WORKER: usize = 128;
#[derive(Clone)]
struct Budget {
    used: Arc<AtomicUsize>,
    limit: usize,
}
struct Charge {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Budget {
    fn reserve(&self, bytes: usize) -> Option<Charge> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .ok()?;
        Some(Charge {
            used: Arc::clone(&self.used),
            bytes,
        })
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Reply {
    bytes: Vec<u8>,
    charge: Charge,
}
struct Job {
    code: u8,
    payload: Vec<u8>,
    charge: Charge,
    reply: SyncSender<Option<Reply>>,
}
struct Connection {
    stream: TcpStream,
    _active: ActiveConnection,
    header: [u8; HEADER_BYTES],
    header_read: usize,
    payload: Vec<u8>,
    payload_read: usize,
    output: Vec<u8>,
    output_written: usize,
    charge: Option<Charge>,
    deadline: Instant,
    close_after_write: bool,
    pending: Option<Receiver<Option<Reply>>>,
}
fn deadline(timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now)
}
impl Connection {
    fn new(stream: TcpStream, active: ActiveConnection, options: &Options) -> Self {
        Self {
            stream,
            _active: active,
            header: [0; HEADER_BYTES],
            header_read: 0,
            payload: Vec::new(),
            payload_read: 0,
            output: Vec::new(),
            output_written: 0,
            charge: None,
            deadline: deadline(options.read_timeout),
            close_after_write: false,
            pending: None,
        }
    }
    fn respond(&mut self, output: Vec<u8>, budget: &Budget, options: &Options) -> bool {
        // Request and encoded response coexist only while this worker handles one frame.
        // Queued input and output buffers share the same reservation budget.
        self.payload = Vec::new();
        self.charge = None;
        let Some(charge) = budget.reserve(output.len()) else {
            return false;
        };
        self.charge = Some(charge);
        self.output = output;
        self.output_written = 0;
        self.deadline = deadline(options.write_timeout);
        true
    }
    fn step(
        &mut self,
        requests: &AtomicU64,
        failures: &AtomicU64,
        jobs: &SyncSender<Job>,
        options: &Options,
        budget: &Budget,
        progressed: &mut bool,
    ) -> bool {
        if Instant::now() >= self.deadline {
            return false;
        }
        if let Some(rx) = &self.pending {
            match rx.try_recv() {
                Ok(Some(reply)) => {
                    self.output = reply.bytes;
                    self.charge = Some(reply.charge);
                    self.output_written = 0;
                    self.deadline = deadline(options.write_timeout);
                    self.pending = None;
                    *progressed = true;
                }
                Ok(None) | Err(TryRecvError::Disconnected) => return false,
                Err(TryRecvError::Empty) => return true,
            }
        }
        if !self.output.is_empty() {
            match self.stream.write(&self.output[self.output_written..]) {
                Ok(0) => return false,
                Ok(n) => {
                    self.output_written += n;
                    *progressed = true;
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
                    return true
                }
                Err(_) => return false,
            }
            if self.output_written == self.output.len() {
                if self.close_after_write {
                    return false;
                }
                self.output = Vec::new();
                self.charge = None;
                self.header_read = 0;
                self.payload_read = 0;
                self.deadline = deadline(options.read_timeout);
            }
            return true;
        }
        if self.header_read < HEADER_BYTES {
            match self.stream.read(&mut self.header[self.header_read..]) {
                Ok(0) => return false,
                Ok(n) => {
                    self.header_read += n;
                    *progressed = true;
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
                    return true
                }
                Err(_) => return false,
            }
            if self.header_read < HEADER_BYTES {
                return true;
            }
            requests.fetch_add(1, Ordering::Relaxed);
            let length = u32::from_be_bytes(self.header[2..6].try_into().unwrap()) as usize;
            let error = check_stamp(self.header[0]).err().or_else(|| {
                (length > options.max_payload_bytes).then(|| {
                    ObjectError::InvalidArgument(format!(
                        "a node frame declares a {length}-byte payload, past the {}-byte bound",
                        options.max_payload_bytes
                    ))
                })
            });
            if let Some(error) = error {
                failures.fetch_add(1, Ordering::Relaxed);
                self.close_after_write = true;
                return self.respond(
                    crate::cluster::message::encode_bounded_failure(
                        StatusCode::for_error(&error),
                        &error.to_string(),
                        options.max_payload_bytes,
                    ),
                    budget,
                    options,
                );
            }
            let Some(charge) = budget.reserve(length) else {
                return false;
            };
            self.charge = Some(charge);
            self.payload = vec![0; length];
        }
        if self.payload_read < self.payload.len() {
            match self.stream.read(&mut self.payload[self.payload_read..]) {
                Ok(0) => return false,
                Ok(n) => {
                    self.payload_read += n;
                    *progressed = true;
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
                    return true
                }
                Err(_) => return false,
            }
            if self.payload_read < self.payload.len() {
                return true;
            }
        }
        let (reply, rx) = mpsc::sync_channel(1);
        let job = Job {
            code: self.header[1],
            payload: std::mem::take(&mut self.payload),
            charge: self
                .charge
                .take()
                .expect("complete frames hold a reservation"),
            reply,
        };
        if jobs.try_send(job).is_err() {
            return false;
        }
        self.pending = Some(rx);
        self.deadline = deadline(options.read_timeout);
        *progressed = true;
        true
    }
}
pub(super) fn accept_loop(
    listener: TcpListener,
    requests: &AtomicU64,
    failures: &AtomicU64,
    mut respond: impl FnMut(u8, &[u8]) -> Vec<u8> + Send,
    stop: Arc<ServerShutdown>,
    options: Options,
) {
    let budget = Budget {
        used: Arc::new(AtomicUsize::new(0)),
        limit: options
            .max_payload_bytes
            .saturating_add(HEADER_BYTES)
            .saturating_mul(2),
    };
    let (jobs, rx) = mpsc::sync_channel::<Job>(CONNECTIONS_PER_WORKER);
    std::thread::scope(|scope| {
        let worker_budget = budget.clone();
        let worker_stop = Arc::clone(&stop);
        scope.spawn(move || {
            while let Ok(job) = rx.recv() {
                if worker_stop.is_stopped() {
                    break;
                }
                let output = respond(job.code, &job.payload);
                drop(job.payload);
                drop(job.charge);
                let response = worker_budget.reserve(output.len()).map(|charge| Reply {
                    bytes: output,
                    charge,
                });
                let _ = job.reply.try_send(response);
            }
        });
        let mut connections: Vec<Connection> = Vec::new();
        while !stop.is_stopped() {
            let mut progressed = false;
            // Bound accept work so a connection burst cannot starve existing frames.
            for _ in 0..16 {
                if connections.len() >= CONNECTIONS_PER_WORKER {
                    break;
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        progressed = true;
                        if stream.set_nonblocking(true).is_err() {
                            continue;
                        }
                        let _ = stream.set_nodelay(true);
                        let active = match stop.track(&stream) {
                            Ok(Some(active)) => active,
                            Ok(None) => break,
                            Err(_) => continue,
                        };
                        connections.push(Connection::new(stream, active, &options));
                    }
                    Err(_) => break,
                }
            }
            let mut i = 0;
            while i < connections.len() && !stop.is_stopped() {
                if connections[i].step(
                    requests,
                    failures,
                    &jobs,
                    &options,
                    &budget,
                    &mut progressed,
                ) {
                    i += 1;
                } else {
                    connections.swap_remove(i);
                    progressed = true;
                }
            }
            if !progressed {
                stop.wait_for(Duration::from_millis(1));
            }
        }
        drop(connections);
        drop(jobs);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frame_budget_bounds_queued_buffers_and_releases_on_disconnect() {
        let budget = Budget {
            used: Arc::new(AtomicUsize::new(0)),
            limit: 64,
        };
        let first = budget.reserve(32).unwrap();
        let second = budget.reserve(32).unwrap();
        assert!(budget.reserve(1).is_none());
        assert!(budget.reserve(usize::MAX).is_none());
        let (tx, rx) = mpsc::sync_channel(1);
        tx.send(Reply {
            bytes: vec![0; 32],
            charge: first,
        })
        .unwrap_or_else(|_| panic!("receiver is open"));
        // A client closing before consuming its reply returns the reserved memory.
        drop(rx);
        assert_eq!(budget.used.load(Ordering::Acquire), 32);
        std::thread::spawn(move || drop(second)).join().unwrap();
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert!(budget.reserve(64).is_some());
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }
}
