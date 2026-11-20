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
    Arc, Mutex,
};
use std::time::Duration;
pub(super) struct Options {
    pub tls: Option<super::ApiTlsConfig>,
    pub max_payload_bytes: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub connections_per_worker: usize,
}
use crate::cluster::shutdown::ActiveConnection;
use std::{io::ErrorKind, time::Instant};

#[path = "connections_readiness.rs"]
mod readiness;

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
    budget: Budget,
    wake: Option<readiness::Notify>,
}
/// A response may finish after its request executor is available again.
pub(super) enum Response {
    Immediate(Vec<u8>),
    Deferred(Box<dyn FnOnce(Completion) + Send>),
}

/// Retains the input reservation until persistence completes or the task is abandoned.
pub(super) struct Completion {
    charge: Option<Charge>,
    reply: Option<SyncSender<Option<Reply>>>,
    budget: Budget,
    wake: Option<readiness::Notify>,
}
impl Completion {
    pub(super) fn finish(mut self, output: Vec<u8>) {
        drop(self.charge.take());
        let response = self.budget.reserve(output.len()).map(|charge| Reply {
            bytes: output,
            charge,
        });
        if let Some(reply) = self.reply.take() {
            let _ = reply.try_send(response);
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        // Disconnect before waking: the I/O loop must observe an abandoned task.
        drop(self.reply.take());
        drop(self.charge.take());
        if let Some(wake) = self.wake.take() {
            wake.notify();
        }
    }
}
// All I/O loops submit to the same bounded executor queue. A persistent socket
// stays with its I/O loop, but does not pin its requests to one executor.
#[derive(Clone)]
pub(super) struct Dispatch {
    jobs: SyncSender<Job>,
    receiver: Arc<Mutex<Receiver<Job>>>,
}
impl Dispatch {
    pub(super) fn new(workers: usize, connections_per_worker: usize) -> Self {
        let (jobs, receiver) =
            mpsc::sync_channel(connections_per_worker.max(1).saturating_mul(workers.max(1)));
        Self {
            jobs,
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }
}
struct Connection {
    wake: Option<readiness::Notify>,
    stream: super::api_tls::ApiSocket,
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
    fn new(
        stream: TcpStream,
        active: ActiveConnection,
        options: &Options,
        wake: Option<&readiness::Notify>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            wake: wake.cloned(),
            stream: super::api_tls::ApiSocket::server(stream, options.tls.as_ref())?,
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
        })
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
            if self.output_written < self.output.len() {
                match self.stream.write(&self.output[self.output_written..]) {
                    Ok(0) => return false,
                    Ok(n) => {
                        self.output_written += n;
                        *progressed = true;
                    }
                    Err(e)
                        if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) =>
                    {
                        return true
                    }
                    Err(_) => return false,
                }
            }
            if self.output_written == self.output.len() {
                match self.stream.flush() {
                    Ok(()) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            ErrorKind::WouldBlock | ErrorKind::Interrupted
                        ) =>
                    {
                        return true
                    }
                    Err(_) => return false,
                }
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
            budget: budget.clone(),
            wake: self.wake.clone(),
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
    respond: impl FnMut(u8, &[u8]) -> Vec<u8> + Send,
    stop: Arc<ServerShutdown>,
    options: Options,
    dispatch: Dispatch,
) {
    accept_loop_with_responders(
        listener,
        requests,
        failures,
        vec![respond],
        stop,
        options,
        dispatch,
    );
}

/// Execution concurrency does not enlarge this loop's connection or byte budgets.
pub(super) fn accept_loop_with_responders(
    listener: TcpListener,
    requests: &AtomicU64,
    failures: &AtomicU64,
    responders: Vec<impl FnMut(u8, &[u8]) -> Vec<u8> + Send>,
    stop: Arc<ServerShutdown>,
    options: Options,
    dispatch: Dispatch,
) {
    let responders = responders
        .into_iter()
        .map(|mut respond| move |code, payload: &[u8]| Response::Immediate(respond(code, payload)))
        .collect();
    accept_loop_with_deferred_responders(
        listener, requests, failures, responders, stop, options, dispatch,
    );
}

pub(super) fn accept_loop_with_deferred_responders(
    listener: TcpListener,
    requests: &AtomicU64,
    failures: &AtomicU64,
    responders: Vec<impl FnMut(u8, &[u8]) -> Response + Send>,
    stop: Arc<ServerShutdown>,
    options: Options,
    dispatch: Dispatch,
) {
    let responders = responders.into_iter()
        .map(|mut respond| move |code, payload: Vec<u8>| respond(code, &payload))
        .collect();
    accept_loop_with_owned_deferred_responders(
        listener, requests, failures, responders, stop, options, dispatch,
    );
}

/// The input reservation outlives decoding even when its buffer moves into a request.
pub(super) fn accept_loop_with_owned_deferred_responders(
    listener: TcpListener,
    requests: &AtomicU64,
    failures: &AtomicU64,
    responders: Vec<impl FnMut(u8, Vec<u8>) -> Response + Send>,
    stop: Arc<ServerShutdown>,
    options: Options,
    dispatch: Dispatch,
) {
    let budget = Budget {
        used: Arc::new(AtomicUsize::new(0)),
        limit: options
            .max_payload_bytes
            .saturating_add(HEADER_BYTES)
            .saturating_mul(2),
    };
    let readiness = readiness::Waiter::new(&stop).ok();
    let wake = readiness.as_ref().map(|waiter| waiter.notifier());
    let jobs = dispatch.jobs;
    let rx = dispatch.receiver;
    std::thread::scope(|scope| {
        for mut respond in responders {
            let worker_stop = Arc::clone(&stop);
            let rx = Arc::clone(&rx);
            scope.spawn(move || loop {
                let job = {
                    let receiver = rx.lock().unwrap_or_else(|e| e.into_inner());
                    if worker_stop.is_stopped() {
                        break;
                    }
                    match receiver.recv_timeout(Duration::from_millis(100)) {
                        Ok(job) => job,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                };
                if worker_stop.is_stopped() {
                    break;
                }
                let response = respond(job.code, job.payload);
                let completion = Completion {
                    charge: Some(job.charge),
                    reply: Some(job.reply),
                    budget: job.budget,
                    wake: job.wake,
                };
                match response {
                    Response::Immediate(output) => completion.finish(output),
                    Response::Deferred(submit) => submit(completion),
                }
            });
        }
        let mut connections: Vec<Connection> = Vec::new();
        while !stop.is_stopped() {
            let mut progressed = false;
            // Bound accept work so a connection burst cannot starve existing frames.
            for _ in 0..16 {
                if connections.len() >= options.connections_per_worker.max(1) {
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
                        if let Ok(connection) =
                            Connection::new(stream, active, &options, wake.as_ref())
                        {
                            connections.push(connection);
                        }
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
                match &readiness {
                    Some(waiter) => {
                        waiter.wait(
                            &listener,
                            &connections,
                            options.connections_per_worker.max(1),
                            &stop,
                        );
                    }
                    None => {
                        stop.wait_for(Duration::from_millis(1));
                    }
                }
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
    fn one_io_loop_can_use_idle_request_executors() {
        exercise_request_executors(2, 1);
    }

    #[test]
    fn independent_executors_share_one_io_loop() {
        exercise_request_executors(1, 2);
    }

    fn exercise_request_executors(io_loops: usize, executors_per_loop: usize) {
        let dispatch = Dispatch::new(2, 128);
        let stop = Arc::new(ServerShutdown::default());
        let (entered, entered_rx) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let mut handles = Vec::new();
        let mut address = None;
        for _ in 0..io_loops {
            // Separate listeners deliberately keep every client on one I/O loop.
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            address.get_or_insert(listener.local_addr().unwrap());
            listener.set_nonblocking(true).unwrap();
            let dispatch = dispatch.clone();
            let stop = Arc::clone(&stop);
            let entered = entered.clone();
            let release_rx = Arc::clone(&release_rx);
            handles.push(std::thread::spawn(move || {
                let responders = (0..executors_per_loop)
                    .map(|_| {
                        let entered = entered.clone();
                        let release_rx = Arc::clone(&release_rx);
                        move |code, _: &[u8]| {
                            if code == 1 {
                                entered.send(()).unwrap();
                                let _ = release_rx
                                    .lock()
                                    .unwrap()
                                    .recv_timeout(Duration::from_secs(5));
                            }
                            vec![code]
                        }
                    })
                    .collect();
                accept_loop_with_responders(
                    listener,
                    &AtomicU64::new(0),
                    &AtomicU64::new(0),
                    responders,
                    stop,
                    Options {
                        tls: None,
                        max_payload_bytes: 64,
                        read_timeout: Duration::from_secs(5),
                        write_timeout: Duration::from_secs(5),
                        connections_per_worker: 128,
                    },
                    dispatch,
                );
            }));
        }
        let mut first = TcpStream::connect(address.unwrap()).unwrap();
        first
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        first
            .write_all(&[super::super::wire::PROTOCOL_STAMP, 1, 0, 0, 0, 0])
            .unwrap();
        let admitted = entered_rx.recv_timeout(Duration::from_secs(2));
        let mut second = TcpStream::connect(address.unwrap()).unwrap();
        second
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        second
            .write_all(&[super::super::wire::PROTOCOL_STAMP, 2, 0, 0, 0, 0])
            .unwrap();
        let mut reply = [0];
        let second_result = second.read_exact(&mut reply);
        // Always release and join, including on assertion failure.
        release.send(()).unwrap();
        let mut first_reply = [0];
        let first_result = first.read_exact(&mut first_reply);
        stop.stop();
        for handle in handles {
            handle.join().unwrap();
        }
        admitted.unwrap();
        second_result
            .expect("idle executor must serve a second connection while the first request blocks");
        first_result.unwrap();
        assert_eq!(reply, [2]);
        assert_eq!(first_reply, [1]);
    }
    #[test]
    fn deferred_completion_retains_budget_and_releases_on_abandonment() {
        let budget = Budget {
            used: Arc::new(AtomicUsize::new(0)),
            limit: 64,
        };
        let (reply, receive) = mpsc::sync_channel(1);
        let completion = Completion {
            charge: Some(budget.reserve(64).unwrap()),
            reply: Some(reply),
            budget: budget.clone(),
            wake: None,
        };
        assert!(budget.reserve(1).is_none());
        assert!(matches!(receive.try_recv(), Err(TryRecvError::Empty)));
        completion.finish(vec![7; 32]);
        assert_eq!(budget.used.load(Ordering::Acquire), 32);
        let reply = receive.recv().unwrap().unwrap();
        assert_eq!(reply.bytes, vec![7; 32]);
        drop(reply);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        let (reply, receive) = mpsc::sync_channel(1);
        let completion = Completion {
            charge: Some(budget.reserve(64).unwrap()),
            reply: Some(reply),
            budget: budget.clone(),
            wake: None,
        };
        drop(completion);
        assert!(matches!(
            receive.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn deferred_completion_refuses_oversized_reply_and_releases_on_disconnect() {
        let budget = Budget {
            used: Arc::new(AtomicUsize::new(0)),
            limit: 64,
        };
        for disconnected in [false, true] {
            let (reply, receive) = mpsc::sync_channel(1);
            let completion = Completion {
                charge: Some(budget.reserve(64).unwrap()),
                reply: Some(reply),
                budget: budget.clone(),
                wake: None,
            };
            if disconnected {
                drop(receive);
                completion.finish(vec![1; 32]);
            } else {
                completion.finish(vec![1; 65]);
                assert!(receive.recv().unwrap().is_none());
            }
            assert_eq!(budget.used.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn one_executor_serves_other_connections_before_deferred_completion() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(ServerShutdown::default());
        let worker_stop = Arc::clone(&stop);
        let (pending, receive) = mpsc::sync_channel(1);
        let handle = std::thread::spawn(move || {
            accept_loop_with_deferred_responders(
                listener,
                &AtomicU64::new(0),
                &AtomicU64::new(0),
                vec![move |code, _: &[u8]| {
                    if code == 1 {
                        let pending = pending.clone();
                        Response::Deferred(Box::new(move |completion| {
                            let _ = pending.send(completion);
                        }))
                    } else {
                        Response::Immediate(vec![code])
                    }
                }],
                worker_stop,
                Options {
                    tls: None,
                    max_payload_bytes: 64,
                    read_timeout: Duration::from_secs(10),
                    write_timeout: Duration::from_secs(10),
                    connections_per_worker: 128,
                },
                Dispatch::new(1, 128),
            );
        });
        let mut first = TcpStream::connect(address).unwrap();
        first
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        first
            .write_all(&[super::super::wire::PROTOCOL_STAMP, 1, 0, 0, 0, 0])
            .unwrap();
        let pending = receive.recv_timeout(Duration::from_secs(5));
        let mut early = [0];
        let early_reply = first.read(&mut early);
        let mut second = TcpStream::connect(address).unwrap();
        second
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        second
            .write_all(&[super::super::wire::PROTOCOL_STAMP, 2, 0, 0, 0, 0])
            .unwrap();
        let mut second_reply = [0];
        let second_result = second.read_exact(&mut second_reply);
        if let Ok(completion) = pending {
            completion.finish(vec![1]);
        }
        first
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut first_reply = [0];
        let first_result = first.read_exact(&mut first_reply);
        // Also abandon a deferred task; dropping it must wake the pending socket.
        first
            .write_all(&[super::super::wire::PROTOCOL_STAMP, 1, 0, 0, 0, 0])
            .unwrap();
        let abandoned = receive.recv_timeout(Duration::from_secs(5));
        drop(abandoned);
        let closed = first.read(&mut early);
        stop.stop();
        handle.join().unwrap();
        assert!(
            matches!(early_reply, Err(ref error) if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock))
        );
        second_result
            .expect("one executor must serve a second connection before persistence completes");
        assert_eq!(second_reply, [2]);
        first_result.unwrap();
        assert_eq!(first_reply, [1]);
        assert_eq!(
            closed.unwrap(),
            0,
            "abandoned completion must disconnect and wake the I/O loop"
        );
    }

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
