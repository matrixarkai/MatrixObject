// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded plaintext metadata frame accounting for local process tests.
use matrixobjectstore_rs::{cluster::MetaRequestCode, staged_allocation::StagedAllocationRequest};
use prost::Message;
use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Clone, Copy, Default, Debug)]
pub(super) struct Counts {
    pub requests: u64,
    pub replies: u64,
    pub publication_bytes: u64,
    pub publications: u64,
}
impl Counts {
    pub fn since(self, before: Self) -> Self {
        Self {
            requests: self.requests.checked_sub(before.requests).unwrap(),
            replies: self.replies.checked_sub(before.replies).unwrap(),
            publication_bytes: self
                .publication_bytes
                .checked_sub(before.publication_bytes)
                .unwrap(),
            publications: self.publications.checked_sub(before.publications).unwrap(),
        }
    }
}
#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    replies: AtomicU64,
    publication_bytes: AtomicU64,
    publications: AtomicU64,
    failed: AtomicBool,
}
pub(super) struct FrameMeter {
    pub endpoint: String,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    worker: Option<JoinHandle<()>>,
}

fn read_part(stream: &mut TcpStream, bytes: &mut [u8], stop: &AtomicBool) -> io::Result<bool> {
    let mut at = 0;
    while at < bytes.len() {
        if stop.load(Ordering::SeqCst) {
            return Ok(false);
        }
        match stream.read(&mut bytes[at..]) {
            Ok(0) if at == 0 => return Ok(false),
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => at += count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}
fn frame(stream: &mut TcpStream, stop: &AtomicBool) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0; 6];
    if !read_part(stream, &mut header, stop)? {
        return Ok(None);
    }
    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    if length > 8 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "meter frame exceeds bound",
        ));
    }
    let mut bytes = header.to_vec();
    bytes.resize(6 + length, 0);
    if !read_part(stream, &mut bytes[6..], stop)? {
        if stop.load(Ordering::SeqCst) {
            return Ok(None);
        }
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(Some(bytes))
}
fn configure(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(100)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))
}

impl FrameMeter {
    pub fn start(backend: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::default());
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let worker = {
            let stop = stop.clone();
            let counters = counters.clone();
            let sockets = sockets.clone();
            thread::spawn(move || {
                let mut workers = Vec::new();
                while !stop.load(Ordering::SeqCst) {
                    let incoming = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        Err(_) => {
                            counters.failed.store(true, Ordering::SeqCst);
                            break;
                        }
                    };
                    if workers.len() >= 64 {
                        counters.failed.store(true, Ordering::SeqCst);
                        break;
                    }
                    let stop = stop.clone();
                    let counters = counters.clone();
                    let sockets = sockets.clone();
                    workers.push(thread::spawn(move || {
                        let mut incoming = incoming;
                        let result = (|| -> io::Result<()> {
                            let mut upstream =
                                TcpStream::connect_timeout(&backend, Duration::from_secs(3))?;
                            configure(&incoming)?;
                            configure(&upstream)?;
                            {
                                let mut sockets = sockets.lock().unwrap();
                                sockets.push(incoming.try_clone()?);
                                sockets.push(upstream.try_clone()?);
                            }
                            while let Some(request) = frame(&mut incoming, &stop)? {
                                counters
                                    .requests
                                    .fetch_add(request.len() as u64, Ordering::SeqCst);
                                if request[1] == MetaRequestCode::StagedAllocation as u8 {
                                    let command = StagedAllocationRequest::decode(&request[6..])
                                        .map_err(|_| io::ErrorKind::InvalidData)?;
                                    if command.action == 4 {
                                        counters.publications.fetch_add(1, Ordering::SeqCst);
                                        counters
                                            .publication_bytes
                                            .fetch_add(request.len() as u64, Ordering::SeqCst);
                                    }
                                }
                                upstream.write_all(&request)?;
                                let reply = frame(&mut upstream, &stop)?
                                    .ok_or(io::ErrorKind::UnexpectedEof)?;
                                counters
                                    .replies
                                    .fetch_add(reply.len() as u64, Ordering::SeqCst);
                                incoming.write_all(&reply)?;
                            }
                            let _ = upstream.shutdown(Shutdown::Both);
                            Ok(())
                        })();
                        if result.is_err() && !stop.load(Ordering::SeqCst) {
                            counters.failed.store(true, Ordering::SeqCst);
                        }
                        let _ = incoming.shutdown(Shutdown::Both);
                    }));
                }
                for worker in workers {
                    if worker.join().is_err() {
                        counters.failed.store(true, Ordering::SeqCst);
                    }
                }
            })
        };
        Self {
            endpoint,
            counters,
            stop,
            sockets,
            worker: Some(worker),
        }
    }
    pub fn counts(&self) -> Counts {
        assert!(
            !self.counters.failed.load(Ordering::SeqCst),
            "metadata meter failed"
        );
        Counts {
            requests: self.counters.requests.load(Ordering::SeqCst),
            replies: self.counters.replies.load(Ordering::SeqCst),
            publication_bytes: self.counters.publication_bytes.load(Ordering::SeqCst),
            publications: self.counters.publications.load(Ordering::SeqCst),
        }
    }
}
impl Drop for FrameMeter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        {
            for socket in self.sockets.lock().unwrap().iter() {
                let _ = socket.shutdown(Shutdown::Both);
            }
        }
        let _ = self.worker.take().unwrap().join();
    }
}
