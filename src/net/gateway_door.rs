// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! What a gateway needs to run next to others: a readiness probe that can say "stop sending
//! me work", and a heartbeat to the metaserver roster that announces it and withdraws it.
//!
//! * `/livez` answers 200 whenever the process can answer at all. A supervisor restarts on it.
//! * `/readyz` answers 200 only when the gateway is not draining and the cluster answers, 503
//!   otherwise. A balancer routes on it. `/healthz` keeps its old meaning.
//! * [`GatewayAdvertiser`] heartbeats the node online on an interval and, when drained,
//!   sends one last heartbeat marked draining so clients stop choosing this gateway before it
//!   stops answering.

use super::{Request, RequestHandler, Response};
use crate::ObjectError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// A request handler with liveness, readiness and a drain switch in front of it.
pub struct GatewayDoor {
    inner: Arc<dyn RequestHandler>,
    draining: Arc<AtomicBool>,
}

impl GatewayDoor {
    pub fn new(inner: Arc<dyn RequestHandler>) -> Self {
        Self { inner, draining: Arc::new(AtomicBool::new(false)) }
    }

    /// The switch a signal handler or an advertiser flips; shared, so cloning is cheap.
    pub fn drain_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.draining)
    }
}

impl RequestHandler for GatewayDoor {
    fn handle(&self, request: &Request) -> Response {
        match request.path.as_str() {
            "/livez" => Response::text(200, "alive\n"),
            "/readyz" => {
                if self.draining.load(Ordering::SeqCst) {
                    return Response::text(503, "draining\n");
                }
                let probe = Request {
                    method: "GET".to_string(),
                    path: "/healthz".to_string(),
                    query: String::new(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    keep_alive: request.keep_alive,
                };
                self.inner.handle(&probe)
            }
            _ => self.inner.handle(request),
        }
    }
}

/// Heartbeats a gateway's roster node until drained.
pub struct GatewayAdvertiser {
    stop: Sender<()>,
    worker: JoinHandle<()>,
}

impl GatewayAdvertiser {
    /// Call `beat(draining)` now and then every `interval`. A failed beat is retried at the
    /// next interval; the metaserver's expiry decides when silence means gone.
    pub fn start<F>(interval: Duration, mut beat: F) -> Self
    where
        F: FnMut(bool) -> Result<(), ObjectError> + Send + 'static,
    {
        let (stop, rx) = mpsc::channel::<()>();
        let worker = std::thread::spawn(move || loop {
            let _ = beat(false);
            match rx.recv_timeout(interval) {
                Err(RecvTimeoutError::Timeout) => continue,
                _ => {
                    // Withdrawn: tell the roster before the listener goes away.
                    let _ = beat(true);
                    return;
                }
            }
        });
        Self { stop, worker }
    }

    /// Withdraw this gateway: one last heartbeat marked draining, then the thread ends.
    pub fn drain(self) {
        let _ = self.stop.send(());
        let _ = self.worker.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Inner {
        healthy: AtomicBool,
        seen: Mutex<Vec<String>>,
    }
    impl RequestHandler for Inner {
        fn handle(&self, request: &Request) -> Response {
            self.seen.lock().unwrap().push(request.path.clone());
            if request.path == "/healthz" && !self.healthy.load(Ordering::SeqCst) {
                return Response::text(503, "down\n");
            }
            Response::text(200, "inner\n")
        }
    }

    fn get(path: &str) -> Request {
        Request {
            method: "GET".into(),
            path: path.into(),
            query: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            keep_alive: true,
        }
    }

    fn door(healthy: bool) -> (GatewayDoor, Arc<Inner>) {
        let inner = Arc::new(Inner { healthy: AtomicBool::new(healthy), seen: Mutex::new(Vec::new()) });
        (GatewayDoor::new(inner.clone()), inner)
    }

    #[test]
    fn livez_answers_without_asking_the_cluster() {
        let (door, inner) = door(false);
        assert_eq!(door.handle(&get("/livez")).status, 200);
        assert!(inner.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn readyz_follows_the_cluster_and_the_drain_switch() {
        let (door, inner) = door(true);
        assert_eq!(door.handle(&get("/readyz")).status, 200);
        inner.healthy.store(false, Ordering::SeqCst);
        assert_eq!(door.handle(&get("/readyz")).status, 503);
        inner.healthy.store(true, Ordering::SeqCst);
        door.drain_flag().store(true, Ordering::SeqCst);
        assert_eq!(door.handle(&get("/readyz")).status, 503);
        // Draining stops new work being routed here; it does not refuse work already arriving.
        assert_eq!(door.handle(&get("/b/k")).status, 200);
        assert_eq!(door.handle(&get("/livez")).status, 200);
    }

    #[test]
    fn other_paths_reach_the_inner_handler_unchanged() {
        let (door, inner) = door(true);
        door.handle(&get("/b/k"));
        door.handle(&get("/healthz"));
        assert_eq!(*inner.seen.lock().unwrap(), ["/b/k", "/healthz"]);
    }

    #[test]
    fn the_advertiser_beats_online_then_withdraws_once_when_drained() {
        let (tx, rx) = mpsc::channel();
        let advertiser = GatewayAdvertiser::start(Duration::from_millis(5), move |draining| {
            tx.send(draining).unwrap();
            Ok(())
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), false);
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), false);
        advertiser.drain();
        let rest: Vec<bool> = rx.try_iter().collect();
        assert_eq!(rest.iter().filter(|d| **d).count(), 1, "exactly one draining beat: {rest:?}");
        assert_eq!(rest.last(), Some(&true), "the draining beat is the last: {rest:?}");
    }

    #[test]
    fn a_failing_heartbeat_does_not_stop_the_advertiser() {
        let (tx, rx) = mpsc::channel();
        let advertiser = GatewayAdvertiser::start(Duration::from_millis(5), move |draining| {
            tx.send(draining).unwrap();
            Err(ObjectError::Corruption("metaserver down".into()))
        });
        for _ in 0..3 {
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        advertiser.drain();
    }
}
