// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Choosing a gateway: the client half of discovery.
//!
//! A client is given the metaserver addresses and nothing else. It asks the roster which
//! gateways are up ([`super::gateway_roster::live_gateways`]), spreads requests over them, and
//! when one fails it stops choosing that one for a while and asks the roster again, so a dead
//! gateway leaves the rotation before the metaserver's own expiry has noticed it.
//!
//! The picker does no I/O of its own: it is handed a function that returns the current list.
//! Time is passed in, so the whole policy is testable without sleeping.

use super::gateway_roster::GatewayEndpoint;
use crate::ObjectError;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Rotates over the live gateways and sidelines ones that failed.
pub struct GatewayPicker<F>
where
    F: FnMut() -> Result<Vec<GatewayEndpoint>, ObjectError>,
{
    fetch: F,
    list: Vec<GatewayEndpoint>,
    fetched_at: Option<Instant>,
    /// The list is asked for again after this long even if nothing failed.
    refresh_after: Duration,
    /// A gateway that failed is not chosen again for this long.
    cooldown: Duration,
    failed_at: HashMap<String, Instant>,
    next: usize,
    stale: bool,
}

impl<F> GatewayPicker<F>
where
    F: FnMut() -> Result<Vec<GatewayEndpoint>, ObjectError>,
{
    pub fn new(fetch: F, refresh_after: Duration, cooldown: Duration) -> Self {
        Self {
            fetch,
            list: Vec::new(),
            fetched_at: None,
            refresh_after,
            cooldown,
            failed_at: HashMap::new(),
            next: 0,
            stale: true,
        }
    }

    /// The gateway to send the next request to.
    pub fn pick(&mut self) -> Result<GatewayEndpoint, ObjectError> {
        self.pick_at(Instant::now())
    }

    /// Tell the picker a request to `node_id` failed at the transport level.
    pub fn report_failure(&mut self, node_id: &str) {
        self.report_failure_at(node_id, Instant::now());
    }

    pub fn pick_at(&mut self, now: Instant) -> Result<GatewayEndpoint, ObjectError> {
        let due = self.stale
            || self.list.is_empty()
            || self
                .fetched_at
                .map_or(true, |at| now.saturating_duration_since(at) >= self.refresh_after);
        if due {
            self.refresh(now);
        }
        // Every known gateway being sidelined is not a reason to read the roster on every pick:
        // a failure already marked the list stale, so it was re-read above.
        if let Some(found) = self.take_usable(now) {
            return Ok(found);
        }
        Err(ObjectError::NotFound("no gateway is available".into()))
    }

    pub fn report_failure_at(&mut self, node_id: &str, now: Instant) {
        self.failed_at.insert(node_id.to_string(), now);
        // Ask the roster again on the next pick: the failed gateway may already be gone.
        self.stale = true;
    }

    fn take_usable(&mut self, now: Instant) -> Option<GatewayEndpoint> {
        let count = self.list.len();
        for step in 0..count {
            let index = (self.next + step) % count;
            let node = &self.list[index];
            let usable = self
                .failed_at
                .get(&node.node_id)
                .map_or(true, |at| now.saturating_duration_since(*at) >= self.cooldown);
            if usable {
                self.next = index + 1;
                return Some(node.clone());
            }
        }
        None
    }

    fn refresh(&mut self, now: Instant) {
        // A failed refresh keeps the list we had; the next pick tries again.
        match (self.fetch)() {
            Ok(list) => {
                self.failed_at.retain(|id, _| list.iter().any(|node| &node.node_id == id));
                self.list = list;
                self.fetched_at = Some(now);
                self.stale = false;
            }
            Err(_) => {
                self.stale = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn gw(id: &str) -> GatewayEndpoint {
        GatewayEndpoint { node_id: id.into(), endpoint: format!("{id}:80"), failure_domain: "r".into() }
    }

    type Source = Rc<RefCell<Result<Vec<GatewayEndpoint>, String>>>;

    fn picker(
        source: &Source,
        fetches: &Rc<RefCell<u32>>,
    ) -> GatewayPicker<impl FnMut() -> Result<Vec<GatewayEndpoint>, ObjectError>> {
        let source = Rc::clone(source);
        let fetches = Rc::clone(fetches);
        GatewayPicker::new(
            move || {
                *fetches.borrow_mut() += 1;
                source.borrow().clone().map_err(ObjectError::Corruption)
            },
            Duration::from_secs(10),
            Duration::from_secs(5),
        )
    }

    fn setup(ids: &[&str]) -> (Source, Rc<RefCell<u32>>) {
        (Rc::new(RefCell::new(Ok(ids.iter().map(|i| gw(i)).collect()))), Rc::new(RefCell::new(0)))
    }

    #[test]
    fn picks_rotate_over_every_live_gateway() {
        let (source, fetches) = setup(&["a", "b", "c"]);
        let mut picker = picker(&source, &fetches);
        let t = Instant::now();
        let got: Vec<_> = (0..6).map(|_| picker.pick_at(t).unwrap().node_id).collect();
        assert_eq!(got, ["a", "b", "c", "a", "b", "c"]);
        assert_eq!(*fetches.borrow(), 1, "one roster read serves many picks");
    }

    #[test]
    fn a_failed_gateway_is_skipped_and_the_roster_is_read_again() {
        let (source, fetches) = setup(&["a", "b", "c"]);
        let mut picker = picker(&source, &fetches);
        let t = Instant::now();
        assert_eq!(picker.pick_at(t).unwrap().node_id, "a");
        // The roster has not noticed yet: "a" is still listed.
        picker.report_failure_at("a", t);
        let got: Vec<_> = (0..4).map(|_| picker.pick_at(t).unwrap().node_id).collect();
        assert!(!got.contains(&"a".to_string()), "{got:?}");
        assert_eq!(*fetches.borrow(), 2, "a failure triggers one re-read");
    }

    #[test]
    fn a_failed_gateway_returns_after_its_cooldown() {
        let (source, fetches) = setup(&["a", "b"]);
        let mut picker = picker(&source, &fetches);
        let t = Instant::now();
        picker.pick_at(t).unwrap();
        picker.report_failure_at("a", t);
        assert_eq!(picker.pick_at(t).unwrap().node_id, "b");
        let later = t + Duration::from_secs(6);
        let got: Vec<_> = (0..2).map(|_| picker.pick_at(later).unwrap().node_id).collect();
        assert!(got.contains(&"a".to_string()), "{got:?}");
    }

    #[test]
    fn the_list_is_refreshed_when_it_is_old_and_new_gateways_are_used() {
        let (source, fetches) = setup(&["a"]);
        let mut picker = picker(&source, &fetches);
        let t = Instant::now();
        assert_eq!(picker.pick_at(t).unwrap().node_id, "a");
        *source.borrow_mut() = Ok(vec![gw("b")]);
        assert_eq!(picker.pick_at(t + Duration::from_secs(1)).unwrap().node_id, "a");
        assert_eq!(picker.pick_at(t + Duration::from_secs(11)).unwrap().node_id, "b");
    }

    #[test]
    fn no_gateway_is_an_error_and_a_failed_roster_read_keeps_the_old_list() {
        let (source, fetches) = setup(&[]);
        let mut picker = picker(&source, &fetches);
        let t = Instant::now();
        assert!(picker.pick_at(t).is_err());
        *source.borrow_mut() = Ok(vec![gw("a")]);
        assert_eq!(picker.pick_at(t).unwrap().node_id, "a");
        *source.borrow_mut() = Err("metaserver down".into());
        // Past the refresh time the read fails; the gateway we knew is still used.
        assert_eq!(picker.pick_at(t + Duration::from_secs(30)).unwrap().node_id, "a");
    }

    #[test]
    fn when_every_gateway_failed_the_roster_is_asked_for_a_replacement() {
        let (source, fetches) = setup(&["a"]);
        let mut picker = picker(&source, &fetches);
        let t = Instant::now();
        picker.pick_at(t).unwrap();
        picker.report_failure_at("a", t);
        *source.borrow_mut() = Ok(vec![gw("z")]);
        assert_eq!(picker.pick_at(t).unwrap().node_id, "z");
    }
}
