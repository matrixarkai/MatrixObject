// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use std::collections::VecDeque;

/// Connections are checked out for one batch. A miss frees a slot before the
/// caller opens sockets, keeping cached plus checked-out groups within budget.
pub(super) struct ConnectionCache<T> {
    capacity: usize,
    entries: VecDeque<(usize, T)>,
}

impl<T> ConnectionCache<T> {
    pub(super) fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn take(&mut self, group: usize) -> Option<T> {
        if let Some(index) = self.entries.iter().position(|(key, _)| *key == group) {
            return self.entries.remove(index).map(|(_, value)| value);
        }
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        None
    }

    pub(super) fn put(&mut self, group: usize, value: T) {
        debug_assert!(!self.entries.iter().any(|(key, _)| key == &group));
        debug_assert!(self.entries.len() < self.capacity);
        self.entries.push_back((group, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};

    #[test]
    fn connection_budget_releases_old_sockets_before_reconnect_and_reuses_recent_groups() {
        struct Sockets(Rc<Cell<usize>>);
        impl Sockets {
            fn open(live: &Rc<Cell<usize>>) -> Self {
                live.set(live.get() + 1);
                Self(Rc::clone(live))
            }
        }
        impl Drop for Sockets {
            fn drop(&mut self) {
                self.0.set(self.0.get() - 1);
            }
        }
        let live = Rc::new(Cell::new(0));
        let mut cache = ConnectionCache::new(2);
        for group in [0, 1] {
            assert!(cache.take(group).is_none());
            cache.put(group, Sockets::open(&live));
        }
        let reused = cache.take(0).unwrap();
        assert_eq!(live.get(), 2);
        cache.put(0, reused);
        assert!(cache.take(2).is_none());
        assert_eq!(
            live.get(),
            1,
            "least recently used sockets must close before opening"
        );
        cache.put(2, Sockets::open(&live));
        assert!(
            cache.take(0).is_some(),
            "recent group must remain connected"
        );
        assert_eq!(live.get(), 1);
        assert!(cache.take(1).is_none(), "evicted group must reconnect");
        cache.put(1, Sockets::open(&live));
        drop(cache);
        assert_eq!(live.get(), 0);

        let mut cache = ConnectionCache::new(1);
        for _ in 0..8 {
            for group in [0, 1, 2] {
                assert!(cache.take(group).is_none());
                assert_eq!(live.get(), 0);
                cache.put(group, Sockets::open(&live));
                assert_eq!(live.get(), 1);
            }
        }
        drop(cache);
        assert_eq!(live.get(), 0);
    }
}
