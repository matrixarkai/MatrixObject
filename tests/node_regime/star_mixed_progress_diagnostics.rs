// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded optional completion counters and failure-time resource observations.
use std::{
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

pub(super) struct Progress {
    completed: Vec<[AtomicU64; 4]>,
}
impl Progress {
    pub(super) fn enabled(clients: usize) -> Option<Arc<Self>> {
        std::env::var_os("MATRIXOBJECT_TEST_MIXED_PROGRESS")?;
        assert!((1..=32).contains(&clients));
        Some(Arc::new(Self {
            completed: (0..clients)
                .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
                .collect(),
        }))
    }
    pub(super) fn record(&self, client: usize, kind: usize) {
        self.completed[client][kind].fetch_add(1, Ordering::Relaxed);
    }
    fn report(&self) {
        // These independent counters are a concurrent observation, not a latency
        // distribution or an assertion that the complete workload succeeded.
        for (client, counts) in self.completed.iter().enumerate() {
            let counts = counts.each_ref().map(|value| value.load(Ordering::Relaxed));
            println!(
                "STAR_MIXED_PROGRESS client={client} read_ops={} write_ops={} append_ops={} setup_ops={}",
                counts[0], counts[1], counts[2], counts[3]
            );
        }
    }
}

pub(super) fn report_failure(
    progress: Option<&Progress>,
    completed_clients: usize,
    clients: usize,
    elapsed: Duration,
    reason: &std::sync::mpsc::RecvTimeoutError,
) {
    println!(
        "STAR_MIXED_COMPLETION_FAILURE completed_clients={completed_clients} clients={clients} elapsed_us={} reason={reason:?}",
        elapsed.as_micros()
    );
    if let Some(progress) = progress {
        progress.report();
    }
    if let Ok(cgroups) = std::fs::read_to_string("/proc/self/cgroup") {
        if let Some(relative) = cgroups.lines().find_map(|line| line.strip_prefix("0::")) {
            let scope =
                std::path::Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
            for name in [
                "memory.current",
                "memory.peak",
                "memory.max",
                "memory.swap.max",
                "memory.events",
            ] {
                if let Ok(value) = std::fs::read_to_string(scope.join(name)) {
                    let value = value.split_whitespace().collect::<Vec<_>>().join(",");
                    println!("STAR_MIXED_RESOURCE name={name} value={value}");
                }
            }
        }
    }
    let _ = std::io::stdout().flush();
}
