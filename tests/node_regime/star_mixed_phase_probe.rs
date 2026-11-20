// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Optional external observation gates, outside the measured request loop.
use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

pub(super) struct Probe {
    done: PathBuf,
}
fn wait(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.is_file() {
        assert!(
            Instant::now() < deadline,
            "mixed phase observer did not acknowledge"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
impl Probe {
    pub(super) fn prepare(round: usize, position: usize, corpus: usize) -> Option<Self> {
        let prefix = std::env::var_os("MATRIXOBJECT_TEST_MIXED_PHASE_CONTROL")?;
        let prefix = PathBuf::from(prefix);
        let ready = prefix.with_extension("ready");
        let done = prefix.with_extension("done");
        assert!(
            !ready.exists() && !done.exists(),
            "mixed phase observer files already exist"
        );
        println!(
            "STAR_MIXED_PHASE_PREPARE round={round} position={position} background_pages={corpus}"
        );
        std::io::stdout().flush().unwrap();
        wait(&ready);
        Some(Self { done })
    }
    pub(super) fn finish(self, request_us: u128) {
        println!("STAR_MIXED_PHASE_END request_us={request_us}");
        std::io::stdout().flush().unwrap();
        wait(&self.done);
    }
}
