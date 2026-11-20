// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Idle journal maintenance does not depend on API traffic or metadata reports.
use super::{NodeReplica, ServerShutdown};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

pub(super) fn run(replica: Arc<Mutex<NodeReplica>>, stop: Arc<ServerShutdown>) {
    while !stop.is_stopped() {
        if let Ok(mut state) = replica.try_lock() {
            if !state.health.failed() {
                let staged = state.staged.is_some();
                let NodeReplica {
                    service,
                    journal,
                    journal_checkpoint_after_bytes,
                    ..
                } = &mut *state;
                if let Some(journal) = journal {
                    let result = if staged {
                        journal.check_health()
                    } else {
                        journal.poll_background(service, *journal_checkpoint_after_bytes)
                    };
                    if result.is_err() {
                        state.health.fail();
                    }
                }
            }
        }
        if stop.wait_for(Duration::from_millis(100)) {
            break;
        }
    }
    if let Ok(mut state) = replica.try_lock() {
        if let Some(journal) = &mut state.journal {
            let _ = journal.cancel_background();
        }
    }
}
