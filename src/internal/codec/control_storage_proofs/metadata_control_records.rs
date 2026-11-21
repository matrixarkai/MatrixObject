// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn change_checkpoint_key(consumer_id: &str, prefix: &str) -> String {
    format!("{consumer_id}\n{prefix}")
}

