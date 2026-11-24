// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn ranges_intersect(a_offset: u64, a_length: u64, b_offset: u64, b_length: u64) -> bool {
    if a_length == 0 || b_length == 0 {
        return false;
    }
    let Some(a_end) = a_offset.checked_add(a_length) else {
        return false;
    };
    let Some(b_end) = b_offset.checked_add(b_length) else {
        return false;
    };
    a_offset < b_end && b_offset < a_end
}
