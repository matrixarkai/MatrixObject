// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Translate physical payload ranges while preserving logical page offsets.
use super::replica_checkpoint_spans::PayloadSpan;
use crate::durable_payload::PayloadExtent;
use std::sync::Arc;

pub(super) struct PayloadMovement {
    pub start: u64,
    pub end: u64,
    pub extent: Arc<PayloadExtent>,
    pub offset: u64,
}
pub(super) fn push_span(spans: &mut Vec<PayloadSpan>, span: PayloadSpan) {
    if let Some(last) = spans.last_mut() {
        if last.offset + last.length == span.offset
            && last.extent_offset + last.length == span.extent_offset
            && Arc::ptr_eq(&last.extent, &span.extent)
        {
            last.length += span.length;
            return;
        }
    }
    spans.push(span);
}
pub(super) fn translate(
    span: &PayloadSpan,
    moved: &[PayloadMovement],
    spans: &mut Vec<PayloadSpan>,
) -> bool {
    let end = span.extent_offset + span.length;
    let mut at = span.extent_offset;
    let mut changed = false;
    let old = |start: u64, end: u64| PayloadSpan {
        offset: span.offset + start - span.extent_offset,
        length: end - start,
        extent: span.extent.clone(),
        extent_offset: start,
    };
    let index = moved.partition_point(|movement| movement.end <= at);
    for movement in &moved[index..] {
        if movement.start >= end {
            break;
        }
        if movement.start > at {
            push_span(spans, old(at, movement.start.min(end)));
        }
        let start = at.max(movement.start);
        let finish = end.min(movement.end);
        if start < finish {
            push_span(
                spans,
                PayloadSpan {
                    offset: span.offset + start - span.extent_offset,
                    length: finish - start,
                    extent: movement.extent.clone(),
                    extent_offset: movement.offset + start - movement.start,
                },
            );
            at = finish;
            changed = true;
        }
    }
    if at < end {
        push_span(spans, old(at, end));
    }
    changed
}
