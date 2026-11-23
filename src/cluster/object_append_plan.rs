// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Allocation-free placement of a whole object append within one LOG chunk.

/// Where a complete request belongs. Record fragmentation happens inside this range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectAppendPlan {
    /// Use the current tail without publishing another metadata entry.
    CurrentTail { chunk_offset: u64, end: u64 },
    /// Seal the complete old tail and publish a fresh tail before sending this request.
    NewTail {
        sealed_length: u64,
        object_offset: u64,
        end: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectAppendPlanError {
    InvalidLimit,
    InvalidTail,
    InvalidRequest,
    OffsetMismatch,
    Overflow,
}

/// Keep one object request in one chunk, switching early when its full body will not fit.
/// `tail_length` must be the settled, published length, with no unresolved append.
/// The caller validates its writer lease and obtains a new chunk before acting on `NewTail`.
/// Planning alone supplies neither durable publication nor attempt settlement.
pub fn plan_object_append(
    tail_object_offset: u64,
    tail_length: u64,
    expected_object_offset: u64,
    request_length: u64,
    chunk_limit: u64,
) -> Result<ObjectAppendPlan, ObjectAppendPlanError> {
    use ObjectAppendPlanError::*;
    if chunk_limit == 0 {
        return Err(InvalidLimit);
    }
    if tail_length > chunk_limit {
        return Err(InvalidTail);
    }
    if request_length == 0 || request_length > chunk_limit {
        return Err(InvalidRequest);
    }
    let offset = tail_object_offset
        .checked_add(tail_length)
        .ok_or(Overflow)?;
    if offset != expected_object_offset {
        return Err(OffsetMismatch);
    }
    let end = offset.checked_add(request_length).ok_or(Overflow)?;
    // Subtraction is safe after checking the tail and avoids overflowing a physical sum.
    if request_length <= chunk_limit - tail_length {
        Ok(ObjectAppendPlan::CurrentTail {
            chunk_offset: tail_length,
            end,
        })
    } else {
        Ok(ObjectAppendPlan::NewTail {
            sealed_length: tail_length,
            object_offset: offset,
            end,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const LIMIT: u64 = 64 * 1024 * 1024;
    #[test]
    fn exact_fit_stays_in_current_tail() {
        assert_eq!(
            plan_object_append(100, LIMIT - 9, 100 + LIMIT - 9, 9, LIMIT),
            Ok(ObjectAppendPlan::CurrentTail {
                chunk_offset: LIMIT - 9,
                end: 100 + LIMIT
            })
        );
    }
    #[test]
    fn one_byte_over_remaining_space_switches_the_whole_request() {
        assert_eq!(
            plan_object_append(100, LIMIT - 9, 100 + LIMIT - 9, 10, LIMIT),
            Ok(ObjectAppendPlan::NewTail {
                sealed_length: LIMIT - 9,
                object_offset: 100 + LIMIT - 9,
                end: 100 + LIMIT + 1
            })
        );
    }
    #[test]
    fn maximum_body_fits_after_a_nonempty_tail_without_splitting() {
        assert_eq!(
            plan_object_append(0, 1, 1, LIMIT, LIMIT),
            Ok(ObjectAppendPlan::NewTail {
                sealed_length: 1,
                object_offset: 1,
                end: LIMIT + 1
            })
        );
    }
    #[test]
    fn full_tail_switches_and_empty_tail_does_not() {
        assert!(matches!(
            plan_object_append(0, LIMIT, LIMIT, 1, LIMIT),
            Ok(ObjectAppendPlan::NewTail { .. })
        ));
        assert!(matches!(
            plan_object_append(0, 0, 0, LIMIT, LIMIT),
            Ok(ObjectAppendPlan::CurrentTail {
                chunk_offset: 0,
                ..
            })
        ));
    }
    #[test]
    fn stale_offset_is_refused_before_placement() {
        assert_eq!(
            plan_object_append(100, 3, 102, 1, LIMIT),
            Err(ObjectAppendPlanError::OffsetMismatch)
        );
    }
    #[test]
    fn invalid_sizes_and_corrupt_tail_are_refused() {
        for n in [0, LIMIT + 1] {
            assert_eq!(
                plan_object_append(0, 0, 0, n, LIMIT),
                Err(ObjectAppendPlanError::InvalidRequest)
            );
        }
        assert_eq!(
            plan_object_append(0, LIMIT + 1, LIMIT + 1, 1, LIMIT),
            Err(ObjectAppendPlanError::InvalidTail)
        );
        assert_eq!(
            plan_object_append(0, 0, 0, 1, 0),
            Err(ObjectAppendPlanError::InvalidLimit)
        );
    }
    #[test]
    fn logical_overflow_is_refused_even_when_physical_space_exists() {
        assert_eq!(
            plan_object_append(u64::MAX, 1, 0, 1, LIMIT),
            Err(ObjectAppendPlanError::Overflow)
        );
        assert_eq!(
            plan_object_append(u64::MAX - 1, 0, u64::MAX - 1, 2, LIMIT),
            Err(ObjectAppendPlanError::Overflow)
        );
    }
    #[test]
    fn physical_arithmetic_does_not_wrap_at_large_limits() {
        assert!(matches!(
            plan_object_append(0, u64::MAX - 1, u64::MAX - 1, 1, u64::MAX),
            Ok(ObjectAppendPlan::CurrentTail { end: u64::MAX, .. })
        ));
    }
    #[test]
    fn every_small_capacity_and_request_preserves_a_whole_contiguous_range() {
        for limit in 1..=64 {
            for tail in 0..=limit {
                for length in 1..=limit {
                    let p = plan_object_append(17, tail, 17 + tail, length, limit).unwrap();
                    match p {
                        ObjectAppendPlan::CurrentTail { chunk_offset, end } => {
                            assert_eq!(chunk_offset, tail);
                            assert!(chunk_offset + length <= limit);
                            assert_eq!(end, 17 + tail + length);
                        }
                        ObjectAppendPlan::NewTail {
                            sealed_length,
                            object_offset,
                            end,
                        } => {
                            assert_eq!(sealed_length, tail);
                            assert_eq!(object_offset, 17 + tail);
                            assert_eq!(end - object_offset, length);
                            assert!(tail + length > limit);
                        }
                    }
                }
            }
        }
    }
}
