// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn validate_page_allocation_batch_lengths(
    lengths: &[u64],
    allocator_chunk_size: u64,
) -> Result<(), ObjectError> {
    if lengths.is_empty() || lengths.len() > 100_000 {
        return Err(ObjectError::InvalidArgument(
            "page batch allocation range_count must be 1..100000".to_string(),
        ));
    }
    for length in lengths {
        if *length == 0 {
            return Err(ObjectError::InvalidArgument(
                "page batch allocation length must be positive".to_string(),
            ));
        }
        if *length > allocator_chunk_size {
            return Err(ObjectError::InvalidArgument(
                "page batch allocation length exceeds allocator chunk size".to_string(),
            ));
        }
    }
    Ok(())
}

