// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The opt-in zero-filled layout of the durable logs: one setting, one environment variable.
//!
//! Unset, every log keeps the layout it has always had. Set to a number of bytes, a log that a
//! node creates, or that a checkpoint replaces, is a file filled with zeros ahead of the log in
//! chunks of at least that size, so an append overwrites space that already exists and does not
//! grow the file. The file is then longer than the log. A log written without the setting is read
//! as before and keeps its layout until a checkpoint replaces it.

/// The environment variable the server binaries read.
pub const WAL_ZERO_FILL_ENV: &str = "MATRIXOBJECT_WAL_ZERO_FILL_BYTES";
/// Smallest and largest chunk a node accepts.
pub const WAL_ZERO_FILL_MIN_BYTES: u64 = 4096;
pub const WAL_ZERO_FILL_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Refuse a chunk outside 4 KiB through 512 MiB. `None` is the original layout.
pub fn validate_wal_zero_fill_bytes(value: Option<u64>) -> Result<Option<u64>, String> {
    match value {
        Some(bytes) if !(WAL_ZERO_FILL_MIN_BYTES..=WAL_ZERO_FILL_MAX_BYTES).contains(&bytes) => {
            Err(format!(
                "{WAL_ZERO_FILL_ENV} must be between {WAL_ZERO_FILL_MIN_BYTES} and \
                 {WAL_ZERO_FILL_MAX_BYTES} bytes, not {bytes}"
            ))
        }
        other => Ok(other),
    }
}

/// Read [`WAL_ZERO_FILL_ENV`]: unset or empty is `None`; anything else must be a byte count in
/// range, so a typo is refused at startup rather than silently ignored.
pub fn wal_zero_fill_from_env() -> Result<Option<u64>, String> {
    match std::env::var(WAL_ZERO_FILL_ENV) {
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(format!("{WAL_ZERO_FILL_ENV} must be valid text")),
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => {
            let bytes = raw
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("{WAL_ZERO_FILL_ENV} must be a number of bytes"))?;
            validate_wal_zero_fill_bytes(Some(bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bounds_are_inclusive_and_none_is_the_original_layout() {
        assert_eq!(validate_wal_zero_fill_bytes(None), Ok(None));
        for bytes in [4096, 1 << 20, 512 << 20] {
            assert_eq!(validate_wal_zero_fill_bytes(Some(bytes)), Ok(Some(bytes)));
        }
        for bytes in [0, 1, 4095, (512 << 20) + 1, u64::MAX] {
            assert!(validate_wal_zero_fill_bytes(Some(bytes)).is_err(), "{bytes}");
        }
    }
}
