// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

struct Fragmented<'a> {
    bytes: &'a [u8],
}
impl Read for Fragmented<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = output.len().min(31).min(self.bytes.len());
        output[..count].copy_from_slice(&self.bytes[..count]);
        self.bytes = &self.bytes[count..];
        Ok(count)
    }
}
#[derive(Default)]
struct Sink {
    bytes: Vec<u8>,
    calls: usize,
    short: bool,
    fail_write: bool,
    fail_flush: bool,
    interrupted: bool,
}
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        if self.fail_write {
            return Err(io::Error::other("checkpoint write failure"));
        }
        if self.interrupted {
            self.interrupted = false;
            return Err(io::ErrorKind::Interrupted.into());
        }
        let count = if self.short {
            bytes.len().min(7)
        } else {
            bytes.len()
        };
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            Err(io::Error::other("checkpoint flush failure"))
        } else {
            Ok(())
        }
    }
}
#[test]
fn journal_checkpoint_buffer_preserves_fragmented_bytes_and_checksum() {
    let bytes = (0..1024 * 1024 + 17)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    for short in [false, true] {
        let mut sink = Sink {
            short,
            interrupted: true,
            ..Default::default()
        };
        let checksum = write_checkpoint_stream(
            &mut Fragmented { bytes: &bytes },
            bytes.len() as u64,
            &mut sink,
        )
        .unwrap();
        assert_eq!(sink.bytes, bytes);
        assert_eq!(checksum, fnv1a64(&bytes));
        if !short {
            assert!(
                sink.calls < 32,
                "fragmented writes were not coalesced: {}",
                sink.calls
            );
        }
    }
}
#[test]
fn journal_checkpoint_buffer_final_write_and_flush_errors_propagate() {
    // Everything fits in the buffer: only an explicit final flush exposes these errors.
    let bytes = [0x5a; 1024];
    for fail_write in [false, true] {
        let mut sink = Sink {
            fail_write,
            fail_flush: !fail_write,
            ..Default::default()
        };
        let error = write_checkpoint_stream(&mut bytes.as_slice(), bytes.len() as u64, &mut sink)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(
            error.to_string(),
            if fail_write {
                "checkpoint write failure"
            } else {
                "checkpoint flush failure"
            }
        );
    }
}
#[test]
fn journal_checkpoint_buffer_rejects_a_changed_stream_length() {
    for expected in [3, 5] {
        let mut sink = Sink::default();
        let error =
            write_checkpoint_stream(&mut b"data".as_slice(), expected, &mut sink).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
