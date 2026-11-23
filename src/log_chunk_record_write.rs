// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Borrowed record I/O. The normal file write uses the original two buffers.
use std::io::{self, IoSlice, Write};

pub(super) fn write_record_parts(
    writer: &mut impl Write,
    mut header: &[u8],
    mut data: &[u8],
) -> io::Result<()> {
    while !header.is_empty() || !data.is_empty() {
        let parts = [IoSlice::new(header), IoSlice::new(data)];
        match writer.write_vectored(&parts) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                let header_bytes = written.min(header.len());
                header = &header[header_bytes..];
                data = &data[written - header_bytes..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Scripted {
        steps: VecDeque<io::Result<usize>>,
        bytes: Vec<u8>,
        calls: usize,
    }
    impl Scripted {
        fn new(steps: impl IntoIterator<Item = io::Result<usize>>) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                bytes: Vec::new(),
                calls: 0,
            }
        }
    }
    impl Write for Scripted {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            panic!("scalar write was used")
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn write_vectored(&mut self, parts: &[IoSlice<'_>]) -> io::Result<usize> {
            self.calls += 1;
            let n = self.steps.pop_front().expect("unexpected write call")?;
            assert!(n <= parts.iter().map(|p| p.len()).sum());
            self.bytes
                .extend(parts.iter().flat_map(|p| p.iter()).take(n));
            Ok(n)
        }
    }
    #[test]
    fn full_record_uses_one_vectored_call_with_the_original_buffers() {
        let header = [1; 64];
        let data = [2; 4096];
        struct BorrowCheck<'a> {
            header: &'a [u8],
            data: &'a [u8],
            calls: usize,
        }
        impl Write for BorrowCheck<'_> {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                panic!("scalar write")
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
            fn write_vectored(&mut self, p: &[IoSlice<'_>]) -> io::Result<usize> {
                self.calls += 1;
                assert_eq!(p.len(), 2);
                assert_eq!(
                    (p[0].as_ptr(), p[0].len()),
                    (self.header.as_ptr(), self.header.len())
                );
                assert_eq!(
                    (p[1].as_ptr(), p[1].len()),
                    (self.data.as_ptr(), self.data.len())
                );
                Ok(p[0].len() + p[1].len())
            }
        }
        let mut w = BorrowCheck {
            header: &header,
            data: &data,
            calls: 0,
        };
        write_record_parts(&mut w, &header, &data).unwrap();
        assert_eq!(w.calls, 1);
    }
    #[test]
    fn every_short_write_width_preserves_the_header_body_boundary() {
        let h = [1; 64];
        let d = [2; 137];
        let expected = [h.as_slice(), d.as_slice()].concat();
        for width in 1..=expected.len() {
            let mut remain = expected.len();
            let mut steps = Vec::new();
            while remain != 0 {
                let n = width.min(remain);
                steps.push(Ok(n));
                remain -= n;
            }
            let mut w = Scripted::new(steps);
            write_record_parts(&mut w, &h, &d).unwrap();
            assert_eq!(w.bytes, expected);
            assert!(w.steps.is_empty());
        }
    }
    #[test]
    fn interruptions_before_and_after_partial_progress_retry_only_remaining_bytes() {
        let mut w = Scripted::new([
            Err(io::ErrorKind::Interrupted.into()),
            Ok(2),
            Err(io::ErrorKind::Interrupted.into()),
            Ok(3),
            Err(io::ErrorKind::Interrupted.into()),
            Ok(2),
        ]);
        write_record_parts(&mut w, b"abc", b"defg").unwrap();
        assert_eq!(w.bytes, b"abcdefg");
        assert_eq!(w.calls, 6);
    }
    #[test]
    fn zero_progress_is_terminal_before_or_after_a_partial_record() {
        for n in [0, 2, 4] {
            let mut steps = Vec::new();
            if n != 0 {
                steps.push(Ok(n));
            }
            steps.push(Ok(0));
            let mut w = Scripted::new(steps);
            let e = write_record_parts(&mut w, b"abc", b"def").unwrap_err();
            assert_eq!(e.kind(), io::ErrorKind::WriteZero);
            assert_eq!(w.bytes, &b"abcdef"[..n]);
            assert!(w.steps.is_empty());
        }
    }
    #[test]
    fn a_non_interrupted_error_is_returned_without_more_io() {
        let mut w = Scripted::new([Ok(4), Err(io::Error::from_raw_os_error(5))]);
        let e = write_record_parts(&mut w, b"abc", b"def").unwrap_err();
        assert_eq!(e.raw_os_error(), Some(5));
        assert_eq!(w.bytes, b"abcd");
        assert_eq!(w.calls, 2);
    }
    #[test]
    fn empty_parts_and_scalar_writer_fallback_are_supported() {
        for (h, d) in [
            (b"".as_slice(), b"".as_slice()),
            (b"abc", b""),
            (b"", b"def"),
        ] {
            let mut v = Vec::new();
            write_record_parts(&mut v, h, d).unwrap();
            assert_eq!(v, [h, d].concat());
        }
        struct Scalar(Vec<u8>);
        impl Write for Scalar {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                let n = b.len().min(2);
                self.0.extend_from_slice(&b[..n]);
                Ok(n)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut w = Scalar(Vec::new());
        write_record_parts(&mut w, b"abc", b"def").unwrap();
        assert_eq!(w.0, b"abcdef");
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn an_actual_record_write_failure_poisons_the_chunk_handle() {
        use super::super::LogChunk;
        let path =
            std::env::temp_dir().join(format!("record-write-failure-{}", std::process::id()));
        let mut chunk = LogChunk::open(&path, 7, 1024).unwrap();
        chunk.file = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/full")
            .unwrap();
        assert!(chunk.append(1, 0, b"abc").is_err());
        assert!(chunk.failed);
        assert_eq!(chunk.length(), 0);
        assert_eq!(chunk.file_len, 0);
        assert!(chunk.append(1, 0, b"abc").is_err());
        assert!(chunk.commit().is_err());
        drop(chunk);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        std::fs::remove_file(path).unwrap();
    }
}
