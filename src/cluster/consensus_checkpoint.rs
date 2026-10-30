// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Application checkpoint envelopes and opt-in WAL reclamation.
use crate::ObjectError;
use matrixraft::{
    peer_runtime::{DurablePeer, MAX_CHECKPOINT_BYTES},
    LogId, RaftError,
};
use prost::Message;
#[cfg(test)]
use std::io::Cursor;
use std::{
    io::{self, Read},
    path::PathBuf,
};

#[derive(Clone, PartialEq, Message)]
pub(super) struct Image {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint32, tag = "2")]
    kind: u32,
    #[prost(uint64, tag = "3")]
    index: u64,
    #[prost(uint64, tag = "4")]
    term: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub state: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    pub catalog: Vec<u8>,
}
pub(super) fn storage(error: impl ToString) -> RaftError {
    RaftError::Storage(error.to_string())
}
impl Image {
    pub(super) fn new(kind: u32, id: LogId, state: Vec<u8>, catalog: Vec<u8>) -> Self {
        Self {
            version: 1,
            kind,
            index: id.index,
            term: id.term,
            state,
            catalog,
        }
    }
    /// Emits the canonical protobuf envelope without copying either body.
    #[cfg(test)]
    fn into_reader(mut self) -> impl Read {
        let state = std::mem::take(&mut self.state);
        let catalog = std::mem::take(&mut self.catalog);
        let mut prefix = self.encode_to_vec();
        prefix.push(42);
        prost::encoding::encode_varint(state.len() as u64, &mut prefix);
        let mut suffix = Vec::new();
        if !catalog.is_empty() {
            suffix.push(50);
            prost::encoding::encode_varint(catalog.len() as u64, &mut suffix);
        }
        Cursor::new(prefix)
            .chain(Cursor::new(state))
            .chain(Cursor::new(suffix))
            .chain(Cursor::new(catalog))
    }

    pub(super) fn read_at(
        reader: &mut dyn Read,
        length: u64,
        kind: u32,
        id: LogId,
    ) -> Result<Self, RaftError> {
        if length > MAX_CHECKPOINT_BYTES as u64 {
            return Err(storage("application checkpoint exceeds size bound"));
        }
        let mut reader = reader.take(length);
        // The schema is canonical: every identity field is present, ordered and
        // minimally encoded. Validate it before allocating application bodies.
        for (tag, value) in [(8, 1), (16, kind as u64), (24, id.index), (32, id.term)] {
            if byte(&mut reader)? != tag || varint(&mut reader)? != value || value == 0 {
                return Err(storage(
                    "application checkpoint identity or encoding mismatch",
                ));
            }
        }
        let state = body(&mut reader, 42)?;
        if state.is_empty() {
            return Err(storage("application checkpoint state is empty"));
        }
        let catalog = if reader.limit() != 0 {
            let bytes = body(&mut reader, 50)?;
            if bytes.is_empty() {
                return Err(storage("application checkpoint catalog is not canonical"));
            }
            bytes
        } else {
            Vec::new()
        };
        if reader.limit() != 0 {
            return Err(storage("application checkpoint has trailing fields"));
        }
        Ok(Self::new(kind, id, state, catalog))
    }

    #[cfg(test)]
    pub(super) fn decode_at(bytes: &[u8], kind: u32, id: LogId) -> Result<Self, RaftError> {
        Self::read_at(&mut Cursor::new(bytes), bytes.len() as u64, kind, id)
    }
}
pub(super) fn byte(reader: &mut dyn Read) -> Result<u8, RaftError> {
    let mut value = [0];
    reader.read_exact(&mut value).map_err(storage)?;
    Ok(value[0])
}
pub(super) fn varint(reader: &mut dyn Read) -> Result<u64, RaftError> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let next = byte(reader)?;
        if shift == 63 && next > 1 {
            return Err(storage("application checkpoint integer overflow"));
        }
        value |= u64::from(next & 127) << shift;
        if next < 128 {
            if shift != 0 && next == 0 {
                return Err(storage("application checkpoint integer is not canonical"));
            }
            return Ok(value);
        }
    }
    Err(storage("application checkpoint integer overflow"))
}
fn body(reader: &mut io::Take<&mut dyn Read>, tag: u8) -> Result<Vec<u8>, RaftError> {
    if byte(reader)? != tag {
        return Err(storage("application checkpoint field order mismatch"));
    }
    let length = varint(reader)?;
    if length > reader.limit() {
        return Err(storage(
            "application checkpoint field exceeds remaining bytes",
        ));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length as usize).map_err(storage)?;
    bytes.resize(length as usize, 0);
    reader.read_exact(&mut bytes).map_err(storage)?;
    Ok(bytes)
}
type CaptureWorker = std::thread::JoinHandle<
    Result<Option<matrixraft::peer_runtime::PreparedCheckpoint>, RaftError>,
>;
pub(super) struct Policy {
    worker: Option<(CaptureWorker, u64)>,
    kind: u32,
    wal: PathBuf,
    interval: u64,
    baseline: u64,
    observed: u64,
    checkpoint: u64,
}
impl Policy {
    pub(super) fn new(kind: u32, wal: PathBuf, interval: u64) -> Result<Self, ObjectError> {
        if interval != 0 && !(1024..=128 * 1024 * 1024).contains(&interval) {
            return Err(ObjectError::InvalidArgument(
                "checkpoint byte interval must be zero or between 1 KiB and 128 MiB".into(),
            ));
        }
        Ok(Self {
            worker: None,
            kind,
            wal,
            interval,
            baseline: 0,
            observed: 0,
            checkpoint: 0,
        })
    }
    pub(super) fn restored(&mut self, index: u64) {
        self.checkpoint = index;
        self.observed = index;
        // A reopened WAL may already contain a large suffix. Reclaim it on the
        // next applied advance rather than treating its total size as new space.
        self.baseline = 0;
    }
    fn due(&mut self, peer: &DurablePeer) -> Result<Option<(u64, u64)>, RaftError> {
        let index = peer.applied_index();
        let membership_due = peer.membership_checkpoint_needed();
        if (!membership_due && (self.interval == 0 || index <= self.observed))
            || index <= self.checkpoint
        {
            return Ok(None);
        }
        self.observed = index;
        let size = std::fs::metadata(&self.wal).map_err(storage)?.len();
        if !membership_due && size.saturating_sub(self.baseline) < self.interval {
            return Ok(None);
        }
        Ok(Some((index, size)))
    }
    pub(super) fn maybe_checkpoint_stream<R, F>(
        &mut self,
        peer: &mut DurablePeer,
        term: u64,
        snapshot: impl FnOnce(LogId) -> Result<F, RaftError>,
    ) -> Result<(), RaftError>
    where
        R: Read,
        F: FnOnce() -> Result<Option<(R, u64)>, RaftError> + Send + 'static,
    {
        if self
            .worker
            .as_ref()
            .is_some_and(|(worker, _)| worker.is_finished())
        {
            let (worker, size) = self.worker.take().unwrap();
            match worker
                .join()
                .map_err(|_| storage("checkpoint worker panicked"))??
            {
                Some(prepared) => {
                    let index = prepared.position().index;
                    if peer.install_checkpoint(&prepared)? {
                        self.checkpoint = index;
                        // Retained writes accumulated during capture must count
                        // toward the next interval rather than disappearing into
                        // the baseline of the compacted WAL.
                        self.baseline = 0;
                    }
                }
                None => {
                    self.baseline = size;
                    eprintln!("application checkpoint exceeds supported size; retaining WAL history (kind={})", self.kind);
                }
            }
        }
        if self.worker.is_some() {
            return Ok(());
        }
        let Some((index, size)) = self.due(peer)? else {
            return Ok(());
        };
        let ticket = peer.prepare_checkpoint()?;
        if ticket.position() != (LogId { index, term }) {
            return Err(storage("checkpoint capture position mismatch"));
        }
        // Export an immutable snapshot on the application thread. Encoding,
        // validation, checksum scans and payload sync run on one worker.
        let capture = snapshot(ticket.position())?;
        let worker = std::thread::Builder::new()
            .name("node-checkpoint".into())
            .spawn(move || {
                let Some((mut reader, length)) = capture()? else {
                    return Ok(None);
                };
                ticket.capture(&mut reader, length).map(Some)
            })
            .map_err(storage)?;
        self.worker = Some((worker, size));
        Ok(())
    }
}
impl Drop for Policy {
    fn drop(&mut self) {
        // Finish before releasing storage ownership; an unpublished result
        // removes its payload when the joined result is dropped.
        if let Some((worker, _)) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_capture_keeps_both_node_policies_writable_and_recovers_suffix() {
        use matrixraft::peer_runtime::PeerOptions;
        use std::{
            cell::Cell,
            sync::mpsc,
            time::{Duration, Instant},
        };
        type TestCapture = fn() -> Result<Option<(Cursor<Vec<u8>>, u64)>, RaftError>;
        for kind in [1, 2] {
            let root = std::env::temp_dir().join(format!(
                "node-capture-{}-{}-{kind}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&root).unwrap();
            struct Cleanup(PathBuf);
            impl Drop for Cleanup {
                fn drop(&mut self) {
                    let _ = std::fs::remove_dir_all(&self.0);
                }
            }
            let _cleanup = Cleanup(root.clone());
            let options = PeerOptions {
                group_id: 7,
                node_id: 1,
                voters: vec![1],
                wal_path: root.join("peer.wal"),
            };
            let mut peer = DurablePeer::open(options.clone()).unwrap();
            peer.campaign().unwrap();
            peer.process_ready(|_, _| Ok(())).unwrap();
            peer.propose(vec![7; 2048]).unwrap();
            peer.process_ready(|_, _| Ok(())).unwrap();
            let mut policy = Policy::new(kind, options.wal_path.clone(), 1024).unwrap();
            let (started_tx, started_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let term = peer.term();
            policy
                .maybe_checkpoint_stream(&mut peer, term, move |id| {
                    Ok(move || {
                        started_tx.send(()).unwrap();
                        resume_rx.recv().unwrap();
                        let bytes = Image::new(kind, id, vec![7], vec![]).encode_to_vec();
                        let length = bytes.len() as u64;
                        Ok(Some((Cursor::new(bytes), length)))
                    })
                })
                .unwrap();
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            peer.propose(vec![9]).unwrap();
            peer.process_ready(|_, bytes| {
                assert_eq!(bytes, [9]);
                Ok(())
            })
            .unwrap();
            let later = peer.applied_index();
            policy
                .maybe_checkpoint_stream(&mut peer, term, |_| -> Result<TestCapture, RaftError> {
                    panic!("second worker started")
                })
                .unwrap();
            resume_tx.send(()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !policy.worker.as_ref().unwrap().0.is_finished() {
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(1));
            }
            policy
                .maybe_checkpoint_stream(&mut peer, term, |_| -> Result<TestCapture, RaftError> {
                    panic!("unnecessary capture")
                })
                .unwrap();
            assert!(policy.worker.is_none());
            assert!(policy.checkpoint < later);
            drop(policy);
            drop(peer);
            let mut peer = DurablePeer::open(options).unwrap();
            let value = Cell::new(0);
            peer.process_ready_with_snapshot_reader(
                |_, bytes| {
                    if !bytes.is_empty() {
                        assert_eq!(value.get(), 7);
                        value.set(bytes[0]);
                    }
                    Ok(())
                },
                |id, reader, length| {
                    let image = Image::read_at(reader, length, kind, id)?;
                    value.set(image.state[0]);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(value.get(), 9);
            assert_eq!(peer.applied_index(), later);
        }
    }
    #[test]
    fn checkpoint_envelope_binds_kind_position_and_canonical_bytes() {
        let image = Image {
            version: 1,
            kind: 1,
            index: 9,
            term: 3,
            state: vec![1, 2, 3],
            catalog: vec![4],
        };
        let bytes = image.encode_to_vec();
        Image::decode_at(&bytes, 1, LogId { index: 9, term: 3 }).unwrap();
        assert!(Image::decode_at(&bytes, 2, LogId { index: 9, term: 3 }).is_err());
        assert!(Image::decode_at(&bytes, 1, LogId { index: 10, term: 3 }).is_err());
        assert!(Image::decode_at(&bytes, 1, LogId { index: 9, term: 4 }).is_err());
        let mut unknown = bytes.clone();
        unknown.extend([0x78, 1]);
        assert!(Image::decode_at(&unknown, 1, LogId { index: 9, term: 3 }).is_err());
        for cut in 0..bytes.len() {
            if Image::decode_at(&bytes[..cut], 1, LogId { index: 9, term: 3 }).is_ok() {
                // A catalog is optional in the envelope; its service-specific
                // equality check is performed before installing metadata state.
                let parsed = Image::decode(&bytes[..cut]).unwrap();
                assert!(parsed.catalog.is_empty());
            }
        }
    }
    #[test]
    fn checkpoint_readers_preserve_protobuf_bytes_and_short_reads() {
        struct Short<R>(R);
        impl<R: Read> Read for Short<R> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                let len = out.len().min(3);
                self.0.read(&mut out[..len])
            }
        }
        for kind in [1, 2] {
            for catalog in [vec![], vec![7; 300]] {
                let id = LogId {
                    index: 300,
                    term: 128,
                };
                let image = Image::new(kind, id.clone(), vec![9; 65537], catalog);
                let canonical = image.encode_to_vec();
                let mut encoded = Vec::new();
                image
                    .clone()
                    .into_reader()
                    .read_to_end(&mut encoded)
                    .unwrap();
                assert_eq!(encoded, canonical);
                let mut input = Short(Cursor::new(&canonical));
                assert_eq!(
                    Image::read_at(&mut input, canonical.len() as u64, kind, id).unwrap(),
                    image
                );
            }
        }
    }

    #[test]
    fn checkpoint_reader_refuses_malformed_lengths_and_noncanonical_fields() {
        let id = LogId { index: 9, term: 3 };
        let good = Image::new(1, id.clone(), vec![1, 2, 3], vec![4]).encode_to_vec();
        for offset in 0..good.len() {
            for value in 0..=255 {
                let mut bytes = good.clone();
                bytes[offset] = value;
                if let Ok(image) = Image::decode_at(&bytes, 1, id.clone()) {
                    assert_eq!(image.encode_to_vec(), bytes);
                    assert_eq!(image.index, 9);
                    assert_eq!(image.term, 3);
                    assert!(!image.state.is_empty());
                }
            }
        }
        let mut overlong = good.clone();
        overlong.splice(1..2, [0x81, 0]);
        assert!(Image::decode_at(&overlong, 1, id.clone()).is_err());
        let mut overflow = good.clone();
        overflow.splice(9..10, [0xff; 10]);
        assert!(Image::decode_at(&overflow, 1, id.clone()).is_err());
        for length in [0, good.len() as u64 - 1, good.len() as u64 + 1, u64::MAX] {
            assert!(Image::read_at(&mut Cursor::new(&good), length, 1, id.clone()).is_err());
        }
        let mut empty_catalog = Image::new(1, id.clone(), vec![1], vec![]).encode_to_vec();
        empty_catalog.extend([50, 0]);
        assert!(Image::decode_at(&empty_catalog, 1, id.clone()).is_err());
        let mut joined = good.clone();
        joined.extend(b"next record");
        let mut reader = Cursor::new(joined);
        Image::read_at(&mut reader, good.len() as u64, 1, id).unwrap();
        assert_eq!(reader.position(), good.len() as u64);
    }

    #[test]
    fn checkpoint_policy_bounds_the_configured_interval() {
        for value in [0, 1024, 128 * 1024 * 1024] {
            Policy::new(1, PathBuf::new(), value).unwrap();
        }
        for value in [1, 1023, 128 * 1024 * 1024 + 1, u64::MAX] {
            assert!(Policy::new(1, PathBuf::new(), value).is_err());
        }
    }
}
