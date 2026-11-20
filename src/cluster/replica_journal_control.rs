// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded standalone journal maintenance and progress. No payloads or paths.
use crate::ObjectError;
use prost::Message;
fn invalid() -> ObjectError {
    ObjectError::Corruption("invalid replica journal control message".into())
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ReplicaJournalControl {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub action: u32,
}
impl ReplicaJournalControl {
    pub const STATUS: u32 = 1;
    pub const CHECKPOINT: u32 = 2;
    pub fn validate(&self) -> Result<(), ObjectError> {
        if ![1, 2, 3].contains(&self.version)
            || !matches!(self.action, Self::STATUS | Self::CHECKPOINT)
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 32 {
            return Err(invalid());
        }
        let value = Self::decode(bytes).map_err(|_| invalid())?;
        value.validate()?;
        if value.encode_to_vec() != bytes {
            return Err(invalid());
        }
        Ok(value)
    }
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ReplicaJournalStatus {
    #[prost(bytes = "vec", tag = "10")]
    pub journal_id: Vec<u8>,
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub layout: u32,
    #[prost(uint64, tag = "3")]
    pub started_epoch: u64,
    #[prost(uint64, tag = "4")]
    pub published_epoch: u64,
    #[prost(bool, tag = "5")]
    pub checkpoint_in_flight: bool,
    #[prost(uint32, tag = "6")]
    pub pending_records: u32,
    #[prost(uint64, tag = "7")]
    pub bytes_since_checkpoint: u64,
    #[prost(uint64, tag = "8")]
    pub journal_bytes: u64,
    #[prost(uint64, tag = "9")]
    pub segments: u64,
    /// Managed retired files still awaiting verified cleanup; version 2 or newer.
    #[prost(uint64, tag = "11")]
    pub retired_segments: u64,
    /// Committed record bytes associated with pending retirement; version 2 or newer.
    #[prost(uint64, tag = "12")]
    pub retired_journal_bytes: u64,
    /// Background analysis, physical cleanup, or retirement publication is pending.
    #[prost(bool, tag = "13")]
    pub retirement_in_flight: bool,
    /// Confirmed record traffic for one open segmented journal; version 3 only.
    #[prost(message, optional, tag = "14")]
    pub write_statistics: Option<crate::durable_payload::outcome_segments::OutcomeWriteStatistics>,
}
impl ReplicaJournalStatus {
    /// Compare a checkpoint reply with a later observation of the same journal.
    /// A legacy journal gets a new identity after reopen; segmented identity persists.
    pub fn checkpoint_is_published(&self, requested: &Self) -> Result<bool, ObjectError> {
        self.validate()?;
        requested.validate()?;
        if self.journal_id != requested.journal_id || self.layout != requested.layout {
            return Err(ObjectError::PreconditionFailed(
                "replica journal changed while waiting for checkpoint".into(),
            ));
        }
        Ok(requested.started_epoch > 0 && self.published_epoch >= requested.started_epoch)
    }

    /// A version-2 or newer observation covers the target checkpoint and all retirement debt.
    /// Writers must be quiescent when this is used as an I/O measurement barrier.
    pub fn maintenance_is_complete(&self, requested: &Self) -> Result<bool, ObjectError> {
        let published = self.checkpoint_is_published(requested)?;
        if self.version < 2 || requested.version < 2 {
            return Err(ObjectError::PreconditionFailed(
                "full maintenance status requires version 2 or newer".into(),
            ));
        }
        Ok(published
            && !self.checkpoint_in_flight
            && self.pending_records == 0
            && self.bytes_since_checkpoint == 0
            && self.retired_segments == 0
            && !self.retirement_in_flight)
    }

    pub const LEGACY: u32 = 1;
    pub const SEGMENTED: u32 = 2;
    pub fn validate(&self) -> Result<(), ObjectError> {
        if let Some(stats) = &self.write_statistics { stats.validate()?; }
        if ![1, 2, 3].contains(&self.version)
            || (self.write_statistics.is_some() != (self.version == 3 && self.layout == Self::SEGMENTED))
            || (self.version == 1
                && (self.retired_segments != 0
                    || self.retired_journal_bytes != 0
                    || self.retirement_in_flight))
            || self.retired_segments >= self.segments
            || self.retired_journal_bytes > self.journal_bytes
            || (self.retired_segments == 0 && self.retired_journal_bytes != 0)
            || (self.retired_segments != 0 && self.retired_journal_bytes == 0)
            || (self.layout == Self::LEGACY
                && (self.retired_segments != 0 || self.retirement_in_flight))
            || self.journal_id.len() != 16
            || !matches!(self.layout, Self::LEGACY | Self::SEGMENTED)
            || self.published_epoch > self.started_epoch
            || self.pending_records > 64
            || self.segments == 0
            || (self.layout == Self::SEGMENTED
                && (self.published_epoch == 0 || self.journal_bytes == 0))
            || (!self.checkpoint_in_flight && self.published_epoch != self.started_epoch)
            || (self.checkpoint_in_flight && self.started_epoch <= self.published_epoch)
            || (self.layout == Self::LEGACY
                && (self.checkpoint_in_flight || self.pending_records != 0 || self.segments != 1))
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 256 {
            return Err(invalid());
        }
        let value = Self::decode(bytes).map_err(|_| invalid())?;
        value.validate()?;
        if value.encode_to_vec() != bytes {
            return Err(invalid());
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn journal_control_maintenance_status_tracks_cleanup_without_changing_v1() {
        let complete = ReplicaJournalStatus {
            journal_id: vec![9; 16],
            version: 2,
            layout: 2,
            started_epoch: 7,
            published_epoch: 7,
            journal_bytes: 8192,
            segments: 2,
            ..Default::default()
        };
        let requested = complete.clone();
        assert!(complete.maintenance_is_complete(&requested).unwrap());
        for action in [
            ReplicaJournalControl::STATUS,
            ReplicaJournalControl::CHECKPOINT,
        ] {
            let request = ReplicaJournalControl { version: 2, action };
            assert_eq!(
                ReplicaJournalControl::from_payload(&request.encode_to_vec()).unwrap(),
                request
            );
        }
        let mut pending = complete.clone();
        pending.retired_segments = 1;
        pending.retired_journal_bytes = 4096;
        assert!(!pending.maintenance_is_complete(&requested).unwrap());
        assert_eq!(
            ReplicaJournalStatus::from_payload(&pending.encode_to_vec()).unwrap(),
            pending
        );
        let mut snapshot = complete.clone();
        snapshot.retirement_in_flight = true;
        assert!(!snapshot.maintenance_is_complete(&requested).unwrap());
        for change in 0..6 {
            let mut bad = pending.clone();
            match change {
                0 => bad.version = 1,
                1 => bad.retired_segments = bad.segments,
                2 => bad.retired_journal_bytes = bad.journal_bytes + 1,
                3 => bad.retired_journal_bytes = 0,
                4 => bad.retired_segments = 0,
                _ => {
                    bad.layout = 1;
                    bad.segments = 1;
                }
            }
            assert!(ReplicaJournalStatus::from_payload(&bad.encode_to_vec()).is_err());
        }
        for suffix in [vec![88, 1], vec![112, 1]] {
            let mut bytes = pending.encode_to_vec();
            bytes.extend(suffix);
            assert!(ReplicaJournalStatus::from_payload(&bytes).is_err());
        }
        let mut largest = pending;
        largest.started_epoch = u64::MAX;
        largest.published_epoch = u64::MAX;
        largest.journal_bytes = u64::MAX;
        largest.segments = u64::MAX;
        largest.retired_segments = u64::MAX - 1;
        largest.retired_journal_bytes = u64::MAX;
        largest.bytes_since_checkpoint = u64::MAX;
        assert!(largest.encode_to_vec().len() < 256);
        assert_eq!(
            ReplicaJournalStatus::from_payload(&largest.encode_to_vec()).unwrap(),
            largest
        );
        let mut legacy_wire = complete;
        legacy_wire.version = 1;
        assert!(legacy_wire.maintenance_is_complete(&requested).is_err());
        assert!(requested.maintenance_is_complete(&legacy_wire).is_err());
        assert!(ReplicaJournalStatus::from_payload(&legacy_wire.encode_to_vec()).is_ok());
    }
    #[test]
    fn journal_control_codecs_are_canonical_bounded_and_versioned() {
        for (action, bytes) in [
            (ReplicaJournalControl::STATUS, vec![8, 1, 16, 1]),
            (ReplicaJournalControl::CHECKPOINT, vec![8, 1, 16, 2]),
        ] {
            let value = ReplicaJournalControl { version: 1, action };
            assert_eq!(value.encode_to_vec(), bytes);
            assert_eq!(ReplicaJournalControl::from_payload(&bytes).unwrap(), value);
            let mut duplicate = bytes.clone();
            duplicate.extend([8, 1]);
            assert!(ReplicaJournalControl::from_payload(&duplicate).is_err());
            let mut unknown = bytes.clone();
            unknown.extend([24, 1]);
            assert!(ReplicaJournalControl::from_payload(&unknown).is_err());
        }
        for bytes in [
            vec![],
            vec![8, 4, 16, 1],
            vec![8, 1, 16, 3],
            vec![8, 0x81, 0, 16, 1],
            vec![0; 33],
            b"{}".to_vec(),
        ] {
            assert!(ReplicaJournalControl::from_payload(&bytes).is_err());
        }
        let status = ReplicaJournalStatus {
            journal_id: vec![7; 16],
            version: 1,
            layout: 2,
            started_epoch: 2,
            published_epoch: 1,
            checkpoint_in_flight: true,
            pending_records: 3,
            bytes_since_checkpoint: 8192,
            journal_bytes: 65536,
            segments: 2,
            ..Default::default()
        };
        let bytes = status.encode_to_vec();
        assert!(bytes.len() < 256);
        assert_eq!(ReplicaJournalStatus::from_payload(&bytes).unwrap(), status);
        for change in 0..7 {
            let mut bad = status.clone();
            match change {
                0 => bad.version = 4,
                1 => bad.layout = 3,
                2 => bad.published_epoch = 3,
                3 => bad.pending_records = 65,
                4 => bad.checkpoint_in_flight = false,
                5 => bad.layout = 1,
                _ => bad.published_epoch = 0,
            };
            assert!(ReplicaJournalStatus::from_payload(&bad.encode_to_vec()).is_err());
        }
        let mut replaced = status.clone();
        replaced.journal_id[0] ^= 1;
        assert!(replaced.checkpoint_is_published(&status).is_err());
        let mut complete = status.clone();
        complete.published_epoch = complete.started_epoch;
        complete.checkpoint_in_flight = false;
        assert!(complete.checkpoint_is_published(&status).unwrap());
        assert!(!status.checkpoint_is_published(&complete).unwrap());
        let mut unknown = bytes;
        unknown.extend([80, 1]);
        assert!(ReplicaJournalStatus::from_payload(&unknown).is_err());
        assert!(ReplicaJournalStatus::from_payload(&vec![0; 257]).is_err());
    }
}
#[cfg(all(test, unix, feature = "replica-server"))]
#[path = "replica_journal_control_tests.rs"]
mod server_tests;

#[cfg(test)]
#[path = "replica_journal_write_statistics_tests.rs"]
mod write_statistics_tests;
