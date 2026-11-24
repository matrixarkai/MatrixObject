// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Confirmed record traffic for one open journal, excluding file initialization
//! and manifests. Record bytes include envelope and sector padding; they are not
//! device I/O. Checkpoint payloads include both relocation and bulk installation.
use crate::durable_payload::outcome_log::{CheckpointFragment, StoredRecordMetadata};
use crate::ObjectError;
use prost::Message;
use ring::rand::{SecureRandom, SystemRandom};
use std::io;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct OutcomeWriteStatistics {
    #[prost(bytes = "vec", tag = "1")]
    pub scope_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub foreground_batches: u64,
    #[prost(uint64, tag = "3")]
    pub foreground_decisions: u64,
    #[prost(uint64, tag = "4")]
    pub foreground_record_bytes: u64,
    #[prost(uint64, tag = "5")]
    pub foreground_payload_bytes: u64,
    #[prost(uint64, tag = "6")]
    pub maintenance_batches: u64,
    #[prost(uint64, tag = "7")]
    pub maintenance_record_bytes: u64,
    #[prost(uint64, tag = "8")]
    pub maintenance_payload_bytes: u64,
    #[prost(uint64, tag = "9")]
    pub maintenance_metadata_bytes: u64,
    #[prost(bool, tag = "10")]
    pub saturated: bool,
}
fn invalid() -> ObjectError {
    ObjectError::Corruption("invalid journal write statistics".into())
}
impl OutcomeWriteStatistics {
    pub(super) fn new() -> io::Result<Self> {
        let mut scope_id = vec![0; 16];
        SystemRandom::new()
            .fill(&mut scope_id)
            .map_err(|_| io::Error::other("journal statistics identity generation failed"))?;
        Ok(Self {
            scope_id,
            ..Self::default()
        })
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.scope_id.len() != 16 {
            return Err(invalid());
        }
        if self.saturated {
            return Ok(());
        }
        let batches = u128::from(self.foreground_batches);
        if u128::from(self.foreground_decisions) < batches
            || u128::from(self.foreground_decisions) > batches * 64
            || u128::from(self.foreground_record_bytes) < batches * 4096
            || self.foreground_record_bytes % 4096 != 0
            || u128::from(self.foreground_payload_bytes) + batches * 4096
                > u128::from(self.foreground_record_bytes)
            || (self.foreground_batches == 0 && self.foreground_record_bytes != 0)
            || u128::from(self.maintenance_record_bytes)
                < u128::from(self.maintenance_batches) * 4096
            || self.maintenance_record_bytes % 4096 != 0
            || u128::from(self.maintenance_payload_bytes)
                + u128::from(self.maintenance_metadata_bytes)
                + u128::from(self.maintenance_batches) * 4096
                > u128::from(self.maintenance_record_bytes)
            || (self.maintenance_batches == 0 && self.maintenance_record_bytes != 0)
        {
            return Err(invalid());
        }
        Ok(())
    }
    /// Refuse mixed journal lifetimes, regressing counters and saturated values.
    /// Quiesce writers and complete maintenance to measure an entire interval.
    pub fn checked_delta(&self, earlier: &Self) -> Result<Self, ObjectError> {
        self.validate()?;
        earlier.validate()?;
        if self.scope_id != earlier.scope_id || self.saturated || earlier.saturated {
            return Err(ObjectError::PreconditionFailed(
                "journal statistics scope changed or saturated".into(),
            ));
        }
        let sub = |a: u64, b: u64| a.checked_sub(b).ok_or_else(invalid);
        let delta = Self {
            scope_id: self.scope_id.clone(),
            foreground_batches: sub(self.foreground_batches, earlier.foreground_batches)?,
            foreground_decisions: sub(self.foreground_decisions, earlier.foreground_decisions)?,
            foreground_record_bytes: sub(
                self.foreground_record_bytes,
                earlier.foreground_record_bytes,
            )?,
            foreground_payload_bytes: sub(
                self.foreground_payload_bytes,
                earlier.foreground_payload_bytes,
            )?,
            maintenance_batches: sub(self.maintenance_batches, earlier.maintenance_batches)?,
            maintenance_record_bytes: sub(
                self.maintenance_record_bytes,
                earlier.maintenance_record_bytes,
            )?,
            maintenance_payload_bytes: sub(
                self.maintenance_payload_bytes,
                earlier.maintenance_payload_bytes,
            )?,
            maintenance_metadata_bytes: sub(
                self.maintenance_metadata_bytes,
                earlier.maintenance_metadata_bytes,
            )?,
            saturated: false,
        };
        delta.validate()?;
        Ok(delta)
    }
    pub(super) fn observe(&mut self, record: &StoredRecordMetadata) {
        fn add(value: &mut u64, increment: u64, saturated: &mut bool) {
            match value.checked_add(increment) {
                Some(next) => *value = next,
                None => {
                    *value = u64::MAX;
                    *saturated = true;
                }
            }
        }
        let saturated = &mut self.saturated;
        if record.batch.version == 2 {
            add(&mut self.maintenance_batches, 1, saturated);
            add(
                &mut self.maintenance_record_bytes,
                record.position.length,
                saturated,
            );
            for decision in &record.batch.decisions {
                let fragment = decision.checkpoint_fragment.as_ref().unwrap();
                let bytes = u64::from(decision.patch_lengths[0]);
                let destination = if fragment.kind == CheckpointFragment::PAYLOAD {
                    &mut self.maintenance_payload_bytes
                } else {
                    &mut self.maintenance_metadata_bytes
                };
                add(destination, bytes, saturated);
            }
        } else {
            add(&mut self.foreground_batches, 1, saturated);
            add(
                &mut self.foreground_decisions,
                record.batch.decisions.len() as u64,
                saturated,
            );
            add(
                &mut self.foreground_record_bytes,
                record.position.length,
                saturated,
            );
            for payload in &record.batch.payloads {
                add(
                    &mut self.foreground_payload_bytes,
                    payload.length,
                    saturated,
                );
            }
        }
    }
}
