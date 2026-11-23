// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Native metadata changes carried by placement outcome version 18.
//! All checks run on private persistent-table clones before live state changes.
use crate::chunk_manifest::{ChunkManifest, CHUNK_FLAG_TAIL};
use crate::cluster_features::{FEATURE_NATIVE_OBJECT_STATE, FEATURE_UNIFIED_LOG_WRITERS};
use crate::log_chunk_outcome::WriterLease;
use crate::native_object_state::{
    NativeObjectState, NativeOwnedChunk, NativeStageStart, STAGE_ADD_TAIL,
};
use crate::{MatrixMetaService, ObjectError, ObjectId};
use prost::Message;
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
fn holder(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.contains('\0')
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeBegin {
    #[prost(message, optional, tag = "1")]
    pub start: Option<NativeStageStart>,
    #[prost(message, optional, tag = "2")]
    pub expected_lease: Option<WriterLease>,
    #[prost(bool, tag = "3")]
    pub bypass_governance: bool,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeStageRef {
    #[prost(uint64, tag = "1")]
    pub stage_id: u64,
    #[prost(uint64, tag = "2")]
    pub expected_revision: u64,
    #[prost(string, tag = "3")]
    pub holder: String,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeExtend {
    #[prost(message, optional, tag = "1")]
    pub stage: Option<NativeStageRef>,
    #[prost(message, optional, tag = "2")]
    pub chunk: Option<NativeOwnedChunk>,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativePublish {
    #[prost(message, optional, tag = "1")]
    pub stage: Option<NativeStageRef>,
    #[prost(message, optional, tag = "2")]
    pub manifest: Option<ChunkManifest>,
    #[prost(message, optional, tag = "3")]
    pub expected_lease: Option<WriterLease>,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeStageExpiry {
    #[prost(uint64, tag = "1")]
    pub stage_id: u64,
    #[prost(uint64, tag = "2")]
    pub expected_revision: u64,
    #[prost(uint64, tag = "3")]
    pub expected_expiry: u64,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativePinRenewal {
    #[prost(uint64, tag = "1")]
    pub pin_id: u64,
    #[prost(uint64, tag = "2")]
    pub expected_expiry: u64,
    #[prost(uint64, tag = "3")]
    pub expires_at_ms: u64,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativePinRelease {
    #[prost(uint64, tag = "1")]
    pub pin_id: u64,
    #[prost(uint64, tag = "2")]
    pub expected_expiry: u64,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeWriterRenewal {
    #[prost(string, tag = "1")]
    pub bucket: String,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(message, optional, tag = "3")]
    pub before: Option<WriterLease>,
    #[prost(message, optional, tag = "4")]
    pub after: Option<WriterLease>,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeWriterRelease {
    #[prost(string, tag = "1")]
    pub bucket: String,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(message, optional, tag = "3")]
    pub before: Option<WriterLease>,
    /// Expiry cleanup may release only after the captured deadline. A holder release
    /// must instead supply the current lease's holder, while the lease is still live.
    #[prost(bool, tag = "4")]
    pub expired_only: bool,
    #[prost(string, tag = "5")]
    pub holder: String,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeObjectChange {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// Leader-captured time; replicas never consult their own wall clock to apply it.
    #[prost(uint64, tag = "2")]
    pub now_ms: u64,
    #[prost(
        oneof = "native_operation::Operation",
        tags = "3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13"
    )]
    pub operation: Option<native_operation::Operation>,
}
pub mod native_operation {
    #[derive(Clone, PartialEq, Eq, prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "3")]
        Begin(super::NativeBegin),
        #[prost(message, tag = "4")]
        Extend(super::NativeExtend),
        #[prost(message, tag = "5")]
        Publish(super::NativePublish),
        #[prost(message, tag = "6")]
        Abort(super::NativeStageRef),
        #[prost(message, tag = "7")]
        ExpireStage(super::NativeStageExpiry),
        #[prost(message, tag = "8")]
        OpenPin(crate::native_object_state::NativeReadPin),
        #[prost(message, tag = "9")]
        RenewPin(super::NativePinRenewal),
        #[prost(message, tag = "10")]
        ClosePin(super::NativePinRelease),
        #[prost(message, tag = "11")]
        ExpirePin(super::NativePinRelease),
        #[prost(message, tag = "12")]
        RenewWriter(super::NativeWriterRenewal),
        #[prost(message, tag = "13")]
        ReleaseWriter(super::NativeWriterRelease),
    }
}
use native_operation::Operation;
struct NativeDraft {
    state: NativeObjectState,
    catalog: crate::chunk_catalog::ChunkCatalog,
    leases: crate::log_chunk_outcome::WriterLeaseTable,
}
fn reference(value: &NativeStageRef, before: u64) -> Result<(), ObjectError> {
    if value.stage_id == 0
        || value.stage_id > before
        || value.expected_revision < value.stage_id
        || value.expected_revision > before
        || !holder(&value.holder)
    {
        return Err(invalid("invalid native stage identity, revision or holder"));
    }
    Ok(())
}
fn prior_lease(lease: Option<&WriterLease>, before: u64) -> Result<(), ObjectError> {
    if let Some(lease) = lease {
        lease.validate()?;
        if lease.epoch > before {
            return Err(invalid("native lease is from the future"));
        }
    }
    Ok(())
}
fn names(bucket: &str, key: &str) -> Result<ObjectId, ObjectError> {
    crate::validate_bucket(bucket)?;
    crate::validate_key(key)?;
    Ok(ObjectId::new(bucket, key))
}
pub(crate) fn reject_legacy_write(
    service: &MatrixMetaService,
    id: &ObjectId,
) -> Result<(), ObjectError> {
    if !service.cluster_features.has(FEATURE_NATIVE_OBJECT_STATE) {
        return Ok(());
    }
    if service.native_objects.is_staged(id)
        || service
            .chunk_catalog
            .get(id)
            .is_some_and(|m| m.initialized_log.is_some())
    {
        return Err(refused("this object is owned by native metadata commands"));
    }
    Ok(())
}
impl NativeObjectChange {
    pub fn decide(
        service: &MatrixMetaService,
        operation: Operation,
        now_ms: u64,
    ) -> Result<crate::placement_outcome::PlacementOutcome, ObjectError> {
        let before_index = service.placement_commit_index;
        let after_index = before_index
            .checked_add(1)
            .ok_or_else(|| invalid("native metadata position exhausted"))?;
        let change = Self {
            version: 1,
            now_ms,
            operation: Some(operation),
        };
        change.check(
            service,
            before_index,
            after_index,
            Some(crate::object_manifest::admission_cap()),
        )?;
        let outcome = crate::placement_outcome::PlacementOutcome {
            version: 18,
            before_index,
            after_index,
            native: Some(change),
            ..Default::default()
        };
        outcome.to_payload()?;
        Ok(outcome)
    }
    pub fn validate(&self, before: u64, after: u64) -> Result<(), ObjectError> {
        if self.version != 1
            || self.now_ms == 0
            || before == 0
            || before.checked_add(1) != Some(after)
        {
            return Err(invalid("invalid native outcome version, time or positions"));
        }
        match self
            .operation
            .as_ref()
            .ok_or_else(|| invalid("native outcome lacks an operation"))?
        {
            Operation::Begin(command) => {
                let start = command
                    .start
                    .as_ref()
                    .ok_or_else(|| invalid("native begin lacks its stage"))?;
                start.validate()?;
                prior_lease(command.expected_lease.as_ref(), before)?;
                if start.stage_id != after
                    || start.writer_epoch != after
                    || start.created_ms != self.now_ms
                {
                    return Err(invalid(
                        "native begin does not bind its allocated epoch and clock",
                    ));
                }
            }
            Operation::Extend(command) => {
                reference(
                    command
                        .stage
                        .as_ref()
                        .ok_or_else(|| invalid("native extension lacks its stage"))?,
                    before,
                )?;
                let chunk = command
                    .chunk
                    .as_ref()
                    .ok_or_else(|| invalid("native extension lacks its chunk"))?;
                if chunk.allocation_id != after
                    || chunk.extent.is_none()
                    || chunk.initialization.is_none()
                {
                    return Err(invalid(
                        "native extension does not bind its committed allocation",
                    ));
                }
            }
            Operation::Publish(command) => {
                reference(
                    command
                        .stage
                        .as_ref()
                        .ok_or_else(|| invalid("native publication lacks its stage"))?,
                    before,
                )?;
                let manifest = command
                    .manifest
                    .as_ref()
                    .ok_or_else(|| invalid("native publication lacks its manifest"))?;
                manifest.validate()?;
                prior_lease(command.expected_lease.as_ref(), before)?;
                if manifest.deleted || manifest.initialized_log.is_none() {
                    return Err(invalid("native publication is not initialized live data"));
                }
            }
            Operation::Abort(command) => reference(command, before)?,
            Operation::ExpireStage(command) => {
                reference(
                    &NativeStageRef {
                        stage_id: command.stage_id,
                        expected_revision: command.expected_revision,
                        holder: "expiry".into(),
                    },
                    before,
                )?;
                if command.expected_expiry == 0 || self.now_ms < command.expected_expiry {
                    return Err(invalid("native stage has not expired"));
                }
            }
            Operation::OpenPin(pin) => {
                names(&pin.bucket, &pin.key)?;
                if pin.pin_id != after
                    || pin.generation == 0
                    || pin.manifest_digest.len() != 32
                    || pin
                        .expires_at_ms
                        .checked_sub(self.now_ms)
                        .is_none_or(|ttl| !(1000..=3_600_000).contains(&ttl))
                {
                    return Err(invalid(
                        "native read pin does not bind its position, view and deadline",
                    ));
                }
            }
            Operation::RenewPin(command) => {
                if command.pin_id == 0
                    || command.pin_id > before
                    || command.expected_expiry <= self.now_ms
                    || command.expires_at_ms <= command.expected_expiry
                {
                    return Err(invalid("invalid native read pin renewal"));
                }
            }
            Operation::ClosePin(command) | Operation::ExpirePin(command) => {
                if command.pin_id == 0
                    || command.pin_id > before
                    || command.expected_expiry == 0
                    || (matches!(self.operation.as_ref(), Some(Operation::ExpirePin(_)))
                        && self.now_ms < command.expected_expiry)
                {
                    return Err(invalid("invalid native read pin release"));
                }
            }
            Operation::RenewWriter(command) => {
                names(&command.bucket, &command.key)?;
                let prior = command
                    .before
                    .as_ref()
                    .ok_or_else(|| invalid("native renewal lacks its prior lease"))?;
                let next = command
                    .after
                    .as_ref()
                    .ok_or_else(|| invalid("native renewal lacks its resulting lease"))?;
                prior_lease(Some(prior), before)?;
                next.validate()?;
                if prior.holder != next.holder
                    || prior.epoch != next.epoch
                    || prior.expires_at_ms <= self.now_ms
                    || next.expires_at_ms <= prior.expires_at_ms
                    || next
                        .expires_at_ms
                        .checked_sub(self.now_ms)
                        .is_none_or(|ttl| ttl > 3_600_000)
                {
                    return Err(invalid(
                        "native renewal changes the writer or loses its live deadline",
                    ));
                }
            }
            Operation::ReleaseWriter(command) => {
                names(&command.bucket, &command.key)?;
                let prior = command
                    .before
                    .as_ref()
                    .ok_or_else(|| invalid("native release lacks its prior lease"))?;
                prior_lease(Some(prior), before)?;
                if (command.expired_only
                    && (self.now_ms < prior.expires_at_ms || !command.holder.is_empty()))
                    || (!command.expired_only
                        && (command.holder != prior.holder || self.now_ms >= prior.expires_at_ms))
                {
                    return Err(invalid("native writer release does not own the deadline"));
                }
            }
        }
        Ok(())
    }
    fn candidate(
        &self,
        service: &MatrixMetaService,
        before: u64,
        after: u64,
        admission: Option<usize>,
    ) -> Result<NativeDraft, ObjectError> {
        self.validate(before, after)?;
        if before != service.placement_commit_index
            || !service
                .cluster_features
                .has(FEATURE_NATIVE_OBJECT_STATE | FEATURE_UNIFIED_LOG_WRITERS)
        {
            return Err(refused(
                "native outcome does not follow the activated metadata state",
            ));
        }
        let mut draft = NativeDraft {
            state: service.native_objects.clone(),
            catalog: service.chunk_catalog.clone(),
            leases: service.writer_leases.clone(),
        };
        match self.operation.as_ref().unwrap() {
            Operation::Begin(command) => {
                let start = command.start.as_ref().unwrap();
                let id = start.header()?.object_id();
                if service.object_manifests.get(&id).is_some()
                    || service.object_manifests.names_key(&id)
                    || service.placements.contains_key(&id)
                {
                    return Err(refused(
                        "native begin requires an explicitly owned catalog namespace",
                    ));
                }
                if let Some(limit) = admission {
                    if draft.catalog.get(&id).is_none() && draft.catalog.len() >= limit {
                        return Err(crate::object_manifest::catalog_full(limit));
                    }
                }
                let held = draft.leases.get(&id.bucket, &id.key);
                if held != command.expected_lease {
                    return Err(refused("native begin writer lease changed"));
                }
                if let Some(held) = &held {
                    if held.epoch >= start.writer_epoch
                        || (start.kind == STAGE_ADD_TAIL
                            && held.holder != start.holder
                            && held.expires_at_ms > self.now_ms)
                    {
                        return Err(refused("native begin does not own the writer transition"));
                    }
                }
                if draft
                    .catalog
                    .get(&id)
                    .is_some_and(|m| m.locked_at(self.now_ms, command.bypass_governance))
                {
                    return Err(refused("native object is locked against mutation"));
                }
                if !draft
                    .state
                    .begin(start.clone(), &draft.catalog, self.now_ms)?
                {
                    return Err(invalid("committed native begin is not a new transition"));
                }
            }
            Operation::Extend(command) => {
                let stage = command.stage.as_ref().unwrap();
                draft.state.extend(
                    stage.stage_id,
                    stage.expected_revision,
                    &stage.holder,
                    self.now_ms,
                    command.chunk.clone().unwrap(),
                )?;
            }
            Operation::Publish(command) => {
                let reference = command.stage.as_ref().unwrap();
                let start = draft
                    .state
                    .stage_start(reference.stage_id)
                    .ok_or_else(|| refused("native publication stage is missing"))?
                    .clone();
                let id = start.header()?.object_id();
                if service.object_manifests.get(&id).is_some()
                    || service.object_manifests.names_key(&id)
                    || service.placements.contains_key(&id)
                {
                    return Err(refused("native publication namespace changed"));
                }
                if draft.leases.get(&id.bucket, &id.key) != command.expected_lease {
                    return Err(refused("native publication writer lease changed"));
                }
                let manifest = command.manifest.as_ref().unwrap();
                let before_manifest = draft.catalog.get(&id).map(|m| (**m).clone());
                draft.catalog.check(
                    &crate::chunk_catalog::ChunkOutcome::decide(before_manifest, manifest.clone())?,
                    admission,
                )?;
                draft.state.publish(
                    reference.stage_id,
                    reference.expected_revision,
                    &reference.holder,
                    self.now_ms,
                    after,
                    manifest.clone(),
                    &mut draft.catalog,
                )?;
                if manifest
                    .extents
                    .last()
                    .is_some_and(|e| e.flags == CHUNK_FLAG_TAIL)
                {
                    if command.expected_lease.is_none()
                        && draft.leases.len() >= crate::log_chunk_outcome::MAX_WRITER_LEASES
                    {
                        return Err(refused("native writer lease table is full"));
                    }
                    draft.leases.set(
                        &id.bucket,
                        &id.key,
                        &WriterLease {
                            holder: start.holder,
                            epoch: start.writer_epoch,
                            expires_at_ms: start.expires_at_ms,
                        },
                    );
                } else {
                    draft.leases.remove(&id.bucket, &id.key);
                }
            }
            Operation::Abort(command) => draft.state.abort(
                command.stage_id,
                command.expected_revision,
                &command.holder,
                self.now_ms,
                after,
            )?,
            Operation::ExpireStage(command) => draft.state.expire_stage(
                command.stage_id,
                command.expected_revision,
                command.expected_expiry,
                self.now_ms,
                after,
            )?,
            Operation::OpenPin(pin) => {
                draft
                    .state
                    .open_pin(pin.clone(), &draft.catalog, self.now_ms)?
            }
            Operation::RenewPin(command) => draft.state.renew_pin(
                command.pin_id,
                command.expected_expiry,
                command.expires_at_ms,
                self.now_ms,
                after,
            )?,
            Operation::ClosePin(command) => {
                draft
                    .state
                    .close_pin(command.pin_id, command.expected_expiry, after)?
            }
            Operation::ExpirePin(command) => draft.state.expire_pin(
                command.pin_id,
                command.expected_expiry,
                self.now_ms,
                after,
            )?,
            Operation::RenewWriter(command) => {
                let id = names(&command.bucket, &command.key)?;
                let prior = command.before.as_ref().unwrap();
                if draft.state.is_staged(&id)
                    || draft.leases.get(&id.bucket, &id.key).as_ref() != Some(prior)
                    || !draft.catalog.get(&id).is_some_and(|m| {
                        !m.deleted
                            && m.initialized_log.is_some()
                            && m.extents.last().is_some_and(|e| e.flags == CHUNK_FLAG_TAIL)
                            && m.initialized_log
                                .as_ref()
                                .unwrap()
                                .chunks
                                .last()
                                .is_some_and(|c| c.writer_epoch == prior.epoch)
                    })
                {
                    return Err(refused(
                        "native writer renewal lost its current tail or mutation slot",
                    ));
                }
                draft
                    .leases
                    .set(&id.bucket, &id.key, command.after.as_ref().unwrap());
            }
            Operation::ReleaseWriter(command) => {
                let id = names(&command.bucket, &command.key)?;
                if draft.state.is_staged(&id)
                    || draft.leases.get(&id.bucket, &id.key) != command.before
                    || !draft
                        .catalog
                        .get(&id)
                        .is_some_and(|m| m.initialized_log.is_some())
                {
                    return Err(refused(
                        "native writer release lost its current object or mutation slot",
                    ));
                }
                draft.leases.remove(&id.bucket, &id.key);
            }
        }
        Ok(draft)
    }
    /// The leader applies admission limits; replicas pass None and validate committed state.
    pub fn check(
        &self,
        service: &MatrixMetaService,
        before: u64,
        after: u64,
        admission: Option<usize>,
    ) -> Result<(), ObjectError> {
        self.candidate(service, before, after, admission)
            .map(|_| ())
    }
    pub fn apply(
        &self,
        service: &mut MatrixMetaService,
        before: u64,
        after: u64,
    ) -> Result<(), ObjectError> {
        let draft = self.candidate(service, before, after, None)?;
        service.native_objects = draft.state;
        service.chunk_catalog = draft.catalog;
        service.writer_leases = draft.leases;
        service.placement_commit_index = after;
        service.invalidate_snapshot_export_cache();
        Ok(())
    }
}
