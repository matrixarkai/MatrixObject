// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Network admission for recoverable LOG object stages and authoritative read pins.
use crate::native_object_outcome::{native_operation::Operation, NativeObjectChange};
use crate::native_object_state::{NativeOwnedChunk, NativeReadPin, NativeStageStart};
use crate::{MatrixMetaService, ObjectError, ObjectId};
use prost::Message;

fn invalid(s: &str) -> ObjectError {
    ObjectError::InvalidArgument(s.into())
}
fn refused(s: &str) -> ObjectError {
    ObjectError::PreconditionFailed(s.into())
}
const LIMIT: usize = 4 * 1024 * 1024;
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeObjectRead {
    /// 1: owned stage, 2: pinned manifest, 3: pin validity, 4: metadata position.
    #[prost(uint32, tag = "1")]
    pub kind: u32,
    #[prost(uint64, tag = "2")]
    pub stage_id: u64,
    #[prost(uint64, tag = "3")]
    pub pin_id: u64,
    #[prost(string, tag = "4")]
    pub bucket: String,
    #[prost(string, tag = "5")]
    pub key: String,
    #[prost(string, tag = "6")]
    pub holder: String,
    #[prost(bytes = "vec", tag = "7")]
    pub request_nonce: Vec<u8>,
}
/// Leader-assigned positions, clocks, allocation coordinates and resulting leases must
/// be zero/absent on input. The TTL is separate from every persisted absolute deadline.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeObjectRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub ttl_ms: u64,
    #[prost(
        oneof = "crate::native_object_outcome::native_operation::Operation",
        tags = "3,4,5,6,7,8,9,10,11,12,13"
    )]
    pub operation: Option<Operation>,
    #[prost(message, optional, tag = "14")]
    pub read: Option<NativeObjectRead>,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct NativeObjectReply {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub commit_index: u64,
    #[prost(message, optional, tag = "3")]
    pub start: Option<NativeStageStart>,
    #[prost(uint64, tag = "4")]
    pub stage_revision: u64,
    #[prost(message, repeated, tag = "5")]
    pub chunks: Vec<NativeOwnedChunk>,
    #[prost(message, optional, tag = "6")]
    pub manifest: Option<crate::chunk_manifest::ChunkManifest>,
    #[prost(message, optional, tag = "7")]
    pub pin: Option<NativeReadPin>,
    #[prost(uint64, tag = "9")]
    pub now_ms: u64,
    #[prost(message, optional, tag = "8")]
    pub lease: Option<crate::log_chunk_outcome::WriterLease>,
}
impl NativeObjectRequest {
    pub fn mutation(operation: Operation, ttl_ms: u64) -> Self {
        Self {
            version: 1,
            ttl_ms,
            operation: Some(operation),
            read: None,
        }
    }
    pub fn query(read: NativeObjectRead) -> Self {
        Self {
            version: 1,
            read: Some(read),
            ..Default::default()
        }
    }
    pub fn is_read(&self) -> bool {
        self.read.is_some()
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.version != 1 || self.operation.is_some() == self.read.is_some() {
            return Err(invalid("invalid native object request shape"));
        }
        let timed = matches!(
            self.operation,
            Some(
                Operation::Begin(_)
                    | Operation::OpenPin(_)
                    | Operation::RenewPin(_)
                    | Operation::RenewWriter(_)
            )
        );
        if (timed && !(1000..=3_600_000).contains(&self.ttl_ms)) || (!timed && self.ttl_ms != 0) {
            return Err(invalid("invalid native object request TTL"));
        }
        if let Some(read) = &self.read {
            let object = read.stage_id == 0
                && read.pin_id == 0
                && crate::validate_bucket(&read.bucket).is_ok()
                && crate::validate_key(&read.key).is_ok();
            let named = read.bucket.is_empty() && read.key.is_empty();
            let stage = read.kind == 1
                && read.pin_id == 0
                && !read.holder.is_empty()
                && read.holder.len() <= 256
                && !read.holder.contains('\0')
                && read.request_nonce.len() == 32
                && read.request_nonce.iter().any(|b| *b != 0)
                && ((read.stage_id != 0 && named) || object);
            let pin = matches!(read.kind, 2 | 3)
                && read.pin_id != 0
                && read.stage_id == 0
                && named
                && read.holder.is_empty()
                && read.request_nonce.is_empty();
            let position = read.kind == 4
                && read.pin_id == 0
                && read.stage_id == 0
                && named
                && read.holder.is_empty()
                && read.request_nonce.is_empty();
            if !(stage || pin || position) {
                return Err(invalid("invalid native object query"));
            }
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > LIMIT {
            return Err(invalid("native object request exceeds its bound"));
        }
        let value =
            Self::decode(bytes).map_err(|_| invalid("invalid native object request protobuf"))?;
        if value.encode_to_vec() != bytes {
            return Err(invalid("noncanonical native object request"));
        }
        value.validate()?;
        Ok(value)
    }
}
impl NativeObjectReply {
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > LIMIT {
            return Err(invalid("native object reply exceeds its bound"));
        }
        let value =
            Self::decode(bytes).map_err(|_| invalid("invalid native object reply protobuf"))?;
        if value.version != 1
            || value.commit_index == 0
            || value.now_ms == 0
            || value.encode_to_vec() != bytes
        {
            return Err(invalid("noncanonical native object reply"));
        }
        Ok(value)
    }
}
fn enabled(service: &MatrixMetaService) -> Result<(), ObjectError> {
    if !service.cluster_features.has(
        crate::cluster_features::FEATURE_NATIVE_OBJECT_STATE
            | crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS,
    ) {
        return Err(refused("native object commands are not activated"));
    }
    Ok(())
}
fn reply(service: &MatrixMetaService, now: u64) -> NativeObjectReply {
    NativeObjectReply {
        version: 1,
        commit_index: service.placement_commit_index,
        now_ms: now,
        ..Default::default()
    }
}
pub(super) fn read(
    service: &MatrixMetaService,
    request: &NativeObjectRequest,
    limit: usize,
    now: u64,
) -> Result<super::MetaReply, ObjectError> {
    request.validate()?;
    enabled(service)?;
    let command = request
        .read
        .as_ref()
        .ok_or_else(|| invalid("native query required"))?;
    let state = &service.native_objects;
    let mut result = reply(service, now);
    match command.kind {
        1 => {
            let start = if command.stage_id != 0 {
                state.stage_start(command.stage_id)
            } else {
                state.stage_for_object(&ObjectId::new(&command.bucket, &command.key))
            }
            .ok_or_else(|| refused("native stage is missing"))?;
            if start.holder != command.holder || start.request_nonce != command.request_nonce {
                return Err(refused("native stage attempt does not match"));
            }
            result.start = Some(start.clone());
            result.stage_revision = state.stage_revision(start.stage_id).unwrap();
            result.chunks = state.owned_chunks(start.stage_id).cloned().collect();
        }
        2 | 3 => {
            let pin = state
                .pin(command.pin_id)
                .ok_or_else(|| refused("native read pin is missing"))?;
            if now >= pin.expires_at_ms {
                return Err(refused("native read pin has expired"));
            }
            if command.kind == 2 {
                result.manifest = Some((*state.pinned_manifest(pin.pin_id, now)?).clone());
            }
            result.pin = Some(pin.clone());
        }
        4 => {}
        _ => return Err(invalid("invalid native query action")),
    }
    super::message::check_reply_size(result.encoded_len(), limit)?;
    Ok(super::MetaReply::NativeObject(result))
}
pub(super) fn prepare(
    service: &MatrixMetaService,
    groups: &super::ChunkGroupCatalog,
    request: &NativeObjectRequest,
    limit: usize,
    now: u64,
) -> Result<(crate::placement_outcome::PlacementOutcome, super::MetaReply), ObjectError> {
    request.validate()?;
    enabled(service)?;
    let after = service
        .placement_commit_index
        .checked_add(1)
        .ok_or_else(|| refused("native metadata position exhausted"))?;
    let mut operation = request
        .operation
        .clone()
        .ok_or_else(|| invalid("native mutation required"))?;
    let expires = now
        .checked_add(request.ttl_ms)
        .ok_or_else(|| invalid("native deadline overflow"))?;
    let mut result = reply(service, now);
    result.commit_index = after;
    match &mut operation {
        Operation::Begin(command) => {
            if command.expected_lease.is_some() {
                return Err(invalid("native begin resulting lease is leader assigned"));
            }
            let start = command
                .start
                .as_mut()
                .ok_or_else(|| invalid("native begin lacks a stage"))?;
            if start.stage_id != 0
                || start.writer_epoch != 0
                || start.created_ms != 0
                || start.expires_at_ms != 0
            {
                return Err(invalid(
                    "native begin positions and clock are leader assigned",
                ));
            }
            let header = start
                .header
                .as_mut()
                .ok_or_else(|| invalid("native begin lacks its header"))?;
            if header.format_version
                != crate::initialized_log_manifest::CHUNK_MANIFEST_PHYSICAL_FORMAT
            {
                return Err(invalid(
                    "native network objects require physical member bindings",
                ));
            }
            if service
                .chunk_catalog
                .get(&header.object_id())
                .is_some_and(|m| {
                    m.extents
                        .last()
                        .is_some_and(|e| e.flags == crate::chunk_manifest::CHUNK_FLAG_TAIL)
                })
            {
                return Err(refused(
                    "active native replacement requires completed storage handoff",
                ));
            }
            command.expected_lease = service.writer_leases.get(&header.bucket, &header.key);
            header.modified_ms = now;
            start.stage_id = after;
            start.writer_epoch = after;
            start.created_ms = now;
            start.expires_at_ms = expires;
            if start.kind == crate::native_object_state::STAGE_REPLACE {
                let incarnation = start.replacement_incarnation()?;
                start
                    .header
                    .as_mut()
                    .unwrap()
                    .initialized_log
                    .as_mut()
                    .ok_or_else(|| invalid("native begin lacks its identity"))?
                    .object_incarnation = incarnation.to_vec();
            }
            result.start = Some(start.clone());
            result.stage_revision = after;
        }
        Operation::Extend(command) => {
            let reference = command
                .stage
                .as_ref()
                .ok_or_else(|| invalid("native extension lacks its owner"))?;
            let start = service
                .native_objects
                .stage_start(reference.stage_id)
                .ok_or_else(|| refused("native stage is missing"))?;
            let chunk = command
                .chunk
                .as_mut()
                .ok_or_else(|| invalid("native extension lacks its chunk"))?;
            let extent = chunk
                .extent
                .as_mut()
                .ok_or_else(|| invalid("native extension lacks its range"))?;
            let binding = chunk
                .initialization
                .as_mut()
                .ok_or_else(|| invalid("native extension lacks its members"))?;
            if chunk.allocation_id != 0
                || extent.volume_id != 0
                || extent.chunk_id != 0
                || binding.chunk_generation != 0
                || binding.writer_epoch != 0
            {
                return Err(invalid(
                    "native chunk coordinates and epochs are leader assigned",
                ));
            }
            let group = groups
                .get(&extent.group)
                .ok_or_else(|| refused("native allocation group is missing"))?;
            if !group
                .identity
                .as_ref()
                .is_some_and(|i| i.is_standalone_star())
                || group.member_ids.len() != 3
                || binding.replicas.len() != 3
                || !binding
                    .replicas
                    .iter()
                    .zip(&group.member_ids)
                    .all(|(r, id)| &r.member_id == id && r.initialization.is_empty())
            {
                return Err(refused(
                    "native allocation members do not match the configured group",
                ));
            }
            chunk.allocation_id = after;
            let (volume, id) = crate::object_manifest::chunk_identity_of_index(after);
            extent.volume_id = volume;
            extent.chunk_id = id;
            binding.chunk_generation = 1;
            binding.writer_epoch = start.writer_epoch;
            for member in &mut binding.replicas {
                let mut material = b"MONATIVE-MEMBER-BIRTH1".to_vec();
                material.extend_from_slice(&start.stage_id.to_le_bytes());
                material.extend_from_slice(&after.to_le_bytes());
                material.extend_from_slice(&start.request_nonce);
                material.extend_from_slice(&(member.member_id.len() as u64).to_le_bytes());
                material.extend_from_slice(member.member_id.as_bytes());
                material.extend_from_slice(&member.storage_incarnation);
                member.initialization = crate::chunk_integrity::sha256(&material)[..16].to_vec();
            }
            result.chunks.push(chunk.clone());
            result.stage_revision = after;
        }
        Operation::Publish(command) => {
            if command.expected_lease.is_some() {
                return Err(invalid("native publication lease is leader captured"));
            }
            let manifest = command
                .manifest
                .as_ref()
                .ok_or_else(|| invalid("native publication lacks its manifest"))?;
            if manifest.format_version
                != crate::initialized_log_manifest::CHUNK_MANIFEST_PHYSICAL_FORMAT
            {
                return Err(invalid(
                    "native publication requires physical member bindings",
                ));
            }
            command.expected_lease = service.writer_leases.get(&manifest.bucket, &manifest.key);
            if manifest
                .extents
                .last()
                .is_some_and(|extent| extent.flags == crate::chunk_manifest::CHUNK_FLAG_TAIL)
            {
                let stage = command
                    .stage
                    .as_ref()
                    .ok_or_else(|| invalid("native publication lacks its stage"))?;
                let start = service
                    .native_objects
                    .stage_start(stage.stage_id)
                    .ok_or_else(|| refused("native publication stage is missing"))?;
                result.lease = Some(crate::log_chunk_outcome::WriterLease {
                    holder: start.holder.clone(),
                    epoch: start.writer_epoch,
                    expires_at_ms: start.expires_at_ms,
                });
            }
            result.manifest = Some(manifest.clone());
        }
        Operation::OpenPin(pin) => {
            if pin.pin_id != 0
                || pin.expires_at_ms != 0
                || pin.generation != 0
                || !pin.manifest_digest.is_empty()
            {
                return Err(invalid("native pin view and deadline are leader assigned"));
            }
            let manifest = service
                .chunk_catalog
                .get(&ObjectId::new(&pin.bucket, &pin.key))
                .ok_or_else(|| refused("native pinned object is missing"))?;
            if manifest.deleted
                || manifest.format_version
                    != crate::initialized_log_manifest::CHUNK_MANIFEST_PHYSICAL_FORMAT
            {
                return Err(refused("native pin requires a live physical object"));
            }
            pin.pin_id = after;
            pin.generation = manifest.generation;
            pin.expires_at_ms = expires;
            pin.manifest_digest = crate::chunk_integrity::sha256(&manifest.to_bytes()?).to_vec();
            result.pin = Some(pin.clone());
            result.manifest = Some((**manifest).clone());
        }
        Operation::RenewPin(command) => {
            if command.expires_at_ms != 0 {
                return Err(invalid("native pin deadline is leader assigned"));
            }
            command.expires_at_ms = expires;
            let mut pin = service
                .native_objects
                .pin(command.pin_id)
                .ok_or_else(|| refused("native pin is missing"))?
                .clone();
            pin.expires_at_ms = expires;
            result.pin = Some(pin);
        }
        Operation::RenewWriter(command) => {
            if command.after.is_some() {
                return Err(invalid("native lease deadline is leader assigned"));
            }
            let mut lease = command
                .before
                .clone()
                .ok_or_else(|| invalid("native renewal lacks its prior lease"))?;
            lease.expires_at_ms = expires;
            result.lease = Some(lease.clone());
            command.after = Some(lease);
        }
        Operation::Abort(_)
        | Operation::ExpireStage(_)
        | Operation::ClosePin(_)
        | Operation::ExpirePin(_)
        | Operation::ReleaseWriter(_) => {}
    }
    let outcome = NativeObjectChange::decide(service, operation, now)?;
    // Admission must reject an oversized answer before the mutation enters a journal or Raft.
    super::message::check_reply_size(result.encoded_len(), limit)?;
    Ok((outcome, super::MetaReply::NativeObject(result)))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn begin_request(key: &str) -> NativeObjectRequest {
        let header = crate::chunk_manifest::ChunkManifest {
            format_version: crate::initialized_log_manifest::CHUNK_MANIFEST_PHYSICAL_FORMAT,
            bucket: "bucket".into(),
            key: key.into(),
            generation: 1,
            kind: crate::chunk_manifest::CHUNK_KIND_LOG,
            initialized_log: Some(
                crate::initialized_log_manifest::InitializedLogManifest::default(),
            ),
            ..Default::default()
        };
        NativeObjectRequest::mutation(
            Operation::Begin(crate::native_object_outcome::NativeBegin {
                start: Some(NativeStageStart {
                    kind: crate::native_object_state::STAGE_REPLACE,
                    header: Some(header),
                    holder: "writer".into(),
                    request_nonce: vec![9; 32],
                    target_length: 4,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            2000,
        )
    }
    #[test]
    fn native_rpc_is_canonical_and_rejects_ambiguous_requests() {
        let request = begin_request("object");
        let bytes = request.encode_to_vec();
        assert_eq!(NativeObjectRequest::from_payload(&bytes).unwrap(), request);
        for suffix in [vec![8, 1], vec![0x80, 1, 1]] {
            let mut malformed = bytes.clone();
            malformed.extend(suffix);
            assert!(NativeObjectRequest::from_payload(&malformed).is_err());
        }
        let mut ambiguous = request;
        ambiguous.read = Some(NativeObjectRead {
            kind: 4,
            ..Default::default()
        });
        assert!(ambiguous.validate().is_err());
    }
    #[test]
    fn leader_assigns_clock_positions_and_receipts_without_mutating_live_state() {
        let mut service = crate::native_object_outcome_tests::service();
        let before = service.export_snapshot().to_bytes().unwrap();
        let request = begin_request("object");
        let (outcome, reply) = prepare(
            &service,
            &super::super::ChunkGroupCatalog::default(),
            &request,
            1 << 20,
            1000,
        )
        .unwrap();
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
        let super::super::MetaReply::NativeObject(reply) = reply else {
            panic!("native receipt missing")
        };
        let start = reply.start.as_ref().unwrap();
        assert_eq!(
            (
                start.stage_id,
                start.writer_epoch,
                start.created_ms,
                start.expires_at_ms
            ),
            (2, 2, 1000, 3000)
        );
        assert_eq!(reply.now_ms, 1000);
        assert_eq!(reply.stage_revision, 2);
        assert_eq!(
            NativeObjectReply::from_payload(&reply.encode_to_vec()).unwrap(),
            reply
        );
        outcome.apply(&mut service).unwrap();
        assert_eq!(service.native_objects.stage_start(2), Some(start));
    }
    #[test]
    fn native_rpc_rejects_client_allocated_epochs_and_oversized_receipts_before_commit() {
        let service = crate::native_object_outcome_tests::service();
        let before = service.export_snapshot().to_bytes().unwrap();
        let groups = super::super::ChunkGroupCatalog::default();
        for field in 0..4 {
            let mut request = begin_request("object");
            let Some(Operation::Begin(begin)) = &mut request.operation else {
                unreachable!()
            };
            let start = begin.start.as_mut().unwrap();
            match field {
                0 => start.stage_id = 1,
                1 => start.writer_epoch = 1,
                2 => start.created_ms = 1,
                _ => start.expires_at_ms = 1,
            };
            assert!(prepare(&service, &groups, &request, 1 << 20, 1000).is_err());
            assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
        }
        assert!(prepare(&service, &groups, &begin_request("object"), 24, 1000).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
        let mut disabled = service.clone();
        disabled.cluster_features.raised = 0;
        assert!(prepare(&disabled, &groups, &begin_request("object"), 1 << 20, 1000).is_err());
    }
    #[test]
    fn native_read_pin_captures_current_view_and_uses_the_leader_deadline() {
        let mut service = crate::native_object_outcome_tests::service();
        for (operation, now) in crate::native_object_outcome_tests::commands() {
            NativeObjectChange::decide(&service, operation, now)
                .unwrap()
                .apply(&mut service)
                .unwrap();
        }
        let manifest = service.chunk_catalog.iter().next().unwrap().1.clone();
        let request = NativeObjectRequest::mutation(
            Operation::OpenPin(NativeReadPin {
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                ..Default::default()
            }),
            2000,
        );
        let groups = super::super::ChunkGroupCatalog::default();
        let (outcome, reply) = prepare(&service, &groups, &request, 1 << 20, 2000).unwrap();
        let super::super::MetaReply::NativeObject(reply) = reply else {
            unreachable!()
        };
        let pin = reply.pin.unwrap();
        assert_eq!(pin.generation, manifest.generation);
        assert_eq!(pin.expires_at_ms, 4000);
        assert_eq!(
            pin.manifest_digest,
            crate::chunk_integrity::sha256(&manifest.to_bytes().unwrap())
        );
        outcome.apply(&mut service).unwrap();
        let query = NativeObjectRequest::query(NativeObjectRead {
            kind: 3,
            pin_id: pin.pin_id,
            ..Default::default()
        });
        assert!(read(&service, &query, 1 << 20, 3999).is_ok());
        assert!(read(&service, &query, 1 << 20, 4000).is_err());
    }
}

#[cfg(test)]
mod publication_grant_tests {
    use super::*;
    #[test]
    fn publication_reply_carries_the_same_writer_grant_that_is_committed() {
        let mut service = crate::native_object_outcome_tests::service();
        let mut commands = crate::native_object_outcome_tests::commands();
        for (operation, now) in commands.drain(..2) {
            NativeObjectChange::decide(&service, operation, now)
                .unwrap()
                .apply(&mut service)
                .unwrap();
        }
        let (operation, now) = commands.remove(0);
        let request = NativeObjectRequest::mutation(operation, 0);
        let before = service.export_snapshot().to_bytes().unwrap();
        let (outcome, reply) = prepare(
            &service,
            &super::super::ChunkGroupCatalog::default(),
            &request,
            1 << 20,
            now,
        )
        .unwrap();
        let super::super::MetaReply::NativeObject(reply) = reply else {
            panic!("wrong reply");
        };
        let lease = reply
            .lease
            .as_ref()
            .expect("publication omitted its resulting grant");
        assert_eq!(
            (lease.holder.as_str(), lease.epoch, lease.expires_at_ms),
            ("writer", 2, 10_000)
        );
        assert_eq!(reply.now_ms, now);
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
        assert!(prepare(
            &service,
            &super::super::ChunkGroupCatalog::default(),
            &request,
            reply.encoded_len() - 1,
            now
        )
        .is_err());
        assert_eq!(
            service.export_snapshot().to_bytes().unwrap(),
            before,
            "oversized grant receipt committed publication"
        );
        outcome.apply(&mut service).unwrap();
        let manifest = reply.manifest.unwrap();
        assert_eq!(
            service
                .writer_leases
                .get(&manifest.bucket, &manifest.key)
                .as_ref(),
            Some(lease)
        );
    }
}
