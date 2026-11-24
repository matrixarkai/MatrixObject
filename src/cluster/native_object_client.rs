// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Recoverable initialized object I/O. This client does not select the gateway backend.
use super::native_object_commands::{NativeObjectRead, NativeObjectReply, NativeObjectRequest};
use super::{
    AppendIdentity, ChunkGroupConfig, ChunkLogError, ChunkLogReader, LogCallError, LogOpReply,
    LogOpRequest, MetaCallOutcome, MetaReply, MetaRequest, RemoteMetaClient, RemoteReplicaClient,
    ReplicaClientOptions, SealedFooter,
};
use crate::chunk_manifest::{
    ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_FLAG_TAIL, CHUNK_KIND_LOG,
};
use crate::initialized_log_manifest::{
    InitializedChunk, InitializedLogManifest, InitializedReplica, PhysicalMemberBinding,
    CHUNK_MANIFEST_PHYSICAL_FORMAT,
};
use crate::native_object_outcome::{
    native_operation::Operation, NativeBegin, NativeExtend, NativePublish, NativeStageRef,
};
use crate::native_object_state::{
    NativeOwnedChunk, NativeReadPin, NativeStageStart, STAGE_REPLACE,
};
use crate::ObjectError;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeObjectError {
    Refused(ObjectError),
    Data(ChunkLogError),
    /// Keep the original nonce, stage and append identity. Do not create a new attempt.
    Unknown(ObjectError),
}
impl std::fmt::Display for NativeObjectError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(formatter, "{error}"),
            Self::Data(error) => write!(formatter, "{error}"),
            Self::Unknown(error) => {
                write!(formatter, "native operation outcome is unknown: {error}")
            }
        }
    }
}
impl std::error::Error for NativeObjectError {}
impl From<ObjectError> for NativeObjectError {
    fn from(e: ObjectError) -> Self {
        Self::Refused(e)
    }
}
impl From<ChunkLogError> for NativeObjectError {
    fn from(e: ChunkLogError) -> Self {
        Self::Data(e)
    }
}
impl From<LogCallError> for NativeObjectError {
    fn from(e: LogCallError) -> Self {
        match e {
            LogCallError::Transport(e) => Self::Unknown(e),
            e => Self::Data(e.into()),
        }
    }
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::InvalidArgument(message.into())
}

#[derive(Clone, Debug)]
pub struct NativeStageHandle {
    pub start: NativeStageStart,
    pub revision: u64,
    pub chunks: Vec<NativeOwnedChunk>,
}
#[derive(Clone, Debug)]
pub struct NativePublishedObject {
    pub manifest: ChunkManifest,
    start: NativeStageStart,
    deadline: Instant,
}
#[derive(Clone, Debug)]
pub struct NativePinnedObject {
    pub manifest: ChunkManifest,
    pin: NativeReadPin,
}
/// Metadata is contacted at object/chunk transitions and read-pin boundaries, not for
/// every append to a leased initialized tail.
pub struct NativeObjectClient {
    meta: RemoteMetaClient,
    replica: ReplicaClientOptions,
    chunk_bytes: u64,
}
impl NativeObjectClient {
    pub fn new(meta: RemoteMetaClient, replica: ReplicaClientOptions) -> Self {
        Self {
            meta,
            replica,
            chunk_bytes: 64 * 1024 * 1024,
        }
    }
    pub fn with_chunk_bytes(mut self, bytes: u64) -> Result<Self, ObjectError> {
        if bytes == 0 || bytes > 512 * 1024 * 1024 {
            return Err(invalid("native chunk capacity is out of range"));
        }
        self.chunk_bytes = bytes;
        Ok(self)
    }
    fn call(
        &mut self,
        request: NativeObjectRequest,
    ) -> Result<(NativeObjectReply, Instant), NativeObjectError> {
        request.validate()?;
        let sent = Instant::now();
        let read = request.is_read();
        match self.meta.try_call(MetaRequest::NativeObject(request)) {
            MetaCallOutcome::Answered(MetaReply::NativeObject(reply)) => Ok((reply, sent)),
            MetaCallOutcome::Answered(_) => Err(NativeObjectError::Unknown(
                ObjectError::Corruption("unexpected native object reply".into()),
            )),
            MetaCallOutcome::Refused(e) if read => Err(NativeObjectError::Refused(e)),
            MetaCallOutcome::Refused(e) | MetaCallOutcome::Unreachable(e) => {
                Err(NativeObjectError::Unknown(e))
            }
        }
    }
    pub fn metadata_position(&mut self) -> Result<u64, NativeObjectError> {
        Ok(self
            .call(NativeObjectRequest::query(NativeObjectRead {
                kind: 4,
                ..Default::default()
            }))?
            .0
            .commit_index)
    }
    /// Begin a replacement in an explicitly owned chunk-catalog namespace. The original
    /// nonce identifies recovery; publication keeps any current object visible until commit.
    pub fn begin(
        &mut self,
        bucket: &str,
        key: &str,
        holder: &str,
        nonce: [u8; 32],
        length: u64,
        expected: Option<&ChunkManifest>,
        ttl_ms: u64,
    ) -> Result<NativeStageHandle, NativeObjectError> {
        let generation = expected
            .map_or(0, |m| m.generation)
            .checked_add(1)
            .ok_or_else(|| invalid("object generation exhausted"))?;
        if expected.is_some_and(|m| m.bucket != bucket || m.key != key) {
            return Err(invalid("expected object names differ").into());
        }
        let mut header = ChunkManifest {
            format_version: CHUNK_MANIFEST_PHYSICAL_FORMAT,
            bucket: bucket.into(),
            key: key.into(),
            generation,
            kind: CHUNK_KIND_LOG,
            initialized_log: Some(InitializedLogManifest::default()),
            ..Default::default()
        };
        if let Some(before) = expected {
            crate::chunk_manifest::ChunkAttributes::of(before).put_on(&mut header);
        }
        let start = NativeStageStart {
            kind: STAGE_REPLACE,
            header: Some(header),
            holder: holder.into(),
            request_nonce: nonce.to_vec(),
            target_length: length,
            expected_generation: generation - 1,
            expected_manifest_digest: expected
                .map(|m| {
                    m.to_bytes()
                        .map(|b| crate::chunk_integrity::sha256(&b).to_vec())
                })
                .transpose()?
                .unwrap_or_default(),
            ..Default::default()
        };
        let reply = self
            .call(NativeObjectRequest::mutation(
                Operation::Begin(NativeBegin {
                    start: Some(start),
                    ..Default::default()
                }),
                ttl_ms,
            ))?
            .0;
        Ok(NativeStageHandle {
            start: reply
                .start
                .ok_or_else(|| invalid("native begin omitted its stage"))?,
            revision: reply.stage_revision,
            chunks: reply.chunks,
        })
    }
    pub fn recover_stage(
        &mut self,
        bucket: &str,
        key: &str,
        holder: &str,
        nonce: [u8; 32],
    ) -> Result<NativeStageHandle, NativeObjectError> {
        let reply = self
            .call(NativeObjectRequest::query(NativeObjectRead {
                kind: 1,
                bucket: bucket.into(),
                key: key.into(),
                holder: holder.into(),
                request_nonce: nonce.to_vec(),
                ..Default::default()
            }))?
            .0;
        Ok(NativeStageHandle {
            start: reply
                .start
                .ok_or_else(|| invalid("native query omitted its stage"))?,
            revision: reply.stage_revision,
            chunks: reply.chunks,
        })
    }
    fn group(&mut self, id: &str) -> Result<ChunkGroupConfig, NativeObjectError> {
        let route = self.meta.read_chunk_route(id)?;
        let (endpoints, quorum) = route
            .standalone_star_route()?
            .ok_or_else(|| invalid("native object writes require a Star group"))?;
        let group = route.group.unwrap();
        if quorum != 3 || endpoints.len() != 3 || group.member_ids.len() != 3 {
            return Err(invalid("native object writes require three durable members").into());
        }
        Ok(group)
    }
    pub fn allocate(
        &mut self,
        stage: &mut NativeStageHandle,
        group_id: &str,
        length: u64,
    ) -> Result<NativeOwnedChunk, NativeObjectError> {
        if length == 0 || length > self.chunk_bytes {
            return Err(invalid("allocation exceeds configured native chunk capacity").into());
        }
        let group = self.group(group_id)?;
        let mut replicas = Vec::with_capacity(3);
        for (id, endpoint) in group.member_ids.iter().zip(&group.api_endpoints) {
            let mut client = RemoteReplicaClient::connect(endpoint, &self.replica)?;
            let probe = client.log_call(LogOpRequest::storage_probe())?;
            let root = probe
                .storage_incarnation()
                .ok_or_else(|| invalid("storage probe omitted its incarnation"))?;
            if !probe.has_storage_write_protocol(root) {
                return Err(invalid("storage publication writes are unavailable").into());
            }
            replicas.push(InitializedReplica {
                member_id: id.clone(),
                storage_incarnation: root.to_vec(),
                ..Default::default()
            });
        }
        let offset = stage
            .chunks
            .last()
            .map_or(Ok(stage.start.base_length), |c| {
                c.extent
                    .as_ref()
                    .unwrap()
                    .object_offset
                    .checked_add(c.extent.as_ref().unwrap().length)
                    .ok_or_else(|| invalid("stage offset overflow"))
            })?;
        let owned = NativeOwnedChunk {
            extent: Some(ChunkExtent {
                group: group_id.into(),
                kind: CHUNK_KIND_LOG,
                object_offset: offset,
                length,
                ..Default::default()
            }),
            initialization: Some(InitializedChunk {
                extent_index: stage
                    .chunks
                    .len()
                    .try_into()
                    .map_err(|_| invalid("too many chunks"))?,
                replicas,
                ..Default::default()
            }),
            ..Default::default()
        };
        let reply = self
            .call(NativeObjectRequest::mutation(
                Operation::Extend(NativeExtend {
                    stage: Some(stage.reference()),
                    chunk: Some(owned),
                }),
                0,
            ))?
            .0;
        let owned = reply
            .chunks
            .into_iter()
            .next()
            .ok_or_else(|| invalid("native extension omitted its allocation"))?;
        stage.revision = reply.stage_revision;
        stage.chunks.push(owned.clone());
        Ok(owned)
    }
    pub fn write_chunk(
        &mut self,
        stage: &NativeStageHandle,
        owned: &NativeOwnedChunk,
        data: &[u8],
        attempt: [u8; 16],
    ) -> Result<(), NativeObjectError> {
        let extent = owned
            .extent
            .as_ref()
            .ok_or_else(|| invalid("chunk range is missing"))?;
        if extent.length != data.len() as u64 || !stage.chunks.iter().any(|c| c == owned) {
            return Err(invalid("payload does not name an owned stage range").into());
        }
        let group = self.group(&extent.group)?;
        let binding = owned.initialization.as_ref().unwrap();
        let mut clients = Vec::with_capacity(3);
        let mut capacity = super::log_op::MAX_OP_DATA;
        let mut sealed = Vec::with_capacity(3);
        let mut expected_footer: Option<SealedFooter> = None;
        for (i, endpoint) in group.api_endpoints.iter().enumerate() {
            let root: [u8; 16] = binding.replicas[i]
                .storage_incarnation
                .as_slice()
                .try_into()
                .map_err(|_| invalid("invalid storage incarnation"))?;
            let init: [u8; 16] = binding.replicas[i]
                .initialization
                .as_slice()
                .try_into()
                .map_err(|_| invalid("invalid storage initialization"))?;
            let mut client = RemoteReplicaClient::connect(endpoint, &self.replica)?;
            let reply = client.log_call(
                LogOpRequest::initialize_publication(
                    extent.volume_id,
                    extent.chunk_id,
                    binding.chunk_generation,
                    binding.writer_epoch,
                    init,
                )
                .for_storage(root, init),
            )?;
            check_member(&reply, root, init, binding.writer_epoch)?;
            if reply.sealed {
                let expected = expected_footer.get_or_insert_with(|| SealedFooter {
                    length: data.len() as u64,
                    crc: crate::log_chunk::crc32c(data),
                    sha256: crate::chunk_integrity::sha256(data).to_vec(),
                });
                if reply.length != expected.length
                    || reply.durable_length != expected.length
                    || reply.published_length != expected.length
                    || reply.crc != expected.crc
                    || (!reply.sha256.is_empty() && reply.sha256 != expected.sha256)
                {
                    return Err(ChunkLogError::AttemptMismatch { offset: 0 }.into());
                }
            }
            capacity = capacity.min(capacity_of(&reply, &self.replica)?);
            sealed.push(reply.sealed);
            clients.push(client);
        }
        let identity = AppendIdentity {
            object_incarnation: stage
                .start
                .header()?
                .initialized_log
                .as_ref()
                .unwrap()
                .object_incarnation
                .as_slice()
                .try_into()
                .map_err(|_| invalid("invalid object incarnation"))?,
            attempt_id: attempt,
            request_offset: 0,
            request_length: stage.start.target_length,
        };
        for (part, body) in data.chunks(capacity).enumerate() {
            let offset = (part * capacity) as u64;
            let end = offset + body.len() as u64;
            let tag = identity.record_tag(
                extent.volume_id,
                extent.chunk_id,
                binding.chunk_generation,
                offset,
                extent.object_offset + offset,
                body.len() as u32,
            )?;
            for (i, client) in clients.iter_mut().enumerate() {
                let root = binding.replicas[i]
                    .storage_incarnation
                    .as_slice()
                    .try_into()
                    .unwrap();
                let init = binding.replicas[i]
                    .initialization
                    .as_slice()
                    .try_into()
                    .unwrap();
                let op = if sealed[i] {
                    LogOpRequest::identity_initialized_publication(
                        extent.volume_id,
                        extent.chunk_id,
                        binding.chunk_generation,
                        offset,
                        init,
                    )
                } else {
                    LogOpRequest::append_published(
                        extent.volume_id,
                        extent.chunk_id,
                        binding.chunk_generation,
                        binding.writer_epoch,
                        offset,
                        body.to_vec(),
                        tag,
                        end,
                        init,
                    )
                };
                let reply = client.log_call(op.for_storage(root, init))?;
                if sealed[i] {
                    check_member(&reply, root, init, binding.writer_epoch)?;
                    // A sealed footer binds the bytes, while each stored tag binds this
                    // original request. Equal payloads alone never acknowledge ownership.
                    if !reply.sealed
                        || reply.durable_length < end
                        || reply.published_length < end
                        || reply.record_identity()
                            != Some(crate::log_chunk::AppendRecordIdentity {
                                offset,
                                length: body.len() as u32,
                                attempt_tag: Some(tag),
                            })
                    {
                        return Err(ChunkLogError::AttemptMismatch { offset }.into());
                    }
                    let read = client.log_call(
                        LogOpRequest::read_initialized_publication(
                            extent.volume_id,
                            extent.chunk_id,
                            binding.chunk_generation,
                            offset,
                            body.len() as u32,
                            init,
                        )
                        .for_storage(root, init),
                    )?;
                    check_member(&read, root, init, binding.writer_epoch)?;
                    if !read.sealed || read.data.as_slice() != body {
                        return Err(ChunkLogError::AttemptMismatch { offset }.into());
                    }
                    continue;
                }
                check_append(
                    &reply,
                    root,
                    init,
                    binding.writer_epoch,
                    offset,
                    body.len() as u32,
                    tag,
                    end,
                )?;
            }
        }
        if extent
            .object_offset
            .checked_add(extent.length)
            .ok_or_else(|| invalid("stage range overflow"))?
            < stage.start.target_length
        {
            self.complete_chunk(owned, false)?;
        }
        Ok(())
    }
    fn complete_chunk(
        &mut self,
        owned: &NativeOwnedChunk,
        tail: bool,
    ) -> Result<ChunkExtent, NativeObjectError> {
        let mut extent = owned.extent.as_ref().unwrap().clone();
        let binding = owned.initialization.as_ref().unwrap();
        let group = self.group(&extent.group)?;
        let mut footer = None;
        for (i, endpoint) in group.api_endpoints.iter().enumerate() {
            let root = binding.replicas[i]
                .storage_incarnation
                .as_slice()
                .try_into()
                .unwrap();
            let init = binding.replicas[i]
                .initialization
                .as_slice()
                .try_into()
                .unwrap();
            let mut client = RemoteReplicaClient::connect(endpoint, &self.replica)?;
            let probe = client.log_call(
                LogOpRequest::publication_probe(
                    extent.volume_id,
                    extent.chunk_id,
                    binding.chunk_generation,
                )
                .for_storage(root, init),
            )?;
            check_member(&probe, root, init, binding.writer_epoch)?;
            let reply = if !tail && !probe.sealed {
                client.log_call(
                    LogOpRequest::seal_publication(
                        extent.volume_id,
                        extent.chunk_id,
                        binding.chunk_generation,
                        binding.writer_epoch,
                        init,
                        false,
                    )
                    .for_storage(root, init),
                )?
            } else {
                probe
            };
            check_member(&reply, root, init, binding.writer_epoch)?;
            if reply.published_length != extent.length
                || reply.durable_length != extent.length
                || (!tail && !reply.sealed)
                || (tail && reply.sealed)
            {
                return Err(invalid("stage publication lacks every durable complete chunk").into());
            }
            if !tail {
                let current = SealedFooter {
                    length: reply.length,
                    crc: reply.crc,
                    sha256: reply.sha256,
                };
                if footer.as_ref().is_some_and(|f| f != &current) {
                    return Err(invalid("sealed stage footers differ").into());
                }
                footer = Some(current);
            }
        }
        extent.flags = if tail {
            CHUNK_FLAG_TAIL
        } else {
            CHUNK_FLAG_SEALED
        };
        if let Some(footer) = footer {
            extent.footer_crc = footer.crc;
            extent.footer_sha256 = footer.sha256;
        }
        Ok(extent)
    }
    pub fn publish(
        &mut self,
        stage: &NativeStageHandle,
        open_tail: bool,
    ) -> Result<NativePublishedObject, NativeObjectError> {
        let mut manifest = stage.start.header()?.clone();
        manifest.length = stage.start.target_length;
        for (index, owned) in stage.chunks.iter().enumerate() {
            let tail = open_tail && index + 1 == stage.chunks.len();
            manifest.extents.push(self.complete_chunk(owned, tail)?);
            manifest
                .initialized_log
                .as_mut()
                .unwrap()
                .chunks
                .push(owned.initialization.as_ref().unwrap().clone());
        }
        manifest.validate()?;
        let (reply, sent) = self.call(NativeObjectRequest::mutation(
            Operation::Publish(NativePublish {
                stage: Some(stage.reference()),
                manifest: Some(manifest),
                expected_lease: None,
            }),
            0,
        ))?;
        let remaining = stage.start.expires_at_ms.saturating_sub(reply.now_ms);
        let deadline = sent
            .checked_add(Duration::from_millis(remaining))
            .ok_or_else(|| invalid("lease deadline overflow"))?;
        Ok(NativePublishedObject {
            manifest: reply
                .manifest
                .ok_or_else(|| invalid("native publication omitted its object"))?,
            start: stage.start.clone(),
            deadline,
        })
    }
    pub fn writer(
        &mut self,
        published: &NativePublishedObject,
    ) -> Result<NativeTailWriter, NativeObjectError> {
        let manifest = &published.manifest;
        let index = manifest
            .extents
            .len()
            .checked_sub(1)
            .ok_or_else(|| invalid("empty object has no tail"))?;
        let extent = &manifest.extents[index];
        if extent.flags != CHUNK_FLAG_TAIL {
            return Err(invalid("object has no open tail").into());
        }
        let binding = manifest
            .initialized_log
            .as_ref()
            .unwrap()
            .physical_binding(manifest, index)?;
        let group = self.group(&extent.group)?;
        if binding.member_ids.as_slice() != group.member_ids {
            return Err(invalid("tail route members changed").into());
        }
        let mut clients = Vec::with_capacity(3);
        let mut capacity = super::log_op::MAX_OP_DATA;
        for (i, endpoint) in group.api_endpoints.iter().enumerate() {
            let mut client = RemoteReplicaClient::connect(endpoint, &self.replica)?;
            let reply = client.log_call(
                LogOpRequest::publication_probe(
                    binding.volume_id,
                    binding.chunk_id,
                    binding.generation,
                )
                .for_storage(binding.storage_incarnations[i], binding.initializations[i]),
            )?;
            check_member(
                &reply,
                binding.storage_incarnations[i],
                binding.initializations[i],
                binding.writer_epoch,
            )?;
            if reply.published_length != extent.length {
                return Err(invalid("tail advanced before this creation writer opened").into());
            }
            capacity = capacity.min(capacity_of(&reply, &self.replica)?);
            clients.push(Some(client));
        }
        Ok(NativeTailWriter {
            binding,
            group,
            options: self.replica.clone(),
            clients,
            capacity,
            chunk_bytes: self.chunk_bytes,
            deadline: published.deadline,
            length: extent.length,
            object_offset: extent.object_offset,
            incarnation: manifest
                .initialized_log
                .as_ref()
                .unwrap()
                .object_incarnation
                .as_slice()
                .try_into()
                .unwrap(),
            pending: None,
            holder: published.start.holder.clone(),
        })
    }
    pub fn pin(
        &mut self,
        bucket: &str,
        key: &str,
        ttl_ms: u64,
    ) -> Result<NativePinnedObject, NativeObjectError> {
        let reply = self
            .call(NativeObjectRequest::mutation(
                Operation::OpenPin(NativeReadPin {
                    bucket: bucket.into(),
                    key: key.into(),
                    ..Default::default()
                }),
                ttl_ms,
            ))?
            .0;
        Ok(NativePinnedObject {
            manifest: reply
                .manifest
                .ok_or_else(|| invalid("native pin omitted its manifest"))?,
            pin: reply
                .pin
                .ok_or_else(|| invalid("native pin omitted its identity"))?,
        })
    }
    fn check_pin(&mut self, pin: &NativeReadPin) -> Result<(), NativeObjectError> {
        let reply = self
            .call(NativeObjectRequest::query(NativeObjectRead {
                kind: 3,
                pin_id: pin.pin_id,
                ..Default::default()
            }))?
            .0;
        if reply.pin.as_ref() != Some(pin) {
            return Err(invalid("read pin changed").into());
        }
        Ok(())
    }
    pub fn close_pin(&mut self, pinned: &NativePinnedObject) -> Result<(), NativeObjectError> {
        self.call(NativeObjectRequest::mutation(
            Operation::ClosePin(crate::native_object_outcome::NativePinRelease {
                pin_id: pinned.pin.pin_id,
                expected_expiry: pinned.pin.expires_at_ms,
            }),
            0,
        ))?;
        Ok(())
    }
    fn reader(
        &mut self,
        pinned: &NativePinnedObject,
        index: usize,
    ) -> Result<(ChunkLogReader, PhysicalMemberBinding), NativeObjectError> {
        let manifest = &pinned.manifest;
        let extent = &manifest.extents[index];
        let group = self.group(&extent.group)?;
        let binding = manifest
            .initialized_log
            .as_ref()
            .ok_or_else(|| invalid("pin lacks initialized chunks"))?
            .physical_binding(manifest, index)?;
        if binding.member_ids.as_slice() != group.member_ids {
            return Err(invalid("pinned route members changed").into());
        }
        Ok((
            ChunkLogReader::new(
                &group.api_endpoints,
                binding.volume_id,
                binding.chunk_id,
                binding.generation,
                self.replica.clone(),
            ),
            binding,
        ))
    }
    pub fn length(&mut self, pinned: &NativePinnedObject) -> Result<u64, NativeObjectError> {
        self.check_pin(&pinned.pin)?;
        let manifest = &pinned.manifest;
        if !manifest
            .extents
            .last()
            .is_some_and(|e| e.flags == CHUNK_FLAG_TAIL)
        {
            return Ok(manifest.length);
        }
        let index = manifest.extents.len() - 1;
        let (mut reader, binding) = self.reader(pinned, index)?;
        let mut error = None;
        let length = reader
            .physical_published_length(&binding, || self.pin_callback(&pinned.pin, &mut error));
        if let Some(error) = error {
            return Err(error);
        }
        Ok(manifest.extents[index].object_offset + length?)
    }
    fn pin_callback(
        &mut self,
        pin: &NativeReadPin,
        error: &mut Option<NativeObjectError>,
    ) -> Result<(), ChunkLogError> {
        match self.check_pin(pin) {
            Ok(()) => Ok(()),
            Err(e) => {
                *error = Some(e);
                Err(ChunkLogError::Refused(ObjectError::PreconditionFailed(
                    "authoritative native pin validation failed".into(),
                )))
            }
        }
    }
    pub fn read_range(
        &mut self,
        pinned: &NativePinnedObject,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, NativeObjectError> {
        offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("object range overflow"))?;
        self.check_pin(&pinned.pin)?;
        let mut bytes = Vec::with_capacity(length);
        let manifest = &pinned.manifest;
        while bytes.len() < length {
            let at = offset + bytes.len() as u64;
            let index = manifest
                .extents
                .partition_point(|e| e.object_offset <= at)
                .checked_sub(1)
                .ok_or_else(|| invalid("object range has no extent"))?;
            let extent = &manifest.extents[index];
            let physical = at - extent.object_offset;
            let count = if extent.flags == CHUNK_FLAG_TAIL {
                length - bytes.len()
            } else {
                let remaining = extent
                    .length
                    .checked_sub(physical)
                    .filter(|n| *n > 0)
                    .ok_or_else(|| invalid("read exceeds sealed object range"))?;
                (length - bytes.len()).min(remaining as usize)
            };
            let (mut reader, binding) = self.reader(pinned, index)?;
            let mut error = None;
            let value = if extent.flags == CHUNK_FLAG_TAIL {
                reader.read_physical_pinned(&binding, physical, count, || {
                    self.pin_callback(&pinned.pin, &mut error)
                })
            } else {
                reader.read_physical_sealed(
                    &binding,
                    &SealedFooter {
                        length: extent.length,
                        crc: extent.footer_crc,
                        sha256: extent.footer_sha256.clone(),
                    },
                    physical,
                    count,
                    || self.pin_callback(&pinned.pin, &mut error),
                )
            };
            if let Some(error) = error {
                return Err(error);
            }
            bytes.extend_from_slice(&value?);
        }
        self.check_pin(&pinned.pin)?;
        Ok(bytes)
    }
}
impl NativeStageHandle {
    pub fn reference(&self) -> NativeStageRef {
        NativeStageRef {
            stage_id: self.start.stage_id,
            expected_revision: self.revision,
            holder: self.start.holder.clone(),
        }
    }
}
fn check_member(
    reply: &LogOpReply,
    root: [u8; 16],
    init: [u8; 16],
    epoch: u64,
) -> Result<(), NativeObjectError> {
    if reply.epoch != epoch {
        return Err(ChunkLogError::StaleEpoch {
            current: reply.epoch,
        }
        .into());
    }
    if reply.storage_incarnation() != Some(root)
        || reply.publication_initialization() != Some(init)
        || !reply.publication_mode
    {
        return Err(invalid("unqualified initialized storage reply").into());
    }
    Ok(())
}
fn check_append(
    reply: &LogOpReply,
    root: [u8; 16],
    init: [u8; 16],
    epoch: u64,
    offset: u64,
    length: u32,
    tag: [u8; 16],
    published: u64,
) -> Result<(), NativeObjectError> {
    // Only a successful response to this exact tagged APPEND reaches here. Each
    // socket has one exchange in flight; storage checks the original range, tag
    // and bytes before replying. A tail probe or an uncertain exchange is not an ACK.
    check_member(reply, root, init, epoch)?;
    if reply.durable_length < offset + u64::from(length)
        || reply.published_length < published
        || reply
            .record_identity()
            .is_some_and(|r| r.offset != offset || r.length != length || r.attempt_tag != Some(tag))
    {
        return Err(ChunkLogError::AttemptMismatch { offset }.into());
    }
    Ok(())
}
fn capacity_of(reply: &LogOpReply, options: &ReplicaClientOptions) -> Result<usize, ObjectError> {
    usize::try_from(reply.publication_payload_limit)
        .ok()
        .map(|n| {
            n.min(options.max_payload_bytes)
                .min(super::log_op::MAX_OP_DATA + 512)
        })
        .and_then(|n| n.checked_sub(512))
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("native storage payload capacity is too small"))
}
struct PendingAppend {
    identity: AppendIdentity,
    progress: usize,
    fragment: Option<std::sync::Arc<Vec<u8>>>,
}
/// One existing initialized tail. A pending whole append owns at most one record buffer;
/// only its final record advances the complete-prefix marker.
pub struct NativeTailWriter {
    binding: PhysicalMemberBinding,
    group: ChunkGroupConfig,
    options: ReplicaClientOptions,
    clients: Vec<Option<RemoteReplicaClient>>,
    capacity: usize,
    chunk_bytes: u64,
    deadline: Instant,
    length: u64,
    object_offset: u64,
    incarnation: [u8; 16],
    pending: Option<PendingAppend>,
    holder: String,
}
impl NativeTailWriter {
    pub fn length(&self) -> u64 {
        self.object_offset + self.length
    }
    pub fn holder(&self) -> &str {
        &self.holder
    }
    pub fn pending_identity(&self) -> Option<AppendIdentity> {
        self.pending.as_ref().map(|p| p.identity)
    }
    pub fn append(
        &mut self,
        data: &[u8],
        identity: AppendIdentity,
    ) -> Result<u64, NativeObjectError> {
        let result = self.append_inner(data, identity);
        if matches!(
            &result,
            Err(NativeObjectError::Data(
                ChunkLogError::StaleEpoch { .. } | ChunkLogError::Sealed
            ))
        ) {
            self.deadline = Instant::now();
        }
        match result {
            Err(error)
                if self.pending.is_some() && !matches!(&error, NativeObjectError::Unknown(_)) =>
            {
                Err(NativeObjectError::Unknown(ObjectError::PreconditionFailed(
                    format!("original native append remains unsettled: {error:?}"),
                )))
            }
            other => other,
        }
    }
    fn append_inner(
        &mut self,
        data: &[u8],
        identity: AppendIdentity,
    ) -> Result<u64, NativeObjectError> {
        if data.is_empty()
            || identity.attempt_id == [0; 16]
            || identity.object_incarnation != self.incarnation
            || identity.request_offset != self.length()
            || identity.request_length != data.len() as u64
        {
            return Err(invalid("native append does not bind the original object range").into());
        }
        let end = self
            .length
            .checked_add(data.len() as u64)
            .filter(|n| *n <= self.chunk_bytes)
            .ok_or_else(|| invalid("native append needs a chunk switch"))?;
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.identity != identity)
        {
            return Err(invalid("settle the original pending append first").into());
        }
        let pending = self.pending.get_or_insert_with(|| PendingAppend {
            identity,
            progress: 0,
            fragment: None,
        });
        while pending.progress < data.len() {
            if Instant::now() >= self.deadline {
                return Err(NativeObjectError::Unknown(invalid(
                    "native writer lease deadline reached",
                )));
            }
            let count = (data.len() - pending.progress).min(self.capacity);
            let bytes = &data[pending.progress..pending.progress + count];
            if let Some(fragment) = &pending.fragment {
                if fragment.as_slice() != bytes {
                    return Err(invalid("pending append bytes changed").into());
                }
            } else {
                pending.fragment = Some(std::sync::Arc::new(bytes.to_vec()));
            }
            let offset = self.length + pending.progress as u64;
            let published = if pending.progress + count == data.len() {
                end
            } else {
                self.length
            };
            let tag = identity.record_tag(
                self.binding.volume_id,
                self.binding.chunk_id,
                self.binding.generation,
                offset,
                self.object_offset + offset,
                count as u32,
            )?;
            append_to_members(
                &self.binding,
                &self.group,
                &self.options,
                &mut self.clients,
                pending.fragment.as_ref().unwrap(),
                offset,
                tag,
                published,
                self.deadline,
            )?;
            pending.progress += count;
            pending.fragment = None;
        }
        if Instant::now() >= self.deadline {
            return Err(NativeObjectError::Unknown(invalid(
                "native append confirmation exceeded its lease deadline",
            )));
        }
        self.length = end;
        self.pending = None;
        Ok(self.length())
    }
}

// All requests are started before waiting for replies. The payload remains owned by
// the original pending attempt; each member has only its own small frame metadata.
fn append_to_members(
    binding: &PhysicalMemberBinding,
    group: &ChunkGroupConfig,
    options: &ReplicaClientOptions,
    clients: &mut [Option<RemoteReplicaClient>],
    data: &std::sync::Arc<Vec<u8>>,
    offset: u64,
    tag: [u8; 16],
    published: u64,
    deadline: Instant,
) -> Result<(), NativeObjectError> {
    let mut errors: [Option<NativeObjectError>; 3] = std::array::from_fn(|_| None);
    for (member, client) in clients.iter_mut().enumerate() {
        if client.is_none() {
            match RemoteReplicaClient::connect(&group.api_endpoints[member], options) {
                Ok(connected) => *client = Some(connected),
                Err(error) => errors[member] = Some(NativeObjectError::Unknown(error)),
            }
        }
    }
    let mut results: [Option<Result<super::message::ReplicaReply, ObjectError>>; 3] =
        std::array::from_fn(|_| None);
    {
        let mut pending: [Option<super::client::PendingReplicaReply<'_>>; 3] =
            std::array::from_fn(|_| None);
        for (member, client) in clients.iter_mut().enumerate() {
            if errors[member].is_some() {
                continue;
            }
            let request = LogOpRequest::append_published(
                binding.volume_id,
                binding.chunk_id,
                binding.generation,
                binding.writer_epoch,
                offset,
                Vec::new(),
                tag,
                published,
                binding.initializations[member],
            )
            .for_storage(
                binding.storage_incarnations[member],
                binding.initializations[member],
            );
            let frame = super::shared_log_append_request::SharedLogAppendRequest::new(
                request,
                std::sync::Arc::clone(data),
            )?;
            match client
                .as_mut()
                .unwrap()
                .begin_shared_log_before(frame, Some(deadline))
            {
                Ok(reply) => pending[member] = Some(reply),
                Err(error) => results[member] = Some(Err(error)),
            }
        }
        while pending.iter().any(Option::is_some) {
            let mut progress = false;
            for (member, slot) in pending.iter_mut().enumerate() {
                let result = slot.as_mut().and_then(|reply| {
                    let before = reply.transferred_bytes();
                    let result = reply.poll();
                    progress |= result.is_some() || reply.transferred_bytes() != before;
                    result
                });
                if let Some(result) = result {
                    results[member] = Some(result);
                    *slot = None;
                }
            }
            if !progress {
                super::replica_set::wait_for_pending(&pending);
            }
        }
    }
    let mut first = None;
    for member in 0..3 {
        if errors[member].is_none() {
            let unusable = clients[member]
                .as_ref()
                .unwrap()
                .needs_standalone_reconnect();
            errors[member] = match results[member].take().unwrap() {
                Ok(super::message::ReplicaReply::LogOp(reply)) => check_append(
                    &reply,
                    binding.storage_incarnations[member],
                    binding.initializations[member],
                    binding.writer_epoch,
                    offset,
                    data.len() as u32,
                    tag,
                    published,
                )
                .err(),
                Ok(_) => Some(NativeObjectError::Unknown(ObjectError::Corruption(
                    "unexpected native append reply".into(),
                ))),
                Err(error) => Some(LogCallError::from_object(error, unusable).into()),
            };
            if unusable {
                clients[member] = None;
            }
        }
        if let Some(error) = errors[member].take() {
            let fatal = matches!(
                &error,
                NativeObjectError::Data(ChunkLogError::StaleEpoch { .. } | ChunkLogError::Sealed)
            );
            if first.is_none() || fatal {
                first = Some(error);
            }
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
mod shared_write_tests {
    use super::*;
    use std::sync::Arc;
    struct Nodes {
        root: std::path::PathBuf,
        stops: Vec<super::super::ReplicaNodeShutdown>,
        workers: Vec<std::thread::JoinHandle<std::io::Result<()>>>,
        endpoints: Vec<String>,
    }
    impl Drop for Nodes {
        fn drop(&mut self) {
            for stop in &self.stops {
                stop.stop();
            }
            for worker in self.workers.drain(..) {
                worker.join().unwrap().unwrap();
            }
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    #[test]
    fn native_append_reaches_every_member_before_waiting_for_any_acknowledgment() {
        let root = std::env::temp_dir().join(format!("native-shared-write-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let mut nodes = Nodes {
            root,
            stops: Vec::new(),
            workers: Vec::new(),
            endpoints: Vec::new(),
        };
        let mut roots = [[0; 16]; 3];
        let initializations = [[1; 16], [2; 16], [3; 16]];
        for member in 0..3 {
            let directory = nodes.root.join(format!("node-{member}"));
            std::fs::create_dir(&directory).unwrap();
            let server = super::super::ReplicaNodeServer::bind_with_journal(
                "127.0.0.1:0",
                super::super::ReplicaNodeOptions {
                    publication_writes: true,
                    ..Default::default()
                },
                &directory.join("journal"),
            )
            .unwrap();
            let endpoint = server.local_addr().unwrap().to_string();
            nodes.stops.push(server.shutdown_handle());
            nodes
                .workers
                .push(std::thread::spawn(move || server.serve()));
            let mut client =
                RemoteReplicaClient::connect(&endpoint, &ReplicaClientOptions::default()).unwrap();
            roots[member] = client
                .log_call(LogOpRequest::storage_probe())
                .unwrap()
                .storage_incarnation()
                .unwrap();
            client
                .log_call(
                    LogOpRequest::initialize_publication(3, 11, 1, 7, initializations[member])
                        .for_storage(roots[member], initializations[member]),
                )
                .unwrap();
            nodes.endpoints.push(endpoint);
        }
        let gate = Arc::new((std::sync::Mutex::new(0usize), std::sync::Condvar::new()));
        let proxies: Vec<_> = nodes
            .endpoints
            .iter()
            .map(|endpoint| {
                super::super::chunk_log_tests::FaultProxy::with_operation_gate(
                    endpoint.clone(),
                    0,
                    false,
                    Some(Arc::clone(&gate)),
                    super::super::log_op::OP_APPEND,
                )
            })
            .collect();
        let endpoints: Vec<_> = proxies.iter().map(|p| p.addr.clone()).collect();
        let group = ChunkGroupConfig {
            api_endpoints: endpoints.clone(),
            ..Default::default()
        };
        let binding = PhysicalMemberBinding {
            volume_id: 3,
            chunk_id: 11,
            generation: 1,
            writer_epoch: 7,
            storage_incarnations: roots,
            initializations,
            member_ids: ["a".into(), "b".into(), "c".into()],
        };
        let mut writer = NativeTailWriter {
            binding,
            group,
            options: ReplicaClientOptions {
                read_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            clients: (0..3).map(|_| None).collect(),
            capacity: 8192,
            chunk_bytes: 64 << 20,
            deadline: Instant::now() + Duration::from_secs(10),
            length: 0,
            object_offset: 0,
            incarnation: [4; 16],
            pending: None,
            holder: "writer".into(),
        };
        let body = vec![77; 4096];
        let identity = AppendIdentity {
            object_incarnation: [4; 16],
            attempt_id: [9; 16],
            request_offset: 0,
            request_length: 4096,
        };
        assert_eq!(writer.append(&body, identity).unwrap(), 4096);
        assert_eq!(*gate.0.lock().unwrap(), 3);
        assert!(writer.pending_identity().is_none());
        for (member, endpoint) in nodes.endpoints.iter().enumerate() {
            let mut client =
                RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
            let reply = client
                .log_call(
                    LogOpRequest::read_initialized_publication(
                        3,
                        11,
                        1,
                        0,
                        4096,
                        initializations[member],
                    )
                    .for_storage(roots[member], initializations[member]),
                )
                .unwrap();
            assert_eq!(reply.data, body);
            assert_eq!(reply.durable_length, 4096);
        }
        assert!(proxies
            .iter()
            .all(|p| p.operations.lock().unwrap().as_slice() == [super::super::log_op::OP_APPEND]));
    }
}
