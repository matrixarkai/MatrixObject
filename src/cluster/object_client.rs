// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded object writes across metadata and data quorums.
use super::{ChunkGroupConfig, RemoteMetaClient, RemoteReplicaClient, ReplicaClientOptions};
use crate::object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest};
use crate::ObjectError;

#[derive(Clone, Debug)]
pub struct ObjectWriteOptions {
    pub max_object_bytes: usize,
    /// Each page is at most 1 MiB, leaving room in the replicated outcome envelope.
    pub page_bytes: usize,
    /// Maximum number of configured data groups selected by placement.
    pub target_groups: u32,
    pub update_time_ms: u64,
    pub replica: ReplicaClientOptions,
}
impl Default for ObjectWriteOptions {
    fn default() -> Self {
        Self {
            max_object_bytes: 64 * 1024 * 1024,
            page_bytes: 1024 * 1024,
            target_groups: 1,
            update_time_ms: 0,
            replica: ReplicaClientOptions::default(),
        }
    }
}

#[derive(Debug)]
pub enum ObjectWriteFailure {
    /// Publication was not admitted. Some candidate pages may already exist.
    BeforePublication {
        error: ObjectError,
        staged: Option<Box<ObjectManifest>>,
    },
    /// Publication was attempted but its exact result is not confirmed visible.
    /// A different current generation does not prove this attempt never committed.
    PublicationUnconfirmed {
        error: ObjectError,
        outcome: Box<ManifestOutcome>,
    },
}
impl std::fmt::Display for ObjectWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforePublication { error, .. } => write!(f, "object was not published: {error}"),
            Self::PublicationUnconfirmed { error, .. } => {
                write!(f, "object publication is unconfirmed: {error}")
            }
        }
    }
}
impl std::error::Error for ObjectWriteFailure {}
fn before(error: ObjectError) -> ObjectWriteFailure {
    ObjectWriteFailure::BeforePublication {
        error,
        staged: None,
    }
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::InvalidArgument(message.into())
}

fn preflight(
    bucket: &str,
    key: &str,
    expected: Option<&ObjectManifest>,
    length: usize,
    options: &ObjectWriteOptions,
) -> Result<(u32, u64), ObjectError> {
    crate::validate_bucket(bucket)?;
    crate::validate_key(key)?;
    if length > options.max_object_bytes
        || !(1..=1024 * 1024).contains(&options.page_bytes)
        || !(1..=4096).contains(&options.target_groups)
        || options.replica.max_payload_bytes < options.page_bytes + 256
    {
        return Err(invalid("invalid object write size or options"));
    }
    let pages = length.div_ceil(options.page_bytes);
    if pages > 4096 {
        return Err(invalid("object write exceeds 4096 pages"));
    }
    let generation = if let Some(previous) = expected {
        previous.validate()?;
        if previous.bucket != bucket || previous.key != key {
            return Err(invalid("expected manifest names another object"));
        }
        previous
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("object generation exhausted"))?
    } else {
        1
    };
    Ok((pages as u32, generation))
}

fn connect_group(
    group: &ChunkGroupConfig,
    options: &ReplicaClientOptions,
) -> Result<RemoteReplicaClient, ObjectError> {
    let identity = group
        .identity
        .as_ref()
        .ok_or_else(|| ObjectError::Corruption("chunk group identity missing".into()))?;
    RemoteReplicaClient::connect_verified_raft_catalog(&group.api_endpoints, identity, options)
}

impl RemoteMetaClient {
    /// Write fresh pages, verify them and conditionally publish one generation.
    /// Data errors never trigger automatic writes or publication. Pages are not
    /// reclaimed on failure because they may still be in flight or held by readers.
    pub fn write_object(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
        options: &ObjectWriteOptions,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        let (pages, generation) =
            preflight(bucket, key, expected.as_ref(), data.len(), options).map_err(before)?;
        let current = match self.read_manifest(bucket, key) {
            Ok(value) => Some(value),
            Err(ObjectError::NotFound(_)) => None,
            Err(error) => return Err(before(error)),
        };
        if current != expected {
            return Err(before(ObjectError::PreconditionFailed(
                "object manifest changed before write".into(),
            )));
        }
        let mut manifest = ObjectManifest {
            format_version: 1,
            bucket: bucket.into(),
            key: key.into(),
            generation,
            length: data.len() as u64,
            extents: Vec::new(),
            deleted: false,
        };
        let mut groups = std::collections::BTreeMap::new();
        let mut connection: Option<(String, RemoteReplicaClient)> = None;
        if pages > 0 {
            let count = options.target_groups.min(pages);
            let plan = self
                .plan_object_placement(bucket, key, manifest.length, u64::from(count))
                .map_err(before)?;
            if plan.node_ids.len() != count as usize
                || plan
                    .node_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != count as usize
            {
                return Err(before(ObjectError::Corruption(
                    "placement returned invalid data groups".into(),
                )));
            }
            // Verify every selected group before allocation or data writes. Visit
            // the first write target last so its verified connection can be reused.
            // Keep configurations only for this operation and retain one connection.
            for group in plan.node_ids.iter().rev() {
                let configured = self.read_chunk_group(group).map_err(before)?;
                let client = connect_group(&configured, &options.replica).map_err(before)?;
                groups.insert(group.clone(), configured);
                connection = Some((group.clone(), client));
            }
            let allocation = self.allocate_object_pages(pages).map_err(before)?;
            for (index, bytes) in data.chunks(options.page_bytes).enumerate() {
                let page = allocation.page(index as u32).map_err(before)?;
                manifest.extents.push(ObjectExtent {
                    group: plan.node_ids[index % plan.node_ids.len()].clone(),
                    volume_id: page.chunk_id.volume_id(),
                    chunk_id: page.chunk_id.id(),
                    page_index: page.page_index,
                    object_offset: (index * options.page_bytes) as u64,
                    page_offset: 0,
                    length: bytes.len() as u64,
                    write_sequence: 1,
                    data_checksum: crate::fnv1a64(bytes),
                });
            }
        }
        let outcome = ManifestOutcome::decide(expected, manifest.clone()).map_err(before)?;
        let staged_error = |error| ObjectWriteFailure::BeforePublication {
            error,
            staged: Some(Box::new(manifest.clone())),
        };
        // Visit each group once while retaining the verified first target. The
        // manifest remains in object order; only physical I/O is reordered.
        let first_group = connection.as_ref().map(|(group, _)| group.as_str());
        let mut order: Vec<_> = (0..manifest.extents.len()).collect();
        order.sort_by_key(|index| {
            let group = manifest.extents[*index].group.as_str();
            (Some(group) != first_group, group)
        });
        for index in order {
            let extent = &manifest.extents[index];
            let start = index * options.page_bytes;
            let tail = &data[start..];
            let bytes = &tail[..tail.len().min(options.page_bytes)];
            if connection.as_ref().map(|(group, _)| group) != Some(&extent.group) {
                connection = Some((
                    extent.group.clone(),
                    connect_group(
                        groups.get(&extent.group).ok_or_else(|| {
                            staged_error(ObjectError::Corruption(
                                "object extent group was not verified".into(),
                            ))
                        })?,
                        &options.replica,
                    )
                    .map_err(&staged_error)?,
                ));
            }
            let page = crate::PageId {
                chunk_id: crate::ChunkId::new(extent.volume_id, extent.chunk_id)
                    .map_err(&staged_error)?,
                page_index: extent.page_index,
            };
            let client = &mut connection.as_mut().unwrap().1;
            client
                .write_chunk_if_sequence(page.clone(), 0, bytes, 0)
                .map_err(&staged_error)?;
            let read = client
                .read_chunk(&page, 0, extent.length)
                .map_err(&staged_error)?;
            if read.write_sequence != 1 || read.data != bytes {
                return Err(staged_error(ObjectError::Corruption(
                    "written page verification failed".into(),
                )));
            }
        }
        let (publication, unadmitted) =
            self.publish_manifest_attempt(outcome.clone(), options.update_time_ms);
        match publication {
            Ok(value) => Ok(value),
            Err(error) if unadmitted => Err(staged_error(error)),
            Err(error) => {
                // Never replay publication. Only the exact resulting value proves
                // current visibility; a failed read or another value stays uncertain.
                if self.read_manifest(bucket, key).ok().as_ref() == Some(&manifest) {
                    Ok(manifest)
                } else {
                    Err(ObjectWriteFailure::PublicationUnconfirmed {
                        error,
                        outcome: Box::new(outcome),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{MetaClientOptions, MetaReply, MetaRequest, MetaRequestCode, StatusCode};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};
    fn accept(listener: &TcpListener) -> TcpStream {
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    return stream;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < end =>
                {
                    std::thread::sleep(Duration::from_millis(5))
                }
                other => panic!("missing client connection: {other:?}"),
            }
        }
    }
    fn receive(stream: &mut TcpStream) -> MetaRequest {
        let mut header = [0; 6];
        stream.read_exact(&mut header).unwrap();
        assert_eq!(header[0], crate::cluster::PROTOCOL_STAMP);
        let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        assert!(size <= 4096);
        let mut payload = vec![0; size];
        stream.read_exact(&mut payload).unwrap();
        MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &payload).unwrap()
    }
    #[test]
    fn object_write_preflight_bounds_work_and_checks_expected_generation() {
        let mut options = ObjectWriteOptions::default();
        assert_eq!(
            preflight("bucket", "key", None, 0, &options).unwrap(),
            (0, 1)
        );
        assert_eq!(
            preflight("bucket", "key", None, options.page_bytes + 1, &options).unwrap(),
            (2, 1)
        );
        assert!(preflight(
            "bucket",
            "key",
            None,
            options.max_object_bytes + 1,
            &options
        )
        .is_err());
        options.page_bytes = 1;
        assert!(preflight("bucket", "key", None, 4097, &options).is_err());
        options.page_bytes = 0;
        assert!(preflight("bucket", "key", None, 0, &options).is_err());
        options = ObjectWriteOptions::default();
        options.target_groups = 0;
        assert!(preflight("bucket", "key", None, 0, &options).is_err());
        options = ObjectWriteOptions::default();
        options.replica.max_payload_bytes = options.page_bytes;
        assert!(preflight("bucket", "key", None, 1, &options).is_err());
        options = ObjectWriteOptions::default();
        let mut previous = ObjectManifest {
            format_version: 1,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 4,
            length: 0,
            extents: vec![],
            deleted: true,
        };
        assert_eq!(
            preflight("bucket", "key", Some(&previous), 0, &options).unwrap(),
            (0, 5)
        );
        assert!(preflight("bucket", "other", Some(&previous), 0, &options).is_err());
        previous.generation = u64::MAX;
        assert!(preflight("bucket", "key", Some(&previous), 0, &options).is_err());
    }
    #[test]
    fn typed_publication_refusals_do_not_become_uncertain_or_replay() {
        for catalog in [false, true] {
            for typed in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                listener.set_nonblocking(true).unwrap();
                let endpoint = listener.local_addr().unwrap().to_string();
                let worker = std::thread::spawn(move || {
                    let mut stream = accept(&listener);
                    assert!(matches!(
                        receive(&mut stream),
                        MetaRequest::ReadManifest { .. }
                    ));
                    stream
                        .write_all(&crate::cluster::message::encode_failure(
                            StatusCode::NotFound,
                            "object missing",
                        ))
                        .unwrap();
                    let MetaRequest::PublishManifest { outcome, .. } = receive(&mut stream) else {
                        panic!("publication missing");
                    };
                    if typed {
                        stream
                            .write_all(&crate::cluster::meta_message::encode_meta_reply(
                                &MetaReply::NotLeader,
                            ))
                            .unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_millis(200)))
                            .unwrap();
                        let mut byte = [0];
                        match stream.read(&mut byte) {
                            Ok(0) => {},
                            Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {},
                            other => panic!("unadmitted publication must not be replayed or read back: {other:?}"),
                        }
                    } else {
                        // Identical text in an ordinary failure is not admission evidence.
                        stream
                            .write_all(&crate::cluster::message::encode_failure(
                                StatusCode::PreconditionFailed,
                                "metadata request was not admitted by a leader",
                            ))
                            .unwrap();
                        assert!(matches!(
                            receive(&mut stream),
                            MetaRequest::ReadManifest { .. }
                        ));
                        stream
                            .write_all(&crate::cluster::message::encode_failure(
                                StatusCode::NotFound,
                                "object missing",
                            ))
                            .unwrap();
                    }
                    outcome
                });
                let options = MetaClientOptions {
                    read_timeout: Duration::from_secs(2),
                    ..Default::default()
                };
                let mut client = if catalog {
                    RemoteMetaClient::connect_catalog(&[endpoint], &options).unwrap()
                } else {
                    RemoteMetaClient::connect(&endpoint, &options).unwrap()
                };
                let result =
                    client.write_object("bucket", "key", None, b"", &ObjectWriteOptions::default());
                let attempted = worker.join().unwrap();
                if typed {
                    let ObjectWriteFailure::BeforePublication {
                        staged: Some(staged),
                        ..
                    } = result.unwrap_err()
                    else {
                        panic!("typed refusal must preserve the unadmitted candidate");
                    };
                    assert_eq!(*staged, attempted.after.unwrap());
                } else {
                    let ObjectWriteFailure::PublicationUnconfirmed { outcome, .. } =
                        result.unwrap_err()
                    else {
                        panic!("ordinary error text cannot prove non-admission");
                    };
                    assert_eq!(*outcome, attempted);
                }
            }
        }
    }

    #[test]
    fn lost_publication_replies_are_confirmed_by_exact_readback_without_replay() {
        for mode in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let worker = std::thread::spawn(move || {
                let mut stream = accept(&listener);
                assert!(matches!(
                    receive(&mut stream),
                    MetaRequest::ReadManifest { .. }
                ));
                stream
                    .write_all(&crate::cluster::message::encode_failure(
                        StatusCode::NotFound,
                        "object missing",
                    ))
                    .unwrap();
                let MetaRequest::PublishManifest { outcome, .. } = receive(&mut stream) else {
                    panic!("publication missing");
                };
                drop(stream); // The publication could have committed, but the reply is lost.
                let mut stream = accept(&listener);
                assert!(matches!(
                    receive(&mut stream),
                    MetaRequest::ReadManifest { .. }
                ));
                if mode != 2 {
                    let mut value = outcome.after.clone().unwrap();
                    if mode == 1 {
                        value.generation += 1;
                    }
                    stream
                        .write_all(&crate::cluster::meta_message::encode_meta_reply(
                            &MetaReply::Manifest(value),
                        ))
                        .unwrap();
                }
                outcome
            });
            let mut client = RemoteMetaClient::connect_catalog(
                &[endpoint],
                &MetaClientOptions {
                    read_timeout: Duration::from_secs(2),
                    ..Default::default()
                },
            )
            .unwrap();
            let result =
                client.write_object("bucket", "key", None, b"", &ObjectWriteOptions::default());
            let attempted = worker.join().unwrap();
            if mode == 0 {
                assert_eq!(result.unwrap(), attempted.after.unwrap());
            } else {
                let ObjectWriteFailure::PublicationUnconfirmed { outcome, .. } =
                    result.unwrap_err()
                else {
                    panic!("publication must stay unconfirmed");
                };
                assert_eq!(*outcome, attempted);
            }
        }
    }
}
