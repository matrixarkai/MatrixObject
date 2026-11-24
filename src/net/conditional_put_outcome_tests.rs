// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::cluster::ObjectWriteFailure;
use crate::net::cluster_gateway::{ClusterGateway, GatewayOptions, ObjectPlane, PlaneFailure};
use crate::net::{Request, RequestHandler};
use crate::object_manifest::{ManifestListPage, ManifestOutcome, ObjectExtent, ObjectManifest};
use crate::ObjectError;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Race {
    current: Option<(ObjectManifest, Vec<u8>)>,
    attempts: usize,
}
struct ForeignWriterPlane(Arc<Mutex<Race>>);

fn manifest(generation: u64, chunk: u64, data: &[u8]) -> ObjectManifest {
    ObjectManifest {
        format_version: 2,
        bucket: "b".into(),
        key: "key".into(),
        generation,
        length: data.len() as u64,
        extents: if data.is_empty() {
            Vec::new()
        } else {
            vec![ObjectExtent {
                group: "group".into(),
                volume_id: 1,
                chunk_id: chunk,
                length: data.len() as u64,
                write_sequence: 1,
                data_sha256: vec![7; 32],
                ..Default::default()
            }]
        },
        deleted: false,
    }
}
impl ObjectPlane for ForeignWriterPlane {
    fn read_manifest(&mut self, _: &str, _: &str) -> Result<ObjectManifest, ObjectError> {
        self.0
            .lock()
            .unwrap()
            .current
            .as_ref()
            .map(|(m, _)| m.clone())
            .ok_or_else(|| ObjectError::NotFound("missing".into()))
    }
    fn write(
        &mut self,
        _: &str,
        _: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        let mut shared = self.0.lock().unwrap();
        assert_eq!(shared.current.as_ref().map(|(m, _)| m.clone()), expected);
        shared.attempts += 1;
        let generation = expected.as_ref().map_or(1, |m| m.generation + 1);
        let intended = manifest(generation, 2, data);
        let outcome = ManifestOutcome::decide(expected, intended).unwrap();
        // A competing operation publishes the same bytes using its own pages.
        // This operation's response is lost without its publication taking place.
        shared.current = Some((manifest(generation, 3, data), data.to_vec()));
        Err(ObjectWriteFailure::PublicationUnconfirmed {
            error: ObjectError::Corruption("publication response lost".into()),
            outcome: Box::new(outcome),
        }
        .into())
    }
    fn append(&mut self, _: ObjectManifest, _: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
        Err(ObjectError::InvalidArgument("unexpected append".into()).into())
    }
    fn read(&mut self, bucket: &str, _: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        // the bucket "b" has its record
        if bucket == crate::bucket_config::SYSTEM_BUCKET {
            let bytes = crate::bucket_config::BucketConfig::new(0).to_bytes()?;
            let mut record = manifest(1, 0, &bytes);
            record.bucket = bucket.into();
            return Ok((record, bytes));
        }
        self.0
            .lock()
            .unwrap()
            .current
            .clone()
            .ok_or_else(|| ObjectError::NotFound("missing".into()))
    }
    fn read_range(
        &mut self,
        _: &str,
        _: &str,
        _: u64,
        _: u64,
    ) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        Err(ObjectError::InvalidArgument("unexpected range read".into()))
    }
    fn delete(&mut self, _: &str, _: &str) -> Result<ObjectManifest, ObjectError> {
        Err(ObjectError::InvalidArgument("unexpected delete".into()))
    }
    fn list(&mut self, _: &str, _: Option<&str>, _: u32) -> Result<ManifestListPage, ObjectError> {
        Err(ObjectError::InvalidArgument("unexpected listing".into()))
    }
}
fn raced_put(seed: bool, headers: &[(&str, &str)], data: &[u8]) -> (u16, String) {
    let state = Arc::new(Mutex::new(Race {
        current: seed.then(|| (manifest(1, 1, b"before"), b"before".to_vec())),
        attempts: 0,
    }));
    let handle = Arc::clone(&state);
    let gateway = ClusterGateway::new(
        move || Ok(Box::new(ForeignWriterPlane(Arc::clone(&handle))) as Box<dyn ObjectPlane>),
        GatewayOptions::default(),
        "b",
    );
    let response = gateway.handle(&Request {
        method: "PUT".into(),
        path: "/b/key".into(),
        query: String::new(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        body: data.to_vec(),
        keep_alive: false,
    });
    let state = state.lock().unwrap();
    assert_eq!(
        state.attempts, 1,
        "an uncertain mutation must not be repeated"
    );
    let (stored, bytes) = state.current.as_ref().unwrap();
    assert_eq!(bytes, data);
    if !data.is_empty() {
        assert_eq!(stored.extents[0].chunk_id, 3);
    }
    (response.status, String::from_utf8(response.body).unwrap())
}
#[test]
fn conditional_create_does_not_claim_another_writers_identical_value() {
    let (status, body) = raced_put(false, &[("if-none-match", "*")], b"same");
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("unconfirmed"));
}
#[test]
fn conditional_replace_does_not_claim_another_writers_identical_value() {
    let (status, body) = raced_put(true, &[("if-match", "\"gen-1\"")], b"same");
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("unconfirmed"));
}
#[test]
fn conditional_empty_create_remains_unknown_without_attempt_identity() {
    let (status, body) = raced_put(false, &[("if-none-match", "*")], b"");
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("unconfirmed"));
}
#[test]
fn unconditional_put_retains_idempotent_value_resolution() {
    let (status, _) = raced_put(true, &[], b"same");
    assert_eq!(status, 200);
}
