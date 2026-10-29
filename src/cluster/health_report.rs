// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::meta_message::MetaRequest;
use crate::ObjectError;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub(super) struct HealthReport {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub node_id: String,
    #[prost(string, tag = "3")]
    pub expected_endpoint: String,
    #[prost(uint64, tag = "4")]
    pub used_bytes: u64,
    #[prost(uint64, tag = "5")]
    pub capacity_bytes: u64,
    #[prost(bool, tag = "6")]
    pub online: bool,
}

pub(super) fn decode(payload: &[u8]) -> Result<MetaRequest, ObjectError> {
    if payload.len() > 1024 {
        return Err(ObjectError::Corruption(
            "chunk health report exceeds bound".into(),
        ));
    }
    let report: HealthReport = super::conditional_requests::decode(payload)?;
    if report.version != 1
        || report.node_id.is_empty()
        || report.node_id.len() > 256
        || report.expected_endpoint.is_empty()
        || report.expected_endpoint.len() > 512
        || report.used_bytes > report.capacity_bytes
    {
        return Err(ObjectError::InvalidArgument(
            "invalid chunk health report".into(),
        ));
    }
    Ok(MetaRequest::ReportChunkNode {
        node_id: report.node_id,
        expected_endpoint: report.expected_endpoint,
        used_bytes: report.used_bytes,
        capacity_bytes: report.capacity_bytes,
        online: report.online,
    })
}

// Called only where a local durable outcome is prepared. Followers do not stamp
// reports, and replay applies the decided timestamp without consulting a clock.
pub(super) fn stamp(request: MetaRequest) -> Result<MetaRequest, ObjectError> {
    if !matches!(request, MetaRequest::ReportChunkNode { .. }) {
        return Ok(request);
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ObjectError::PreconditionFailed("metadata clock precedes epoch".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| {
            ObjectError::PreconditionFailed("metadata clock exceeds health range".into())
        })?;
    Ok(stamp_at(request, now_ms))
}

fn stamp_at(request: MetaRequest, now_ms: u64) -> MetaRequest {
    match request {
        MetaRequest::ReportChunkNode {
            node_id,
            expected_endpoint,
            used_bytes,
            capacity_bytes,
            online,
        } => MetaRequest::HeartbeatChunkNode {
            node_id,
            expected_endpoint,
            used_bytes,
            capacity_bytes,
            online,
            now_ms,
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::meta_wire::MetaRequestCode;

    #[test]
    fn health_reports_are_bounded_canonical_and_stamped_only_when_decided() {
        let request = MetaRequest::ReportChunkNode {
            node_id: "chunk-1".into(),
            expected_endpoint: "127.0.0.1:9100".into(),
            used_bytes: 17,
            capacity_bytes: 1024,
            online: true,
        };
        let payload = request.encode_payload();
        assert_eq!(request.code(), MetaRequestCode::ReportChunkNode);
        assert_eq!(
            MetaRequest::decode(request.code(), &payload).unwrap(),
            request
        );
        let mut unknown = payload.clone();
        unknown.extend_from_slice(&[0x38, 1]);
        assert!(decode(&unknown).is_err());
        let mut duplicate = payload.clone();
        duplicate.extend_from_slice(&[8, 1]);
        assert!(decode(&duplicate).is_err());
        assert!(decode(&vec![0; 1025]).is_err());
        assert!(decode(&[]).is_err());
        let original = HealthReport::decode(payload.as_slice()).unwrap();
        for field in 0..5 {
            let mut invalid = original.clone();
            match field {
                0 => invalid.version = 2,
                1 => invalid.node_id.clear(),
                2 => invalid.expected_endpoint.clear(),
                3 => invalid.used_bytes = 1025,
                _ => invalid.node_id = "a".repeat(257),
            }
            assert!(decode(&invalid.encode_to_vec()).is_err());
        }
        let stamped = stamp_at(request.clone(), 1234);
        assert!(matches!(
            &stamped,
            MetaRequest::HeartbeatChunkNode {
                now_ms: 1234,
                used_bytes: 17,
                ..
            }
        ));
        assert_eq!(stamp_at(stamped.clone(), 9999), stamped);
        assert_eq!(stamp(stamped.clone()).unwrap(), stamped);
        let decoded = MetaRequest::decode(request.code(), &payload).unwrap();
        assert_eq!(decoded, request, "decoding must not consult a clock");
    }
}
