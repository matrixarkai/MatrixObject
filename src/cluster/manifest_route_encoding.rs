// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Borrowed encoding of the existing manifest-route protobuf envelopes.
use super::ManifestRouteEntry;
use crate::{cluster::ChunkGroupRoute, object_manifest::ObjectManifest};
use prost::encoding::{self, message, uint32, uint64, WireType};

fn version(fence: u64) -> u32 {
    if fence == 0 {
        1
    } else {
        2
    }
}

pub(super) fn single_len(
    manifest: &ObjectManifest,
    routes: &[ChunkGroupRoute],
    fence: u64,
) -> usize {
    uint32::encoded_len(1, &version(fence))
        + message::encoded_len(2, manifest)
        + message::encoded_len_repeated(3, routes)
        + if fence == 0 {
            0
        } else {
            uint64::encoded_len(4, &fence)
        }
}

pub(super) fn write_single(
    manifest: &ObjectManifest,
    routes: &[ChunkGroupRoute],
    fence: u64,
    output: &mut Vec<u8>,
) {
    uint32::encode(1, &version(fence), output);
    message::encode(2, manifest, output);
    for route in routes {
        message::encode(3, route, output);
    }
    if fence != 0 {
        uint64::encode(4, &fence, output);
    }
}

pub(super) fn encode_single(
    manifest: &ObjectManifest,
    routes: &[ChunkGroupRoute],
    fence: u64,
) -> Vec<u8> {
    let mut output = Vec::with_capacity(single_len(manifest, routes, fence));
    write_single(manifest, routes, fence, &mut output);
    output
}

pub(super) fn batch_len(objects: &[ManifestRouteEntry]) -> usize {
    uint32::encoded_len(1, &1)
        + objects
            .iter()
            .map(|object| {
                let length = single_len(&object.manifest, &object.routes, object.publication_fence);
                encoding::key_len(2) + encoding::encoded_len_varint(length as u64) + length
            })
            .sum::<usize>()
}

pub(super) fn write_batch(objects: &[ManifestRouteEntry], output: &mut Vec<u8>) {
    uint32::encode(1, &1, output);
    for object in objects {
        let length = single_len(&object.manifest, &object.routes, object.publication_fence);
        encoding::encode_key(2, WireType::LengthDelimited, output);
        encoding::encode_varint(length as u64, output);
        write_single(
            &object.manifest,
            &object.routes,
            object.publication_fence,
            output,
        );
    }
}

pub(super) fn encode_batch(objects: &[ManifestRouteEntry]) -> Vec<u8> {
    let mut output = Vec::with_capacity(batch_len(objects));
    write_batch(objects, &mut output);
    output
}

#[cfg(test)]
#[path = "manifest_route_encoding_tests.rs"]
mod tests;
