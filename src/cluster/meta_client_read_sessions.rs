// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

impl RemoteMetaClient {
    fn observe_read_protection(&mut self, fence: u64) -> Result<(), ObjectError> {
        if self.read_protection_fence != 0 && fence != self.read_protection_fence {
            return Err(ObjectError::PreconditionFailed(
                "metadata read protection changed or disappeared".into(),
            ));
        }
        self.read_protection_fence = fence;
        Ok(())
    }

    #[cfg(feature = "replica-server")]
    fn open_selected_read_session(
        &mut self,
        manifest: &crate::object_manifest::ObjectManifest,
        ttl_ms: u64,
    ) -> Result<u64, ObjectError> {
        manifest.validate()?;
        let digest = crate::read_session_outcome::manifest_sha256(manifest)?;
        let reply = self.retention_call(crate::manifest_retention::ManifestRetentionRequest {
            version: 3,
            action: 8,
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            generation: manifest.generation,
            ttl_ms,
            manifest_sha256: digest.clone(),
            ..Default::default()
        })?;
        if reply.version != 4 || reply.lease_ms != ttl_ms || reply.manifest_sha256 != digest {
            return Err(ObjectError::Corruption(
                "selected manifest grant changed identity or protection window".into(),
            ));
        }
        Ok(reply.retention_id)
    }

    #[cfg(feature = "replica-server")]
    fn with_selected_read_sessions<T>(
        &mut self,
        entries: Vec<(
            crate::object_manifest::ObjectManifest,
            Option<Vec<super::ChunkGroupRoute>>,
        )>,
        options: &super::ReplicaClientOptions,
        read: impl FnOnce(
            &mut Self,
            Vec<(
                crate::object_manifest::ObjectManifest,
                Option<Vec<super::ChunkGroupRoute>>,
            )>,
            &super::ReplicaClientOptions,
        ) -> Result<T, ObjectError>,
    ) -> Result<T, ObjectError> {
        if self.read_protection_fence == 0 {
            return read(self, entries, options);
        }
        // Later grants cannot extend the first protected generation's window.
        const TTL_MS: u64 = 60_000;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(TTL_MS - 100))
            .ok_or_else(|| ObjectError::InvalidArgument("read session deadline overflow".into()))?;
        self.with_operation_deadline(Some(deadline), |client| {
            let mut bounded = options.clone();
            bounded.deadline = client.operation_deadline();
            let mut handles = Vec::new();
            let result = (|| {
                let mut selected = std::collections::BTreeMap::new();
                for (manifest, _) in &entries {
                    let identity = (&manifest.bucket, &manifest.key);
                    if let Some(previous) = selected.insert(identity, manifest) {
                        if previous != manifest {
                            return Err(ObjectError::Corruption(
                                "batch selected different generations of one object".into(),
                            ));
                        }
                        continue;
                    }
                    handles.push(client.open_selected_read_session(manifest, TTL_MS)?);
                }
                client.check_operation_deadline()?;
                read(client, entries, &bounded)
            })();
            // Payload I/O has ended. Uncertain cleanup leaves bounded leases.
            for handle in handles {
                let _ = client.release_manifest(handle);
            }
            result.and_then(|value| {
                client.check_operation_deadline()?;
                Ok(value)
            })
        })
    }

    #[cfg(feature = "replica-server")]
    fn read_selected_object(
        &mut self,
        manifest: crate::object_manifest::ObjectManifest,
        routes: Option<Vec<super::ChunkGroupRoute>>,
        max_bytes: usize,
        options: &super::ReplicaClientOptions,
        range: Option<(u64, u64)>,
    ) -> Result<(crate::object_manifest::ObjectManifest, Vec<u8>), ObjectError> {
        self.with_selected_read_sessions(
            vec![(manifest, routes)],
            options,
            |client, mut entries, bounded| {
                let (manifest, routes) = entries.pop().unwrap();
                client.read_object_from_manifest_routes(manifest, routes, max_bytes, bounded, range)
            },
        )
    }

    #[cfg(feature = "replica-server")]
    fn read_selected_batch(
        &mut self,
        entries: Vec<super::ManifestRouteEntry>,
        objects: &[(&str, &str, usize)],
        options: &super::ReplicaClientOptions,
    ) -> Result<Vec<(crate::object_manifest::ObjectManifest, Vec<u8>)>, ObjectError> {
        let entries = entries
            .into_iter()
            .map(|entry| (entry.manifest, Some(entry.routes)))
            .collect();
        self.with_selected_read_sessions(entries, options, |client, entries, bounded| {
            entries
                .into_iter()
                .zip(objects)
                .map(|((manifest, routes), (_, _, limit))| {
                    client.read_object_from_manifest_routes(manifest, routes, *limit, bounded, None)
                })
                .collect()
        })
    }
}

#[cfg(all(test, feature = "replica-server"))]
mod compact_selected_grants {
    use super::*;

    #[test]
    fn client_refuses_changed_digest_window_or_reply_kind() {
        for mode in 0..5 {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut header = [0; HEADER_BYTES];
                socket.read_exact(&mut header).unwrap();
                let mut payload =
                    vec![0; u32::from_be_bytes(header[2..].try_into().unwrap()) as usize];
                socket.read_exact(&mut payload).unwrap();
                let MetaRequest::ManifestRetention(request) =
                    MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &payload)
                        .unwrap()
                else {
                    panic!()
                };
                assert_eq!(
                    (request.version, request.action, request.generation),
                    (3, 8, 1)
                );
                let mut reply = crate::manifest_retention::ManifestRetentionReply {
                    version: 4,
                    retention_id: 42,
                    expires_at_ms: 2000,
                    lease_ms: 1000,
                    manifest_sha256: request.manifest_sha256,
                    ..Default::default()
                };
                match mode {
                    1 => reply.manifest_sha256[0] ^= 1,
                    2 => reply.lease_ms = 2000,
                    3 => {
                        reply.version = 2;
                        reply.lease_ms = 0;
                        reply.manifest_sha256.clear();
                    }
                    4 => reply.retention_id = 0,
                    _ => {}
                }
                socket
                    .write_all(&super::super::meta_message::encode_meta_reply(
                        &MetaReply::ManifestRetention(reply),
                    ))
                    .unwrap();
            });
            let mut client =
                RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
            let manifest = crate::object_manifest::ObjectManifest {
                format_version: 2,
                bucket: "bucket".into(),
                key: "object".into(),
                generation: 1,
                ..Default::default()
            };
            let result = client.open_selected_read_session(&manifest, 1000);
            if mode == 0 {
                assert_eq!(result.unwrap(), 42);
            } else {
                assert!(result.is_err());
            }
            worker.join().unwrap();
        }
    }
}

#[cfg(test)]
mod ordinary_read_protection {
    use super::*;
    #[test]
    fn policy_cannot_regress_across_connection_replacement() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let mut client =
            RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        client.observe_read_protection(0).unwrap();
        client.observe_read_protection(7).unwrap();
        let next = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        client.adopt_connection(next);
        assert!(client.observe_read_protection(0).is_err());
        assert!(client.observe_read_protection(8).is_err());
        client.observe_read_protection(7).unwrap();
        assert_eq!(client.read_protection_fence, 7);
    }
}
