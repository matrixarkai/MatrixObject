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
                    let session = client.open_read_session(
                        &manifest.bucket,
                        &manifest.key,
                        Some(manifest.generation),
                        TTL_MS,
                    )?;
                    handles.push(session.retention_id);
                    if session.manifest.as_ref() != Some(manifest) {
                        return Err(ObjectError::Corruption(
                            "read session differs from selected manifest".into(),
                        ));
                    }
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
