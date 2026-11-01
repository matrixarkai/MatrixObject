// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub(crate) fn capture_object_mutation_rollback(&self) -> ObjectMutationRollback {
        self.capture_object_mutation_rollback_inner(None)
    }

    /// The shared capture. When `put_targeted` is given, the three containers it
    /// undoes by hand are not copied at all -- building them and discarding them
    /// would cost exactly what this is here to avoid.
    fn capture_object_mutation_rollback_inner(
        &self,
        put_targeted: Option<PutTargetedUndo>,
    ) -> ObjectMutationRollback {
        let targeted = put_targeted.is_some();
        ObjectMutationRollback {
            put_targeted,
            buckets: if targeted {
                Default::default()
            } else {
                self.buckets.clone()
            },
            bucket_versioning: self.bucket_versioning.clone(),
            bucket_retention: self.bucket_retention.clone(),
            bucket_quotas: self.bucket_quotas.clone(),
            bucket_lifecycle: self.bucket_lifecycle.clone(),
            bucket_policy_documents: self.bucket_policy_documents.clone(),
            bucket_tags: self.bucket_tags.clone(),
            bucket_cors: self.bucket_cors.clone(),
            bucket_policy_receipts: self.bucket_policy_receipts.clone(),
            bucket_cors_receipts: self.bucket_cors_receipts.clone(),
            objects: if targeted {
                Default::default()
            } else {
                self.objects.clone()
            },
            versions: self.versions.clone(),
            object_changes: if targeted {
                Default::default()
            } else {
                self.object_changes.clone()
            },
            next_object_change_sequence: self.next_object_change_sequence,
            // An upload in flight holds the part bytes it has buffered, and
            // this copied all of them into the undo of every mutation. With one
            // 8 MiB upload open, a 1 KiB put's undo measured 8,391,068 bytes
            // against 1,153 with none open.
            //
            // A put, an append and a delete never touch this map. Its writers
            // are the four multipart commands, the lifecycle abort and a
            // snapshot install, and every one of those captures the whole store
            // -- so the narrowed undo does not need it, and the restore below
            // must not put an empty map back in its place.
            multipart_uploads: if targeted {
                Default::default()
            } else {
                self.multipart_uploads.clone()
            },
            multipart_complete_receipts: self.multipart_complete_receipts.clone(),
            multipart_abort_receipts: self.multipart_abort_receipts.clone(),
            next_upload_sequence: self.next_upload_sequence,
            meta_service: if targeted {
                None
            } else {
                Some(self.meta_service.clone())
            },
            chunks: self.chunks.clone(),
            payloads: if targeted {
                Default::default()
            } else {
                self.payloads.clone()
            },
            chunk_service: if targeted {
                None
            } else {
                Some(self.chunk_service.clone())
            },
            logical_written_bytes: self.logical_written_bytes,
            physical_written_bytes: self.physical_written_bytes,
            last_object_manifest_checkpoint_index: self.last_object_manifest_checkpoint_index,
            last_object_metadata_checkpoint_index: self.last_object_metadata_checkpoint_index,
        }
    }

    /// Captures a rollback for a single-key object mutation.
    ///
    /// `chunk_service` is not copied either. `begin_mutation_batch` takes its own
    /// full snapshot of the page service and holds it for the batch, nothing
    /// mutates the page service between that call and this capture, and every
    /// error path aborts the batch before restoring here -- so this copy was
    /// overwriting a restore that had already happened. One consequence is
    /// deliberate: the batch snapshot does not carry the rate-limit token state
    /// or the cumulative read counters, so an aborted put now keeps both
    /// instead of rewinding them. Work that happened stays counted, and a
    /// failed put does not refund its tokens.
    ///
    /// `buckets`, `objects` and `object_changes` are left empty and recorded as
    /// a targeted undo instead: a put inserts one bucket, writes one object key
    /// and appends one change record, so each is reversible exactly. Copying
    /// those three is 83.7% of the allocations a put makes, and it is what made
    /// a put cost O(stored objects). Every other field, `versions` included, is
    /// still copied, and every other caller keeps the copying behaviour.
    /// A delete uses this too. Its mutations are a strict subset of a put's --
    /// `mark_extents_stale` or a `versions` push, one `objects` entry, one
    /// metadata record and one appended change -- where a put also inserts a
    /// bucket, allocates extents and moves `logical_written_bytes`. So an undo
    /// sufficient for a put is sufficient for a delete.
    pub(crate) fn capture_targeted_mutation_rollback(
        &self,
        bucket: &str,
        key: &str,
    ) -> ObjectMutationRollback {
        let object_id = ObjectId::new(bucket.to_string(), key.to_string());
        let previous_object = self.object(&object_id).cloned();
        // Sized to the extents it may hold rather than collected, because a
        // `Vec` grown from an iterator whose lower bound is zero takes four
        // slots to hold one entry -- 160 bytes where 40 will do, resident for
        // as long as the undo is. The same arithmetic that oversized the keys
        // in the previous change, in the fix for this one.
        let previous_payloads = previous_object
            .as_ref()
            .map(|previous| {
                let mut captured = Vec::with_capacity(previous.extents.len());
                for extent in previous.extents.iter() {
                    if let Some(payload) = self.payloads.get(&extent.chunk_id, extent.offset()) {
                        captured.push((
                            (extent.chunk_id.clone(), extent.offset()),
                            payload.clone(),
                        ));
                    }
                }
                captured
            })
            .unwrap_or_default();
        let targeted = PutTargetedUndo {
            meta: self.meta_service.capture_record_undo(&meta_object_key(&object_id)),
            bucket_existed: self.buckets.contains(bucket),
            bucket: bucket.to_string(),
            previous_object: match previous_object {
                None => PutTargetedPrevious::Absent,
                Some(previous) => PutTargetedPrevious::Whole(previous),
            },
            previous_payloads,
            object_id,
        };
        self.capture_object_mutation_rollback_inner(Some(targeted))
    }

    /// The same undo for a mutation that only APPENDS to the object.
    ///
    /// Records the extent list's old LENGTH and the six scalars an append advances
    /// instead of cloning the metadata -- see `AppendedObjectUndo` for why those
    /// are all of them. The payload entries are still captured, because the
    /// restore drops whatever the mutation added and re-inserts these, and that is
    /// the same work whichever shape the metadata undo takes.
    pub(crate) fn capture_appended_mutation_rollback(
        &self,
        bucket: &str,
        key: &str,
    ) -> ObjectMutationRollback {
        let object_id = ObjectId::new(bucket.to_string(), key.to_string());
        // BORROWED, not cloned. This is the whole point of the variant.
        let previous = self.object(&object_id);
        // ---- AND NO PAYLOAD ENTRIES, because an append destroys none.
        //
        // `previous_payloads` exists for a mutation that STALES the extents it replaces:
        // `mark_extents_stale` removes the payload entry for each one, and the entry holds
        // a length and a digest the store keeps nowhere else, so it has to be carried. An
        // append calls `mark_extents_stale` nowhere -- its callers are retention,
        // noncurrent versions, a lock transition, a multipart delete and a replacing put --
        // so there is nothing for it to carry.
        //
        // The restore is unchanged in effect. `restore_targeted_payloads` first RETAINS,
        // dropping entries at or past the chunk's `next_offset` as it was before the
        // mutation, which is exactly the entries an append added; the old entries sit below
        // that offset and outside the before-stale ranges, so they survive. Re-inserting
        // them was writing back values that had never been removed.
        //
        // That loop was the undo's real cost. Taking the metadata clone out of this capture
        // and leaving the loop in bought 0.72% of the integration run -- 14,500,411,257
        // instructions against 14,396,401,536 -- because the loop walks an entry per extent
        // and clones a `StoredPayload` for every hit, and the extent list is what grows.
        let previous_payloads = Vec::new();
        let previous_object = match previous {
            None => PutTargetedPrevious::Absent,
            Some(previous) => PutTargetedPrevious::Appended(AppendedObjectUndo {
                extents_len: previous.extents.len(),
                length: previous.length,
                etag_digest_bits: previous.etag_digest_bits,
                has_etag: previous.has_etag,
                update_time_ms: previous.update_time_ms,
                version: previous.version,
            }),
        };
        let targeted = PutTargetedUndo {
            meta: self.meta_service.capture_record_undo(&meta_object_key(&object_id)),
            bucket_existed: self.buckets.contains(bucket),
            bucket: bucket.to_string(),
            previous_object,
            previous_payloads,
            object_id,
        };
        self.capture_object_mutation_rollback_inner(Some(targeted))
    }

    /// Undoes a targeted mutation's changes to the payload index without having
    /// copied it.
    ///
    /// The index was copied in full by every mutation -- 40 bytes per stored
    /// object, to protect one object's worth of change -- and it was the last
    /// container on the put path still doing that. What makes it unnecessary is
    /// that an entry this mutation inserted is identifiable from the chunk
    /// records, which the undo already carries and which cost 120 bytes:
    ///
    /// * an entry in a chunk the undo has never heard of is new, because the
    ///   mutation created the chunk;
    /// * an entry at or past the chunk's write cursor as it was is new, because
    ///   that is where a fresh allocation goes;
    /// * an entry inside a range that was stale is new, because
    ///   `mark_extents_stale` removes the payload entry when a range goes
    ///   stale, so a reclaimed range holds none -- which is also why an insert
    ///   can never overwrite an entry and why there is nothing else to restore.
    ///
    /// Everything else was there before and is left alone. The scan is linear
    /// in the index, but it runs only when a mutation is refused, where copying
    /// the index ran on every mutation that succeeded.
    fn restore_targeted_payloads(
        &mut self,
        chunks_before: &BTreeMap<ChunkId, ChunkInfo>,
        previous_payloads: Vec<((ChunkId, u64), StoredPayload)>,
    ) {
        self.payloads.retain(|chunk_id, offset, _| {
            let Some(before) = chunks_before.get(chunk_id) else {
                return false;
            };
            if offset >= before.next_offset {
                return false;
            }
            !before.stale_ranges.iter().any(|range| {
                offset >= range.offset && offset < range.offset.saturating_add(range.length)
            })
        });
        // What the mutation removed. A put that replaces an object stales the
        // old extents, and the digest in the entry it dropped is not derivable
        // from anything the store still holds.
        for ((chunk_id, offset), payload) in previous_payloads {
            self.payloads.insert(chunk_id, offset, payload);
        }
    }

    pub(crate) fn restore_object_mutation_rollback(&mut self, mut rollback: ObjectMutationRollback) {
        let was_targeted = rollback.put_targeted.is_some();
        match rollback.put_targeted.take() {
            Some(targeted) => {
                if !targeted.bucket_existed {
                    self.buckets.remove(targeted.bucket.as_str());
                }
                match targeted.previous_object {
                    PutTargetedPrevious::Whole(mut previous) => {
                        self.file_object(targeted.object_id, &mut previous);
                    }
                    PutTargetedPrevious::Absent => {
                        self.objects.remove(&targeted.object_id);
                    }
                    // THE APPEND'S UNDO IS A LENGTH. Take the object out by value --
                    // `take` moves it, so nothing is copied -- truncate the list the
                    // append grew, put the six scalars back, and file it again.
                    //
                    // An object that is no longer there cannot be restored from a
                    // length, and nothing between the capture and here removes one: the
                    // append refuses a deleted object, and a removal is a different
                    // mutation with its own undo. So this is `debug_assert` plus the
                    // conservative arm rather than a silent skip.
                    PutTargetedPrevious::Appended(appended) => {
                        match self.take_object(&targeted.object_id) {
                            Some(mut previous) => {
                                let mut extents = previous.extents.to_vec();
                                extents.truncate(appended.extents_len);
                                previous.extents = extents.into();
                                previous.length = appended.length;
                                previous.etag_digest_bits = appended.etag_digest_bits;
                                previous.has_etag = appended.has_etag;
                                previous.update_time_ms = appended.update_time_ms;
                                previous.version = appended.version;
                                // PUT BACK AS IT WAS, not re-filed. `file_object`
                                // re-interns the object key and the content type, and
                                // the object taken here already holds the interned
                                // ones -- re-interning after the set's copy was
                                // removed makes a FRESH allocation, which the
                                // footprint audit sees as a string that stopped being
                                // shared: `object_changes.object_id_bytes 0 -> 17` on
                                // a refused append, caught by
                                // `an_append_that_fails_after_mutating_leaves_the_store_unchanged`.
                                self.objects.replace(KeyedObject::new(previous));
                            }
                            None => {
                                debug_assert!(
                                    false,
                                    "an append undo outlived the object it describes"
                                );
                            }
                        }
                    }
                }
                // Dropping the tail by length was exact while a mutation only
                // ever appended here. It no longer only appends: the feed is
                // also trimmed from the front once it reaches
                // `UNCONSUMED_OBJECT_CHANGE_RETENTION` with no consumer
                // registered, and a trim plus an append leaves the length
                // unchanged -- so `truncate` became a no-op that kept the
                // appended record and had already lost the oldest one.
                //
                // A sequence does not move when the front is trimmed, and the
                // undo already carries the one this mutation started from, so
                // drop by sequence instead. Everything this mutation appended
                // is at or above it, and nothing else is.
                //
                // The record the trim dropped is not recoverable, so a rolled
                // back mutation can leave the retained floor one record higher
                // than it found it. That is within what the feed already
                // promises a late consumer -- `cursor_compacted` and
                // `resync_required` -- whereas a retained change at or above
                // `next_object_change_sequence` is what the snapshot validator
                // calls corruption.
                self.object_changes
                    .retain(|change| change.sequence < rollback.next_object_change_sequence);
                self.meta_service.restore_record_undo(targeted.meta);
                self.restore_targeted_payloads(&rollback.chunks, targeted.previous_payloads);
            }
            None => {
                self.buckets = rollback.buckets;
                self.objects = rollback.objects;
                self.object_changes = rollback.object_changes;
            }
        }
        self.bucket_versioning = rollback.bucket_versioning;
        self.bucket_retention = rollback.bucket_retention;
        self.bucket_quotas = rollback.bucket_quotas;
        self.bucket_lifecycle = rollback.bucket_lifecycle;
        self.bucket_policy_documents = rollback.bucket_policy_documents;
        self.bucket_tags = rollback.bucket_tags;
        self.bucket_cors = rollback.bucket_cors;
        self.bucket_policy_receipts = rollback.bucket_policy_receipts;
        self.bucket_cors_receipts = rollback.bucket_cors_receipts;
        self.versions = rollback.versions;
        self.next_object_change_sequence = rollback.next_object_change_sequence;
        if !was_targeted {
            self.multipart_uploads = rollback.multipart_uploads;
        }
        self.multipart_complete_receipts = rollback.multipart_complete_receipts;
        self.multipart_abort_receipts = rollback.multipart_abort_receipts;
        self.next_upload_sequence = rollback.next_upload_sequence;
        if let Some(meta_service) = rollback.meta_service {
            self.meta_service = meta_service;
        }
        self.chunks = rollback.chunks;
        if !was_targeted {
            self.payloads = rollback.payloads;
        }
        if let Some(chunk_service) = rollback.chunk_service {
            self.chunk_service = chunk_service;
        }
        self.logical_written_bytes = rollback.logical_written_bytes;
        self.physical_written_bytes = rollback.physical_written_bytes;
        self.last_object_manifest_checkpoint_index = rollback.last_object_manifest_checkpoint_index;
        self.last_object_metadata_checkpoint_index = rollback.last_object_metadata_checkpoint_index;
    }

}
