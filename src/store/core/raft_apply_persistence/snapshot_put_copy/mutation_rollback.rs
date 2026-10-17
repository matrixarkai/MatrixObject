impl MatrixObjectStore {
    fn capture_object_mutation_rollback(&self) -> ObjectMutationRollback {
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
            multipart_uploads: self.multipart_uploads.clone(),
            multipart_complete_receipts: self.multipart_complete_receipts.clone(),
            multipart_abort_receipts: self.multipart_abort_receipts.clone(),
            next_upload_sequence: self.next_upload_sequence,
            meta_service: if targeted {
                None
            } else {
                Some(self.meta_service.clone())
            },
            slabs: self.slabs.clone(),
            payloads: self.payloads.clone(),
            page_service: if targeted {
                None
            } else {
                Some(self.page_service.clone())
            },
            logical_written_bytes: self.logical_written_bytes,
            physical_written_bytes: self.physical_written_bytes,
            last_object_manifest_checkpoint_index: self.last_object_manifest_checkpoint_index,
            last_object_metadata_checkpoint_index: self.last_object_metadata_checkpoint_index,
        }
    }

    /// Captures a rollback for a single-key object mutation.
    ///
    /// `page_service` is not copied either. `begin_mutation_batch` takes its own
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
    fn capture_targeted_mutation_rollback(
        &self,
        bucket: &str,
        key: &str,
    ) -> ObjectMutationRollback {
        let object_id = ObjectId::new(bucket.to_string(), key.to_string());
        let targeted = PutTargetedUndo {
            meta: self.meta_service.capture_record_undo(&meta_object_key(&object_id)),
            bucket_existed: self.buckets.contains(bucket),
            bucket: bucket.to_string(),
            previous_object: self.objects.get(&object_id).cloned(),
            object_id,
            object_changes_len: self.object_changes.len(),
        };
        self.capture_object_mutation_rollback_inner(Some(targeted))
    }

    fn restore_object_mutation_rollback(&mut self, mut rollback: ObjectMutationRollback) {
        match rollback.put_targeted.take() {
            Some(targeted) => {
                if !targeted.bucket_existed {
                    self.buckets.remove(&targeted.bucket);
                }
                match targeted.previous_object {
                    Some(previous) => {
                        self.objects.insert(targeted.object_id, previous);
                    }
                    None => {
                        self.objects.remove(&targeted.object_id);
                    }
                }
                // A put only appends here, so dropping the tail is exact.
                self.object_changes.truncate(targeted.object_changes_len);
                self.meta_service.restore_record_undo(targeted.meta);
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
        self.multipart_uploads = rollback.multipart_uploads;
        self.multipart_complete_receipts = rollback.multipart_complete_receipts;
        self.multipart_abort_receipts = rollback.multipart_abort_receipts;
        self.next_upload_sequence = rollback.next_upload_sequence;
        if let Some(meta_service) = rollback.meta_service {
            self.meta_service = meta_service;
        }
        self.slabs = rollback.slabs;
        self.payloads = rollback.payloads;
        if let Some(page_service) = rollback.page_service {
            self.page_service = page_service;
        }
        self.logical_written_bytes = rollback.logical_written_bytes;
        self.physical_written_bytes = rollback.physical_written_bytes;
        self.last_object_manifest_checkpoint_index = rollback.last_object_manifest_checkpoint_index;
        self.last_object_metadata_checkpoint_index = rollback.last_object_metadata_checkpoint_index;
    }

}
