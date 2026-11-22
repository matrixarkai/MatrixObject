// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The lifecycle executor: the bucket's lifecycle rules ([`crate::bucket_config::LifecycleRule`]) acted on.
//!
//! [`ClusterGateway::run_lifecycle`] makes one pass over every bucket that has rules. An enabled rule with an
//! expiration deletes the live objects under its prefix that were last written that many days ago, and a rule with
//! `abort_multipart_days` removes the multipart uploads under its prefix begun that many days ago and never
//! completed. An object an object lock still protects is left alone and counted, never deleted around its lock.
//! The door runs the pass on a timer (`MATRIXOBJECT_LIFECYCLE_INTERVAL_MS`); a pass is safe to repeat and to run
//! from several doors at once, because every deletion is conditional on the manifest it decided from.
use super::attributes::LockCheck;
use super::*;

const DAY_MS: u64 = 86_400_000;

/// What one pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LifecycleReport {
    /// Buckets that had at least one enabled rule.
    pub buckets: u64,
    /// Objects deleted by an expiration.
    pub expired: u64,
    /// Objects an expiration reached and a lock protected.
    pub kept_locked: u64,
    /// Multipart uploads removed.
    pub aborted_uploads: u64,
    /// Kept versions deleted by a noncurrent expiration.
    pub expired_versions: u64,
}

impl ClusterGateway {
    /// One lifecycle pass at `now_ms`.
    pub fn run_lifecycle(&self, now_ms: u64) -> Result<LifecycleReport, ObjectError> {
        let mut report = LifecycleReport::default();
        for bucket in self.bucket_names()? {
            let Some((_, config)) = self.load_bucket(&bucket)? else { continue };
            let rules: Vec<_> = config.lifecycle.iter().filter(|rule| rule.enabled).collect();
            if rules.is_empty() {
                continue;
            }
            report.buckets += 1;
            let lock = LockCheck { enabled: config.object_lock_enabled, bypass: false, quota: None };
            for rule in rules.iter().filter(|rule| rule.expiration_days > 0) {
                self.expire_under(&bucket, &rule.prefix, u64::from(rule.expiration_days) * DAY_MS, now_ms, lock, &mut report)?;
            }
            for rule in rules.iter().filter(|rule| rule.noncurrent_expiration_days > 0) {
                self.expire_noncurrent(&bucket, &rule.prefix, u64::from(rule.noncurrent_expiration_days) * DAY_MS, now_ms, lock, &mut report)?;
            }
            for rule in rules.iter().filter(|rule| rule.abort_multipart_days > 0) {
                self.abort_stale_uploads(&bucket, &rule.prefix, u64::from(rule.abort_multipart_days) * DAY_MS, now_ms, &mut report)?;
            }
        }
        Ok(report)
    }

    /// The live keys of `bucket` under `prefix`.
    fn keys_under(&self, bucket: &str, prefix: &str) -> Result<Vec<String>, ObjectError> {
        self.with_plane(|plane| {
            let mut keys = Vec::new();
            let mut after: Option<String> = None;
            'pages: loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if summary.key.as_str() < prefix {
                        continue;
                    }
                    if !summary.key.starts_with(prefix) {
                        break 'pages;
                    }
                    if !summary.deleted && !s3_verbs::is_reserved(&summary.key) {
                        keys.push(summary.key.clone());
                    }
                }
                if !page.has_more || page.entries.is_empty() {
                    break;
                }
            }
            Ok(keys)
        })
    }

    fn expire_under(
        &self,
        bucket: &str,
        prefix: &str,
        age_ms: u64,
        now_ms: u64,
        lock: LockCheck,
        report: &mut LifecycleReport,
    ) -> Result<(), ObjectError> {
        for key in self.keys_under(bucket, prefix)? {
            let written = match self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, &key)) {
                Ok((manifest, carried)) if !manifest.deleted => carried.modified_ms,
                Ok(_) | Err(ObjectError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            // an object written before the time was recorded has no age to act on
            if written == 0 || now_ms.saturating_sub(written) < age_ms {
                continue;
            }
            self.forget(bucket, &key);
            // in a versioned bucket an expiration leaves a delete marker over the kept versions, as a delete does
            let removed = if self.versioning_on(bucket)? {
                self.versioned_delete(bucket, &key, lock)?.ok_or_else(|| ObjectError::NotFound("already deleted".into()))
            } else {
                self.delete_checked(bucket, &key, lock)
            };
            match removed {
                Ok(manifest) => {
                    self.remember(&manifest);
                    self.note_change(bucket, "expire", &key, manifest.generation, 0);
                    report.expired += 1;
                }
                Err(ObjectError::PreconditionFailed(message)) if message.starts_with(attributes::LOCKED) => report.kept_locked += 1,
                // another writer got there first: the next pass decides again on what it left
                Err(ObjectError::NotFound(_)) | Err(ObjectError::PreconditionFailed(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn abort_stale_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        age_ms: u64,
        now_ms: u64,
        report: &mut LifecycleReport,
    ) -> Result<(), ObjectError> {
        // the upload records sort together under the reserved prefix; '.' sorts just before '/'
        let records: Vec<String> = self.with_plane(|plane| {
            let mut found = Vec::new();
            let mut after: Option<String> = Some(".multipart.".to_string());
            loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if !summary.key.starts_with(".multipart/") {
                        return Ok(found);
                    }
                    if !summary.deleted && summary.key.ends_with("/meta") {
                        found.push(summary.key.clone());
                    }
                }
                if !page.has_more || page.entries.is_empty() {
                    return Ok(found);
                }
            }
        })?;
        for record in records {
            let id = record.trim_start_matches(".multipart/").trim_end_matches("/meta").to_string();
            let text = match self.with_plane(|plane| plane.read(bucket, &record)) {
                Ok((_, bytes)) => String::from_utf8_lossy(&bytes).to_string(),
                Err(ObjectError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            let Some((target, begun)) = text.split_once('\n') else { continue };
            let begun: u64 = begun.lines().next().unwrap_or("").trim().parse().unwrap_or(0);
            if !target.starts_with(prefix) || begun == 0 || now_ms.saturating_sub(begun) < age_ms {
                continue;
            }
            if self.abort_multipart_upload(bucket, target, &id).status == 204 {
                report.aborted_uploads += 1;
            }
        }
        Ok(())
    }
}
