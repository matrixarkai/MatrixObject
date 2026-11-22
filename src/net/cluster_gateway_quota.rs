// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bucket quotas: a write that would take the live objects of a bucket past its `quota_bytes` is refused with
//! `QuotaExceeded` (403).
//!
//! # WHAT THE LIMIT PROMISES
//!
//! The usage a gateway compares against is the sum of the live object lengths it listed at most [`USAGE_TTL`] ago,
//! plus what this gateway has written since. A write is refused when that usage, less the object it replaces, plus
//! the bytes it adds, passes the quota. Two gateways, or two writers in one interval, each see the usage as it was:
//! together they can pass the quota by what they add within one interval. The quota is a bound that holds in steady
//! state, not a lock; a bucket that must never pass its limit takes its writes through one gateway. A request in a
//! bucket without a quota reads nothing here.
use super::attributes::LockCheck;
use super::*;

/// How long a listed usage is trusted.
const USAGE_TTL: Duration = Duration::from_secs(5);
/// The start of the refusal a write over the quota produces; [`error_response`] answers it with `QuotaExceeded`.
pub(super) const OVER_QUOTA: &str = "the bucket's quota would be exceeded";

impl LockCheck {
    /// The refusal for a write adding `incoming` bytes and replacing an object of `replaced` bytes, when the bucket's
    /// quota would be passed.
    pub fn refuse_quota(&self, incoming: u64, replaced: u64) -> Option<PlaneFailure> {
        let (limit, used) = self.quota?;
        (used.saturating_sub(replaced).saturating_add(incoming) > limit).then(|| {
            PlaneFailure::from(ObjectError::PreconditionFailed(format!(
                "{OVER_QUOTA}: {used} bytes are stored of {limit}, and this write adds {incoming}"
            )))
        })
    }
}

impl ClusterGateway {
    /// The live bytes of `bucket`, listed at most [`USAGE_TTL`] ago.
    fn bucket_usage(&self, bucket: &str) -> Result<u64, ObjectError> {
        if let Ok(cache) = self.bucket_usage.lock() {
            if let Some((at, bytes)) = cache.get(bucket) {
                if at.elapsed() < USAGE_TTL {
                    return Ok(*bytes);
                }
            }
        }
        let total = self.with_plane(|plane| {
            let mut after: Option<String> = None;
            let mut sum = 0u64;
            loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if !summary.deleted && !s3_verbs::is_reserved(&summary.key) {
                        sum = sum.saturating_add(summary.length);
                    }
                }
                if !page.has_more || page.entries.is_empty() {
                    return Ok(sum);
                }
            }
        })?;
        if let Ok(mut cache) = self.bucket_usage.lock() {
            if cache.len() > 4096 {
                cache.clear();
            }
            cache.insert(bucket.to_string(), (Instant::now(), total));
        }
        Ok(total)
    }

    /// `check` with the bucket's quota and its current usage, when the bucket has a quota.
    pub(super) fn with_quota(&self, mut check: LockCheck, bucket: &str, resource: &str) -> Result<LockCheck, Response> {
        let rules = self.bucket_rules(bucket).map_err(|error| error_response(error, resource, true))?;
        if let Some(rules) = rules.filter(|rules| rules.quota_bytes > 0) {
            let used = self.bucket_usage(bucket).map_err(|error| error_response(error, resource, true))?;
            check.quota = Some((rules.quota_bytes, used));
        }
        Ok(check)
    }

    /// Account for `added` bytes this gateway wrote, so a burst of writes inside one interval is counted.
    pub(super) fn note_usage(&self, bucket: &str, added: i64) {
        if let Ok(mut cache) = self.bucket_usage.lock() {
            if let Some((_, bytes)) = cache.get_mut(bucket) {
                *bytes = if added >= 0 { bytes.saturating_add(added as u64) } else { bytes.saturating_sub(added.unsigned_abs()) };
            }
        }
    }
}
