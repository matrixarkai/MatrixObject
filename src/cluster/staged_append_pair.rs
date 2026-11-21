// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

/// One append using an allocation already reserved in metadata.
#[derive(Debug)]
pub struct StagedObjectAppend<'a> {
    pub allocation: &'a crate::staged_allocation::StagedAllocation,
    pub expected: ObjectManifest,
    pub data: &'a [u8],
    /// Reserve pages for the next append in this append's publication, as `(pages, ttl_ms)`.
    pub reserve_next: Option<(u32, u64)>,
}

#[derive(Debug)]
pub struct StagedObjectAppendReceipt {
    pub manifest: ObjectManifest,
    pub next_allocation: Option<crate::staged_allocation::StagedAllocation>,
}

/// Publication is ordered, so an error can leave the first append committed.
#[derive(Debug)]
pub struct OrderedObjectAppendFailure {
    /// The publication to settle or retry: zero or one. A shared staging failure uses zero.
    pub failed_append: usize,
    /// Confirmed first publication. None does not settle an unconfirmed first publication;
    /// the nested failure retains the exact candidate for read-back.
    pub committed_first: Option<StagedObjectAppendReceipt>,
    pub failure: ObjectWriteFailure,
}
impl std::fmt::Display for OrderedObjectAppendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ordered append {} failed: {}",
            self.failed_append, self.failure
        )
    }
}
impl std::error::Error for OrderedObjectAppendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.failure)
    }
}
const MAX_ORDERED_APPEND_BYTES: usize = 2 * 1024 * 1024;

fn join_staged_data(
    mut first: ObjectWriteData,
    first_bytes: &[u8],
    second: ObjectWriteData,
    second_bytes: &[u8],
) -> Result<(ObjectWriteData, Vec<u8>), ObjectError> {
    let total = first_bytes
        .len()
        .checked_add(second_bytes.len())
        .filter(|total| *total <= MAX_ORDERED_APPEND_BYTES)
        .ok_or_else(|| invalid("ordered append data exceeds its byte limit"))?;
    if first.manifest.length != first_bytes.len() as u64
        || second.manifest.length != second_bytes.len() as u64
    {
        return Err(invalid("staged data length differs from its plan"));
    }
    for (group, route) in second.groups {
        if first
            .groups
            .get(&group)
            .is_some_and(|before| before != &route)
        {
            return Err(ObjectError::PreconditionFailed(
                "a data group changed between staged append plans".into(),
            ));
        }
        first.groups.insert(group, route);
    }
    for mut extent in second.manifest.extents {
        extent.object_offset = extent
            .object_offset
            .checked_add(first_bytes.len() as u64)
            .ok_or_else(|| invalid("ordered append offset overflow"))?;
        first.manifest.extents.push(extent);
    }
    first.manifest.length = total as u64;
    for connection in second.connections {
        if !first
            .connections
            .iter()
            .any(|(group, _)| group == &connection.0)
        {
            first.connections.push(connection);
        }
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(first_bytes);
    bytes.extend_from_slice(second_bytes);
    Ok((first, bytes))
}

impl RemoteMetaClient {
    /// Stage both payloads together, then publish the first append before the second.
    /// The combined payload is at most 2 MiB. Replica write policy and existing batch
    /// negotiation are preserved. The caller retains both allocations for cleanup.
    /// An unconfirmed first publication prevents any second publication.
    pub fn append_staged_pair(
        &mut self,
        first: StagedObjectAppend<'_>,
        second: StagedObjectAppend<'_>,
        options: &ObjectWriteOptions,
    ) -> Result<[StagedObjectAppendReceipt; 2], OrderedObjectAppendFailure> {
        self.append_staged_pair_inner(first, second, options, false)
    }

    /// Like `append_staged_pair`, using allocations the caller has just obtained from
    /// committed metadata. As with `confirm_staged_allocation`, allocation read-back
    /// is skipped. The caller must keep both allocations within their lease window.
    pub fn append_confirmed_staged_pair(
        &mut self,
        first: StagedObjectAppend<'_>,
        second: StagedObjectAppend<'_>,
        options: &ObjectWriteOptions,
    ) -> Result<[StagedObjectAppendReceipt; 2], OrderedObjectAppendFailure> {
        self.append_staged_pair_inner(first, second, options, true)
    }

    fn append_staged_pair_inner(
        &mut self,
        first: StagedObjectAppend<'_>,
        second: StagedObjectAppend<'_>,
        options: &ObjectWriteOptions,
        confirmed: bool,
    ) -> Result<[StagedObjectAppendReceipt; 2], OrderedObjectAppendFailure> {
        let without_confirmed_prefix = |failed_append, failure| OrderedObjectAppendFailure {
            failed_append,
            committed_first: None,
            failure,
        };
        if first.data.is_empty()
            || second.data.is_empty()
            || first
                .data
                .len()
                .checked_add(second.data.len())
                .is_none_or(|bytes| bytes > MAX_ORDERED_APPEND_BYTES)
            || first.allocation.allocation_id == second.allocation.allocation_id
            || (first.allocation.bucket == second.allocation.bucket
                && first.allocation.key == second.allocation.key)
        {
            return Err(without_confirmed_prefix(
                0,
                before(invalid("invalid ordered staged appends")),
            ));
        }
        let result = self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            if confirmed {
                client.confirm_staged_allocation(first.allocation.allocation_id);
            }
            let PreparedObjectWrite {
                publication: first_publication,
                data: mut first_plan,
            } = client
                .prepare_object_write(
                    &first.allocation.bucket,
                    &first.allocation.key,
                    Some(first.expected),
                    first.data,
                    &bounded,
                    true,
                    Some(first.allocation),
                    true,
                    None,
                )
                .map_err(|failure| without_confirmed_prefix(0, failure))?;
            // Keep verified sessions reusable while preparing the other object.
            for (group, connection) in first_plan.connections.drain(..) {
                let route = first_plan.groups.get(&group).cloned().ok_or_else(|| {
                    without_confirmed_prefix(
                        0,
                        first_publication.before_publication(ObjectError::Corruption(
                            "prepared group route missing".into(),
                        )),
                    )
                })?;
                client.cache_group_connection(group, route, connection);
            }
            if confirmed {
                client.confirm_staged_allocation(second.allocation.allocation_id);
            }
            let PreparedObjectWrite {
                publication: second_publication,
                data: second_plan,
            } = client
                .prepare_object_write(
                    &second.allocation.bucket,
                    &second.allocation.key,
                    Some(second.expected),
                    second.data,
                    &bounded,
                    true,
                    Some(second.allocation),
                    true,
                    None,
                )
                .map_err(|failure| without_confirmed_prefix(1, failure))?;
            let (mut combined, bytes) =
                join_staged_data(first_plan, first.data, second_plan, second.data).map_err(
                    |error| {
                        without_confirmed_prefix(0, first_publication.before_publication(error))
                    },
                )?;
            client
                .write_object_data(&mut combined, &bytes, &bounded)
                .map_err(|error| {
                    without_confirmed_prefix(0, first_publication.before_publication(error))
                })?;
            let first = client
                .publish_staged_receipt(first_publication, first.reserve_next, &bounded)
                .map_err(|failure| without_confirmed_prefix(0, failure))?;
            match client.publish_staged_receipt(second_publication, second.reserve_next, &bounded) {
                Ok(second) => Ok([first, second]),
                Err(failure) => Err(OrderedObjectAppendFailure {
                    failed_append: 1,
                    committed_first: Some(first),
                    failure,
                }),
            }
        });
        if result.is_err() {
            self.drop_append_plan();
        }
        result
    }

    fn publish_staged_receipt(
        &mut self,
        prepared: PreparedPublication<'_>,
        reserve_next: Option<(u32, u64)>,
        options: &ObjectWriteOptions,
    ) -> Result<StagedObjectAppendReceipt, ObjectWriteFailure> {
        self.reserve_next = reserve_next;
        self.reservation = None;
        let result = self.publish_prepared_object(prepared, options);
        self.reserve_next = None;
        let next_allocation = self.reservation.take();
        result.map(|manifest| StagedObjectAppendReceipt {
            manifest,
            next_allocation,
        })
    }
}

#[cfg(test)]
#[path = "staged_append_pair_tests.rs"]
mod staged_append_pair_tests;
