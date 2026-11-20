// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded payload residency over a complete durable page catalog.
use super::*;
use crate::cluster::replica_page_backing::LivePageBacking;
use crate::{PageId, ReplicaHead};
use std::collections::{BTreeMap, BTreeSet};
fn corrupt(error: impl ToString) -> ObjectError {
    ObjectError::Corruption(error.to_string())
}

pub(super) struct ResidentCache {
    pub backing: Arc<LivePageBacking>,
    pub total_pages: usize,
    budget: u64,
    entries: usize,
    clock: u64,
    bytes: u64,
    pages: BTreeMap<PageId, (u64, u64)>,
    order: BTreeSet<(u64, PageId)>,
}
impl ResidentCache {
    pub fn new(
        backing: Arc<LivePageBacking>,
        budget: u64,
        entries: usize,
    ) -> std::io::Result<Self> {
        let (total_pages, _) = backing.totals()?;
        Ok(Self {
            backing,
            total_pages,
            budget,
            entries,
            clock: 0,
            bytes: 0,
            pages: BTreeMap::new(),
            order: BTreeSet::new(),
        })
    }
    pub fn touch(
        &mut self,
        service: &MatrixReplicaService,
        id: &PageId,
    ) -> Result<(), ObjectError> {
        let Some(bytes) = service.resident_page_capacity(id) else {
            return Ok(());
        };
        self.clock = self
            .clock
            .checked_add(1)
            .ok_or_else(|| corrupt("resident access sequence overflow"))?;
        if let Some((clock, old)) = self.pages.remove(id) {
            self.order.remove(&(clock, id.clone()));
            self.bytes = self
                .bytes
                .checked_sub(old)
                .ok_or_else(|| corrupt("resident byte count underflow"))?;
        }
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| corrupt("resident byte count overflow"))?;
        self.pages.insert(id.clone(), (self.clock, bytes));
        self.order.insert((self.clock, id.clone()));
        Ok(())
    }
    pub fn over_budget(&self) -> bool {
        self.bytes > self.budget || self.pages.len() > self.entries
    }
    // A false result leaves the oldest unsynchronized version resident.
    // Strict trimming refuses it if synchronization cannot make it durable.
    fn try_trim_to(
        &mut self,
        service: &mut MatrixReplicaService,
        bytes: u64,
        entries: usize,
    ) -> Result<bool, ObjectError> {
        while self.bytes > bytes || self.pages.len() > entries {
            let (clock, id) = self
                .order
                .first()
                .cloned()
                .ok_or_else(|| corrupt("resident eviction order is empty"))?;
            let metadata = self.backing.metadata(&id).map_err(corrupt)?;
            let Some(metadata) = metadata else {
                return Ok(false);
            };
            if service.resident_page_stamp(&id).as_deref() != Some(metadata.state_checksum.as_str())
            {
                return Ok(false);
            }
            service.evict_resident_page(&id)?;
            let (_, size) = self
                .pages
                .remove(&id)
                .ok_or_else(|| corrupt("resident entry is missing"))?;
            self.order.remove(&(clock, id));
            self.bytes = self
                .bytes
                .checked_sub(size)
                .ok_or_else(|| corrupt("resident byte count underflow"))?;
        }
        Ok(true)
    }
    fn trim_to(
        &mut self,
        service: &mut MatrixReplicaService,
        bytes: u64,
        entries: usize,
    ) -> Result<(), ObjectError> {
        if !self.try_trim_to(service, bytes, entries)? {
            return Err(corrupt("resident eviction needs synchronized backing"));
        }
        Ok(())
    }
    pub fn try_trim(&mut self, service: &mut MatrixReplicaService) -> Result<bool, ObjectError> {
        self.try_trim_to(service, self.budget, self.entries)
    }
    pub fn trim(&mut self, service: &mut MatrixReplicaService) -> Result<(), ObjectError> {
        self.trim_to(service, self.budget, self.entries)
    }
}
impl NodeReplica {
    pub(super) fn global_page_count(&self) -> usize {
        self.resident
            .as_ref()
            .map_or_else(|| self.service.page_count(), |cache| cache.total_pages)
    }
    pub(super) fn page_head(&self, id: &PageId) -> Result<ReplicaHead, ObjectError> {
        if self.service.page_write_sequence(id).is_some() {
            return self.service.head_chunk(id);
        }
        match &self.resident {
            Some(cache) => cache
                .backing
                .metadata(id)
                .map_err(corrupt)?
                .map(|metadata| metadata.head(id))
                .ok_or_else(|| ObjectError::NotFound("chunk is missing".into())),
            None => self.service.head_chunk(id),
        }
    }
    pub(super) fn global_page_bytes(&self, id: &PageId) -> Result<u64, ObjectError> {
        if self.service.page_write_sequence(id).is_some() {
            return Ok(self.service.page_physical_len(id));
        }
        Ok(match &self.resident {
            Some(cache) => cache
                .backing
                .metadata(id)
                .map_err(corrupt)?
                .map_or(0, |m| m.data_length),
            None => 0,
        })
    }
    pub(super) fn resident_inventory(
        &self,
        after: Option<&PageId>,
        limit: usize,
    ) -> Result<Vec<(PageId, u64)>, ObjectError> {
        let mut result: BTreeMap<_, _> = match &self.resident {
            Some(cache) => cache
                .backing
                .sequences_after(after, limit)
                .map_err(corrupt)?
                .into_iter()
                .collect(),
            None => BTreeMap::new(),
        };
        result.extend(self.service.page_write_sequences_after(after, limit));
        Ok(result.into_iter().take(limit).collect())
    }
    pub(super) fn load_resident_page(&mut self, id: &PageId) -> Result<(), ObjectError> {
        let Some(cache) = &mut self.resident else {
            return Ok(());
        };
        if self.service.page_write_sequence(id).is_some() {
            return cache.touch(&self.service, id);
        }
        let Some(metadata) = cache.backing.metadata(id).map_err(corrupt)? else {
            return Ok(());
        };
        let allowance = cache.budget.saturating_sub(metadata.data_length);
        let entries = cache.entries.saturating_sub(1);
        if cache.bytes > allowance || cache.pages.len() > entries {
            let ready = match cache.try_trim_to(&mut self.service, allowance, entries) {
                Ok(done) => done,
                Err(error) => {
                    self.health.fail();
                    return Err(error);
                }
            };
            if !ready {
                // A pending victim must become durable before it can leave memory.
                if let Err(error) = self
                    .journal
                    .as_mut()
                    .ok_or_else(|| corrupt("resident cache has no journal"))?
                    .sync_deferred()
                {
                    self.health.fail();
                    return Err(corrupt(error));
                }
                cache.trim_to(&mut self.service, allowance, entries)?;
            }
        }
        let plan = cache
            .backing
            .capture_page(id)
            .map_err(corrupt)?
            .ok_or_else(|| corrupt("resident admission lost its page"))?;
        let state = plan.materialize().map_err(corrupt)?;
        self.service.admit_resident_page(id.clone(), state)?;
        cache.touch(&self.service, id)
    }
    pub(super) fn trim_residency(&mut self) -> Result<(), ObjectError> {
        let result = match &mut self.resident {
            Some(cache) => cache.trim(&mut self.service),
            None => Ok(()),
        };
        if result.is_err() {
            self.health.fail();
        }
        result
    }
    pub(super) fn try_trim_residency(&mut self) -> Result<bool, ObjectError> {
        let result = match &mut self.resident {
            Some(cache) => cache.try_trim(&mut self.service),
            None => Ok(true),
        };
        if result.is_err() {
            self.health.fail();
        }
        result
    }
    pub(super) fn track_resident_mutation(
        &mut self,
        id: &PageId,
        existed: bool,
    ) -> Result<(), ObjectError> {
        if let Some(cache) = &mut self.resident {
            if !existed && self.service.page_write_sequence(id).is_some() {
                cache.total_pages = cache
                    .total_pages
                    .checked_add(1)
                    .ok_or_else(|| corrupt("page count overflow"))?;
            }
            cache.touch(&self.service, id)?;
        }
        Ok(())
    }
}
