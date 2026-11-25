// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Resident bytes of an open log chunk, grown without reallocating its payload.
const BLOCK_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct ResidentData {
    blocks: Vec<Vec<u8>>,
    len: usize,
    capacity: usize,
}
impl ResidentData {
    pub(super) fn extend_from_slice(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if self
                .blocks
                .last()
                .is_none_or(|block| block.len() == BLOCK_BYTES)
            {
                let block = Vec::with_capacity(bytes.len().min(BLOCK_BYTES));
                self.capacity += block.capacity();
                self.blocks.push(block);
            }
            let block = self.blocks.last_mut().unwrap();
            let count = bytes.len().min(BLOCK_BYTES - block.len());
            let before = block.capacity();
            if count > block.capacity() - block.len() {
                // A new tail starts at its actual write size. Further growth can move
                // at most this one block, never the accumulated chunk payload.
                block.reserve_exact(BLOCK_BYTES - block.len());
            }
            block.extend_from_slice(&bytes[..count]);
            self.capacity += block.capacity() - before;
            self.len += count;
            bytes = &bytes[count..];
        }
    }
    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }
    pub(super) fn matches(&self, offset: usize, mut bytes: &[u8]) -> bool {
        if offset
            .checked_add(bytes.len())
            .is_none_or(|end| end > self.len)
        {
            return false;
        }
        let mut at = offset;
        while !bytes.is_empty() {
            let block = &self.blocks[at / BLOCK_BYTES];
            let start = at % BLOCK_BYTES;
            let count = bytes.len().min(block.len() - start);
            if block[start..start + count] != bytes[..count] {
                return false;
            }
            at += count;
            bytes = &bytes[count..];
        }
        true
    }
    pub(super) fn copy_into(&self, start: usize, end: usize, out: &mut Vec<u8>) {
        debug_assert!(start <= end && end <= self.len);
        let mut at = start;
        while at < end {
            let block = &self.blocks[at / BLOCK_BYTES];
            let start = at % BLOCK_BYTES;
            let count = (end - at).min(block.len() - start);
            out.extend_from_slice(&block[start..start + count]);
            at += count;
        }
    }
    pub(super) fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        let retained = len.div_ceil(BLOCK_BYTES);
        for block in self.blocks.drain(retained..) {
            self.capacity -= block.capacity();
        }
        if let Some(last) = self.blocks.last_mut() {
            last.truncate((len - 1) % BLOCK_BYTES + 1);
        }
        self.len = len;
    }
    pub(super) fn chunks(&self) -> impl Iterator<Item = &[u8]> {
        self.blocks.iter().map(Vec::as_slice)
    }
    pub(super) fn sha256(&self) -> [u8; 32] {
        let mut context = ring::digest::Context::new(&ring::digest::SHA256);
        for block in self.chunks() {
            context.update(block);
        }
        context.finish().as_ref().try_into().unwrap()
    }
}
