// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// The heap-byte helpers every footprint audit shares: what a `String` owns, and a ledger that
// charges a shared text allocation once however many holders name it.
/// Heap bytes a `String` owns -- its capacity, not its length, because the
/// slack is resident too and making slack visible is what this audit is for.
/// The 24 bytes of pointer, length and capacity live inside whatever holds the
/// string and are counted there.
pub(crate) fn audit_string_heap(value: &String) -> u64 {
    value.capacity() as u64
}
/// Charges the characters behind a `SharedText` once per allocation, however
/// many holders name it.
///
/// Several holders name the same object, and since they share the characters,
/// adding the length at each one would report copies that do not exist -- and
/// would leave the figure unmoved by the sharing it exists to show. `Arc<str>`
/// has no spare capacity, so the charge is the length.
#[derive(Default)]
pub(crate) struct SharedTextLedger {
    charged: std::collections::HashSet<usize>,
}

impl SharedTextLedger {
    /// The characters plus the two counts in front of them.
    ///
    /// A `String` has no header, so a shared handle only pays for itself once
    /// the sharing, or the smaller inline handle, covers these bytes. Leaving
    /// them out made every figure the sharing produced look better than it is.
    /// The allocator's own size rounding is excluded, as capacity rounding is
    /// everywhere else in this audit.
    const SHARED_ALLOCATION_HEADER: u64 = 2 * std::mem::size_of::<usize>() as u64;

    pub(crate) fn charge(&mut self, value: &SharedText) -> u64 {
        if self.charged.insert(value.allocation_id()) {
            (value.as_str().len() as u64).saturating_add(Self::SHARED_ALLOCATION_HEADER)
        } else {
            0
        }
    }
}
