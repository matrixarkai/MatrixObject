// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn sort_metadata_maintenance_actions(actions: &mut [MetadataMaintenanceAction]) {
    actions.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| b.estimated_bytes.cmp(&a.estimated_bytes))
            .then_with(|| b.estimated_items.cmp(&a.estimated_items))
            .then_with(|| a.action.cmp(&b.action))
            .then_with(|| a.key.cmp(&b.key))
            .then_with(|| a.marker.cmp(&b.marker))
    });
}
