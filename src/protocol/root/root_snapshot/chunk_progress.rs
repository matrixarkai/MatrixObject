// SPDX-License-Identifier: Apache-2.0
impl RootSnapshotChunk {
    pub fn install_progress(&self) -> SnapshotInstallProgress {
        snapshot_install_progress(
            true,
            !self.eof,
            self.offset.saturating_add(self.payload.len() as u64),
            self.total_bytes,
            self.full_checksum.clone(),
            0,
            0,
        )
    }
}
