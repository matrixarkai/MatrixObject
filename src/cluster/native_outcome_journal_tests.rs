// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
mod native_journal {
    use super::*;
    use crate::native_object_outcome::NativeObjectChange;
    use crate::native_object_outcome_tests::{commands, service};
    #[test]
    fn native_staging_and_publication_survive_actual_journal_and_snapshot_restarts() {
        let directory = std::env::temp_dir().join(format!(
            "native-journal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let mut current = service();
        let (mut store, recovered) = MetaSnapshotFile::open(&directory, &mut current).unwrap();
        assert!(!recovered);
        store.persist(&current, &Default::default()).unwrap();
        for (op, now) in commands().into_iter().take(2) {
            let decided = NativeObjectChange::decide(&current, op, now).unwrap();
            store.append_outcome(&decided).unwrap();
            decided.apply(&mut current).unwrap();
        }
        let pending = current.export_snapshot().to_bytes().unwrap();
        assert!(current.chunk_catalog.is_empty());
        drop(store);
        let mut restarted = MatrixMetaService::new();
        let (mut store, recovered) = MetaSnapshotFile::open(&directory, &mut restarted).unwrap();
        assert!(recovered);
        assert_eq!(restarted.export_snapshot().to_bytes().unwrap(), pending);
        assert_eq!(restarted.native_objects.owned_chunks(2).count(), 1);
        let (op, now) = commands().remove(2);
        let decided = NativeObjectChange::decide(&restarted, op, now).unwrap();
        store.append_outcome(&decided).unwrap();
        decided.apply(&mut restarted).unwrap();
        let published = restarted.export_snapshot().to_bytes().unwrap();
        drop(store);
        let mut replayed = MatrixMetaService::new();
        let (store, recovered) = MetaSnapshotFile::open(&directory, &mut replayed).unwrap();
        assert!(recovered);
        assert_eq!(replayed.export_snapshot().to_bytes().unwrap(), published);
        store.persist(&replayed, &Default::default()).unwrap();
        drop(store);
        let mut snapshotted = MatrixMetaService::new();
        let (_store, recovered) = MetaSnapshotFile::open(&directory, &mut snapshotted).unwrap();
        assert!(recovered);
        assert_eq!(snapshotted.export_snapshot().to_bytes().unwrap(), published);
        assert_eq!(snapshotted.native_objects.stage_count(), 0);
        assert_eq!(snapshotted.chunk_catalog.len(), 1);
        drop(_store);
        let path = directory.join("metadata.outcomes");
        let journal = std::fs::read(&path).unwrap();
        let mut torn = journal.clone();
        torn.push(b'M');
        std::fs::write(&path, torn).unwrap();
        let mut recovered = MatrixMetaService::new();
        let (store, _) = MetaSnapshotFile::open(&directory, &mut recovered).unwrap();
        assert_eq!(recovered.export_snapshot().to_bytes().unwrap(), published);
        assert_eq!(std::fs::read(&path).unwrap(), journal);
        drop(store);
        let mut duplicate = journal.clone();
        duplicate.extend_from_slice(&journal);
        std::fs::write(&path, duplicate).unwrap();
        assert!(MetaSnapshotFile::open(&directory, &mut MatrixMetaService::new()).is_err());
        let mut corrupt = journal.clone();
        corrupt[24] ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        assert!(MetaSnapshotFile::open(&directory, &mut MatrixMetaService::new()).is_err());
        std::fs::write(&path, journal).unwrap();
    }
}
