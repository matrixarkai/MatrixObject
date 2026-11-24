// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Dense byte oracles for conservative live-payload accounting.
use super::*;

#[test]
fn checkpoint_payload_budget_never_exceeds_dense_payload_union() {
    let mut cache = PlannerCoverage::default();
    let mut all = [[0u32; 256]; 3];
    let mut payload = [[0u32; 256]; 3];
    let mut active = Vec::new();
    let mut random = 0x510e527fade682d1u64;
    let change = |cache: &mut PlannerCoverage,
                  metadata: bool,
                  segment: usize,
                  start: usize,
                  length: usize,
                  add: bool| {
        let record = ExtentRecord {
            store_id: [segment as u8 + 1; 16],
            offset: 4096 + start as u64,
            length: length as u64,
        };
        if metadata {
            if add {
                cache.add_record(record).unwrap();
            } else {
                cache.remove_record(record).unwrap();
            }
        } else {
            cache
                .physical(PhysicalRange::record(record).unwrap(), add)
                .unwrap();
        }
    };
    for _ in 0..2048 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let add = active.is_empty() || random & 3 != 0;
        let (metadata, segment, start, length) = if add {
            let start = (random as usize >> 8) % 256;
            let value = (
                random & 4 != 0,
                (random as usize >> 20) % 3,
                start,
                1 + ((random as usize >> 24) % (256 - start)),
            );
            active.push(value);
            value
        } else {
            active.swap_remove((random as usize >> 8) % active.len())
        };
        change(&mut cache, metadata, segment, start, length, add);
        for offset in start..start + length {
            if add {
                all[segment][offset] += 1;
            } else {
                all[segment][offset] -= 1;
            }
            if !metadata {
                if add {
                    payload[segment][offset] += 1;
                } else {
                    payload[segment][offset] -= 1;
                }
            }
        }
        let union = all.iter().flatten().filter(|&&value| value != 0).count() as u64;
        let data = payload
            .iter()
            .flatten()
            .filter(|&&value| value != 0)
            .count() as u64;
        assert_eq!(cache.covered_bytes, union);
        assert!(cache.payload_lower_bound(0).unwrap() <= data);
    }
    for (metadata, segment, start, length) in active {
        change(&mut cache, metadata, segment, start, length, false);
    }
    assert_eq!(cache.covered_bytes, 0);
    assert_eq!(cache.metadata_record_bytes, 0);
    assert_eq!(cache.payload_lower_bound(0).unwrap(), 0);
    assert!(cache.payload_lower_bound(1).is_err());
}

#[test]
fn checkpoint_payload_budget_counts_shared_metadata_once_and_remains_conservative() {
    let mut cache = PlannerCoverage::default();
    let record = ExtentRecord {
        store_id: [3; 16],
        offset: 4096,
        length: 8192,
    };
    let payload = PhysicalRange {
        segment: [3; 16],
        start: 8192,
        end: 16384,
    };
    for _ in 0..64 {
        cache.add_record(record).unwrap();
    }
    cache.physical(payload, true).unwrap();
    assert_eq!(cache.metadata_record_bytes, 8192);
    assert_eq!(cache.payload_lower_bound(0).unwrap(), 4096);
    for _ in 0..63 {
        cache.remove_record(record).unwrap();
    }
    assert_eq!(cache.payload_lower_bound(0).unwrap(), 4096);
    cache.remove_record(record).unwrap();
    assert_eq!(cache.payload_lower_bound(0).unwrap(), 8192);
    cache.physical(payload, false).unwrap();
    assert_eq!(cache.payload_lower_bound(0).unwrap(), 0);
}
