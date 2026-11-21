// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::chunk_catalog::ChunkOutcome;
use crate::chunk_manifest::{ChunkManifest, CHUNK_MANIFEST_FORMAT};
use crate::chunk_object::{build_manifest, extent_for, pages_of, plan_stripes, Placement};

const PAGE: u64 = 8;
const STRIPE: u64 = 24;
const GRACE: u64 = 100;

/// A group's pages and the catalog, driven the way a client and a sweeper would drive them.
struct World {
    catalog: ChunkCatalog,
    pages: BTreeMap<PageKey, u64>,
    next_page: u64,
}

fn key(page: u64) -> PageKey {
    PageKey { group: "g0".into(), volume_id: 0, chunk_id: 7, page_index: page }
}

impl World {
    fn new() -> Self {
        Self { catalog: ChunkCatalog::new(), pages: BTreeMap::new(), next_page: 3 }
    }

    /// Write an object's pages (data pages, then the digest page of a multi-page stripe) and return
    /// the manifest that would name them. Nothing is published.
    fn write(&mut self, name: &str, generation: u64, data: &[u8]) -> ChunkManifest {
        let mut extents = Vec::new();
        for stripe in plan_stripes(data.len() as u64, PAGE, STRIPE).unwrap() {
            let bytes = &data[stripe.range()];
            let digests = crate::chunk_integrity::digest_extent(bytes, PAGE as usize).unwrap();
            let placement = Placement { group: "g0".into(), volume_id: 0, chunk_id: 7, first_page: self.next_page };
            for (offset, _) in pages_of(bytes, PAGE as usize, &digests) {
                assert!(self.pages.insert(key(self.next_page + offset), 1).is_none());
            }
            extents.push(extent_for(&placement, &stripe, &digests, PAGE));
            self.next_page += crate::chunk_integrity::stored_pages(stripe.length, PAGE);
        }
        build_manifest("b", name, generation, PAGE, extents).unwrap()
    }

    fn publish(&mut self, before: Option<&ChunkManifest>, after: &ChunkManifest) {
        let outcome = ChunkOutcome::decide(before.cloned(), after.clone()).unwrap();
        assert!(self.catalog.apply(&outcome).unwrap());
    }

    fn delete(&mut self, current: &ChunkManifest) {
        let tombstone = ChunkManifest {
            format_version: CHUNK_MANIFEST_FORMAT,
            bucket: current.bucket.clone(),
            key: current.key.clone(),
            generation: current.generation + 1,
            deleted: true,
            ..Default::default()
        };
        self.publish(Some(current), &tombstone);
    }

    fn inventory(&self) -> Vec<(PageKey, u64)> {
        self.pages.iter().map(|(page, sequence)| (page.clone(), *sequence)).collect()
    }

    /// One sweep: the inventory, then the catalog, then the pages that are due and still unnamed are
    /// discarded (a discard moves a page's sequence past its first write).
    fn sweep(&mut self, candidates: &mut Candidates, now: u64) -> Vec<PageKey> {
        let inventory = self.inventory();
        let live = live_pages(&self.catalog);
        candidates.scan(&inventory, &live, now);
        let freed = confirm(candidates.due(now, GRACE), &live_pages(&self.catalog));
        for page in &freed {
            *self.pages.get_mut(page).unwrap() += 1;
        }
        freed
    }

    fn pages_of_manifest(&self, manifest: &ChunkManifest) -> BTreeSet<PageKey> {
        // a catalog of this one manifest, whatever its generation (a first publication takes generation 1)
        let mut first = manifest.clone();
        first.generation = 1;
        let mut only = ChunkCatalog::new();
        only.apply(&ChunkOutcome::decide(None, first).unwrap()).unwrap();
        live_pages(&only)
    }
}

fn bytes(length: usize, salt: u8) -> Vec<u8> {
    (0..length).map(|index| (index as u8).wrapping_mul(7).wrapping_add(salt)).collect()
}

#[test]
fn the_live_pages_are_the_data_pages_and_the_digest_page_of_every_live_extent() {
    let mut world = World::new();
    // 20 bytes over 8-byte pages: three data pages and a digest page; 5 bytes: one page
    let long = world.write("long", 1, &bytes(20, 1));
    let short = world.write("short", 1, &bytes(5, 2));
    assert_eq!(world.pages.len(), 5);
    world.publish(None, &long);
    world.publish(None, &short);
    let live = live_pages(&world.catalog);
    assert_eq!(live, world.pages.keys().cloned().collect::<BTreeSet<_>>(), "every page written is named");
    assert!(live.contains(&key(3 + 3)), "the digest page of the multi-page extent is named");
    // a deleted object names nothing
    world.delete(&long);
    let live = live_pages(&world.catalog);
    assert_eq!(live, world.pages_of_manifest(&short));
    assert_eq!(live.len(), 1);
    // a stripe boundary: 50 bytes make three extents, each with its own run
    let mut world = World::new();
    let big = world.write("big", 1, &bytes(50, 3));
    world.publish(None, &big);
    assert_eq!(big.extents.len(), 3);
    assert_eq!(live_pages(&world.catalog), world.pages.keys().cloned().collect::<BTreeSet<_>>());
}

#[test]
fn a_deleted_objects_pages_are_freed_after_the_grace_and_a_live_objects_are_not() {
    let mut world = World::new();
    let a = world.write("a", 1, &bytes(50, 1));
    let b = world.write("b", 1, &bytes(50, 2));
    world.publish(None, &a);
    world.publish(None, &b);
    let a_pages = world.pages_of_manifest(&a);
    let b_pages = world.pages_of_manifest(&b);
    world.delete(&a);
    let mut candidates = Candidates::new();
    assert!(world.sweep(&mut candidates, 0).is_empty(), "the first sight starts a clock and frees nothing");
    assert!(world.sweep(&mut candidates, GRACE - 1).is_empty(), "not yet");
    let freed: BTreeSet<PageKey> = world.sweep(&mut candidates, GRACE).into_iter().collect();
    assert_eq!(freed, a_pages, "exactly the deleted object's pages, data and digest pages alike");
    for page in &b_pages {
        assert_eq!(world.pages[page], 1, "a live object's page is untouched");
    }
    for page in &a_pages {
        assert_eq!(world.pages[page], 2, "a freed page has been discarded");
    }
    // a swept page is not swept again, and the live object is still whole
    assert!(world.sweep(&mut candidates, 10 * GRACE).is_empty());
    assert_eq!(live_pages(&world.catalog), b_pages);
    assert!(candidates.is_empty());
}

#[test]
fn an_overwrite_frees_the_old_generation_and_keeps_the_new_one() {
    let mut world = World::new();
    let first = world.write("a", 1, &bytes(40, 1));
    world.publish(None, &first);
    let second = world.write("a", 2, &bytes(30, 9));
    world.publish(Some(&first), &second);
    let old_pages = world.pages_of_manifest(&first);
    let new_pages = world.pages_of_manifest(&second);
    let mut candidates = Candidates::new();
    world.sweep(&mut candidates, 0);
    let freed: BTreeSet<PageKey> = world.sweep(&mut candidates, GRACE).into_iter().collect();
    assert_eq!(freed, old_pages);
    assert!(freed.is_disjoint(&new_pages));
    assert_eq!(live_pages(&world.catalog), new_pages);
}

#[test]
fn pages_of_a_writer_that_never_published_are_freed_but_only_after_the_grace() {
    let mut world = World::new();
    let abandoned = world.write("a", 1, &bytes(30, 1));
    let pages = world.pages_of_manifest(&abandoned);
    let mut candidates = Candidates::new();
    for now in [0, 40, GRACE - 1] {
        assert!(world.sweep(&mut candidates, now).is_empty(), "at {now}");
    }
    let freed: BTreeSet<PageKey> = world.sweep(&mut candidates, GRACE).into_iter().collect();
    assert_eq!(freed, pages);
}

#[test]
fn a_manifest_published_between_two_scans_keeps_its_pages() {
    let mut world = World::new();
    let slow = world.write("a", 1, &bytes(30, 1));
    let mut candidates = Candidates::new();
    // the sweeper sees the written pages unnamed, then the writer publishes
    world.sweep(&mut candidates, 0);
    assert!(!candidates.is_empty());
    world.publish(None, &slow);
    assert!(world.sweep(&mut candidates, GRACE * 10).is_empty(), "the pages are named now, however old the sighting");
    assert!(candidates.is_empty(), "a named page leaves the table");
    for page in world.pages_of_manifest(&slow) {
        assert_eq!(world.pages[&page], 1);
    }
}

#[test]
fn the_last_check_keeps_pages_published_after_the_due_list_was_made() {
    let mut world = World::new();
    let late = world.write("a", 1, &bytes(30, 1));
    let mut candidates = Candidates::new();
    world.sweep(&mut candidates, 0);
    let due = candidates.due(GRACE, GRACE);
    assert!(!due.is_empty());
    // the writer publishes after the list was made and before the discard
    world.publish(None, &late);
    let confirmed = confirm(due.clone(), &live_pages(&world.catalog));
    assert!(confirmed.is_empty(), "the catalog read again names every one of them");
    // without that read the discard would have freed live pages
    assert!(!due.is_empty());
}

#[test]
fn a_restart_forgets_the_sightings_and_starts_every_clock_again() {
    let mut world = World::new();
    let abandoned = world.write("a", 1, &bytes(30, 1));
    let pages = world.pages_of_manifest(&abandoned);
    let mut before = Candidates::new();
    world.sweep(&mut before, 0);
    assert_eq!(before.len(), pages.len());
    // a new process holds nothing, so what it sees at 1,000 is first seen at 1,000
    let mut after = Candidates::new();
    assert!(world.sweep(&mut after, 1_000).is_empty());
    assert!(world.sweep(&mut after, 1_000 + GRACE - 1).is_empty());
    assert_eq!(world.sweep(&mut after, 1_000 + GRACE).len(), pages.len());
}

#[test]
fn a_page_already_swept_or_rewritten_is_never_a_candidate_and_a_clock_cannot_run_backwards() {
    let mut candidates = Candidates::new();
    let live = BTreeSet::new();
    // sequence 2: swept before, or rewritten by whoever owns it
    candidates.scan(&[(key(1), 2), (key(2), 1)], &live, 50);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates.due(50 + GRACE, GRACE), vec![key(2)]);
    // a clock that reads earlier than the sighting is not "due"
    assert!(candidates.due(10, GRACE).is_empty());
    // a page that left the inventory leaves the table
    candidates.scan(&[], &live, 60);
    assert!(candidates.is_empty());
}
