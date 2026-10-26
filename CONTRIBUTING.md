# Contributing to MatrixObject

MatrixObject is an Apache-2.0 object store written in Rust. Anyone may fork it, change
it, file issues against it and send pull requests; you do not need to ask first.

## What this repository is, in one paragraph

A single crate, `matrixobjectstore-rs`, holding the store: an S3-shaped object surface
over a chunk/page/fragment storage tier, with erasure-coded striping, write receipts, a
metaserver control plane, and an optional MatrixRaft backend behind the `matrix-raft`
feature. The S3 HTTP front door (`http-server`, also spelled `s3`) is on by default and
adds no dependencies. The library is `src/lib.rs` plus a tree of included modules; the
whole test suite lives in `src/tests.rs`.

## Build and test

```bash
cargo build --all-targets --locked
cargo test --lib
```

Both are what CI builds, except that CI also sets `RUSTFLAGS=-D warnings`, so run that
way before you push:

```bash
RUSTFLAGS="-D warnings" cargo build --all-targets --locked
RUSTFLAGS="-D warnings" cargo build --all-targets --locked --all-features
```

**17 tests fail on a clean checkout**, and they are recorded by
name in [`tools/known_failures.txt`](tools/known_failures.txt). That is why a bare
`cargo test` looks alarming on a clean checkout. Use the gate, which compares the
failures against that baseline in both directions — a new failure fails it, and so does a
recorded failure that has stopped happening:

```bash
tools/suite_gate.sh
```

Two things about the suite that will otherwise cost you an afternoon:

- **One test does not return.** Skip it:
  `cargo test --lib -- --skip round_backlog_cost::a_round_with_an_empty_backlog_still_costs_the_whole_store`.
- **`cargo test <filter>` exits 0 when the filter matches nothing.** If you run one test
  by name, check the output says `test result: ok. 1 passed` rather than trusting the exit
  code. `--exact` needs the full path, `tests::the_name`.

`cargo fmt` is not clean on this tree and CI does not run it, so format the lines you
add and leave the rest alone.

## What CI runs, and what it does not

Two jobs, both on a pinned toolchain:

| job | what it does |
|---|---|
| **build & docs** | `cargo build --all-targets --locked` and `--all-features`, then `cargo doc --no-deps --locked` and `--all-features`, all with warnings denied |
| **format and rollback guards** | 65 individual tests, each run by name with `--exact` |

**CI does not run the suite.** It compiles everything and runs those 65 guards. A green
tick means the tree builds and those guards hold; it does not mean the suite passed. Run
`tools/suite_gate.sh` yourself.

Each guard step greps its own output for `test result: ok. 1 passed`, because a filter
that matches nothing exits 0. If you rename a guard the workflow pins, the suite tells you
before you push: `every_guard_the_workflow_pins_by_name_exists` reads the workflow and
asserts every name it pins still exists and names exactly one test.

## How a change is expected to look

- **Add a test that would fail without the change.** If the change claims a number — bytes
  per object, an amplification ratio, an allocation count — the test measures it rather
  than restating it, and the number goes in the README beside the others.
- **A guard worth keeping gets pinned.** Add a step to
  [`.github/workflows/ci.yml`](.github/workflows/ci.yml) naming it with `--exact`, in the
  shape the steps already use.
- **Check your guard can fail.** Break the thing it guards, watch it go red, put it back.
  A test written for code that is already correct can pass for the wrong reason, and a
  guard nobody has seen fail is a guess.
- **Every `.rs` file carries two header lines**, and
  `every_rust_source_carries_the_licence_header` enforces it:

  ```rust
  // SPDX-License-Identifier: Apache-2.0
  // Copyright 2026 MatrixArkAI
  ```

- **Durable formats are not renamed in passing.** Several format constants are hashed into
  the proofs the store writes, so changing the text changes every checksum derived from it.
  `every_durable_format_constant_is_accounted_for` will stop you; if you need such a
  change, say in the pull request that it is a migration and what reads the old form.
- There is no `CHANGELOG.md`. The commit message is the record, so write it for someone
  reading `git log` a year from now.

The declared minimum Rust version is in `Cargo.toml` as `rust-version`. Nothing in CI
checks it, so treat it as a declaration rather than a tested floor.

## Pull requests

Open one against `main`. `main` carries no branch protection, so an outside pull request
can land without anything you cannot obtain. Say in the description what you measured, not
only what you changed — and if a part of it is unverified, say which part.

## Issues

Use the templates. For anything with a security or durability dimension, read
[`SECURITY.md`](SECURITY.md) first: in a store that acknowledges writes, a durability
defect is handled as a vulnerability and is reported privately rather than in a public
issue.

## Licence

By contributing you agree your contribution is licensed under Apache-2.0, the licence in
[`LICENSE`](LICENSE). Keep the attribution in [`NOTICE`](NOTICE) intact.
