// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The guards that read the repository itself -- its workflow, its guides, its licence headers
//! and the binaries its README names -- rather than any store. They live apart from the store
//! test files so that no store can be removed without these still running.
use super::*;

#[test]
#[ignore = "compiled-harness discovery spawns a process; run separately with --ignored"]
fn every_guard_the_workflow_pins_by_name_exists() {
    // CI runs named guards as separate steps, each selecting one test with `--exact`.
    // `cargo test <filter>` EXITS 0 WHEN THE FILTER MATCHES NOTHING, so every step greps its
    // own output for `test result: ok. 1 passed` and fails with "the guard did not run"
    // otherwise. That is what caught two renames in this change -- but it caught them in CI,
    // after a push, because nothing in the tree knew the workflow had opinions about test
    // names.
    //
    // This is that missing half: rename a pinned guard and the suite says so, here, before
    // the push.
    const WORKFLOW: &str = include_str!("../.github/workflows/ci.yml");
    // Ask the compiled test harness, including tests in nested server modules.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--list", "--format", "terse"])
        .output()
        .expect("the test harness should list its tests");
    assert!(output.status.success(), "listing compiled tests failed");
    let listing = String::from_utf8(output.stdout).unwrap();
    let compiled: Vec<&str> = listing
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .collect();
    assert!(
        compiled.len() >= 25,
        "the compiled test list must not be empty"
    );

    // `tests::<name>` is how the workflow spells a filter. Taken from the workflow rather
    // than from a list kept here, so a step ADDED later is covered without anyone
    // remembering to add it twice.
    let mut pinned: Vec<&str> = Vec::new();
    // Validate every available test in this harness. Optional tests absent from
    // this build are checked by the all-feature invocation in CI.
    let mut requested_features_enabled = true;
    let mut unavailable = BTreeSet::new();
    for line in WORKFLOW.lines() {
        if line.trim_start().starts_with("cargo test ") {
            requested_features_enabled = true;
        }
        let mut words = line.split_whitespace();
        while let Some(word) = words.next() {
            if word == "--all-features" {
                requested_features_enabled &= cfg!(all(
                    feature = "matrix-raft",
                    feature = "metadata-consensus",
                    feature = "chunk-consensus",
                    feature = "peer-tls"
                ));
            }
            if word == "--features" {
                for feature in words.next().expect("features need a value").split(',') {
                    let enabled = match feature {
                        "matrix-raft" => cfg!(feature = "matrix-raft"),
                        "metadata-consensus" => cfg!(feature = "metadata-consensus"),
                        "chunk-consensus" => cfg!(feature = "chunk-consensus"),
                        "peer-tls" => cfg!(feature = "peer-tls"),
                        _ => panic!("workflow guard must account for feature {feature}"),
                    };
                    requested_features_enabled &= enabled;
                }
            }
        }
        let Some(at) = line.find("tests::") else {
            continue;
        };
        let name: &str = line[at + "tests::".len()..]
            .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .next()
            .unwrap_or("");
        if !name.is_empty() {
            if requested_features_enabled
                || compiled
                    .iter()
                    .any(|test| test.ends_with(&format!("::{name}")))
            {
                pinned.push(name);
            } else {
                unavailable.insert(name);
            }
        }
    }
    pinned.sort_unstable();
    pinned.dedup();
    // Not vacuous: a parse that found nothing would pass every assertion below.
    assert!(
        pinned.len() >= 25,
        "the workflow should pin a couple of dozen guards, and this found {}: {pinned:?}",
        pinned.len()
    );

    let missing: Vec<&str> = pinned
        .iter()
        .copied()
        .filter(|name| {
            // A PINNED FILTER SELECTS A TEST OR A MODULE OF THEM, and this only knew the first.
            // The workflow runs `cluster::meta_consensus::tests::catalog_batches` and asserts
            // `4 passed`; `catalog_batches` is a `mod`, so no compiled test ENDS with it -- four
            // of them contain it -- and this guard called a working filter missing.
            //
            // What it is for is that the filter matches something, because `cargo test` exits 0
            // when it matches nothing. A module filter that selects four tests matches something.
            // Renaming either the test or the module still matches neither shape and is still
            // caught, which is the half this check was added to cover.
            let selects_a_test = compiled
                .iter()
                .any(|test| test.ends_with(&format!("::{name}")));
            let selects_a_module = compiled
                .iter()
                .any(|test| test.contains(&format!("::{name}::")));
            !(selects_a_test || selects_a_module)
        })
        .collect();
    assert!(
        missing.is_empty(),
        "the workflow runs these by name and this tree has no such test: {missing:?} -- \
         rename them in the workflow too, or the step will pass nothing and report that it \
         ran no guard"
    );
    if cfg!(all(
        feature = "metadata-consensus",
        feature = "chunk-consensus"
    )) {
        assert!(
            unavailable.is_empty(),
            "all-feature validation must check every guard"
        );
    }
    println!(
        "WORKFLOWPINS {} enabled guards checked; {} need optional features",
        pinned.len(),
        unavailable.len()
    );

    // AND THE SPELLING THE STEPS CHECK FOR IS THE ONE CARGO PRINTS. If that line ever
    // changed, every step would fail open -- the grep would never match and the step would
    // always error, which is noisy rather than dangerous, but the reverse is worth pinning:
    // the steps rely on a single test passing, so none of them may name a filter that
    // matches more than one test.
    // The rule was "exactly one test", reasoned from every step grepping `ok. 1 passed`. Not
    // every step does: the one pinning `catalog_batches` greps `ok. 4 passed`, because that
    // filter names a module of four. So the invariant is the PAIRING -- a filter selects as many
    // tests as its own step asserts passed -- which is stronger than a fixed one, since it
    // catches a filter that grew or lost a test as well as one that matches nothing, and it is
    // true of a test name and a module alike.
    let selected = |name: &str| {
        compiled
            .iter()
            .filter(|test| {
                test.ends_with(&format!("::{name}")) || test.contains(&format!("::{name}::"))
            })
            .count()
    };
    let workflow_lines: Vec<&str> = WORKFLOW.lines().collect();
    let mut asserted: std::collections::BTreeMap<&str, usize> =
        std::collections::BTreeMap::new();
    for (index, line) in workflow_lines.iter().enumerate() {
        let Some(at) = line.find("tests::") else {
            continue;
        };
        let name = line[at + "tests::".len()..]
            .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .next()
            .unwrap_or("");
        if name.is_empty() {
            continue;
        }
        // The step's own check sits within a few lines of the command it checks.
        for ahead in workflow_lines.iter().skip(index).take(8) {
            let Some(rest) = ahead.split("test result: ok. ").nth(1) else {
                continue;
            };
            if let Some(count) = rest
                .split_whitespace()
                .next()
                .and_then(|word| word.parse::<usize>().ok())
            {
                asserted.insert(name, count);
                break;
            }
        }
    }
    // ONLY FOR NAMES THIS BUILD HAS. `pinned` is already feature-aware: a step asking for
    // `--features matrix-raft` contributes to `unavailable` instead when that feature is off,
    // and the pairing has to respect the same thing. CI runs this guard on DEFAULT features --
    // `cargo test --locked --lib repo_guard_tests::every_guard_the_workflow_pins_by_name_exists -- --exact`
    // -- so there the consensus and raft guards are not compiled, select zero, and would fail a
    // pairing against the one their own step asserts. Verifying this only under `--all-features`
    // is what hid that, and CI found it.
    let paired: Vec<(&&str, &usize)> = asserted
        .iter()
        .filter(|(name, _)| pinned.iter().any(|candidate| candidate == *name))
        .collect();
    // Not vacuous: if the pairing found nothing, the loop below would assert nothing.
    assert!(
        paired.len() >= 20,
        "only {} pinned guards present in this build could be paired with the count their own \
         step greps for, so the assertion below is proving almost nothing",
        paired.len()
    );
    for (name, expected) in paired {
        assert_eq!(
            selected(name),
            *expected,
            "the step pinning {name} greps for {expected} passed, and that filter selects {} \
             test(s) in this build",
            selected(name)
        );
    }
}

#[test]
fn the_repository_carries_what_an_outside_contributor_needs() {
    // The licence grants a stranger the right to change this store. It does not tell them
    // how, who reviews, or what not to put in a public issue -- and a file that answers
    // those questions ABOUT A DIFFERENT PROJECT answers none of them.
    //
    // That was the state: `CONTRIBUTING.md` opened "Contributing to MatrixRaft" and sent a
    // reader to `scripts/verify_like_ci.sh`, `CHANGELOG.md` and a safety-review document,
    // none of which are in this tree, while naming an MSRV and a `cargo-deny` gate this
    // repository does not have. Every openness check passed it, because every one of them
    // asks whether the file is PRESENT.
    //
    // So this guard asks two further things, and the second is the one that would have
    // caught it: that the guide names this project, and that every repo-relative path it
    // points at exists.
    const CONTRIBUTING: &str = include_str!("../CONTRIBUTING.md");

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

    // What a stranger needs before they can land anything: what the thing is, how to build
    // and test it, what they may do with it, who reviews, and how to report something that
    // must not be public.
    let required = [
        "README.md",
        "CONTRIBUTING.md",
        "LICENSE",
        "NOTICE",
        "SECURITY.md",
        "CODE_OF_CONDUCT.md",
        ".github/CODEOWNERS",
        ".github/pull_request_template.md",
        ".github/ISSUE_TEMPLATE/config.yml",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
    ];
    let absent: Vec<&str> = required
        .iter()
        .copied()
        .filter(|name| !root.join(name).exists())
        .collect();
    assert!(
        absent.is_empty(),
        "an outside contributor needs these and they are not in the tree: {absent:?}"
    );

    let first_heading = CONTRIBUTING
        .lines()
        .find(|line| line.starts_with("# "))
        .unwrap_or("");
    assert_eq!(
        first_heading, "# Contributing to MatrixObject",
        "the contributing guide should be this project's, and it opens {first_heading:?}"
    );

    // Markdown link targets and backticked tokens, kept only when they look like a path in
    // this repository: a slash, no space, no scheme. That leaves `tools/suite_gate.sh` and
    // `.github/workflows/ci.yml` in, and keeps commands and option strings out.
    let mut cited: Vec<&str> = Vec::new();
    for (index, _) in CONTRIBUTING.match_indices("](") {
        let rest = &CONTRIBUTING[index + 2..];
        if let Some(end) = rest.find(')') {
            cited.push(&rest[..end]);
        }
    }
    for piece in CONTRIBUTING.split('`').skip(1).step_by(2) {
        cited.push(piece);
    }
    let mut paths: Vec<&str> = cited
        .into_iter()
        .filter(|cite| {
            cite.contains('/')
                && !cite.contains(' ')
                && !cite.contains('\n')
                && !cite.contains("://")
                && !cite.starts_with('#')
        })
        .collect();
    paths.sort_unstable();
    paths.dedup();
    // Not vacuous: an extraction that found nothing would pass the check below, which is
    // the shape every blind guard in this repository has had.
    assert!(
        paths.len() >= 5,
        "the guide should point at several files in this tree, and this found {paths:?}"
    );
    println!("CONTRIBUTING cites {} in-tree paths: {paths:?}", paths.len());
    let missing: Vec<&str> = paths
        .iter()
        .copied()
        .filter(|path| !root.join(path).exists())
        .collect();
    assert!(
        missing.is_empty(),
        "the contributing guide sends a reader to paths that are not here: {missing:?}"
    );

    // AND THE FIGURE THE GUIDE QUOTES IS THE WORKFLOW'S. "33 individual tests" is a
    // literal in prose, which goes stale the moment a step is added -- so it is read back
    // out of the workflow rather than trusted. `every_guard_the_workflow_pins_by_name_exists`
    // does the same extraction for a different purpose; this one only needs the count.
    const WORKFLOW: &str = include_str!("../.github/workflows/ci.yml");
    let mut pinned: Vec<&str> = Vec::new();
    for line in WORKFLOW.lines() {
        let Some(at) = line.find("tests::") else {
            continue;
        };
        let name = line[at + "tests::".len()..]
            .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .next()
            .unwrap_or("");
        if !name.is_empty() {
            pinned.push(name);
        }
    }
    pinned.sort_unstable();
    pinned.dedup();
    assert!(
        pinned.len() >= 25,
        "the extraction should find a couple of dozen pinned guards, and it found {}",
        pinned.len()
    );
    let quoted = format!("{} individual tests", pinned.len());
    assert!(
        CONTRIBUTING.contains(&quoted),
        "the guide should say {quoted:?}, because that is how many guards the workflow pins"
    );
    let quoted = format!("those {} guards", pinned.len());
    assert!(
        CONTRIBUTING.contains(&quoted),
        "the guide should say {quoted:?} as well, so the two figures cannot drift apart"
    );

    // NOT CHECKED HERE, deliberately: that `main` carries no branch protection, and that
    // the repository is public, forkable and issues-open. Those are account state rather
    // than tree state -- a test cannot see them, and one that pretended to would be the
    // most reassuring kind of blind guard. They are audited against the GitHub API.
}

#[test]
fn every_rust_source_carries_the_licence_header() {
    // Two lines, at the top of every Rust source, the way the sibling store carries them:
    //
    //     // SPDX-License-Identifier: Apache-2.0
    //     // Copyright 2026 MatrixArkAI
    //
    // The SPDX line was already on all 1,303 sources. The copyright line was on 11 of them,
    // which is the state this guard exists to stop coming back: a licence header that is
    // true of most of a repository is not a licence header, and nothing fails when a new
    // file arrives without one.
    //
    // READ OFF DISK rather than through `include_str!`, because the claim is about EVERY
    // source and a macro cannot glob. The walk starts at `CARGO_MANIFEST_DIR`, which is
    // absolute and fixed at compile time, so the test does not depend on a working
    // directory.
    const SPDX: &str = "// SPDX-License-Identifier: Apache-2.0";
    const COPYRIGHT: &str = "// Copyright 2026 MatrixArkAI";

    fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let entries = std::fs::read_dir(dir)
            .unwrap_or_else(|error| panic!("the source tree should be readable: {error}"));
        for entry in entries {
            let path = entry.expect("a directory entry should be readable").path();
            if path.is_dir() {
                collect(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources = Vec::new();
    collect(&root, &mut sources);
    sources.sort();

    // A WALK THAT FINDS NOTHING PASSES EVERYTHING BELOW IT, which is how a licence gate goes
    // quietly blind: the headers do not have to disappear, the walk only has to stop reaching
    // them -- a directory moved, a path that no longer resolves. 1,303 sources were here when
    // this floor was written, so it has room for ordinary churn and only fires when the walk
    // itself has stopped working.
    assert!(
        sources.len() >= 1_000,
        "only {} Rust sources were found under {}, so a clean result below would mean \
         nothing",
        sources.len(),
        root.display()
    );

    let mut missing_spdx: Vec<String> = Vec::new();
    let mut missing_copyright: Vec<String> = Vec::new();
    for path in &sources {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("{} should be readable: {error}", path.display()));
        // Trimmed of a carriage return, because the repository has CRLF files -- its own
        // LICENSE is one -- and a header is no less present for the ending it uses.
        let mut lines = text.lines().map(|line| line.trim_end_matches('\r'));
        let shown = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .display()
            .to_string();
        if lines.next() != Some(SPDX) {
            missing_spdx.push(shown.clone());
        }
        if lines.next() != Some(COPYRIGHT) {
            missing_copyright.push(shown);
        }
    }

    println!(
        "LICENCEHEADER sources={} missing_spdx={} missing_copyright={}",
        sources.len(),
        missing_spdx.len(),
        missing_copyright.len()
    );
    assert!(
        missing_spdx.is_empty(),
        "these sources do not open with {SPDX:?}: {missing_spdx:?}"
    );
    assert!(
        missing_copyright.is_empty(),
        "these sources do not carry {COPYRIGHT:?} on their second line: {missing_copyright:?}"
    );

    // NOT VACUOUS, and this is the part that keeps the two assertions above honest: the
    // strings being searched for have to be the ones the repository's own licensing says.
    // A guard asserting a header nobody declares would pass a tree that had quietly switched
    // licence.
    let at_root = |name: &str| -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("the repository should ship a {name}: {error}"))
    };
    assert!(
        at_root("LICENSE").contains("Apache License"),
        "the SPDX line says Apache-2.0, so the LICENSE must be the Apache licence"
    );
    assert!(
        at_root("NOTICE").contains("MatrixArkAI"),
        "the copyright line names MatrixArkAI, so the NOTICE must too"
    );
}

#[test]
fn the_readme_describes_the_continuous_integration_it_has() {
    // The README's continuous-integration section said three things the workflow does not do:
    // that the library suite and a "measurement suite" run in CI, that CI gates on the
    // recorded-failure list, and that it all happens "on every push". CI builds, documents
    // and runs named guards, on pushes to `main` and on pull requests; the suite-against-
    // baseline comparison is `tools/suite_gate.sh`, which runs locally.
    //
    // Correcting prose does not keep it correct. These are the four claims in that section
    // that can be checked mechanically, so they are.
    const README: &str = include_str!("../README.md");
    const WORKFLOW: &str = include_str!("../.github/workflows/ci.yml");
    const BASELINE: &str = include_str!("../tools/known_failures.txt");

    // 1. The jobs the README names are the jobs the workflow declares.
    for job in ["build & docs", "format and rollback guards",
        "network metadata consensus", "network chunk consensus"] {
        assert!(
            README.contains(job),
            "the README should name the {job:?} job"
        );
        assert!(
            WORKFLOW.contains(&format!("name: {job}")),
            "the workflow should declare a job called {job:?}"
        );
    }

    // 2. The number of recorded failures, which is a figure and so goes stale.
    let recorded = BASELINE
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
        .count();
    if recorded == 0 {
        assert!(README.contains("There are no recorded library failures."));
    } else {
        assert!(
            README.contains(&format!("the {recorded} recorded in")),
            "the README should state the number of recorded failures"
        );
    }

    // 3. "CI does not run the whole library suite." Every `cargo test` in the workflow must
    //    carry a named filter while CI claims to run only selected guards.
    //    Continuations are joined first,
    //    because these commands are written across lines with a trailing backslash.
    assert!(
        README.contains("CI does not run the whole library suite"),
        "this test is checking a claim the README no longer makes"
    );
    let mut joined: Vec<String> = Vec::new();
    let mut pending = String::new();
    for line in WORKFLOW.lines() {
        let trimmed = line.trim();
        if let Some(head) = trimmed.strip_suffix('\\') {
            pending.push_str(head);
            pending.push(' ');
        } else {
            pending.push_str(trimmed);
            joined.push(std::mem::take(&mut pending));
        }
    }
    // COMMANDS ONLY. The workflow explains in a comment why each guard checks that it ran,
    // and that comment contains the words this looks for -- it flagged itself first time.
    let is_command = |line: &&String| !line.trim_start().starts_with('#');

    // A named integration or binary target excludes the library harness, unless
    // --lib or --all-targets is also selected. A positional filter limits every
    // selected harness. Inspect Cargo arguments before the harness separator or
    // shell pipeline, skipping values belonging to flags.
    let takes_a_value = |flag: &str| {
        matches!(
            flag,
            "--features"
                | "--test"
                | "--bin"
                | "--example"
                | "--package"
                | "-p"
                | "--manifest-path"
                | "--target"
                | "--profile"
                | "--jobs"
                | "-j"
        )
    };
    let has_a_filter = |command: &str| {
        let mut words = command.split_whitespace().skip_while(|word| *word != "test");
        words.next();
        let mut words = words.peekable();
        while let Some(word) = words.next() {
            if word == "--" || word == "|" {
                return false;
            }
            if word.starts_with('-') {
                if takes_a_value(word) {
                    words.next();
                }
                continue;
            }
            return true;
        }
        false
    };
    let runs_the_whole_library_suite = |command: &str| {
        if has_a_filter(command) {
            return false;
        }
        let args: Vec<_> = command.split_whitespace()
            .skip_while(|word| *word != "test").skip(1)
            .take_while(|word| *word != "--" && *word != "|").collect();
        if args.iter().any(|word| matches!(*word, "--lib" | "--all-targets")) {
            return true;
        }
        !args.iter().any(|word| matches!(*word, "--test" | "--bin"))
    };
    for command in ["cargo test --bin star_repaird", "cargo test --test automatic_staged_expiry",
        "cargo test --lib --features metadata-consensus selected_test"] {
        assert!(!runs_the_whole_library_suite(command), "scoped command rejected: {command}");
    }
    for command in ["cargo test", "cargo test --lib", "cargo test --all-targets",
        "cargo test --lib --bin star_repaird", "cargo test --all-targets --test automatic_staged_expiry",
        "cargo test -- --bin star_repaird"] {
        assert!(runs_the_whole_library_suite(command), "unfiltered library run admitted: {command}");
    }
    let filters_a_library_run = |command: &str| {
        command.split_whitespace().any(|word| word == "--lib")
            && !runs_the_whole_library_suite(command)
    };
    let uses_an_integration_target =
        |command: &str| command.split_whitespace().any(|word| word == "--test");
    let unfiltered: Vec<&String> = joined
        .iter()
        .filter(is_command)
        .filter(|command| {
            command.contains("cargo test") && runs_the_whole_library_suite(command)
        })
        .collect();
    assert!(
        unfiltered.is_empty(),
        "every cargo test in the workflow must stop short of the whole library suite, by \
         filtering the library harness or selecting only a `--test`/`--bin` target: \
         {unfiltered:?}"
    );
    // NOT VACUOUS: if the join produced nothing, the filter above would find nothing either.
    let test_commands = joined
        .iter()
        .filter(is_command)
        .filter(|command| command.contains("cargo test"))
        .count();
    assert!(
        test_commands >= 20,
        "only {test_commands} cargo test commands parsed out of the workflow, so the check \
         above proved nothing"
    );
    // AND NEITHER CLAUSE IS VACUOUS ON ITS OWN. Both shapes have to be present, or one of
    // the two is admitting nothing and the check has quietly become the narrower one again.
    let library_filtered = joined
        .iter()
        .filter(is_command)
        .filter(|command| command.contains("cargo test") && filters_a_library_run(command))
        .count();
    let integration_named = joined
        .iter()
        .filter(is_command)
        .filter(|command| {
            command.contains("cargo test") && uses_an_integration_target(command)
        })
        .count();
    let binary_named = joined.iter().filter(is_command).filter(|command| {
        command.contains("cargo test") && command.split_whitespace().any(|word| word == "--bin")
    }).count();
    assert!(
        library_filtered >= 20,
        "only {library_filtered} steps run a filtered `--lib`, so that clause is not carrying \
         the weight this check thinks it is"
    );
    assert!(
        integration_named >= 1,
        "no step runs a `--test` target, so the clause admitting that shape is admitting \
         nothing and could be removed without failing anything"
    );
    assert!(binary_named >= 1, "no binary target exercises the binary-selection clause");

    // 4. When it runs.
    assert!(
        README.contains("On pushes to `main` and on every pull request"),
        "the README should say when CI runs"
    );
    assert!(
        WORKFLOW.contains("branches: [main]") && WORKFLOW.contains("pull_request:"),
        "the workflow should trigger on pushes to main and on pull requests"
    );

    println!(
        "READMECI jobs=4 recorded={recorded} cargo_test_commands={test_commands} \
         library_filtered={library_filtered} integration_named={integration_named} binary_named={binary_named} unfiltered=0"
    );
}

/// Every binary the README tells a reader to run is a binary this crate has.
///
/// # WHY THIS EXISTS
///
/// The README says `cargo run --release --bin replica_server`. That name is prose: renaming the
/// binary does not change it, does not fail to compile, and does not fail a test. The reader
/// finds out instead, by running a command that no longer resolves.
///
/// The same shape has already cost this project once in this work. Four CI guards named tests
/// that a rename had moved, and because `cargo test` exits 0 when a filter matches nothing,
/// those guards RAN NOTHING WHILE PASSING. A name in a file that nothing checks is the same
/// hazard whether the file is a workflow or a README.
///
/// # WHY THE BINARY LIST IS include_str! AND NOT A DIRECTORY LISTING
///
/// Each entry below pulls in that binary's source at COMPILE time. Renaming or removing one
/// stops this test building, which forces a visit. A runtime directory listing would quietly
/// report whatever happened to be on disk, and would pass after a rename while the README
/// stayed wrong -- which is the failure this is for.
#[test]
fn every_binary_the_readme_tells_a_reader_to_run_exists() {
    const README: &str = include_str!("../README.md");

    // Compile-time proof that each of these sources is where this list says it is.
    const BINARIES: &[(&str, &str)] = &[
        ("metaserver", include_str!("bin/metaserver.rs")),
        ("object_server", include_str!("bin/object_server.rs")),
        ("chunkserver", include_str!("bin/chunkserver.rs")),
        ("scale_bench", include_str!("bin/scale_bench.rs")),
    ];
    for (name, source) in BINARIES {
        assert!(
            !source.is_empty(),
            "{name} is listed as a binary and its source is empty"
        );
    }

    // Every `--bin NAME` the README spells out.
    let mut named: Vec<&str> = Vec::new();
    for (index, piece) in README.split("--bin ").enumerate() {
        if index == 0 {
            continue;
        }
        let name: &str = piece
            .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .next()
            .unwrap_or("");
        if !name.is_empty() && !named.contains(&name) {
            named.push(name);
        }
    }

    // NON-VACUITY. A parse that found nothing would pass by having nothing to complain about,
    // and that is the failure mode this whole class of guard keeps hitting.
    assert!(
        named.len() >= 2,
        "only {} binaries parsed out of the README, so this is measuring its own parse",
        named.len()
    );
    assert_eq!(
        BINARIES.len(),
        4,
        "the binary list is meant to be every binary in src/bin"
    );

    let mut missing: Vec<&str> = Vec::new();
    for name in &named {
        if !BINARIES.iter().any(|(binary, _)| binary == name) {
            missing.push(name);
        }
    }
    missing.sort_unstable();
    assert!(
        missing.is_empty(),
        "the README tells a reader to run {missing:?}, and this crate has no such binary -- \
         either a binary was renamed without the README, or the README invented one"
    );
}
