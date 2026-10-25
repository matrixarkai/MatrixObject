#!/usr/bin/env bash
#
# Measure what the layer above a replica asks it to do, with the compiler.
#
#   tools/replica_surface.sh
#
# WHY THIS IS A SCRIPT AND NOT A NUMBER IN A COMMENT
#
# `src/cluster/mod.rs` carried a measured figure -- 298 compile errors, 82 methods, 198 call
# sites, 57 files -- and every one of those numbers went stale, because nothing re-ran the
# experiment that produced them. A figure recorded by hand is a figure that is true once.
# This is the experiment, so the figure can be re-derived instead of remembered.
#
# WHAT IT DOES
#
# Replaces the ELEMENT type of `MatrixChunkService.replicas` with a struct that has no methods
# and no fields, compiles, and reads what the compiler says is missing. Then puts the tree
# back. It refuses to start on a dirty tree, because it cannot put back what it did not change.
#
# THREE THINGS IT GETS RIGHT THAT A FIRST ATTEMPT DID NOT
#
#   * The `Vec` is left alone. Replacing the whole collection surfaces the VEC's methods --
#     iter, get, len, get_mut, clone, iter_mut -- and never reaches the element's. A
#     collection's own API is not what has to cross a socket.
#   * The stand-in is APPENDED, not inserted above the struct. Inserted above it, the struct's
#     own `#[derive(Clone, Debug, Eq, PartialEq)]` attaches to the stand-in instead, and
#     `MatrixChunkService` silently loses Clone -- which produced 24 cascade `clone` failures
#     that looked exactly like a real part of the surface.
#   * It counts E0631 as well as E0599. A method passed as a function path --
#     `.map(MatrixReplicaService::mutation_qos_config)` -- is a third spelling, and 25 sites
#     use it. A count of "no method named" misses every one.
#
# It also counts E0609, direct field reads, which should be ZERO: #226 closed them.
set -u

cd "$(dirname "$0")/.." || exit 2
FIELD=src/services/page/service_state.inc.rs
LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT

if [ -n "$(git status --porcelain)" ]; then
  echo "the tree is dirty; this script edits a source file and puts it back, so it refuses"
  echo "to run where it cannot tell its own change from yours."
  exit 2
fi

python3 - "$FIELD" <<'PY' || exit 2
import sys
p = sys.argv[1]
s = open(p).read()
old = '    pub(crate) replicas: Vec<MatrixReplicaService>,'
if s.count(old) != 1:
    sys.exit('the replica collection is not where this script expects it (%d matches)'
             % s.count(old))
s = s.replace(old, '    pub(crate) replicas: Vec<MethodlessReplica>,')
s += '''
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MethodlessReplica;
'''
open(p, 'w').write(s)
PY

if [ -z "$(git status --porcelain)" ]; then
  echo "the edit did not land, so there is nothing to measure"
  exit 2
fi

cargo check --all-targets --locked --all-features > "$LOG" 2>&1
git checkout -- "$FIELD"

if [ -n "$(git status --porcelain)" ]; then
  echo "WARNING: the tree is still dirty after putting the file back"
  git status --porcelain
fi

python3 - "$LOG" <<'PY'
import collections, re, sys
text = open(sys.argv[1], errors='replace').read()

# A cascade check first. If the stand-in disturbed MatrixChunkService itself, every figure
# below is a measurement of that accident instead of the surface.
if 'error[E0119]' in text or 'MatrixChunkService: Clone' in text:
    sys.exit('REFUSED TO SCORE: the stand-in disturbed MatrixChunkService itself')

calls = re.findall(
    r"error\[E0599\]: no method named `([a-zA-Z_0-9]+)` found for [^\n]*\n\s*-->\s*([^\s:]+):(\d+)",
    text)
paths = re.findall(
    r"error\[E0631\]: type mismatch in function arguments\s*\n\s*-->\s*([^\s:]+):(\d+):\d+"
    r"(?:.|\n)*?MatrixReplicaService::([a-zA-Z_0-9]+)",
    text)
fields = re.findall(
    r"error\[E0609\]: no field `([a-zA-Z_0-9]+)` on type [^\n]*\n\s*-->\s*([^\s:]+):(\d+)",
    text)

sites = [(n, f) for n, f, _ in calls] + [(n, f) for f, _, n in paths]
if not sites:
    sys.exit('REFUSED TO SCORE: no method sites found at all, so the stand-in did nothing')

per = collections.Counter(n for n, _ in sites)
prod = collections.Counter(n for n, f in sites if f != 'src/tests.rs')
prod_sites = [s for s in sites if s[1] != 'src/tests.rs']

print('REPLICA SURFACE, as the compiler sees it')
print('  method calls (E0599)        : %d' % len(calls))
print('  methods as a path (E0631)   : %d' % len(paths))
print('  direct field reads (E0609)  : %d   <- should be 0 since #226' % len(fields))
print('  DISTINCT METHODS            : %d' % len(per))
print('  TOTAL SITES                 : %d' % len(sites))
print('    production                : %d in %d files'
      % (len(prod_sites), len(set(f for _, f in prod_sites))))
print('    library suite             : %d' % (len(sites) - len(prod_sites)))
print('  methods with ONE production site : %d' % len([m for m in per if prod[m] == 1]))
print('  methods with NO production site  : %d' % len([m for m in per if prod[m] == 0]))
print()
print('  the heaviest, by production sites:')
for name, _ in sorted(per.items(), key=lambda kv: (-prod[kv[0]], kv[0]))[:10]:
    print('    %-48s prod %3d  test %3d' % (name, prod[name], per[name] - prod[name]))
if fields:
    print()
    print('  DIRECT FIELD READS ARE BACK, and a field read cannot cross a process boundary:')
    for name, count in collections.Counter(n for n, _, _ in fields).most_common():
        print('    %-48s %3d' % (name, count))
PY
