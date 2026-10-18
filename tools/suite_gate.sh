#!/usr/bin/env bash
#
# Run the library suite and compare its failures, by name, against the recorded
# baseline. Exits non-zero if any failure is new, or if any recorded failure has
# stopped happening -- both directions, because a baseline that only forbids new
# failures silently rots as tests get fixed.
#
#   tools/suite_gate.sh
#
# 21 tests have failed since the repository's first commit. They are listed in
# tools/known_failures.txt. This script is how you tell a pre-existing failure
# from one you just caused.
set -u

cd "$(dirname "$0")/.." || exit 2
BASELINE=tools/known_failures.txt
LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT

cargo test --lib > "$LOG" 2>&1

# A suite that failed to COMPILE prints no "test result:" line and no FAILED
# lines, which would otherwise read as "every recorded failure is fixed". This
# gate scored exactly that way once, so both conditions are checked.
if ! grep -q '^test result:' "$LOG"; then
  echo "the suite produced no 'test result:' line -- it did not run."
  grep -m5 -E '^error' "$LOG" | sed 's/^/    /'
  exit 2
fi
PASSED=$(sed -n 's/^test result:.* \([0-9]*\) passed.*/\1/p' "$LOG" | head -1)
if [ "${PASSED:-0}" -lt 200 ]; then
  echo "only ${PASSED:-0} tests passed; expected 200 or more. The suite did not run properly."
  exit 2
fi

grep -E '^test .* FAILED$' "$LOG" | sed 's/^test //; s/ \.\.\. FAILED$//' | sort -u > "$LOG.now"
sort -u "$BASELINE" > "$LOG.base"

INTRODUCED=$(comm -13 "$LOG.base" "$LOG.now")
VANISHED=$(comm -23 "$LOG.base" "$LOG.now")

echo "passed: $PASSED    failures now: $(wc -l < "$LOG.now")    recorded: $(wc -l < "$LOG.base")"
if [ -n "$INTRODUCED" ]; then
  echo "INTRODUCED -- these failures are new:"
  echo "$INTRODUCED" | sed 's/^/    /'
fi
if [ -n "$VANISHED" ]; then
  echo "VANISHED -- these recorded failures now pass. Remove them from $BASELINE:"
  echo "$VANISHED" | sed 's/^/    /'
fi
if [ -n "$INTRODUCED" ] || [ -n "$VANISHED" ]; then
  exit 1
fi
echo "exactly the recorded baseline, nothing introduced and nothing stale."
