#!/usr/bin/env python3
"""Turns the E2E_* result lines of the log-object end-to-end measurement into markdown tables.

usage: table.py RESULTS_FILE [RESULTS_FILE ...]

A results file holds the lines printed by the two ignored tests. Each run starts with an
E2E_CONFIG line, so one file may hold the log run and several page-path runs.
"""
import re
import sys

GIB = float(1 << 30)
MARKS = [0.25, 0.5, 1.0, 2.0, 4.0]
PAIR = re.compile(r'(\w+)=(\[[^\]]*\]|"(?:[^"\\]|\\.)*"|\S+)')


def parse(line):
    head = line.split(" ", 1)[0]
    fields = {k: v for k, v in PAIR.findall(line)}
    return head, fields


def num(fields, key, default=None):
    try:
        return float(fields[key])
    except (KeyError, ValueError):
        return default


def runs(paths):
    current = None
    out = []
    for path in paths:
        with open(path) as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw.startswith("E2E_"):
                    continue
                head, fields = parse(raw)
                if head == "E2E_CONFIG":
                    current = {"config": fields, "append": [], "meta": [], "other": []}
                    out.append(current)
                elif current is None:
                    continue
                elif head == "E2E_APPEND":
                    current["append"].append(fields)
                elif head == "E2E_META":
                    current["meta"].append(fields)
                else:
                    current["other"].append((head, raw))
    return out


def at_marks(rows):
    """The first window whose end reached each mark, in GiB."""
    chosen = []
    for mark in MARKS:
        for row in rows:
            if num(row, "object_bytes", 0) >= mark * GIB:
                if row not in [c[1] for c in chosen]:
                    chosen.append((mark, row))
                break
    return chosen


def per_gib(meta, key):
    """Growth of `key` per GiB between the first and last window, and the last value per GiB."""
    rows = [r for r in meta if num(r, key) is not None and num(r, key) >= 0]
    if len(rows) < 2:
        return None, None
    first, last = rows[0], rows[-1]
    span = (num(last, "object_bytes") - num(first, "object_bytes")) / GIB
    if span <= 0:
        return None, None
    return (num(last, key) - num(first, key)) / span, num(last, key) / (num(last, "object_bytes") / GIB)


def fmt(value, digits=1):
    return "n/a" if value is None else f"{value:,.{digits}f}"


def latency_table(run):
    title = run["config"]
    lines = [
        f"### Append latency, kind={title.get('kind')}, append_bytes={title.get('append_bytes')}",
        "",
        "| window end (GiB) | appends | p50 us | p99 us | max us | load1 max | swap kB max |",
        "|---|---|---|---|---|---|---|",
    ]
    for mark, row in at_marks(run["append"]):
        lines.append(
            f"| {num(row, 'object_bytes') / GIB:.2f} (mark {mark}) | {row.get('appends')} | {row.get('p50_us')} | "
            f"{row.get('p99_us')} | {row.get('max_us')} | {row.get('load1_max')} | {row.get('swap_used_kb_max')} |"
        )
    lines.append("")
    lines.append("All windows:")
    lines.append("")
    lines.append("| window end (GiB) | appends | p50 us | p99 us | max us |")
    lines.append("|---|---|---|---|---|")
    for row in run["append"]:
        lines.append(
            f"| {num(row, 'object_bytes') / GIB:.3f} | {row.get('appends')} | {row.get('p50_us')} | "
            f"{row.get('p99_us')} | {row.get('max_us')} |"
        )
    return lines


def entries_table(run):
    lines = [
        f"### Metadata entries per append, kind={run['config'].get('kind')}",
        "",
        "| window end (GiB) | appends | commit entries | per append | grants | switches | renewals | unexplained |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in run["meta"]:
        appends = num(row, "appends", 0)
        entries = num(row, "commit_entries", -1)
        per = None if entries < 0 or appends == 0 else entries / appends
        lines.append(
            f"| {num(row, 'object_bytes') / GIB:.3f} | {int(appends)} | {int(entries)} | {fmt(per, 4)} | "
            f"{row.get('grants')} | {row.get('switches')} | {row.get('renewals')} | {row.get('unexplained_entries')} |"
        )
    lines.append("")
    lines.append(
        "Expected: one entry per chunk switch, one per renewal batch, one per grant; "
        "`unexplained` is the commit-index delta minus those and other writers' entries "
        "(a nonzero value on a cluster used only by this test is a finding)."
    )
    return lines


def bytes_table(runs_by_name):
    lines = [
        "### Metaserver bytes per GiB: log object versus the same appender on a page-path object",
        "",
        "| run | append bytes | end GiB | meta WAL B / GiB (growth) | meta WAL B / GiB (total) | manifest B / GiB (total) | extents | ended by |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for name, run in runs_by_name:
        meta = run["meta"]
        if not meta:
            continue
        last = meta[-1]
        growth, total = per_gib(meta, "meta_wal_bytes")
        manifest_total = num(last, "manifest_bytes", -1) / (num(last, "object_bytes") / GIB) if num(last, "object_bytes") else None
        ended = "reached target"
        for head, raw in run["other"]:
            if head == "E2E_BASELINE_END":
                _, f = parse(raw)
                if f.get("failure") not in (None, '""'):
                    ended = f.get("failure")
        lines.append(
            f"| {name} | {run['config'].get('append_bytes')} | {num(last, 'object_bytes') / GIB:.2f} | {fmt(growth, 0)} | "
            f"{fmt(total, 0)} | {fmt(manifest_total, 0)} | {last.get('extents')} | {ended} |"
        )
    lines.append("")
    lines.append(
        "The WAL columns sum the three voters' write-ahead logs, so they include replication; "
        "compare rows with each other, not with a single-copy size. `growth` is measured between the "
        "first and last window, `total` includes the cluster's start-up entries."
    )
    return lines


def others(run):
    lines = ["### Events", ""]
    for head, raw in run["other"]:
        lines.append(f"- `{raw}`")
    return lines


def main(paths):
    all_runs = runs(paths)
    if not all_runs:
        print("no E2E_CONFIG line found")
        return 1
    named = []
    for index, run in enumerate(all_runs):
        kind = run["config"].get("kind")
        named.append((f"{kind}#{index}", run))
        print(f"## Run {index}: kind={kind}")
        print()
        print("\n".join(latency_table(run)))
        print()
        print("\n".join(entries_table(run)))
        print()
        print("\n".join(others(run)))
        print()
    print("## Comparison")
    print()
    print("\n".join(bytes_table(named)))
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1:]))
