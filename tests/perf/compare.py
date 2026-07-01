# -*- coding: utf-8 -*-
"""Compare two pytest-benchmark JSON files (master vs branch).

Usage: python compare.py MASTER.json BRANCH.json

Compares the `min` time of each shared test and prints a table with the
absolute and relative delta.  We use min because it's the least noisy
statistic: it filters out GC pauses, kernel scheduling and JIT warmup.
"""
from __future__ import print_function

import json
import sys


def _load(path):
    with open(path) as f:
        data = json.load(f)
    return {b['name']: b['stats']['min'] for b in data['benchmarks']}


def main(master_path, branch_path):
    master = _load(master_path)
    branch = _load(branch_path)

    shared = sorted(set(master) & set(branch))
    rows = []
    for name in shared:
        m_ms = master[name] * 1000.0
        b_ms = branch[name] * 1000.0
        delta = b_ms - m_ms
        pct = (delta / m_ms) * 100.0 if m_ms else float('nan')
        rows.append((name, m_ms, b_ms, delta, pct))

    header = ("Test", "master min (ms)", "branch min (ms)",
              "delta (ms)", "delta %")
    print("%-40s %16s %16s %14s %10s" % header)
    print("-" * 100)
    for name, m, b, d, p in rows:
        print("%-40s %16.3f %16.3f %+14.3f %+9.2f%%" % (name, m, b, d, p))

    only_master = sorted(set(master) - set(branch))
    only_branch = sorted(set(branch) - set(master))
    if only_master:
        print("\nOnly in master: %s" % ", ".join(only_master))
    if only_branch:
        print("Only in branch: %s" % ", ".join(only_branch))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("usage: python compare.py MASTER.json BRANCH.json",
              file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
