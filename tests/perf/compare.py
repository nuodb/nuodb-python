# -*- coding: utf-8 -*-
"""Compare two pytest-benchmark JSON files (master vs branch).

Prints a table of master min, branch min, and the absolute + percentage
delta per test.  Min is the least noisy summary; on a quiet enough
runner the delta % is straight-up meaningful.
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

    print("%-40s %16s %16s %14s %10s" % (
        "Test", "master min (ms)", "branch min (ms)",
        "delta (ms)", "delta %"))
    print("-" * 100)
    for name in sorted(set(master) & set(branch)):
        m = master[name] * 1000.0
        b = branch[name] * 1000.0
        d = b - m
        p = (d / m) * 100.0 if m else float('nan')
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
