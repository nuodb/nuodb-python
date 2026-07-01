# -*- coding: utf-8 -*-
"""Compare two pytest-benchmark JSON files (master vs branch).

Prints a table of master min, branch min, and the absolute + percentage
delta per test.  Exits non-zero if any test regressed by more than
--fail-threshold (default 10%), so CI turns a real regression into a
failed build.  Improvements never fail the build.
"""
from __future__ import print_function

import argparse
import json
import sys


def _load(path):
    with open(path) as f:
        data = json.load(f)
    return {b['name']: b['stats']['min'] for b in data['benchmarks']}


def main(args):
    master = _load(args.master)
    branch = _load(args.branch)

    print("%-40s %16s %16s %14s %10s" % (
        "Test", "master min (ms)", "branch min (ms)",
        "delta (ms)", "delta %"))
    print("-" * 100)

    regressed = []
    for name in sorted(set(master) & set(branch)):
        m = master[name] * 1000.0
        b = branch[name] * 1000.0
        d = b - m
        p = (d / m) * 100.0 if m else float('nan')
        print("%-40s %16.3f %16.3f %+14.3f %+9.2f%%" % (name, m, b, d, p))
        if p > args.fail_threshold:
            regressed.append((name, p))

    only_master = sorted(set(master) - set(branch))
    only_branch = sorted(set(branch) - set(master))
    if only_master:
        print("\nOnly in master: %s" % ", ".join(only_master))
    if only_branch:
        print("Only in branch: %s" % ", ".join(only_branch))

    print()
    if regressed:
        print("FAIL: %d test(s) regressed by more than %.2f%%:"
              % (len(regressed), args.fail_threshold))
        for name, p in regressed:
            print("  %s: %+.2f%%" % (name, p))
        sys.exit(1)
    print("OK: no test regressed by more than %.2f%%" % args.fail_threshold)


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('master', help='pytest-benchmark JSON for master')
    p.add_argument('branch', help='pytest-benchmark JSON for this branch')
    p.add_argument('--fail-threshold', type=float, default=10.0,
                   help='percent slowdown that fails the build (default 10)')
    return p.parse_args()


if __name__ == '__main__':
    main(_parse_args())
