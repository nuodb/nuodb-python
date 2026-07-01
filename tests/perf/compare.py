# -*- coding: utf-8 -*-
"""Compare two pytest-benchmark JSON files (master vs branch).

For each shared test:

  * Reports master and branch min time (ms)
  * Reports the point delta as a percentage
  * Reports a bootstrap 95% CI on that delta, resampled from the raw
    per-round timings in stats.data.  We compare mins because the min is
    the least contaminated statistic on a shared runner.

If --noise-floor is passed, reads a JSON produced by calibrate.py and
flags tests whose CI clears +/- floor in either direction.  A test whose
CI overlaps the floor is treated as noise, no matter how nice the point
delta looks.
"""
from __future__ import print_function

import argparse
import json
import random


def _load(path):
    with open(path) as f:
        data = json.load(f)
    return {b['name']: b['stats'] for b in data['benchmarks']}


def _bootstrap_delta_ci(master_data, branch_data, n=5000, ci=0.95):
    """95% bootstrap CI on (min(branch) - min(master)) / min(master) * 100."""
    n_m = len(master_data)
    n_b = len(branch_data)
    deltas = []
    for _ in range(n):
        m = min(random.choice(master_data) for _ in range(n_m))
        b = min(random.choice(branch_data) for _ in range(n_b))
        deltas.append((b - m) / m * 100.0)
    deltas.sort()
    lo_idx = int(n * (1 - ci) / 2)
    hi_idx = int(n * (1 + ci) / 2) - 1
    return deltas[lo_idx], deltas[hi_idx]


def _row_fmt(has_floor):
    if has_floor:
        return "%-40s %12s %12s %8s %20s %8s %5s"
    return "%-40s %12s %12s %8s %20s"


def main(args):
    master = _load(args.master)
    branch = _load(args.branch)
    floor = {}
    if args.noise_floor:
        with open(args.noise_floor) as f:
            floor = json.load(f)

    random.seed(1)  # deterministic across CI runs

    fmt = _row_fmt(bool(floor))
    header = ["Test", "master (ms)", "branch (ms)", "delta %", "95% CI"]
    if floor:
        header += ["floor %", "flag"]
    print(fmt % tuple(header))
    print("-" * (len(fmt % tuple(header))))

    for name in sorted(set(master) & set(branch)):
        m_stats = master[name]
        b_stats = branch[name]
        m_ms = m_stats['min'] * 1000.0
        b_ms = b_stats['min'] * 1000.0
        delta_pct = (b_ms - m_ms) / m_ms * 100.0
        lo, hi = _bootstrap_delta_ci(m_stats['data'], b_stats['data'])
        row = [name, "%.3f" % m_ms, "%.3f" % b_ms,
               "%+.2f" % delta_pct,
               "[%+6.2f, %+6.2f]" % (lo, hi)]
        if floor:
            f = float(floor.get(name, 0.0))
            # Flag when the CI sits entirely outside +/- floor.
            flagged = (lo > f) or (hi < -f)
            row += ["%.2f" % f, "*" if flagged else ""]
        print(fmt % tuple(row))

    only_master = sorted(set(master) - set(branch))
    only_branch = sorted(set(branch) - set(master))
    if only_master:
        print("\nOnly in master: %s" % ", ".join(only_master))
    if only_branch:
        print("Only in branch: %s" % ", ".join(only_branch))


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('master', help='pytest-benchmark JSON for master')
    p.add_argument('branch', help='pytest-benchmark JSON for this branch')
    p.add_argument('--noise-floor',
                   help='JSON produced by calibrate.py; enables flag column')
    return p.parse_args()


if __name__ == '__main__':
    main(_parse_args())
