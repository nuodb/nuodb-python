# -*- coding: utf-8 -*-
"""Compute a per-test noise floor from same-code benchmark runs.

Takes 2+ pytest-benchmark JSON files produced by running the *same* code
on the same runner (e.g., master benchmarked twice back-to-back).  For
each test, pools the raw per-round timings from all runs and runs a
permutation-style bootstrap: draw two disjoint groups of the size of a
typical single run, compute |delta%| between their mins, repeat many
times.  The 95th percentile of that distribution is the noise floor:
the smallest |delta%| that noise alone can plausibly produce on this
runner.  compare.py reads this file and flags PR deltas that clear it.

Writes {test_name: floor_pct} to the output path.
"""
from __future__ import print_function

import argparse
import json
import random


def _load_pooled(paths):
    """Return (pooled_data, typical_size_per_test) for every shared test."""
    pooled = {}
    sizes = {}
    for path in paths:
        with open(path) as f:
            data = json.load(f)
        for b in data['benchmarks']:
            name = b['name']
            d = b['stats']['data']
            pooled.setdefault(name, []).extend(d)
            sizes.setdefault(name, []).append(len(d))
    typical = {n: sum(s) // len(s) for n, s in sizes.items()}
    return pooled, typical


def _floor(pooled, group_size, n_permutations=5000, quantile=0.95):
    if len(pooled) < 2 * group_size:
        # Not enough data to draw two disjoint groups: fall back to
        # bootstrap-with-replacement.  Rare, but keeps the script robust.
        deltas = _bootstrap_with_replacement(
            pooled, group_size, n_permutations)
    else:
        deltas = _permutation(pooled, group_size, n_permutations)
    deltas.sort()
    return deltas[int(len(deltas) * quantile)]


def _permutation(pooled, group_size, n):
    deltas = []
    data = list(pooled)
    two_groups = 2 * group_size
    for _ in range(n):
        random.shuffle(data)
        a = data[:group_size]
        b = data[group_size:two_groups]
        m_a = min(a)
        m_b = min(b)
        deltas.append(abs((m_b - m_a) / m_a * 100.0))
    return deltas


def _bootstrap_with_replacement(pooled, group_size, n):
    deltas = []
    for _ in range(n):
        a = [random.choice(pooled) for _ in range(group_size)]
        b = [random.choice(pooled) for _ in range(group_size)]
        m_a = min(a)
        m_b = min(b)
        deltas.append(abs((m_b - m_a) / m_a * 100.0))
    return deltas


def main(args):
    if len(args.inputs) < 2:
        raise SystemExit("need >= 2 same-code JSON files to calibrate")

    pooled, typical = _load_pooled(args.inputs)
    random.seed(1)

    floors = {n: _floor(pooled[n], typical[n]) for n in sorted(pooled)}

    with open(args.output, 'w') as f:
        json.dump(floors, f, indent=2, sort_keys=True)

    print("Noise floor per test (95th %ile of |delta%%| under H0):")
    for name in sorted(floors):
        print("  %-40s  %6.2f%%" % (name, floors[name]))
    print("Wrote %s" % args.output)


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('output', help='where to write noise_floor.json')
    p.add_argument('inputs', nargs='+',
                   help='2+ same-code pytest-benchmark JSON files')
    return p.parse_args()


if __name__ == '__main__':
    main(_parse_args())
