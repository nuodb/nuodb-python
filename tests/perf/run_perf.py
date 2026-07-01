# -*- coding: utf-8 -*-
"""Run the perf suite, then re-run any test whose stddev is too high.

pytest-benchmark reports stddev per test.  If a test's coefficient of
variation (stddev / mean) is above --cv-threshold we consider that run
untrustworthy and rerun *just that test* (via pytest -k), keeping the
lowest observed min across attempts.  We stop when every test is quiet
or --max-attempts is reached; either way we always produce an output
JSON so compare.py can run.

CLI is deliberately narrow: point it at an output path and it does the
right pytest invocation for our perf suite.
"""
from __future__ import print_function

import argparse
import json
import os
import subprocess
import sys


def _cv_pct(stats):
    m = stats.get('mean', 0.0)
    return (stats['stddev'] / m) * 100.0 if m else 0.0


def _load(path):
    with open(path) as f:
        return json.load(f)


def _noisy(merged, threshold):
    return [b['name'] for b in merged['benchmarks']
            if _cv_pct(b['stats']) > threshold]


def _merge_min(base, new):
    """For each test, replace the base entry if `new` observed a lower min."""
    idx = {b['name']: i for i, b in enumerate(base['benchmarks'])}
    for b in new['benchmarks']:
        i = idx.get(b['name'])
        if i is None:
            base['benchmarks'].append(b)
            idx[b['name']] = len(base['benchmarks']) - 1
        elif b['stats']['min'] < base['benchmarks'][i]['stats']['min']:
            base['benchmarks'][i] = b
    return base


def _pytest(output, k_filter=None):
    cmd = [sys.executable, '-m', 'pytest', 'tests/perf',
           '--run-perf', '--benchmark-only',
           '--benchmark-json=' + output,
           '--benchmark-columns=min,mean,median,stddev,rounds']
    if k_filter:
        cmd += ['-k', k_filter]
    r = subprocess.run(cmd)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def main(args):
    tmp_dir = os.path.dirname(os.path.abspath(args.output)) or '.'
    base_name = os.path.basename(args.output)

    first = os.path.join(tmp_dir, base_name + '.attempt_1')
    _pytest(first)
    merged = _load(first)

    for attempt in range(2, args.max_attempts + 1):
        noisy = _noisy(merged, args.cv_threshold)
        if not noisy:
            print("All tests within CV %.1f%% after %d attempt(s)"
                  % (args.cv_threshold, attempt - 1))
            break
        print("Attempt %d/%d: rerunning %s"
              % (attempt, args.max_attempts, ", ".join(noisy)))
        out = os.path.join(tmp_dir, base_name + ('.attempt_%d' % attempt))
        _pytest(out, k_filter=" or ".join(noisy))
        merged = _merge_min(merged, _load(out))
    else:
        still_noisy = _noisy(merged, args.cv_threshold)
        if still_noisy:
            print("WARN: %d test(s) still above CV %.1f%% after %d attempts: %s"
                  % (len(still_noisy), args.cv_threshold,
                     args.max_attempts, ", ".join(still_noisy)))

    with open(args.output, 'w') as f:
        json.dump(merged, f)


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--output', required=True,
                   help='final merged pytest-benchmark JSON path')
    p.add_argument('--cv-threshold', type=float, default=5.0,
                   help='per-test CV%% ceiling before retrying (default 5)')
    p.add_argument('--max-attempts', type=int, default=3,
                   help='max attempts including the first run (default 3)')
    return p.parse_args()


if __name__ == '__main__':
    main(_parse_args())
