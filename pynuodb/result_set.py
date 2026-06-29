"""NuoDB Python Driver result set.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

try:
    from typing import Any, List, Optional, Tuple  # pylint: disable=unused-import
    Value = Any
    Row = Tuple[Value, ...]
except ImportError:
    pass

# Use the Cython-accelerated ResultSet when the extension has been built.
# It exposes the same public interface; fetchone() and is_complete() become
# near-C-speed cpdef calls.
try:
    from ._fetch import ResultSet  # pylint: disable=unused-import
except ImportError:

    class ResultSet(object):  # type: ignore[no-redef]
        """Manage a SQL result set."""

        def __init__(self, handle, col_count, initial_results, complete):
            # type: (int, int, List[Row], bool) -> None
            self.handle = handle
            self.col_count = col_count
            self.results = initial_results
            self.results_idx = 0
            self.complete = complete

        def clear_results(self):
            # type: () -> None
            del self.results[:]
            self.results_idx = 0

        def add_row(self, row):
            # type: (Row) -> None
            self.results.append(row)

        def is_complete(self):
            # type: () -> bool
            return self.complete or self.results_idx != len(self.results)

        def fetchone(self):
            # type: () -> Optional[Row]
            if self.results_idx == len(self.results):
                return None
            res = self.results[self.results_idx]
            self.results_idx += 1
            return res
