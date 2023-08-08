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


class ResultSet(object):
    """Manage a SQL result set."""

    def __init__(self, handle, col_count, initial_results, complete):
        # type: (int, int, List[Row], bool) -> None
        """Create a ResultSet object.

        :param handle: Connection handle.
        :param col_count: Column count.
        :param initial_results: Initial results for this set.
        :param complete: True if the result set is complete.
        """
        self.handle = handle
        self.col_count = col_count
        self.results = initial_results
        self.results_idx = 0
        self.complete = complete

    def clear_results(self):
        # type: () -> None
        """Clear the result set."""
        del self.results[:]
        self.results_idx = 0

    def add_row(self, row):
        # type: (Row) -> None
        """Add a new row to the result set."""
        self.results.append(row)

    def is_complete(self):
        # type: () -> bool
        """Return True if the result set is complete."""
        return self.complete or self.results_idx != len(self.results)

    def fetchone(self):
        # type: () -> Optional[Row]
        """Return the next row in the result set.

        :returns: The next row, or None if there are no more.
        """
        if self.results_idx == len(self.results):
            return None

        res = self.results[self.results_idx]
        self.results_idx += 1
        return res
