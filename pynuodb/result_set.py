""" NuoDB Python Driver result set

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""


class ResultSet(object):
    def __init__(self, handle, col_count, initial_results, complete):
        """
        :type handle int
        :type col_count int
        :type initial_results list
        :type complete bool
        """
        self.handle = handle
        self.col_count = col_count
        self.results = initial_results
        self.results_idx = 0
        self.complete = complete

    def clear_results(self):
        del self.results[:]
        self.results_idx = 0

    def add_row(self, row):
        self.results.append(row)

    def fetchone(self, session):
        """
        :type session EncodedSession
        """
        if self.results_idx == len(self.results) and not self.complete:
            session.fetch_result_set_next(self)

        if self.results_idx == len(self.results):
            return None

        res = self.results[self.results_idx]
        self.results_idx += 1
        return res

    def close(self, session):
        """
        :type session EncodedSession
        """
        session.close_result_set(self)
