cdef class ResultSet:
    """Drop-in replacement for result_set.ResultSet with C-typed attributes.

    fetchone() and is_complete() become direct C calls when invoked from
    other Cython code (cpdef dispatch). Python callers see the same
    interface.
    """

    cdef public int handle
    cdef public int col_count
    cdef public list results
    cdef public int results_idx
    cdef public bint complete

    def __init__(self, int handle, int col_count, list initial_results,
                 bint complete):
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

    cpdef bint is_complete(self):
        # Looks backwards: True if the server signalled end-of-results, OR
        # there are still buffered rows unread. Matches result_set.ResultSet.
        return self.complete or self.results_idx != len(self.results)

    cpdef object fetchone(self):
        cdef int idx = self.results_idx
        if idx == len(self.results):
            return None
        self.results_idx = idx + 1
        return self.results[idx]
