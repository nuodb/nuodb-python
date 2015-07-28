__author__ = 'jgetto'


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
        if self.results_idx == len(self.results):
            if not self.complete:
                session.fetch_result_set_next(self)
            else:
                return None

        res = self.results[self.results_idx]
        self.results_idx += 1
        return res
