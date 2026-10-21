"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import pytest

from pynuodb.exception import DataError, ProgrammingError, BatchError, OperationalError

from . import nuodb_base


class TestNuoDBCursor(nuodb_base.NuoBase):

    def test_cursor_description(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("SELECT 'abc' AS XYZ, 123 AS `123` FROM DUAL")
        descriptions = cursor.description
        dstr = "Descriptions: %s" % (str(descriptions))
        assert len(descriptions) == 2, dstr

        assert descriptions[0][0] == 'XYZ', dstr
        assert descriptions[0][1] == self.driver.STRING, dstr
        # We don't get back a length for this type (it's 0)
        # assert descriptions[0][2] == 3, dstr

        assert descriptions[1][0] == '123', dstr
        assert descriptions[1][1] == self.driver.NUMBER, dstr
        # I think this should be 6 but there is disagreement?
        # assert descriptions[1][2] == 5, dstr

    def test_cursor_rowcount_and_last_query(self):
        con = self._connect()
        cursor = con.cursor()
        statement = "SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL"
        cursor.execute(statement)
        assert cursor.rowcount == -1
        assert cursor.query == statement

    # A statement's first execution prepares it on the server
    # (PREPAREANDEXECUTETOGETHER), so the first wrong parameter count is
    # detected by the server and later ones client-side; PEP 249 requires
    # ProgrammingError either way.

    def test_insufficient_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ?, ? FROM DUAL", [1])
        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ?, ? FROM DUAL", [1])

        cursor.execute("SELECT ?, ? FROM DUAL", [1, 2])
        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ?, ? FROM DUAL", [1])

    def test_toomany_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT 1 FROM DUAL", [1])

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ? FROM DUAL", [1, 2])

        cursor.execute("SELECT ? FROM DUAL", [1])
        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ? FROM DUAL", [1, 2])

    def test_wrong_parameter_count_does_not_execute(self):
        """The server rejects a bad first execution before running it."""
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS paet_param_table")
        cursor.execute("CREATE TABLE paet_param_table (f1 INTEGER)")

        with pytest.raises(ProgrammingError):
            cursor.execute("INSERT INTO paet_param_table VALUES (?)", [1, 2])
        with pytest.raises(ProgrammingError):
            cursor.execute("INSERT INTO paet_param_table VALUES (?)", [])

        cursor.execute("SELECT COUNT(*) FROM paet_param_table")
        assert cursor.fetchone()[0] == 0
        cursor.execute("DROP TABLE paet_param_table")

    @staticmethod
    def _count_round_trips(cursor):
        """Wrap the session so blocking request/reply exchanges are counted."""
        session = cursor.session
        orig = session._exchangeMessages
        counter = [0]

        def counting(getResponse=True):
            if getResponse:
                counter[0] += 1
            return orig(getResponse)

        session._exchangeMessages = counting
        return counter

    def test_prepared_statement_round_trips(self):
        """First execution prepares and executes in one round trip; later
        ones reuse the handle. Neither fetches metadata unless asked."""
        con = self._connect()
        cursor = con.cursor()
        trips = self._count_round_trips(cursor)

        trips[0] = 0
        cursor.execute("SELECT CAST(? AS INTEGER) FROM DUAL", [7])  # PAET + GETRESULTSET
        assert cursor.fetchone()[0] == 7
        assert trips[0] == 2

        trips[0] = 0
        cursor.execute("SELECT CAST(? AS INTEGER) FROM DUAL", [8])  # EXECUTEPREPARED + GETRESULTSET
        assert cursor.fetchone()[0] == 8
        assert trips[0] == 2

        trips[0] = 0
        cursor.execute("SELECT ? + 1 FROM DUAL WHERE 1 = 0", [1])
        assert cursor.fetchall() == []
        assert trips[0] == 2

    def test_description_is_fetched_lazily(self):
        con = self._connect()
        cursor = con.cursor()
        trips = self._count_round_trips(cursor)

        trips[0] = 0
        cursor.execute("SELECT 1 AS one, 'a' AS two FROM DUAL")
        assert trips[0] == 2                        # EXECUTE + GETRESULTSET
        assert cursor.fetchall() == [(1, 'a')]

        # Read after the rows are drained: the result set is still open.
        assert [d[0] for d in cursor.description] == ['ONE', 'TWO']
        assert trips[0] == 3                        # + GETMETADATA, once
        assert cursor.description[1][0] == 'TWO'
        assert trips[0] == 3

        cursor.execute("CREATE TABLE IF NOT EXISTS paet_desc_table (f1 INTEGER)")
        assert cursor.description is None
        cursor.execute("DROP TABLE paet_desc_table")

    def test_failed_first_execution_keeps_its_error(self):
        """A first execution that fails for some other reason still raises
        the server's error, and the statement remains usable."""
        con = self._connect()
        cursor = con.cursor()
        with pytest.raises(DataError):
            cursor.execute("SELECT CAST(? AS INTEGER) FROM DUAL", ['abc'])
        cursor.execute("SELECT CAST(? AS INTEGER) FROM DUAL", ['5'])
        assert cursor.fetchone()[0] == 5
        cursor.close()
        con.close()

    def test_failed_first_execution_retries_prepare(self):
        """A first execution that can't even be prepared leaves the statement
        unprepared, and closing it never sends handle -1."""
        con = self._connect()
        cursor = con.cursor()
        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ? FROM no_such_table_paet", [1])
        cursor.execute("SELECT ? FROM DUAL", ['ok'])
        assert cursor.fetchone()[0] == 'ok'
        cursor.close()
        con.close()

    def test_pre_paet_protocol_prepares_eagerly(self):
        """Against a server older than PREPARE_AND_EXECUTE_TOGETHER the
        driver must keep using PREPARE + EXECUTEPREPAREDSTATEMENT, and keep
        its client-side parameter-count check on the first execution."""
        from pynuodb import protocol

        con = self._connect()
        cursor = con.cursor()
        cursor.session._EncodedSession__sessionVersion = protocol.TIMESTAMP_WITHOUT_TZ
        trips = self._count_round_trips(cursor)

        trips[0] = 0
        cursor.execute("SELECT CAST(? AS INTEGER) FROM DUAL", [5])
        assert cursor.fetchone()[0] == 5
        assert trips[0] == 3                        # PREPARE + EXECUTEPREPARED + GETRESULTSET

        with pytest.raises(ProgrammingError):
            cursor.execute("SELECT ?, ? FROM DUAL", [1])

    def test_executemany_on_unprepared_statement(self):
        """executemany() prepares an unprepared cached statement first, as
        JDBC's addBatch() does via ensurePrepared()."""
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS paet_batch_table")
        cursor.execute("CREATE TABLE paet_batch_table (f1 INTEGER)")
        cursor.executemany("INSERT INTO paet_batch_table VALUES (?)", [[1], [2]])
        cursor.execute("INSERT INTO paet_batch_table VALUES (?)", [3])
        cursor.executemany("INSERT INTO paet_batch_table VALUES (?)", [[4]])
        cursor.execute("SELECT f1 FROM paet_batch_table ORDER BY f1")
        assert [r[0] for r in cursor.fetchall()] == [1, 2, 3, 4]
        cursor.execute("DROP TABLE paet_batch_table")

    @pytest.mark.parametrize('row_type', [list, tuple])
    @pytest.mark.parametrize('accelerated', [False, True])
    def test_executemany_parameter_sequences(self, monkeypatch, row_type, accelerated):
        """List and tuple batches work with either encoder."""
        from pynuodb import encodedsession

        if accelerated and not encodedsession._HAVE_FETCH_ACCEL:
            pytest.skip("Cython extension not loaded")
        monkeypatch.setattr(encodedsession, '_HAVE_FETCH_ACCEL', accelerated)

        con = self._connect()
        try:
            cursor = con.cursor()
            cursor.execute("DROP TABLE IF EXISTS sequence_batch_table")
            cursor.execute("CREATE TABLE sequence_batch_table (f1 INTEGER)")
            rows = row_type([(1,), (2,)])
            assert cursor.executemany(
                "INSERT INTO sequence_batch_table VALUES (?)", rows) == [1, 1]
            cursor.execute("SELECT f1 FROM sequence_batch_table ORDER BY f1")
            assert cursor.fetchall() == [(1,), (2,)]
            cursor.execute("DROP TABLE sequence_batch_table")
        finally:
            con.close()

    def test_incorrect_parameters(self):
        con = self._connect()
        cursor = con.cursor()

        with pytest.raises(DataError):
            cursor.execute("SELECT ? + 1 FROM DUAL", ['abc'])

    def test_executemany(self):
        con = self._connect()
        cursor = con.cursor()

        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [[1, 2], [3, 4]])

        cursor.execute("SELECT * FROM executemany_table")

        ret = cursor.fetchall()

        assert ret[0][0] == 1
        assert ret[0][1] == 2
        assert ret[1][0] == 3
        assert ret[1][1] == 4

        cursor.execute("DROP TABLE executemany_table")

    def test_executemany_bad_parameters(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        # 3rd tuple has too many params
        with pytest.raises(ProgrammingError):
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)", [[1, 2], [3, 4], [1, 2, 3]])

        cursor.execute("DROP TABLE executemany_table")

    def test_executemany_somefail(self):
        con = self._connect()
        cursor = con.cursor()
        cursor.execute("DROP TABLE IF EXISTS executemany_table")
        cursor.execute("CREATE TABLE executemany_table (f1 INTEGER, f2 INTEGER)")
        cursor.execute('CREATE UNIQUE INDEX "f1idx" ON "executemany_table" ("f1");')
        # 3rd tuple has uniqueness conflict
        with pytest.raises(BatchError) as ex:
            cursor.executemany("INSERT INTO executemany_table VALUES (?, ?)",
                               [[1, 2], [3, 4], [1, 2], [5, 6], [5, 6]])

        assert ex.value.results[0] == 1
        assert ex.value.results[1] == 1
        assert ex.value.results[2] == -3
        assert ex.value.results[3] == 1
        assert ex.value.results[4] == -3

        # test that they all made it save the bogus one
        cursor.execute("select * from executemany_table;")
        assert len(cursor.fetchall()) == 3

        cursor.execute("DROP TABLE executemany_table")

    def test_result_set_gets_closed(self):
        # Server will throw error after 1000 open result sets
        con = self._connect()
        for j in [False, True]:
            for i in range(2015):
                if not j:
                    cursor = con.cursor()
                    cursor.execute('select 1 from dual;')
                    con.commit()
                    cursor.close()
                else:
                    if i >= 1000:
                        with pytest.raises(OperationalError):
                            cursor = con.cursor()
                            cursor.execute('select 1 from dual;')
                            con.commit()
                    else:
                        cursor = con.cursor()
                        cursor.execute('select 1 from dual;')
                        con.commit()
