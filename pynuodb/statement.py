"""NuoDB Python driver SQL statement.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

try:
    from typing import Any, List, Optional  # pylint: disable=unused-import
except ImportError:
    pass


class Statement(object):
    """A SQL statement."""

    def __init__(self, handle):
        # type: (int) -> None
        """Create a statement.

        :param handle: Handle of the connection.
        """
        self.handle = handle


class PreparedStatement(Statement):
    """A SQL prepared statement.

    A statement created with UNPREPARED_HANDLE has not been sent to the
    server yet: like the JDBC driver, it is prepared by its first execution
    (PREPAREANDEXECUTETOGETHER), so its parameter count is unknown until then.
    """

    UNPREPARED_HANDLE = -1

    def __init__(self, handle, parameter_count, query=None):
        # type: (int, Optional[int], Optional[str]) -> None
        """Create a prepared statement.

        :param handle: Handle of the connection, or UNPREPARED_HANDLE.
        :param parameter_count: Number of parameters needed (None if unprepared).
        :param query: The SQL text, needed to prepare an unprepared statement.
        """
        super(PreparedStatement, self).__init__(handle)
        self.parameter_count = parameter_count
        self.description = None  # type: Optional[List[List[Any]]]
        self.query = query

    @property
    def is_prepared(self):
        # type: () -> bool
        """True once the server has assigned this statement a handle."""
        return self.handle != PreparedStatement.UNPREPARED_HANDLE


class ExecutionResult(object):
    """Result of a statement execution."""

    def __init__(self, statement, result, row_count):
        # type: (Statement, int, int) -> None
        """Create the result of a statement execution.

        :param statement: Statement that was executed.
        :param result: Result of execution.
        :param row_count: Number of rows in the result.
        """
        self.result = result
        self.row_count = row_count
        self.statement = statement
