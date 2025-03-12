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
    """A SQL prepared statement."""

    def __init__(self, handle, parameter_count):
        # type: (int, int) -> None
        """Create a prepared statement.

        :param handle: Handle of the connection.
        :param parameter_count: Number of parameters needed.
        """
        super(PreparedStatement, self).__init__(handle)
        self.parameter_count = parameter_count
        self.description = None  # type: Optional[List[List[Any]]]


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
