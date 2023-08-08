"""Classes containing the exceptions for reporting errors.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

try:
    from typing import Iterable, NoReturn  # pylint: disable=unused-import
except ImportError:
    pass

from . import protocol

__all__ = ['Warning', 'Error', 'InterfaceError', 'DatabaseError', 'BatchError',
           'DataError', 'OperationalError', 'IntegrityError', 'InternalError',
           'ProgrammingError', 'NotSupportedError', 'EndOfStream',
           'db_error_handler']


# These exceptions are defined by PEP 249.
# See the PEP for a fuller description of each one.

class Warning(Warning):  # type: ignore # pylint: disable=redefined-builtin
    """Raised for important warnings."""

    pass


class Error(Exception):
    """The base class of all other error exceptions."""

    pass


class InterfaceError(Error):
    """Raised for errors that are related to the database interface."""

    pass


class DatabaseError(Error):
    """Raised for errors that are related to the database."""

    pass


class DataError(DatabaseError):
    """Raised for errors that are due to problems with the processed data."""

    pass


class OperationalError(DatabaseError):
    """Raised for errors that are related to the database's operation."""

    pass


class IntegrityError(DatabaseError):
    """Raised when the relational integrity of the database is affected."""

    pass


class InternalError(DatabaseError):
    """Raised when the database encounters an internal error."""

    pass


class ProgrammingError(DatabaseError):
    """Raised for programming errors."""

    pass


class NotSupportedError(DatabaseError):
    """Raised for using a method or database API which is not supported."""

    pass


# These exceptions are specific to the pynuodb implementation.

class BatchError(DatabaseError):
    """Raised for errors encountered during batch operations.

    In addition tot the mess, a result for each operation in the batch is
    available in the results attribute.
    """

    def __init__(self, value, results):
        # type: (str, Iterable[int]) -> None
        super(BatchError, self).__init__(value)
        self.results = results


class EndOfStream(Exception):
    """End-of-stream means a network or protocol error."""

    pass


def db_error_handler(error_code, error_string):
    # type: (int, str) -> NoReturn
    """Raise the appropriate exception based on the error.

    :param error_code: The error code.
    :param error_string: Extra error information.
    :raises Error: The correct Error exception subclass.
    """
    info = '%s: %s' % (protocol.lookup_code(error_code), error_string)

    if error_code in protocol.DATA_ERRORS:
        raise DataError(info)
    if error_code in protocol.OPERATIONAL_ERRORS:
        raise OperationalError(info)
    if error_code in protocol.INTEGRITY_ERRORS:
        raise IntegrityError(info)
    if error_code in protocol.INTERNAL_ERRORS:
        raise InternalError(info)
    if error_code in protocol.PROGRAMMING_ERRORS:
        raise ProgrammingError(info)
    if error_code in protocol.NOT_SUPPORTED_ERRORS:
        raise NotSupportedError(info)

    raise DatabaseError(info)
