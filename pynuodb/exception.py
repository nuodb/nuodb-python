"""Classes containing the exceptions for reporting errors.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

from . import protocol

__all__ = ['Warning', 'Error', 'InterfaceError', 'DatabaseError', 'BatchError',
           'DataError', 'OperationalError', 'IntegrityError', 'InternalError',
           'ProgrammingError', 'NotSupportedError', 'EndOfStream',
           'db_error_handler']


class Warning(Exception):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return repr(self.__value)


class Error(Exception):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return repr(self.__value)


class InterfaceError(Error):
    def __init__(self, value):
        Error.__init__(self, value)


class DatabaseError(Error):
    def __init__(self, value):
        Error.__init__(self, value)


class BatchError(DatabaseError):
    results = None

    def __init__(self, value, results):
        Error.__init__(self, value)
        self.results = results


class DataError(DatabaseError):
    def __init__(self, value):
        DatabaseError.__init__(self, value)


class OperationalError(DatabaseError):
    def __init__(self, value):
        DatabaseError.__init__(self, value)


class IntegrityError(DatabaseError):
    def __init__(self, value):
        DatabaseError.__init__(self, value)


class InternalError(DatabaseError):
    def __init__(self, value):
        DatabaseError.__init__(self, value)


class ProgrammingError(DatabaseError):
    def __init__(self, value):
        DatabaseError.__init__(self, value)


class NotSupportedError(DatabaseError):
    def __init__(self, value):
        DatabaseError.__init__(self, value)


class EndOfStream(Exception):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return repr(self.__value)


def db_error_handler(error_code, error_string):
    """
    :type error_code int
    :type error_string str
    """
    error_code_string = protocol.lookup_code(error_code)
    if error_code in protocol.DATA_ERRORS:
        raise DataError(error_code_string + ': ' + error_string)
    elif error_code in protocol.OPERATIONAL_ERRORS:
        raise OperationalError(error_code_string + ': ' + error_string)
    elif error_code in protocol.INTEGRITY_ERRORS:
        raise IntegrityError(error_code_string + ': ' + error_string)
    elif error_code in protocol.INTERNAL_ERRORS:
        raise InternalError(error_code_string + ': ' + error_string)
    elif error_code in protocol.PROGRAMMING_ERRORS:
        raise ProgrammingError(error_code_string + ': ' + error_string)
    elif error_code in protocol.NOT_SUPPORTED_ERRORS:
        raise NotSupportedError(error_code_string + ': ' + error_string)
    else:
        raise DatabaseError(error_code_string + ': ' + error_string)
