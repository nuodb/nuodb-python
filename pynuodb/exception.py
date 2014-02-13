"""Classes containing the exceptions for reporting errors."""

import protocol

__all__ = ['Warning', 'Error', 'InterfaceError', 'DatabaseError', 'DataError',
           'OperationalError', 'IntegrityError', 'InternalError',
           'ProgrammingError', 'NotSupportedError', 'EndOfStream', 'db_error_handler']


class Warning(StandardError):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return repr(self.__value)


class Error(StandardError):
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


class EndOfStream(StandardError):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return repr(self.__value)


def db_error_handler(error_code, error_string):
    """
    @type error_code int
    @type error_string str
    """
    if error_code in protocol.DATA_ERRORS:
        raise DataError(protocol.stringifyError[error_code] + ': ' + error_string)
    elif error_code in protocol.OPERATIONAL_ERRORS:
        raise OperationalError(protocol.stringifyError[error_code] + ': ' + error_string)
    # elif errorCode in []:
    #     raise IntegrityError(protocol.stringifyError[errorCode] + ': ' + errorString)
    elif error_code in protocol.INTERNAL_ERRORS:
        raise InternalError(protocol.stringifyError[error_code] + ': ' + error_string)
    elif error_code in protocol.PROGRAMMING_ERRORS:
        raise ProgrammingError(protocol.stringifyError[error_code] + ': ' + error_string)
    elif error_code in protocol.NOT_SUPPORTED_ERRORS:
        raise NotSupportedError(protocol.stringifyError[error_code] + ': ' + error_string)
    else:
        raise DatabaseError(protocol.stringifyError[error_code] + ': ' + error_string)
