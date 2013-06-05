"""Classes containing the exceptions for reporting errors."""

import protocol

__all__ = [ 'Warning', 'Error', 'InterfaceError', 'DatabaseError', 'DataError',
            'OperationalError', 'IntegrityError', 'InternalError',
            'ProgrammingError', 'NotSupportedError', 'EndOfStream', 'dbErrorHandler']

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

def dbErrorHandler(errorCode, errorString):
    if   errorCode in [
                           protocol.COMPILE_ERROR,
                           protocol.RUNTIME_ERROR,
                           protocol.CONVERSION_ERROR,
                           protocol.TRUNCATION_ERROR,
                           protocol.VERSION_ERROR,
                           protocol.INVALID_UTF8,
                           protocol.I18N_ERROR,
                      ]:
        raise DataError(protocol.stringifyError[errorCode] + ': ' + errorString)
    elif errorCode in [
                           protocol.NETWORK_ERROR,
                           protocol.DDL_ERROR,
                           protocol.PLATFORM_ERROR,
                           protocol.BATCH_UPDATE_ERROR,
                           protocol.OPERATION_KILLED,
                           protocol.INVALID_STATEMENT,
                      ]:
        raise OperationalError(protocol.stringifyError[errorCode] + ': ' + errorString)
    elif errorCode in []:
        raise IntegrityError(protocol.stringifyError[errorCode] + ': ' + errorString)
    elif errorCode in [
                           protocol.DATABASE_CORRUPTION,
                           protocol.INTERNAL_ERROR,
                           protocol.UPDATE_CONFLICT,
                           protocol.DEADLOCK,
                           protocol.IS_SHUTDOWN,
                      ]:
        raise InternalError(protocol.stringifyError[errorCode] + ': ' + errorString)
    elif errorCode in [
                           protocol.SYNTAX_ERROR,
                           protocol.CONNECTION_ERROR,
                           protocol.APPLICATION_ERROR,
                           protocol.SECURITY_ERROR,
                           protocol.NO_SUCH_TABLE,
                           protocol.NO_SCHEMA,
                           protocol.CONFIGURATION_ERROR,
                           protocol.READ_ONLY_ERROR,
                           protocol.IN_QUOTED_STRING, 
                      ]:
        raise ProgrammingError(protocol.stringifyError[errorCode] + ': ' + errorString)
    elif errorCode in [
                           protocol.FEATURE_NOT_YET_IMPLEMENTED,
                           protocol.UNSUPPORTED_TRANSACTION_ISOLATION,
                      ]:
        raise NotSupportedError(protocol.stringifyError[errorCode] + ': ' + errorString)
    else:
        raise DatabaseError(protocol.stringifyError[errorCode] + ': ' + errorString)
    
    
