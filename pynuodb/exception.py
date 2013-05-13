
__all__ = [ 'Warning', 'Error', 'InterfaceError', 'DatabaseError', 'DataError',
            'OperationalError', 'IntegrityError', 'InternalError',
            'ProgrammingError', 'NotSupportedError' ]

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
