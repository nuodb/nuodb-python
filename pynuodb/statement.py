__author__ = 'jgetto'


class Statement(object):
    def __init__(self, handle):
        """
        @type handle int
        """
        self.handle = handle


class PreparedStatement(Statement):
    def __init__(self, handle, parameter_count):
        """
        @type handle int
        @type parameter_count int
        """
        super(PreparedStatement, self).__init__(handle)
        self.parameter_count = parameter_count


class ExecutionResult(object):
    def __init__(self, statement, result, row_count):
        """
        @type statement Statement
        @type result int
        @type row_count int
        """
        self.result = result
        self.row_count = row_count
        self.statement = statement
