"""A module for housing the Cursor class.

Exported Classes:

Cursor -- Class for representing a database cursor.

"""

import protocol
from datatype import TypeObjectFromNuodb
from exception import Error, NotSupportedError, EndOfStream, ProgrammingError, InterfaceError, dbErrorHandler

class Cursor(object):

    """Class for representing a database cursor.
    
    Public Functions:
    close -- Closes the cursor into the database.
    callproc -- Currently not supported.
    execute -- Executes an SQL operation.
    executemany -- Executes the operation for each list of paramaters passed in.
    fetchone -- Fetches the first row of results generated by the previous execute.
    fetchmany -- Fetches the number of rows that are passed in.
    fetchall -- Fetches everything generated by the previous execute.
    nextset -- Currently not supported.
    setinputsizes -- Currently not supported.
    setoutputsize -- Currently not supported.
    
    Private Functions:
    __init__ -- Constructor for the Cursor class.
    _check_closed -- Checks if the cursor is closed.
    _reset -- Resets SQL transaction variables.
    _execute -- Handles operations without parameters.
    _executeprepared -- Handles operations with parameters.
    _get_next_results -- Gets the next set of results.    
    """
    
    def __init__(self, session):
        """Constructor for the Cursor class."""
        self.session = session
        self.closed = False
        self.arraysize = 1
        
        self.description = None
        self.rowcount = -1
        self.colcount = -1
        
        self._st_handle = None
        self._rs_handle = None
        self._results = []
        self._results_pos = 0
        
        self._complete = False
        
        self.__query = None
        
    @property    
    def query(self):
        """Return the most recent query"""
        return self.__query

    def close(self):
        """Closes the cursor into the database."""
        self._check_closed()
        self.closed = True

    def _check_closed(self):
        """Checks if the cursor is closed."""
        if self.closed:
            raise Error("cursor is closed")
        if self.session.closed:
            raise Error("connection is closed")

    def _reset(self):
        """Resets SQL transaction variables.
        
        Also closes any open statements and result sets.
        """
        
        #Always close statement (and rs) before new query. This will need to change for #22
        if self._st_handle is not None:
            self._close_statement()
        
        self.description = None
        self.rowcount = -1
        self.colcount = -1
        
        self._st_handle = None
        self._rs_handle = None
        self._results = []
        self._results_pos = 0
        
        self._complete = False

    def callproc(self, procname, parameters=None):
        """Currently not supported."""
        raise NotSupportedError

    def execute(self, operation, parameters=None):
        """Executes an SQL operation.
        
        The SQL operations can be with or without parameters, if parameters are included
        then _executeprepared is invoked to prepare and execute the operation.
        
        Arguments:
        operation -- SQL operation to be performed.
        parameters -- Additional parameters for the operation may be supplied, but these
                      are optional.
        
        Returns:
        None
        """
        self._check_closed()
        self._reset()
        self.__query = operation
        if parameters is None:
            self._execute(operation)
            
        else:
            self._executeprepared(operation, parameters)
            
        result = self.session.getInt()

        # TODO: check this, should be -1 on select?
        self.rowcount = self.session.getInt()
        if result > 0:
            self.session.putMessageId(protocol.GETRESULTSET).putInt(self._st_handle)
            self.session.exchangeMessages()

            self._rs_handle = self.session.getInt()
            self.colcount = self.session.getInt()

            col_num_iter = xrange(self.colcount)                  

            for i in col_num_iter:
                self.session.getString()

            next_row = self.session.getInt()
            while next_row == 1:
                row = [None] * self.colcount
                for i in col_num_iter:
                    row[i] = self.session.getValue()
        
                self._results.append(tuple(row))
            
                try:
                    next_row = self.session.getInt()  
                except EndOfStream:
                    break
                    
            # the first chunk might be all of the data
            if next_row == 0:
                self._complete = True
                    
            # add description attribute
            self.session.putMessageId(protocol.GETMETADATA).putInt(self._rs_handle)
            self.session.exchangeMessages()
            
            self.description = [None] * self.session.getInt()
            for i in col_num_iter:
                catalogName = self.session.getString()
                schemaName = self.session.getString()
                tableName = self.session.getString()
                columnName = self.session.getString()
                columnLabel = self.session.getString()
                collationSequence = self.session.getValue()
                columnTypeName = self.session.getString()
                columnType = self.session.getInt()
                columnDisplaySize = self.session.getInt()
                precision = self.session.getInt()
                scale = self.session.getInt()
                flags = self.session.getInt()
                self.description[i] = [columnName, TypeObjectFromNuodb(columnTypeName), 
                                       columnDisplaySize, None, precision, scale, None]
                                       
        if self.rowcount < 0:
            self.rowcount = -1

    def _execute(self, operation):
        """Handles operations without parameters."""
        # Create a statement handle
        self.session.putMessageId(protocol.CREATE)
        self.session.exchangeMessages()
        self._st_handle = self.session.getInt()
        
        # Use handle to query
        self.session.putMessageId(protocol.EXECUTE).putInt(self._st_handle).putString(operation)
        self.session.exchangeMessages()

    def _executeprepared(self, operation, parameters):
        """Handles operations with parameters."""
        # Create a statement handle
        self.session.putMessageId(protocol.PREPARE).putString(operation)
        self.session.exchangeMessages()
        self._st_handle = self.session.getInt()
        p_count = self.session.getInt()
        
        if p_count != len(parameters):
            raise ProgrammingError("Incorrect number of parameters specified, expected %d, got %d" % (p_count, len(parameters)))
        
        # Use handle to query
        self.session.putMessageId(protocol.EXECUTEPREPAREDSTATEMENT)
        self.session.putInt(self._st_handle).putInt(p_count)
        for param in parameters[:]:
            self.session.putValue(param)
        self.session.exchangeMessages()

    def executemany(self, operation, seq_of_parameters):
        """Executes the operation for each list of paramaters passed in."""
        self._check_closed()
        
        self.session.putMessageId(protocol.PREPARE).putString(operation)
        self.session.exchangeMessages()
        self._st_handle = self.session.getInt()
        p_count = self.session.getInt()
        
        self.session.putMessageId(protocol.EXECUTEBATCHPREPAREDSTATEMENT)
        self.session.putInt(self._st_handle)
        for parameters in seq_of_parameters[:]:
            if p_count != len(parameters):
                raise ProgrammingError("Incorrect number of parameters specified, expected %d, got %d" % (p_count, len(parameters)))
            self.session.putInt(len(parameters))
            for param in parameters[:]:
                self.session.putValue(param)
        self.session.putInt(-1)
        self.session.putInt(len(seq_of_parameters))
        self.session.exchangeMessages()
            
        for _ in seq_of_parameters[:]:
            result = self.session.getInt()
            if result == -3:
                error_code = self.session.getInt()
                error_string = self.session.getString()
                dbErrorHandler(error_code, error_string)
                      

    def fetchone(self):
        """Fetches the first row of results generated by the previous execute."""
        self._check_closed()
        if self._rs_handle == None:
            raise Error("Previous execute did not produce any results or no call was issued yet")
        
        if self._results_pos == len(self._results):
            if not self._complete:
                self._get_next_results()
            else:
                return None
                
        res = self._results[self._results_pos]
        self._results_pos += 1
        return res

    def fetchmany(self, size=None):
        """Fetches the number of rows that are passed in."""
        self._check_closed()
        
        if size == None:
            size = self.arraysize
            
        fetched_rows = []
        num_fetched_rows = 0
        while num_fetched_rows < size:
            row = self.fetchone()
            if row == None:
                break
            else:
                fetched_rows.append(row)
                num_fetched_rows += 1
        
        return fetched_rows

    def fetchall(self):
        """Fetches everything generated by the previous execute."""
        self._check_closed()

        fetched_rows = []
        while True:
            row = self.fetchone()
            if row == None:
                break
            else:
                fetched_rows.append(row)
                
        return fetched_rows   


    def nextset(self):
        """Currently not supported."""
        raise NotSupportedError

    def setinputsizes(self, sizes):
        """Currently not supported."""
        pass

    def setoutputsize(self, size, column=None):
        """Currently not supported."""
        pass
    
    def _close_statement(self):
        """Closes the current statement or prepared statement
        
        This will cause any open result sets to be closed as well
        """
        
        if self._st_handle is None:
            raise InterfaceError('Statement is not open')
        self.session.putMessageId(protocol.CLOSESTATMENT).putInt(self._st_handle)
        self.session.exchangeMessages(False)

    def _get_next_results(self):
        """Gets the next set of results."""
        self.session.putMessageId(protocol.NEXT).putInt(self._rs_handle)
        self.session.exchangeMessages()
        
        col_num_iter = xrange(self.colcount)
        
        self._results = []
        next_row = self.session.getInt()
        while next_row == 1:
            row = [None] * self.colcount
            for i in col_num_iter:
                row[i] = self.session.getValue()
        
            self._results.append(tuple(row))
            
            try:
                next_row = self.session.getInt()  
            except EndOfStream:
                break
            
        
        self._results_pos = 0
        
        if next_row == 0:
            self._complete = True
