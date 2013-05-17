import protocol
from datatype import TypeObjectFromNuodb
from exception import *

class Cursor(object):

    def __init__(self, session):
        self.session = session
        self.closed = False
        self.arraysize = 1
        
        self._reset()

    def close(self):
        self._check_closed()
        self.closed = True

    def _check_closed(self):
        if self.closed:
            raise Error("cursor is closed")
        if self.session.closed:
            raise Error("connection is closed")

    def _reset(self):
        self.description = None
        self.rowcount = -1
        self.colcount = -1
        
        self._st_handle = None
        self._rs_handle = None
        self._results = []
        self._results_pos = 0
        
        self._complete = False

    def callproc(self, procname, parameters=None):
        raise NotSupportedError

    def execute(self, operation, parameters=None):
        self._check_closed()
        self._reset()
        if not parameters:
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
        # Create a statement handle
        self.session.putMessageId(protocol.CREATE)
        self.session.exchangeMessages()
        self._st_handle = self.session.getInt()
        
        # Use handle to query
        self.session.putMessageId(protocol.EXECUTE).putInt(self._st_handle).putString(operation)
        self.session.exchangeMessages()

    def _executeprepared(self, operation, parameters):
        # Create a statement handle
        self.session.putMessageId(protocol.PREPARE).putString(operation)
        self.session.exchangeMessages()
        self._st_handle = self.session.getInt()
        p_count = self.session.getInt()
        
        if p_count != len(parameters):
            raise OperationalError
        
        # Use handle to query
        self.session.putMessageId(protocol.EXECUTEPREPAREDSTATEMENT)
        self.session.putInt(self._st_handle).putInt(p_count)
        for param in parameters[:]:
            self.session.putValue(param)
        self.session.exchangeMessages()

    def executemany(self, operation, seq_of_parameters):
        self._check_closed()
        rowCount = 0
        for parameters in seq_of_parameters[:]:
            self.execute(operation, parameters)
            if self.rowcount >= 0:
                rowCount += self.rowcount
        if rowCount != 0:
            self.rowcount = rowCount            

    def fetchone(self):
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
        raise NotSupportedError
    
    def arraysize(self):
        raise NotSupportedError

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
        
    def _get_next_results(self):

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
        