
__all__ = [ 'apilevel', 'threadsafety', 'paramstyle', 'connect', 'Connection',
            'Cursor' ]

from encodedsession import EncodedSession
from nuodb.crypt import ClientPassword, RC4Cipher
from nuodb.session import SessionException
from nuodb.util import getCloudEntry


import string
import protocol

from exception import *


# http://www.python.org/dev/peps/pep-0249

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

def connect(database=None, user=None, password=None, host=None, port=48004):
    # TODO: figure out which options to use, and use that to create the
    # connection instance correctly
    return Connection(host, database, user, password)


class Connection(object):

    def __init__(self, broker, dbName, username='dba', password='dba', description='nuosql'):
        (host, port) = getCloudEntry(broker, dbName)
        self.__session = EncodedSession(host, port)

        cp = ClientPassword()
#         parameters = {'user' : username }
        parameters = {'user' : username, 'schema' : 'user' }

        self.__session.putMessageId(protocol.OPENDATABASE).putInt(protocol.EXECUTEPREPAREDUPDATE).putString(dbName).putInt(len(parameters))
        for (k, v) in parameters.iteritems():
            self.__session.putString(k).putString(v)
        self.__session.putNull().putString(cp.genClientKey())

        self.__session.exchangeMessages()

        version = self.__session.getInt()
        serverKey = self.__session.getString()
        salt = self.__session.getString()

        sessionKey = cp.computeSessionKey(string.upper(username), password, salt, serverKey)
        self.__session.setCiphers(RC4Cipher(sessionKey), RC4Cipher(sessionKey))

        self.__session.putMessageId(protocol.AUTHENTICATION).putString('Success!')

        self.__session.exchangeMessages()

    def testConnection(self):

        # Create a statement handle
        self.__session.putMessageId(protocol.CREATE)
        self.__session.exchangeMessages()
        handle = self.__session.getInt()

        # Use handle to query dual
        self.__session.putMessageId(protocol.EXECUTEQUERY).putInt(handle).putString('select 1 as one from dual')
        self.__session.exchangeMessages()

        rsHandle = self.__session.getInt()
        count = self.__session.getInt()
        colname = self.__session.getString()
        result = self.__session.getInt()
        fieldValue = self.__session.getInt()
        r2 = self.__session.getInt()

        print "count: " + str(count)
        print "name: " + colname
        print "value: " + str(fieldValue)

    def close(self):
        pass

    def commit(self):
        # isn't commit automatic?
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self.__session)
    
class Cursor(object):

    def __init__(self, session):
        self.session = session
        
        self.description = None
        self.rowcount = -1
        self.colcount = -1
        self.arraysize = 1
        
        self._st_handle = None
        self._rs_handle = None
        self._results = []
        self._results_pos = 0
        
        self._complete = False

    def close(self):
        pass

    def callproc(self, procname, parameters=None):
        raise NotSupportedError("not supported")

    def execute(self, operation, parameters=None):
        
        if not parameters:
            # Create a statement handle
            self.session.putMessageId(protocol.CREATE)
            self.session.exchangeMessages()
            self._st_handle = self.session.getInt()
        
            # Use handle to query
            self.session.putMessageId(protocol.EXECUTE).putInt(self._st_handle).putString(operation)
            self.session.exchangeMessages()
        
        else:
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
                
        result = self.session.getInt()

        # TODO: check this, should be -1 on select?
        self.rowcount = self.session.getInt() 

        if result > 0:
            self.session.putMessageId(protocol.GETRESULTSET).putInt(self._st_handle)
            self.session.exchangeMessages()

            self._rs_handle = self.session.getInt()
            self.colcount = self.session.getInt()

            col_num_iter = xrange(self.colcount)   
        
            #TODO: add type_code to description
            self.description = [None] * self.colcount
            for i in col_num_iter:
                self.description[i] = [self.session.getString()] + [None] * 6

            next_row = self.session.getInt()
            while next_row == 1:
                row = [None] * self.colcount
                for i in col_num_iter:
                    row[i] = self.session.getValue()
        
                self._results.append(tuple(row))
            
                try:
                    next_row = self.session.getInt()  
                except:
                    break


    def executemany(self, operation, seq_of_parameters):
        try:
            pass
        except Exception, error:
            print "NuoDB error: %s" % str(error)

    def fetchone(self):
        try:
            if self._results_pos == len(self._results):
                if not self._complete:
                    self._get_next_results()
                else:
                    return None
                    
            res = self._results[self._results_pos]
            self._results_pos += 1
            return res
            
        except Exception, error:
            print "NuoDB error: %s" % str(error)

    def fetchmany(self, size=None):
        try:
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
        
            
        except Exception, error:
            print "NuoDB error: %s" % str(error)

    def fetchall(self):
        try:
            fetched_rows = []
            while True:
                row = self.fetchone()
                if row == None:
                    break
                else:
                    fetched_rows.append(row)
                    
            return fetched_rows   
                    
                
        except Exception, error:
            print "NuoDB error: %s" % str(error)

    def nextset(self):
        raise NotSupportedError("not supported")
    
    def arraysize(self):
        raise NotSupportedError("not supported")

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
            except:
                break
        
        self._results_pos = 0
        
        if next_row == 0:
            self._complete = True
        