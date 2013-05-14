
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


class Connection:

    def __init__(self, broker, dbName, username='dba', password='dba', description='nuosql'):
        (host, port) = getCloudEntry(broker, dbName)
        self.__session = EncodedSession(host, port)

        cp = ClientPassword()
        parameters = {'user' : username, 'schema' : 'test' }

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
    
class Cursor:

    def __init__(self, session):
        self.session = session
        
        self.description = None
        self.rowcount = -1
        self.arraysize = 1

    def close(self):
        pass

    def callproc(self, procname, parameters=None):
        raise NotSupportedError

    def execute(self, operation, parameters=None):
        print 'execute: %s' % operation
        try:
            # Create a statement handle
            self.session.putMessageId(protocol.CREATE)
            self.session.exchangeMessages()
            handle = self.session.getInt()
            
            # Use handle to query dual
            self.session.putMessageId(protocol.EXECUTEQUERY).putInt(handle).putString(operation)
            
            self.session.exchangeMessages()
             
#             rsHandle = self.session.getInt()
#             count = self.session.getInt()
#             colname = self.session.getString()
#             result = self.session.getInt()
#             fieldValue = self.session.getInt()
#             r2 = self.session.getInt()
#              
#             print "count: " + str(count)
#             print "name: " + colname
#             print "value: " + str(fieldValue)
            
        except Exception, error:
            print "database error: %s" % str(error)

    def executemany(self, operation, seq_of_parameters):
        try:
            pass
        except Exception, error:
            pass

    def fetchone(self):
        try:
            pass
        except Exception, error:
            pass

    def fetchmany(self, size=None):
        try:
            pass
        except Exception, error:
            pass

    def fetchall(self):
        try:
            pass
        except Exception, error:
            pass

    def nextset(self):
        raise NotSupportedError
    
    def arraysize(self):
        raise NotSupportedError

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
