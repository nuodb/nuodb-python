
__all__ = [ 'apilevel', 'threadsafety', 'paramstyle', 'connect', 'Connection',
            'Cursor' ]

from encodedsession import EncodedSession
from nuodb.crypt import ClientPassword, RC4Cipher
from nuodb.session import SessionException
from nuodb.util import getCloudEntry

import string

# http://www.python.org/dev/peps/pep-0249

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

def connect(dsn=None, user=None, password=None, host=None, database=None):
    # TODO: figure out which options to use, and use that to create the
    # connection instance correctly
    return Connection()

class Connection:

    def __init__(self, broker, dbName, username='dba', password='dba'):
        (host, port) = getCloudEntry(broker, dbName)
        self.__session = EncodedSession(host, port)

        cp = ClientPassword()
        parameters = {'user' : username, 'schema' : 'test' }

        self.__session.putMessageId(3).putInt(24).putString(dbName).putInt(len(parameters))
        for (k, v) in parameters.iteritems():
            self.__session.putString(k).putString(v)
        self.__session.putNull().putString(cp.genClientKey())

        self.__session.exchangeMessages()

        version = self.__session.getInt()
        serverKey = self.__session.getString()
        salt = self.__session.getString()

        sessionKey = cp.computeSessionKey(string.upper(username), password, salt, serverKey)
        self.__session.setCiphers(RC4Cipher(sessionKey), RC4Cipher(sessionKey))

        self.__session.putMessageId(86).putString('Success!')

        self.__session.exchangeMessages()

    def testConnection(self):

        # Create a statement handle
        
        self.__session.putMessageId(11)
        self.__session.exchangeMessages()
        handle = self.__session.getInt()

        # Use handle to query dual

        self.__session.putMessageId(19).putInt(handle).putString('select 1 as one from dual')
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
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor
    
class Cursor:

    def __init__(self):
        self.description = None
        self.rowcount = -1
        self.arraysize = 1

    # def callproc(self, procname, parameters=None):

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        pass

    def executemany(self, operation, seq_of_parameters):
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size=None):
        pass

    def fetchall(self):
        pass

    # def nextset(self):

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
