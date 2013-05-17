
__all__ = [ 'apilevel', 'threadsafety', 'paramstyle', 'connect', 'Connection' ]

from cursor import Cursor
from encodedsession import EncodedSession
from nuodb.crypt import ClientPassword, RC4Cipher
from nuodb.session import SessionException
from nuodb.util import getCloudEntry

import string
import protocol

# http://www.python.org/dev/peps/pep-0249

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

def connect(database, host, user, password, port=48004, schema='user', auto_commit=False):
    return Connection(database, host, user, password, port, schema, auto_commit)

class Connection(object):
    from exception import Warning, Error, InterfaceError, DatabaseError, DataError, \
            OperationalError, IntegrityError, InternalError, \
            ProgrammingError, NotSupportedError
    
    def __init__(self, dbName, broker, username, password, port, schema, auto_commit, description='nuosql'):
        (host, port) = getCloudEntry(broker, dbName)
        self.__session = EncodedSession(host, port)
        self._trans_id = None

        cp = ClientPassword()
#         parameters = {'user' : username }
        
        # still need to do schema stuff
        parameters = {'user' : username, 'schema' : schema }

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
        
        # set auto commit to false by default
        if auto_commit:
            self.__session.putMessageId(protocol.SETAUTOCOMMIT).putInt(1)
        else:
            self.__session.putMessageId(protocol.SETAUTOCOMMIT).putInt(0)
        
        self.__session.exchangeMessages(False)

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

    @property
    def auto_commit(self):
        self._check_closed()
        self.__session.putMessageId(protocol.GETAUTOCOMMIT)
        self.__session.exchangeMessages()
        return self.__session.getValue()
    
    @auto_commit.setter
    def auto_commit(self, value):
        self._check_closed()
        self.__session.putMessageId(protocol.SETAUTOCOMMIT).putInt(value)
        self.__session.exchangeMessages()

    def close(self):
        self._check_closed()
        self.__session.putMessageId(protocol.CLOSE)
        self.__session.exchangeMessages()
        self.__session.closed = True

    def _check_closed(self):
        if self.__session.closed:
            raise self.Error("connection is closed")

    def commit(self):
        self._check_closed()
        self.__session.putMessageId(protocol.COMMITTRANSACTION)
        self.__session.exchangeMessages()
        self._trans_id = self.__session.getValue()

    def rollback(self):
        self._check_closed()
        self.__session.putMessageId(protocol.ROLLBACKTRANSACTION)
        self.__session.exchangeMessages()

    def cursor(self):
        self._check_closed()
        return Cursor(self.__session)
    
