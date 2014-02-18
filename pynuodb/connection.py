"""A module for connecting to a NuoDB database.

Exported Classes:
Connection -- Class for establishing connection with host.

Exported Functions:
connect -- Creates a connection object.
"""

__all__ = [ 'apilevel', 'threadsafety', 'paramstyle', 'connect', 'Connection' ]

from cursor import Cursor
from encodedsession import EncodedSession
from crypt import ClientPassword, RC4Cipher
from util import getCloudEntry

import time
import string

# http://www.python.org/dev/peps/pep-0249

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"
#schema='user', auto_commit=False

def connect(database, host, user, password, options=None):
    """Creates a connection object.
    Arguments:
    database -- Name of the database to access.
    host -- NuoDB Broker host. This can include a port (e.g. "localhost:48005")
    user -- The database user
    password -- The database user's password
    options -- A dictionary of NuoDB connection options
        Some common options include:
        "schema"

    @type database str
    @type host str
    @type user str
    @type password str
    @type options dict[str,str]
    """
    return Connection(database, host, user, password, options)

class Connection(object):
    
    """Class for establishing a connection with host.
    
    Public Functions:
    testConnection -- Tests to ensure the connection was properly established.
    close -- Closes the connection with the host.
    commit -- Sends a message to the host to commit transaction.
    rollback -- Sends a message to the host to rollback uncommitted changes.
    cursor -- Return a new Cursor object using the connection.
    
    Private Functions:
    __init__ -- Constructor for the Connection class.
    _check_closed -- Checks if the connection to the host is closed.
    
    Special Function:
    auto_commit (getter) -- Gets the value of auto_commit from the database.
    auto_commit (setter) -- Sets the value of auto_commit on the database.
    """

    from exception import Warning, Error, InterfaceError, DatabaseError, DataError, \
            OperationalError, IntegrityError, InternalError, \
            ProgrammingError, NotSupportedError
    
    def __init__(self, dbName, broker, username, password, options):
        """Constructor for the Connection class.
        
        Arguments:
        dbName -- Name of database you are accessing.
        broker -- Address of the broker you are connecting too.
        username -- NuoDB username.
        password -- NuoDB password.
        options -- A dictionary of NuoDB connection options
            Some common options include:
            "schema"
        
        Returns:
        a Connection instance

        @type dbName str
        @type broker str
        @type username str
        @type password str
        @type options dict[str,str]
        """
        (host, port) = getCloudEntry(broker, dbName)
        self.__session = EncodedSession(host, port)
        self._trans_id = None

        cp = ClientPassword()
        
        parameters = {'user' : username, 'timezone' : time.strftime('%Z')}
        if options:
            parameters.update(options)

        version, serverKey, salt = self.__session.open_database(dbName, parameters, cp)

        sessionKey = cp.computeSessionKey(string.upper(username), password, salt, serverKey)
        self.__session.setCiphers(RC4Cipher(sessionKey), RC4Cipher(sessionKey))

        self.__session.check_auth()

        # set auto commit to false by default per PEP
        self.__session.set_autocommit(0)

    def testConnection(self):
        """Tests to ensure the connection was properly established.
        
        This function will test the connection and if it was created should print out:
        count: 1
        name: ONE
        value: 1
        
        Arguments:
        None
        
        Returns:
        None
        """
        self.__session.test_connection()

    @property
    def auto_commit(self):
        """Gets the value of auto_commit from the database."""
        self._check_closed()
        return self.__session.get_autocommit()
    
    @auto_commit.setter
    def auto_commit(self, value):
        """Sets the value of auto_commit on the database."""
        self._check_closed()
        self.__session.set_autocommit(value)

    def close(self):
        """Closes the connection with the host."""
        self._check_closed()
        self.__session.send_close()
        #TODO: Nope
        self.__session.closed = True

    def _check_closed(self):
        """Checks if the connection to the host is closed."""
        if self.__session.closed:
            raise self.Error("connection is closed")

    def commit(self):
        """Sends a message to the host to commit transaction."""
        self._check_closed()
        self._trans_id = self.__session.send_commit()

    def rollback(self):
        """Sends a message to the host to rollback uncommitted changes."""
        self._check_closed()
        self.__session.send_rollback()

    def cursor(self, prepared_statement_cache_size=50):
        """Return a new Cursor object using the connection."""
        self._check_closed()
        return Cursor(self.__session, prepared_statement_cache_size)
