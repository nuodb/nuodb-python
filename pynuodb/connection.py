"""A module for connecting to a NuoDB database.

(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

Exported Classes:
Connection -- Class for establishing connection with host.

Exported Functions:
connect -- Creates a connection object.
"""

__all__ = ['apilevel', 'threadsafety', 'paramstyle', 'connect',
           'reset', 'Connection']

import os
import copy
import xml.etree.ElementTree as ElementTree

try:
    from typing import Any, Dict, Mapping, Optional, Tuple  # pylint: disable=unused-import
except ImportError:
    pass


from . import __version__
from .exception import Error, InterfaceError
from .session import SessionException

from . import cursor
from . import session
from . import encodedsession
from .datatype import LOCALZONE_NAME

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


def connect(database=None,  # type: Optional[str]
            host=None,      # type: Optional[str]
            user=None,      # type: Optional[str]
            password=None,  # type: Optional[str]
            options=None,   # type: Optional[Mapping[str, str]]
            **kwargs
            ):
    # type: (...) -> Connection
    """Return a new NuoDB SQL Connection object.

    :param database: Name of the database.
    :param host: Hostname (and port if non-default) of the AP to connect to.
    :param user: Username to connect with.
    :param password: Password to connect with.
    :param options: Connection options.
    :returns: A new Connection object.
    """
    return Connection(database=database, host=host,
                      user=user, password=password,
                      options=options, **kwargs)


def reset():
    # type: () -> None
    """Reset the module to its initial state.

    Forget any global state maintained by the module.
    NOTE: this does not impact existing connections or cursors.
    It only impacts new connections.
    """
    encodedsession.EncodedSession.reset()


class Connection(object):
    """An established SQL connection with a NuoDB database.

    Public Functions:
    testConnection -- Tests to ensure the connection was properly established.
    close -- Closes the connection with the host.
    commit -- Sends a message to the host to commit transaction.
    rollback -- Sends a message to the host to rollback uncommitted changes.
    cursor -- Return a new Cursor object using the connection.
    setautocommit -- Change the auto-commit mode of the connection.

    Private Functions:
    __init__ -- Constructor for the Connection class.
    _check_closed -- Checks if the connection to the host is closed.

    Special Function:
    autocommit (getter) -- Gets the value of auto-commit from the database.
    autocommit (setter) -- Sets the value of auto-commit on the database.

    Deprecated: These names were used in older versions of the driver but they
    are not part of PEP 249.
    auto_commit (getter) -- Gets the value of auto-commit from the database.
    auto_commit (setter) -- Sets the value of auto-commit on the database.
    """

    # PEP 249 recommends that all exceptions be exposed as attributes in the
    # Connection object.
    from .exception import Warning, Error, InterfaceError, DatabaseError
    from .exception import OperationalError, IntegrityError, InternalError
    from .exception import ProgrammingError, NotSupportedError

    _trans_id = None          # type: Optional[int]
    __session = None          # type: encodedsession.EncodedSession

    __config = None           # type: Dict[str, Any]

    def __init__(self, database=None,  # type: Optional[str]
                 host=None,            # type: Optional[str]
                 user=None,            # type: Optional[str]
                 password=None,        # type: Optional[str]
                 options=None,         # type: Optional[Mapping[str, str]]
                 **kwargs
                 ):
        # type: (...) -> None
        """Construct a Connection object.

        :param database: Name of the database to connect to
        :param host: Host (and port if needed) of the AP to connect to.
        :param username: Username to connect with.
        :param password: Password to connect with.
        :param options: Connection options.
        :param kwargs: Extra arguments to pass to EncodedSession.
        """
        if database is None:
            raise InterfaceError("No database provided.")
        if user is None:
            raise InterfaceError("No user provided.")
        if password is None:
            raise InterfaceError("No password provided.")

        self.__config = {'driver_version': __version__,
                         'db_name': database,
                         'user': user,
                         'options': copy.deepcopy(options)}

        # Split the options into connection parameters and session options
        params, opts = session.Session.session_options(options)

        params['Database'] = database

        if host is None:
            host = 'localhost'

        port = None
        direct = opts.get('direct', 'false')
        if direct.lower() == 'true':
            self.__config['ap_host'] = None
            # In direct mode the port is part of the host string already
            self.__config['engine_host'] = host
        else:
            self.__config['ap_host'] = host
            # Pass all the connection parameters to the AP just in case.
            (host, port) = self._getTE(host, params, opts)
            self.__config['engine_host'] = '%s:%d' % (host, port)

        # Connect to the NuoDB TE.  It needs all the options.
        self.__session = encodedsession.EncodedSession(
            host, port=port, options=options, **kwargs)
        self.__session.doConnect(params)

        # updates params['TimeZone'] if not set and returns
        # loalzone_name either params['TimeZone'] or based
        # upon tzlocal.
        localzone_name = self._init_local_timezone(params)
        params.update({'user': user, 'clientProcessId': str(os.getpid())})

        self.__session.timezone_name = localzone_name
        self.__session.open_database(database, password, params)

        self.__config['client_protocol_id'] = self.__session.protocol_id
        self.__config['connection_id'] = self.__session.connection_id
        self.__config['db_uuid'] = self.__session.db_uuid
        self.__config['db_protocol_id'] = self.__session.db_protocol_id
        self.__config['engine_id'] = self.__session.engine_id

        if self.__session.tls_encrypted:
            self.__config['tls_enabled'] = True
            self.__config['cipher'] = None
        else:
            self.__config['tls_enabled'] = False
            self.__config['cipher'] = self.__session.cipher_name

        # Set auto commit to false by default per PEP 249
        if 'autocommit' in kwargs:
            self.setautocommit(kwargs['autocommit'])
        else:
            self.setautocommit(False)

    @staticmethod
    def _init_local_timezone(params):
        # type: (Dict[str, str]) -> str
        # params['timezone'] updated if not set
        # returns timezone
        localzone_name = None
        for k, v in params.items():
            if k.lower() == 'timezone':
                localzone_name = v
                break
        if localzone_name is None:
            params['timezone'] = LOCALZONE_NAME
            localzone_name = LOCALZONE_NAME
        return localzone_name

    @staticmethod
    def _getTE(admin, attributes, options):
        # type: (str, Mapping[str, str], Mapping[str, str]) -> Tuple[str, int]
        """Connect to the AP and ask it to direct us a TE for this database."""
        s = session.Session(admin, service="SQL2", options=options)
        try:
            s.doConnect(attributes=attributes)
            connectDetail = s.recv()
        finally:
            s.close()

        if connectDetail is None:
            # Since we don't pass a timeout to recv, it must return a value
            raise RuntimeError("Session.rev() returned None without timeout!")

        connString = connectDetail.decode()
        session.checkForError(connString)

        root = ElementTree.fromstring(connString)
        if root.tag != "Cloud":
            raise SessionException("Unexpected AP response type: " + root.tag)
        address = root.get('Address')
        if address is None:
            raise SessionException("Invalid AP response: missing address")
        port = root.get('Port')
        if port is None:
            raise SessionException("Invalid AP response: missing port")
        return (address, int(port))

    def testConnection(self):
        # type: () -> None
        """Ensure the connection was properly established.

        :raises ProgrammingError: If the connection is not established.
        """
        self.__session.test_connection()

    @property
    def autocommit(self):
        # type: () -> bool
        """Return the value of autocommit for the connection.

        :returns: True if autocommit is enabled.
        """
        self._check_closed()
        return self.__session.get_autocommit() == 1

    @autocommit.setter
    def autocommit(self, value):
        # type: (bool) -> None
        """Set the value of autocommit for the connection.

        :param bool value: True to enable autocommit, False to disable.
        """
        self.setautocommit(value)

    @property
    def auto_commit(self):
        # type: () -> int
        """Return the value of autocommit for the connection.

        DEPRECATED.
        :returns: 0 if autocommit is not enabled, 1 if it is enabled.
        """
        return self.autocommit

    @auto_commit.setter
    def auto_commit(self, value):
        # type: (int) -> None
        """Set the value of auto_commit for the connection.

        DEPRECATED.
        :param value: 1 to enable autocommit, 0 to disable.
        """
        self.setautocommit(value != 0)

    def connection_config(self):
        # type: () -> Dict[str, Any]
        """Returns a copy of the connection configuration.

        Configuration:
          ap_host        :str:  Address of the AP host or None for direct
          cipher         :str:  Name of the cipher if using SRP, else None
          connected      :bool: True if the connection is active
          connection_id  :int:  ID of the connection
          db_name        :str:  name of the connected database
          db_protocol_id :int:  Server protocol ID of the connected database
          db_uuid        :uuid: UUID for the connected database
          driver_version :str:  Version of this driver
          engine_host    :str:  Address of the TE we're connected to
          engine_id      :int:  ID for the TE we're connected to
          options        :dict: Dictionary of connection options
          protocol_id    :int:  Negotiated client protocol ID
          tls_enabled    :bool: True if we're connected using TLS
          user           :str:  name of the connected user

        :returns: Copy of the connection config names and values.
                  Modifying these values has no effect on the connection.
        """
        config = copy.deepcopy(self.__config)
        config['connected'] = not self.__session.closed
        return config

    def setautocommit(self, value):
        # type: (bool) -> None
        """Change the auto-commit status of the connection."""
        self._check_closed()
        self.__session.set_autocommit(1 if value else 0)

    def close(self):
        # type: () -> None
        """Close this connection to the database."""
        self._check_closed()
        self.__session.send_close()
        self.__session.closed = True

    def _check_closed(self):
        # type: () -> None
        """Check if the connection is available.

        :raises Error: If the connection to the host is closed.
        """
        if self.__session.closed:
            raise Error("connection is closed")

    def commit(self):
        # type: () -> None
        """Commit the current transaction."""
        self._check_closed()
        self._trans_id = self.__session.send_commit()

    def rollback(self):
        # type: () -> None
        """Rollback any uncommitted changes."""
        self._check_closed()
        self.__session.send_rollback()

    def cursor(self, prepared_statement_cache_size=50):
        # type: (int) -> cursor.Cursor
        """Return a new Cursor object using the connection.

        :param cache_size: Size of the prepared statement cache.
        """
        self._check_closed()
        return cursor.Cursor(self.__session, prepared_statement_cache_size)

    def __enter__(self):
        # Return self to allow use within the 'with' block
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # type: () -> None
        # exc_type is None if the block completed normally.
        # If an exception occurred, exc_type, exc_val, exc_tb hold details.
        try:
            if exc_type is None:
                # No error: commit if needed
                if not self.autocommit:
                    self.commit()
            else:
                # Error! Rollback if needed.
                if not self.autocommit:
                    self.rollback()
        finally:
            # Always close the connection!
            self.close()
