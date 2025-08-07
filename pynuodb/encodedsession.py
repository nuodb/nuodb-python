"""A module for housing the EncodedSession class.

(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

Exported Classes:
EncodedSession -- Class for representing an encoded session with the database.
"""

__all__ = ['EncodedSession']

import uuid
import struct
import decimal
import sys
import threading
import datetime  # pylint: disable=unused-import

try:
    from typing import Any, Collection, Dict, List  # pylint: disable=unused-import
    from typing import Mapping, Optional, Tuple  # pylint: disable=unused-import
except ImportError:
    pass

from .exception import DataError, EndOfStream, ProgrammingError
from .exception import db_error_handler, BatchError
from .session import SessionException

from . import crypt
from . import protocol
from . import datatype
from . import session
from . import statement
from . import result_set
from .datatype import LOCALZONE_NAME

# ZoneInfo is preferred but not introduced until 3.9
if sys.version_info >= (3, 9):
    # preferred python >= 3.9
    from zoneinfo import ZoneInfo
else:
    # fallback to pytz if python < 3.9
    from pytz import timezone as ZoneInfo

isP2 = sys.version[0] == '2'
REMOVE_FORMAT = 0


class EncodedSession(session.Session):  # pylint: disable=too-many-public-methods
    """Class for representing an encoded session with the database.

    Public Functions:
    putMessageId -- Start a message with the messageId.
    putInt -- Appends an Integer value to the message.
    putScaledInt -- Appends a Scaled Integer value to the message.
    putString -- Appends a String to the message.
    putBoolean -- Appends a Boolean value to the message.
    putNull -- Appends a Null value to the message.
    putUUID -- Appends a UUID to the message.
    putOpaque -- Appends an Opaque data value to the message.
    putDouble -- Appends a Double to the message.
    putMsSinceEpoch -- Appends the MsSinceEpoch value to the message.
    putNsSinceEpoch -- Appends the NsSinceEpoch value to the message.
    putMsSinceMidnight -- Appends the MsSinceMidnight value to the message.
    putBlob -- Appends the Blob(Binary Large OBject) value to the message.
    putClob -- Appends the Clob(Character Large OBject) value to the message.
    putScaledTime -- Appends a Scaled Time value to the message.
    putScaledTimestamp -- Appends a Scaled Timestamp value to the message.
    putScaledDate -- Appends a Scaled Date value to the message.
    putScaledCount2 -- Appends a scaled and signed decimal to the message
    putValue -- Determines the probable type of the value and calls the supporting function.
    getInt -- Read the next Integer value off the session.
    getScaledInt -- Read the next Scaled Integer value off the session.
    getString -- Read the next String off the session.
    getBoolean -- Read the next Boolean value off the session.
    getNull -- Read the next Null value off the session.
    getDouble -- Read the next Double off the session.
    getTime -- Read the next Time value off the session.
    getOpaque -- Read the next Opaque value off the session.
    getBlob -- Read the next Blob(Binary Large OBject) value off the session.
    getClob -- Read the next Clob(Character Large OBject) value off the session.
    getScaledTime -- Read the next Scaled Time value off the session.
    getScaledTimestamp -- Read the next Scaled Timestamp value off the session.
    getScaledDate -- Read the next Scaled Date value off the session.
    getUUID -- Read the next UUID value off the session.
    getValue -- Determine the datatype of the next value off the session, then
                call the supporting function.
    exchangeMessages -- Exchange the pending message for an optional response
                        from the server.
    set_encryption -- Takes a value of type boolean. Setting encryption to False
                      will result in disabling encryption after the handshake.
    """

    # This is managed by the connection
    closed = False

    __output = None  # type: bytearray
    # If we did not need to be compatible with Python 2 this should be bytes
    # But in Python 2, bytes is just another name for str, so use bytearray
    __input = None   # type: bytearray
    __inpos = 0      # type: int
    __encryption = True
    __sessionVersion = 0
    __connectedNodeID = -1

    __connectionDatabaseUUID = None    # type: Optional[uuid.UUID]
    __connectionID = -1
    __effectivePlatformVersion = 0
    __maxNodes = -1

    __lastTxnId = 0
    __lastNodeId = 0
    __lastCommitSeq = 0

    __dbinfo = None  # type: Dict[int, Tuple[int, int]]

    # Manage the last commit info

    # If we decide this lock is causing performance issues we can implement
    # a reader/writer lock.
    __dblock = threading.Lock()
    __databases = {}  # type: Dict[str, Dict[int, Tuple[int, int]]]

    # timezone to use for this connection, set on open database
    __timezone_name = ''  # type: str

    @staticmethod
    def reset():
        # type: () -> None
        """Reset the EncodedSession global data."""
        with EncodedSession.__dblock:
            EncodedSession.__databases = {}

    @property
    def db_uuid(self):
        # type: () -> Optional[uuid.UUID]
        """Return the database's UUID"""
        return self.__connectionDatabaseUUID

    @property
    def db_protocol_id(self):
        # type: () -> int
        """Return the database protocol version."""
        return self.__effectivePlatformVersion

    @property
    def protocol_id(self):
        # type: () -> int
        """Return the client protocol version."""
        return self.__sessionVersion

    @property
    def connection_id(self):
        # type: () -> int
        """Return the database connection ID"""
        return self.__connectionID

    @property
    def engine_id(self):
        # type: () -> int
        """Return the ID for the TE, or -1 if unknown."""
        return self.__connectedNodeID

    def __init__(self, host, service='SQL2', options=None, **kwargs):
        # type: (str, str, Optional[Mapping[str, str]], Any) -> None
        """Construct an EncodedSession object."""
        self.__output = bytearray()
        self.__input = bytearray()
        if options and options.get('cipher') == 'None':
            self.__encryption = False
        super(EncodedSession, self).__init__(host, service=service,
                                             options=options, **kwargs)
        self.__timezone_name = LOCALZONE_NAME

    @property
    def timezone_name(self):
        # type: () -> Optional[str]
        """ read name of timezone for this connection """
        return self.__timezone_name

    @timezone_name.setter
    def timezone_name(self, tzname):
        # type: (str) -> None
        try:
            # fails if tzname is bad
            ZoneInfo(tzname)
        except KeyError:
            raise ProgrammingError('Invalid TimeZone ' + tzname)
        except LookupError:
            raise ProgrammingError('Invalid TimeZone ' + tzname)
        self.__timezone_name = tzname

    @property
    def timezone_info(self):
        # type: () -> datetime.tzinfo
        """ get a tzinfo for this connection """
        tz_info = ZoneInfo(self.__timezone_name)
        return tz_info

    def open_database(self, db_name, password, parameters):  # pylint: disable=too-many-branches,too-many-statements
        # type: (str, str, Dict[str, str]) -> None
        """Perform a handshake as a SQL client with a NuoDB TE.

        If we have a TLS session use it, else use SRP for authentication.
        :param db_name: Name of the database to connect to.
        :param password: The user's password.
        :param parameters: Connection parameters.
        """
        params = parameters.copy()
        if 'clientInfo' not in params:
            params['clientInfo'] = 'pynuodb'
        # With TLS send the password; otherwise send supported ciphers for SRP
        if self.tls_encrypted:
            params['password'] = password
        elif 'ciphers' not in params:
            params['ciphers'] = crypt.get_ciphers()

        self._putMessageId(protocol.OPENDATABASE)
        self.putInt(protocol.CURRENT_PROTOCOL_VERSION)
        self.putString(db_name)

        self.putInt(len(params))
        for (k, v) in params.items():
            self.putString(k).putString(v)

        # Ignored for backward-compat
        self.putInt(0)

        # If we're not using TLS, add the client key for the SRP handshake.
        if not self.tls_encrypted:
            cp = crypt.ClientPassword()
            self.putString(cp.genClientKey())

        self._exchangeMessages()
        protocolVersion = self.getInt()

        cipher = None
        incomingIV = None
        outgoingIV = None

        if not self.tls_encrypted:
            serverKey = self.getString()
            salt = self.getString()

            # Determine our chosen cipher
            if protocolVersion < protocol.MULTI_CIPHER:
                cipher = 'RC4'
            else:
                cipher = self.getString()
                # We're the client so use the server's outgoing IV as our
                # incoming and vice versa.
                incomingIV = self.getOpaque()
                outgoingIV = self.getOpaque()

        self.__connectionDatabaseUUID = self.getUUID()

        if protocolVersion >= protocol.SEND_CONNID_TO_CLIENT:
            self.__connectionID = self.getInt()

        if protocolVersion >= protocol.SEND_EFFECTIVE_PLATFORM_VERSION_TO_CLIENT:
            self.__effectivePlatformVersion = self.getInt()

        if protocolVersion >= protocol.LAST_COMMIT_INFO:
            self.__connectedNodeID = self.getInt()
            self.__maxNodes = self.getInt()

            dbid = str(self.db_uuid)
            with EncodedSession.__dblock:
                if dbid not in EncodedSession.__databases:
                    EncodedSession.__databases[dbid] = {}
                self.__dbinfo = EncodedSession.__databases[dbid]

        self.__sessionVersion = protocolVersion

        if not self.tls_encrypted:
            # Pacify mypy
            assert cipher
            try:
                self._setup_auth(params['user'].upper(), password, cipher,
                                 serverKey, salt, cp, incomingIV, outgoingIV)

                # Complete the authentication protocol
                self._putMessageId(protocol.AUTHENTICATION)
                self.putString(protocol.AUTH_TEST_STR)
                self._exchangeMessages()

            except SessionException as e:
                raise ProgrammingError('Failed to authenticate: ' + str(e))

    def get_auth_types(self):
        # type: () -> int
        """Return the authorized types for the connection."""
        self._putMessageId(protocol.AUTHORIZETYPESREQUEST)
        self._exchangeMessages()
        val = self.getInt()
        return val

    def get_autocommit(self):
        # type: () -> bool
        """Return the autocommit setting for this connection."""
        self._putMessageId(protocol.GETAUTOCOMMIT)
        self._exchangeMessages()
        if self.getValue():
            return True
        return False

    def set_autocommit(self, value):
        # type: (int) -> None
        """Set autocommit for this connection."""
        self._putMessageId(protocol.SETAUTOCOMMIT).putInt(value)
        self._exchangeMessages(False)

    def send_close(self):
        # type: () -> None
        """Close this connection."""
        self._putMessageId(protocol.CLOSE)
        self._exchangeMessages()
        self.close()

    def __set_dbinfo(self, sid, txid, seqid):
        # type: (int, int, int) -> None
        with EncodedSession.__dblock:
            # 0 is an invalid sequence ID
            lci = self.__dbinfo.get(sid, (0, 0))
            if seqid > lci[1]:
                self.__dbinfo[sid] = (txid, seqid)

    def send_commit(self):
        # type: () -> int
        """Commit an open transaction on this connection.

        :returns: The transaction ID of the committed transaction.
        """
        self._putMessageId(protocol.COMMITTRANSACTION)
        self._exchangeMessages()
        self.__lastTxnId = self.getInt()
        self.__lastNodeId = self.getInt()
        self.__lastCommitSeq = self.getInt()
        self.__set_dbinfo(self.__lastNodeId, self.__lastTxnId, self.__lastCommitSeq)
        return self.__lastTxnId

    def send_rollback(self):
        # type: () -> None
        """Roll back the currently open transaction."""
        self._putMessageId(protocol.ROLLBACKTRANSACTION)
        self._exchangeMessages()

    def set_encryption(self, value):
        # type: (bool) -> None
        """Enable or disable encryption."""
        self.__encryption = value

    def test_connection(self):
        # type: () -> None
        """Test this connection to the database."""
        # Create a statement handle
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        handle = self.getInt()

        # Use handle to query dual
        self._setup_statement(handle, protocol.EXECUTEQUERY)
        self.putString('select 1 as one from dual')
        self._exchangeMessages()

        # returns: rsHandle, count, colname, result, fieldValue, r2
        res = [self.getInt(), self.getInt(), self.getString(),
               self.getInt(), self.getInt(), self.getInt()]

        if None in res:
            raise ProgrammingError('Failed to connect!')

    # Mostly for cursors
    def create_statement(self):
        # type: () -> statement.Statement
        """Create a statement and return a Statement object."""
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        return statement.Statement(self.getInt())

    def __execute_postfix(self):
        # type: () -> None
        tzUpdate = self.getBoolean()
        if tzUpdate:
            server_tz = self.getValue()
            self.__timezone_name = server_tz
        txid = self.getInt()
        sid = self.getInt()
        seqid = self.getInt()
        self.__set_dbinfo(sid, txid, seqid)

    def execute_statement(self, stmt, query):
        # type: (statement.Statement, str) -> statement.ExecutionResult
        """Execute a query using the given statement.

        :param stmt: Statement to use for the query.
        :param query: Operation to be executed.
        :returns: The result of the operation execution.
        """
        self._setup_statement(stmt.handle, protocol.EXECUTE).putString(query)
        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()
        self.__execute_postfix()

        return statement.ExecutionResult(stmt, result, rowcount)

    def close_statement(self, stmt):
        # type: (statement.Statement) -> None
        """Close the statement."""
        self._putMessageId(protocol.CLOSESTATEMENT).putInt(stmt.handle)
        self._exchangeMessages(False)

    def close_result_set(self, resultset):
        # type: (result_set.ResultSet) -> None
        """Close the result set."""
        self._putMessageId(protocol.CLOSERESULTSET).putInt(resultset.handle)
        self._exchangeMessages(False)

    def create_prepared_statement(self, query):
        # type: (str) -> statement.PreparedStatement
        """Create a prepared statement for the given query."""
        self._putMessageId(protocol.PREPARE).putString(query)
        self._exchangeMessages()

        handle = self.getInt()
        param_count = self.getInt()

        stmt = statement.PreparedStatement(handle, param_count)

        if self.__sessionVersion >= protocol.SEND_PREPARE_STMT_RESULT_SET_METADATA_TO_CLIENT:
            if self.getBoolean():
                stmt.description = self._parse_result_set_description()

        return stmt

    def execute_prepared_statement(
            self, prepared_statement,  # type: statement.PreparedStatement
            parameters                 # type: Collection[result_set.Value]
    ):
        # type: (...) -> statement.ExecutionResult
        """Execute a prepared statement with the given parameters."""
        self._setup_statement(prepared_statement.handle, protocol.EXECUTEPREPAREDSTATEMENT)

        self.putInt(len(parameters))
        for param in parameters:
            self.putValue(param)

        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()
        self.__execute_postfix()

        return statement.ExecutionResult(prepared_statement, result, rowcount)

    def execute_batch_prepared_statement(self, prepared_statement, param_lists):
        # type: (statement.PreparedStatement, Collection[Collection[result_set.Value]]) -> List[int]
        """Batch the prepared statement with the given parameters."""
        self._setup_statement(prepared_statement.handle, protocol.EXECUTEBATCHPREPAREDSTATEMENT)

        for parameters in param_lists:
            plen = len(parameters)
            if prepared_statement.parameter_count != plen:
                raise ProgrammingError("Incorrect number of parameters specified,"
                                       " expected %d, got %d"
                                       % (prepared_statement.parameter_count,
                                          plen))
            self.putInt(plen)
            for param in parameters:
                self.putValue(param)
        self.putInt(-1)
        self.putInt(len(param_lists))
        self._exchangeMessages()

        results = []  # type: List[int]
        error_string = None

        for _ in param_lists:
            result = self.getInt()
            results.append(result)
            if result == -3:
                ec = self.getInt()
                es = self.getString()
                # only report first
                if error_string is None:
                    error_string = '%s:%s' % (protocol.stringifyError[ec], es)

        if error_string is not None:
            raise BatchError(error_string, results)

        self.__execute_postfix()

        return results

    def fetch_result_set(self, stmt):
        # type: (statement.Statement) -> result_set.ResultSet
        """Get the ResultSet from the previous operation."""
        self._putMessageId(protocol.GETRESULTSET).putInt(stmt.handle)
        self._exchangeMessages()

        handle = self.getInt()
        colcount = self.getInt()

        # skip the header labels
        for _ in range(colcount):
            self.getString()

        complete = False
        init_results = []  # type: List[result_set.Row]

        # If we hit the end of the stream without next==0, there are more
        # results to fetch.
        while self._hasBytes(1):
            next_row = self.getInt()
            if next_row == 0:
                complete = True
                break

            row = [None] * colcount
            for i in range(colcount):
                row[i] = self.getValue()

            init_results.append(tuple(row))

        return result_set.ResultSet(handle, colcount, init_results, complete)

    def fetch_result_set_next(self, resultset):
        # type: (result_set.ResultSet) -> None
        """Get more rows from this result set."""
        self._putMessageId(protocol.NEXT).putInt(resultset.handle)
        self._exchangeMessages()

        resultset.clear_results()

        while self._hasBytes(1):
            if self.getInt() == 0:
                resultset.complete = True
                break

            row = [None] * resultset.col_count
            for i in range(resultset.col_count):
                row[i] = self.getValue()

            resultset.add_row(tuple(row))

    def _parse_result_set_description(self):
        # type: () -> List[List[Any]]
        """Parse the result set metadata from the message."""
        description = [list()] * self.getInt()  # type: List[List[Any]]
        for i in range(len(description)):  # pylint: disable=consider-using-enumerate
            self.getString()    # catalog_name
            self.getString()    # schema_name
            self.getString()    # table_name
            self.getString()    # column_name
            column_label = self.getString()    # column_label
            self.getValue()     # collation_sequence
            column_type_name = self.getString()
            self.getInt()       # column_type
            column_display_size = self.getInt()
            precision = self.getInt()
            scale = self.getInt()
            self.getInt()       # flags

            # TODO: type information should be derived from the type
            # (column_type) not the typename.
            description[i] = [column_label,
                              datatype.TypeObjectFromNuodb(column_type_name),
                              column_display_size, None, precision, scale, None]

        return description

    def fetch_result_set_description(self, resultset):
        # type: (result_set.ResultSet) -> List[List[Any]]
        """Return the metadata for this result set."""
        self._putMessageId(protocol.GETMETADATA).putInt(resultset.handle)
        self._exchangeMessages()
        return self._parse_result_set_description()

    # Methods to put values into the next message

    def _putMessageId(self, messageId):
        # type: (int) -> EncodedSession
        """Start a message with the messageId.

        :type messageId: int
        """
        self.__output = bytearray()
        self.putInt(messageId, isMessageId=True)
        return self

    def putInt(self, value, isMessageId=False):
        # type: (int, bool) -> EncodedSession
        """Append an integer value to the message.

        :type value: int
        :type isMessageId: bool
        """
        if value > -11 and value < 32:
            self.__output.append(protocol.INT0 + value)
        else:
            if isMessageId:
                data = crypt.toByteString(value)
            else:
                data = crypt.toSignedByteString(value)
            self.__output.append(protocol.INTLEN0 + len(data))
            self.__output += data
        return self

    # Does not preserve E notation
    def putScaledInt(self, value):
        # type: (decimal.Decimal) -> EncodedSession
        """Append a Scaled Integer value to the message.

        :type value: decimal.Decimal
        """
        # Convert the decimal's notation into decimal
        value += REMOVE_FORMAT
        exponent = value.as_tuple()[2]
        if not isinstance(exponent, int):
            # this should not occur
            raise ValueError("Invalid exponent in Decimal: %r" % exponent)
        scale = abs(exponent)
        data = crypt.toSignedByteString(int(value * decimal.Decimal(10**scale)))

        # If our length including the tag is more than 9 bytes we will need to
        # send the data using ScaledCount2
        if len(data) > 8:
            return self.putScaledCount2(value)

        self.__output.append(protocol.SCALEDLEN0 + len(data))
        self.__output.append(scale)
        self.__output += data
        return self

    def putString(self, value):
        # type: (str) -> EncodedSession
        """Append a String to the message.

        :type value: str
        """
        data = bytes(value) if isP2 else value.encode('utf-8')  # type: ignore
        length = len(data)
        if length < 40:
            self.__output.append(protocol.UTF8LEN0 + length)
        else:
            lengthStr = crypt.toByteString(length)
            self.__output.append(protocol.UTF8COUNT0 + len(lengthStr))
            self.__output += lengthStr
        self.__output += data
        return self

    def putOpaque(self, value):
        # type: (datatype.Binary) -> EncodedSession
        """Append an Opaque data value to the message.

        :type value: datatype.Binary
        """
        length = len(value)
        if length < 40:
            self.__output.append(protocol.OPAQUELEN0 + length)
        else:
            lenData = crypt.toByteString(length)
            self.__output.append(protocol.OPAQUECOUNT0 + len(lenData))
            self.__output += lenData
        self.__output += value
        return self

    def putBoolean(self, value):
        # type: (bool) -> EncodedSession
        """Append a Boolean value to the message.

        :type value: bool
        """
        self.__output.append(protocol.TRUE if value is True else protocol.FALSE)
        return self

    def putNull(self):
        # type: () -> EncodedSession
        """Append a Null value to the message."""
        self.__output.append(protocol.NULL)
        return self

    def putUUID(self, value):
        # type: (uuid.UUID) -> EncodedSession
        """Append a UUID to the message.

        :type value: uuid.UUID
        """
        self.__output.append(protocol.UUID)
        self.__output += value.bytes
        return self

    def putDouble(self, value):
        # type: (float) -> EncodedSession
        """Append a Double to the message.

        :type value: float
        """
        data = struct.pack('!d', value)
        self.__output.append(protocol.DOUBLELEN0 + len(data))
        self.__output += data
        return self

    def putMsSinceEpoch(self, value):
        # type: (int) -> EncodedSession
        """Append the MsSinceEpoch value to the message.

        :type value: int
        """
        data = crypt.toSignedByteString(value)
        self.__output.append(protocol.MILLISECLEN0 + len(data))
        self.__output += data
        return self

    def putNsSinceEpoch(self, value):
        # type: (int) -> EncodedSession
        """Append the NsSinceEpoch value to the message.

        :type value: int
        """
        data = crypt.toSignedByteString(value)
        self.__output.append(protocol.NANOSECLEN0 + len(data))
        self.__output += data
        return self

    def putMsSinceMidnight(self, value):
        # type: (int) -> EncodedSession
        """Append the MsSinceMidnight value to the message.

        :type value: int
        """
        data = crypt.toByteString(value)
        self.__output.append(protocol.TIMELEN0 + len(data))
        self.__output += data
        return self

    # Not currently used by Python driver
    def putBlob(self, value):
        # type: (datatype.Binary) -> EncodedSession
        """Append the Blob(Binary Large OBject) value to the message.

        :type value: datatype.Binary
        """
        length = len(value)
        lenData = crypt.toByteString(length)
        self.__output.append(protocol.BLOBLEN0 + len(lenData))
        self.__output += lenData
        self.__output += value
        return self

    def putClob(self, value):
        # type: (datatype.Binary) -> EncodedSession
        """Append the Clob(Character Large OBject) value to the message.

        :type value: datatype.Binary
        """
        length = len(value)
        lenData = crypt.toByteString(length)
        self.__output.append(protocol.CLOBLEN0 + len(lenData))
        self.__output += lenData
        self.__output += value
        return self

    def _putScaled(self, base, ticks, scale):
        # type: (int, int, int) -> EncodedSession
        data = crypt.toSignedByteString(ticks)
        if data:
            self.__output.append(base + len(data))
            self.__output.append(scale)
            self.__output += data
        else:
            self.__output.append(base + 1)
            self.__output.append(0)
            self.__output.append(0)
        return self

    def putScaledTime(self, value):
        # type: (datatype.Time) -> EncodedSession
        """Append a Scaled Time value to the message.

        :type value: datetype.Time
        """
        return self._putScaled(protocol.SCALEDTIMELEN0,
                               *datatype.TimeToTicks(value, self.timezone_info))

    def putScaledTimestamp(self, value):
        # type: (datatype.Timestamp) -> EncodedSession
        """Append a Scaled Timestamp value to the message.

        :type value: datetime.datetime
        """
        return self._putScaled(protocol.SCALEDTIMESTAMPLEN0,
                               *datatype.TimestampToTicks(value, self.timezone_info))

    def putScaledDate(self, value):
        # type: (datatype.Date) -> EncodedSession
        """Append a Scaled Date value to the message.

        :type value: datatime.date
        """
        return self._putScaled(protocol.SCALEDDATELEN0,
                               datatype.DateToTicks(value), 0)

    def putScaledCount2(self, value):
        # type: (decimal.Decimal) -> EncodedSession
        """Append a scaled and signed decimal to the message.

        :type value: decimal.Decimal
        """
        exponent = value.as_tuple()[2]
        if not isinstance(exponent, int):
            # this should not occur
            raise ValueError("Invalid exponent in Decimal: %r" % exponent)
        scale = abs(exponent)

        sign = 1 if value.as_tuple()[0] == 0 else -1
        signData = crypt.toSignedByteString(sign)
        data = crypt.toByteString(int(abs(value) * decimal.Decimal(10**scale)))
        self.__output.append(protocol.SCALEDCOUNT2)
        self.__output += crypt.toByteString(scale)
        self.__output += signData
        self.__output.append(len(data))
        self.__output += data
        return self

    def putVectorDouble(self, value):
        # type: (datatype.Vector) -> EncodedSession
        """Append a Vector with subtype Vector.DOUBLE to the message.

        :type value: datatype.Vector
        """
        self.__output.append(protocol.VECTOR)
        # subtype
        self.__output.append(protocol.VECTOR_DOUBLE)
        # length in bytes in count notation, i.e. first
        # number of bytes needed for the length, then the
        # encoded length
        lengthStr = crypt.toByteString(len(value) * 8)
        self.__output.append(len(lengthStr))
        self.__output += lengthStr

        # the actual vector: Each value as double in little endian encoding
        for val in value:
            self.__output += struct.pack('<d', float(val))

        return self

    def putVector(self, value):
        # type: (datatype.Vector) -> EncodedSession
        """Append a Vector type to the message.

        :type value: datatype.Vector
        """
        if value.getSubtype() == datatype.Vector.DOUBLE:
            return self.putVectorDouble(value)

        raise DataError("unsupported value for VECTOR subtype: %d" % (value.getSubtype()))

    def putValue(self, value):  # pylint: disable=too-many-return-statements
        # type: (Any) -> EncodedSession
        """Call the supporting function based on the type of the value."""
        if value is None:
            return self.putNull()

        if isinstance(value, int):
            return self.putInt(value)

        if isinstance(value, float):
            return self.putDouble(value)

        if isinstance(value, decimal.Decimal):
            return self.putScaledInt(value)

        if isinstance(value, datatype.Timestamp):
            # Note: Timestamp must be above Date because it inherits from Date
            return self.putScaledTimestamp(value)

        if isinstance(value, datatype.Date):
            return self.putScaledDate(value)

        if isinstance(value, datatype.Time):
            return self.putScaledTime(value)

        if isinstance(value, datatype.Binary):
            return self.putOpaque(value)

        if isinstance(value, bool):
            return self.putBoolean(value)

        # we don't want to autodetect lists as being VECTOR, so we
        # only bind double if it is the explicit type
        if isinstance(value, datatype.Vector):
            return self.putVector(value)

        # I find it pretty bogus that we pass str(value) here: why not value?
        return self.putString(str(value))

    # GET methods

    def getInt(self):
        # type: () -> int
        """Read the next Integer value off the session.

        :rtype: int
        """
        code = self._getTypeCode()
        if code >= protocol.INTMINUS10 and code <= protocol.INT31:
            return code - protocol.INT0

        elif code >= protocol.INTLEN1 and code <= protocol.INTLEN8:
            return crypt.fromSignedByteString(self._takeBytes(code - protocol.INTLEN0))

        raise DataError('Not an integer: %d' % (code))

    # Does not preserve E notation
    def getScaledInt(self):
        # type: () -> decimal.Decimal
        """Read the next Scaled Integer value off the session.

        :rtype: decimal.Decimal
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDLEN0 and code <= protocol.SCALEDLEN8:
            scale = crypt.fromByteString(self._takeBytes(1))
            value = crypt.fromSignedByteString(self._takeBytes(code - protocol.SCALEDLEN0))
            # preserves Decimal sign, exp, int...
            sign = 1 if value < 0 else 0
            data = tuple(int(i) for i in str(abs(value)))
            return decimal.Decimal((sign, data, -1 * scale))

        raise DataError('Not a scaled integer')

    def getString(self):
        # type: () -> str
        """Read the next String off the session.

        :rtype: str
        """
        code = self._getTypeCode()

        if code >= protocol.UTF8LEN0 and code <= protocol.UTF8LEN39:
            data = self._takeBytes(code - protocol.UTF8LEN0)
            return crypt.arrayToStr(data)

        if code >= protocol.UTF8COUNT1 and code <= protocol.UTF8COUNT4:
            length = crypt.fromByteString(self._takeBytes(code - protocol.UTF8COUNT0))
            data = self._takeBytes(length)
            return crypt.arrayToStr(data)

        raise DataError('Not a string: %s' % (code))

    def getOpaque(self):
        # type: () -> datatype.Binary
        """Read the next Opaque value off the session.

        :rtype: datatype.Binary
        """
        code = self._getTypeCode()

        if code >= protocol.OPAQUELEN0 and code <= protocol.OPAQUELEN39:
            value = self._takeBytes(code - protocol.OPAQUELEN0)
            return datatype.Binary(value)
        if code >= protocol.OPAQUECOUNT1 and code <= protocol.OPAQUECOUNT4:
            length = crypt.fromByteString(self._takeBytes(code - protocol.OPAQUECOUNT0))
            value = self._takeBytes(length)
            return datatype.Binary(value)

        raise DataError('Not an opaque value')

    def getBoolean(self):
        # type: () -> bool
        """Read the next Boolean value off the session.

        :rtype: boolean
        """
        code = self._getTypeCode()

        if code == protocol.TRUE:
            return True
        if code == protocol.FALSE:
            return False

        raise DataError('Not a boolean')

    def getNull(self):
        # type: () -> None
        """Read the next Null value off the session.

        :rtype: None
        """
        if self._getTypeCode() != protocol.NULL:
            raise DataError('Not null')

    def getDouble(self):
        # type: () -> float
        """Read the next Double off the session.

        :rtype: float
        """
        code = self._getTypeCode()

        if code == protocol.DOUBLELEN0:
            return 0.0

        if code > protocol.DOUBLELEN0 and code <= protocol.DOUBLELEN8:
            data = bytearray(self._takeBytes(code - protocol.DOUBLELEN0))
            if code != protocol.DOUBLELEN8:
                for _ in range(0, protocol.DOUBLELEN8 - code):
                    data.append(0)
            return struct.unpack('!d', data)[0]

        raise DataError('Not a double')

    def getTime(self):
        # type: () -> int
        """Read the next Time value off the session.

        :rtype: int
        """
        code = self._getTypeCode()

        if code >= protocol.MILLISECLEN0 and code <= protocol.MILLISECLEN8:
            return crypt.fromSignedByteString(self._takeBytes(code - protocol.MILLISECLEN0))

        if code >= protocol.NANOSECLEN0 and code <= protocol.NANOSECLEN8:
            return crypt.fromSignedByteString(self._takeBytes(code - protocol.NANOSECLEN0))

        if code >= protocol.TIMELEN0 and code <= protocol.TIMELEN4:
            return crypt.fromByteString(self._takeBytes(code - protocol.TIMELEN0))

        raise DataError('Not a time')

    # Not currently used by Python driver
    def getBlob(self):
        # type: () -> datatype.Binary
        """Read the next Blob(Binary Large OBject) value off the session.

        :rtype: datatype.Binary
        """
        code = self._getTypeCode()

        if code >= protocol.BLOBLEN0 and code <= protocol.BLOBLEN4:
            length = crypt.fromByteString(self._takeBytes(code - protocol.BLOBLEN0))
            return datatype.Binary(self._takeBytes(length))

        raise DataError('Not a blob')

    def getClob(self):
        # type: () -> bytes
        """Read the next Clob(Character Large OBject) value off the session.

        :rtype: bytes
        """
        code = self._getTypeCode()

        if code >= protocol.CLOBLEN0 and code <= protocol.CLOBLEN4:
            length = crypt.fromByteString(self._takeBytes(code - protocol.CLOBLEN0))
            return self._takeBytes(length)

        raise DataError('Not a clob')

    @staticmethod
    def __unpack(scale, time):
        # type: (int, int) -> Tuple[int, int]
        shiftr = 10 ** scale
        ticks = time // shiftr
        fraction = time % shiftr
        if scale > 6:
            micros = fraction // 10 ** (scale - 6)
        else:
            micros = fraction * 10 ** (6 - scale)
        if micros < 0:
            micros %= 1000000
            ticks += 1
        return (ticks, micros)

    def getScaledTime(self):
        # type: () -> datatype.Time
        """Read the next Scaled Time value off the session.

        :rtype: datetime.time
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDTIMELEN1 and code <= protocol.SCALEDTIMELEN8:
            scale = crypt.fromByteString(self._takeBytes(1))
            time = crypt.fromSignedByteString(self._takeBytes(code - protocol.SCALEDTIMELEN0))
            seconds, micros = self.__unpack(scale, time)
            return datatype.TimeFromTicks(seconds, micros, self.timezone_info)

        raise DataError('Not a scaled time')

    def getScaledTimestamp(self):
        # type: () -> datatype.Timestamp
        """Read the next Scaled Timestamp value off the session.

        :rtype: datetime.datetime
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDTIMESTAMPLEN1 and code <= protocol.SCALEDTIMESTAMPLEN8:
            scale = crypt.fromByteString(self._takeBytes(1))
            stamp = crypt.fromSignedByteString(self._takeBytes(code - protocol.SCALEDTIMESTAMPLEN0))
            seconds, micros = self.__unpack(scale, stamp)
            return datatype.TimestampFromTicks(seconds, micros, self.timezone_info)

        raise DataError('Not a scaled timestamp')
    
    def getScaledTimestampNoTz(self):
        # type: () -> datatype.Timestamp
        """Read the next Scaled Timestamp without timezone value off the session.

        :rtype: datetime.datetime
        """
        code = self._getTypeCode()

        if code == protocol.SCALEDTIMESTAMPNOTZ:
            scale = crypt.fromByteString(self._takeBytes(1))
            stamp = crypt.fromSignedByteString(self._takeBytes(code-protocol.SCALEDTIMESTAMPNOTZLEN0))
            seconds, micros = self.__unpack(scale, stamp)
            return datatype.TimestampFromTicks(seconds, micros, None)

        raise DataError('Not a scaled timestamp without time zone')

    def getScaledDate(self):
        # type: () -> datatype.Date
        """Read the next Scaled Date value off the session.

        :rtype: datetime.date
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDDATELEN1 and code <= protocol.SCALEDDATELEN8:
            scale = crypt.fromByteString(self._takeBytes(1))
            date = crypt.fromSignedByteString(self._takeBytes(code - protocol.SCALEDDATELEN0))
            return datatype.DateFromTicks(date // (10 ** scale))

        raise DataError('Not a scaled date')

    def getUUID(self):
        # type: () -> uuid.UUID
        """Read the next UUID value off the session.

        :rtype uuid.UUID
        """
        if self._getTypeCode() == protocol.UUID:
            data = self._takeBytes(16)
            return uuid.UUID(bytes=bytes(data))

        raise DataError('Not a UUID')

    def getVector(self):
        # type: () -> datatype.Vector
        """Read the next vector off the session.

        :rtype datatype.Vector
        """
        if self._getTypeCode() == protocol.VECTOR:
            subtype = crypt.fromByteString(self._takeBytes(1))
            if subtype == protocol.VECTOR_DOUBLE:
                # VECTOR(<dim>, DOUBLE)
                lengthBytes = crypt.fromByteString(self._takeBytes(1))
                length = crypt.fromByteString(self._takeBytes(lengthBytes))

                if length % 8 != 0:
                    raise DataError("Invalid size for VECTOR DOUBLE data: %d" % (length))

                dimension = length // 8

                # VECTOR DOUBLE stores the data as little endian
                vector = datatype.Vector(datatype.Vector.DOUBLE,
                                         [struct.unpack('<d', self._takeBytes(8))[0]
                                          for _ in range(dimension)])

                return vector
            else:
                raise DataError("Unknown VECTOR type: %d" % (subtype))
            return 1

        raise DataError('Not a VECTOR')

    def getScaledCount2(self):
        # type: () -> decimal.Decimal
        """Read a scaled and signed decimal from the session.

        :rtype: decimal.Decimal
        """
        code = self._getTypeCode()
        if code is protocol.SCALEDCOUNT2:
            scale = decimal.Decimal(crypt.fromByteString(self._takeBytes(1)))
            sign = crypt.fromSignedByteString(self._takeBytes(1))
            sign = 1 if sign < 0 else 0
            length = crypt.fromByteString(self._takeBytes(1))
            data = crypt.fromByteString(self._takeBytes(length))
            value = tuple(int(i) for i in str(abs(data)))
            scaledcount = decimal.Decimal((sign, value, -1 * int(scale)))
            return scaledcount

        raise DataError('Not a Scaled Count 2')

    def getScaledCount3(self):
        # type: () -> decimal.Decimal
        """Read a scaled and signed decimal from the session.

        :rtype: decimal.Decimal
        """
        code = self._getTypeCode()
        if code is protocol.SCALEDCOUNT3:
            sz = crypt.fromByteString(self._takeBytes(1))
            if sz == 0:
                scale = 0
            else:
                scale = crypt.fromByteString(self._takeBytes(sz))

            sign = crypt.fromSignedByteString(self._takeBytes(1))
            sign = 1 if sign < 0 else 0

            sz = crypt.fromByteString(self._takeBytes(1))
            if sz == 0:
                length = 0
            else:
                length = crypt.fromByteString(self._takeBytes(sz))

            data = crypt.fromByteString(self._takeBytes(length))
            value = tuple(int(i) for i in str(abs(data)))
            scaledcount = decimal.Decimal((sign, value, -1 * int(scale)))
            return scaledcount

        raise DataError('Not a Scaled Count 3')

    # pylint: disable=too-many-return-statements, too-many-branches
    def getValue(self):
        # type: () -> Any
        """Return the next value available in the session."""
        code = self._peekTypeCode()

        if code >= protocol.INTMINUS10 and code <= protocol.INTLEN8:
            return self.getInt()

        if (code >= protocol.UTF8LEN0 and code <= protocol.UTF8LEN39) or \
           (code >= protocol.UTF8COUNT1 and code <= protocol.UTF8COUNT4):
            return self.getString()

        if (code >= protocol.OPAQUELEN0 and code <= protocol.OPAQUELEN39) or \
           (code >= protocol.OPAQUECOUNT1 and code <= protocol.OPAQUECOUNT4):
            return self.getOpaque()

        if code >= protocol.DOUBLELEN0 and code <= protocol.DOUBLELEN8:
            return self.getDouble()

        if code == protocol.TRUE or code == protocol.FALSE:
            return self.getBoolean()

        if code == protocol.UUID:
            return self.getUUID()

        if code == protocol.VECTOR:
            return self.getVector()

        if code == protocol.SCALEDCOUNT2:
            return self.getScaledCount2()

        if code == protocol.SCALEDCOUNT3:
            return self.getScaledCount3()

        if code >= protocol.SCALEDLEN0 and code <= protocol.SCALEDLEN8:
            return self.getScaledInt()

        if code >= protocol.BLOBLEN0 and code <= protocol.CLOBLEN4:
            return self.getBlob()

        if code >= protocol.MILLISECLEN0 and code <= protocol.TIMELEN4:
            return self.getTime()

        if code >= protocol.SCALEDTIMELEN1 and code <= protocol.SCALEDTIMELEN8:
            return self.getScaledTime()

        if code >= protocol.SCALEDTIMESTAMPLEN1 and code <= protocol.SCALEDTIMESTAMPLEN8:
            return self.getScaledTimestamp()

        if code >= protocol.SCALEDDATELEN1 and code <= protocol.SCALEDDATELEN8:
            return self.getScaledDate()

        if code == protocol.SCALEDTIMESTAMPNOTZ:
            return self.getScaledTimestampNoTz()

        if code == protocol.NULL:
            return self.getNull()

        raise DataError("getValue: Invalid type code: %d" % (code))

    def _exchangeMessages(self, getResponse=True):
        # type: (bool) -> None
        """Send the pending message and read a response from the server."""
        out = bytes(self.__output)
        self.__output = bytearray()
        self.__input = bytearray()
        self.__inpos = 0

        self.send(out)

        if getResponse is True:
            resp = self.recv(timeout=None)
            if resp is None:
                db_error_handler(protocol.OPERATION_TIMEOUT, "timed out")
            self.__input = crypt.bytesToArray(resp)

            error = self.getInt()
            if error != 0:
                db_error_handler(error, self.getString())

    # Protected utility routines

    def _setup_statement(self, handle, msgId):
        # type: (int, int) -> EncodedSession
        """Set up a new statement.

        :type handle: int
        :type msgId: int
        """
        self._putMessageId(msgId)
        if self.__sessionVersion >= protocol.LAST_COMMIT_INFO:
            with EncodedSession.__dblock:
                self.putInt(len(self.__dbinfo))
                for sid, tup in self.__dbinfo.items():
                    self.putInt(sid)
                    self.putInt(tup[0])
                    self.putInt(tup[1])
        self.putInt(handle)

        return self

    def _hasBytes(self, length):
        # type: (int) -> bool
        return self.__inpos + length <= len(self.__input)

    def _peekTypeCode(self):
        # type: () -> int
        """Peek the next Type Code off the session. (Does not move inpos)."""
        if not self._hasBytes(1):
            raise EndOfStream('end of stream reached')
        return self.__input[self.__inpos]

    def _getTypeCode(self):
        # type: () -> int
        """Read the next Type Code off the session."""
        try:
            return self._peekTypeCode()
        finally:
            self.__inpos += 1

    def _takeBytes(self, length):
        # type: (int) -> bytearray
        """Get the next length of bytes off the session.

        :type length: int
        :rtype: bytes
        """
        if not self._hasBytes(length):
            raise EndOfStream('end of stream reached (need %d bytes)' % (length))
        try:
            return self.__input[self.__inpos:self.__inpos + length]
        finally:
            self.__inpos += length
