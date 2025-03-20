"""A module for housing the EncodedSession class.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

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
from zoneinfo import ZoneInfo
import tzlocal

try:
    from typing import Any, Collection, Dict, Iterable, List  # pylint: disable=unused-import
    from typing import Mapping, Optional, Tuple  # pylint: disable=unused-import
    from .result_set import Row, Value           # pylint: disable=unused-import
except ImportError:
    pass

from .crypt import bytesToArray, arrayToStr, toByteString, fromByteString
from .crypt import toSignedByteString, fromSignedByteString
from .crypt import NoCipher, RC4Cipher
from .session import Session, SessionException
from . import protocol
from . import datatype
from .exception import DataError, EndOfStream, ProgrammingError
from .exception import db_error_handler, BatchError
from .datatype import TypeObjectFromNuodb
from .statement import Statement, PreparedStatement, ExecutionResult
from .result_set import ResultSet

from .crypt import BaseCipher, ClientPassword  # pylint: disable=unused-import

isP2 = sys.version[0] == '2'
REMOVE_FORMAT = 0


class EncodedSession(Session):  # pylint: disable=too-many-public-methods
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
    setCiphers -- Re-sets the incoming and outgoing ciphers for the session.
    set_encryption -- Takes a value of type boolean. Setting encryption to False
                      will result in disabling encryption after the handshake.
    """

    closed = False

    __output = None  # type: bytearray
    # If we did not need to be compatible with Python 2 this should be bytes
    # But in Python 2, bytes is just another name for str, so use bytearray
    __input = None   # type: bytearray
    __inpos = 0      # type: int
    __encryption = True

    __connectionDatabaseUUID = None  # type: Optional[uuid.UUID]
    __sessionVersion = 0
    __connectionID = 0
    __effectivePlatformVersion = 0
    __connectedNodeID = 0
    __maxNodes = 128

    __lastTxnId = 0
    __lastNodeId = 0
    __lastCommitSeq = 0

    __cipher = None  # Optional[str]

    def __init__(self, host, service='SQL2', options=None, **kwargs):
        # type: (str, str, Optional[Mapping[str, str]], Any) -> None
        """Construct an EncodedSession object."""
        self.__output = bytearray()
        self.__input = bytearray()
        if options and options.get('cipher') == 'None':
            self.__encryption = False
        super(EncodedSession, self).__init__(host, service=service,
                                             options=options, **kwargs)
        self.__timezone_name = None
        self.__timezone_info = None

    @property
    def timezone_name(self):
        return self.__timezone_name

    @property
    def timezone_info(self):
        return self.__timezone_info

    @timezone_name.setter
    def timezone_name(self,tzname):
        try:
            self.__timezone_info = ZoneInfo(tzname)
            self.__timezone_name = tzname
        except Exception as e:
            print(e)
            self.__timezone_info = tzlocal.get_localzone()
            self.__timezone_name = tzlocal.get_localzone_name()
            


    def open_database(self, db_name, password, parameters):
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

        self._putMessageId(protocol.OPENDATABASE)
        self.putInt(protocol.CURRENT_PROTOCOL_VERSION)
        self.putString(db_name)

        self.putInt(len(params) + (1 if self.tls_encrypted else 0))
        for (k, v) in params.items():
            self.putString(k).putString(v)

        # Add the password if the session is already encrypted, else add the
        # client key for the SRP handshake.
        if self.tls_encrypted:
            cp = None
            self.putString('password').putString(password)
        else:
            cp = ClientPassword()
            self.putNull().putString(cp.genClientKey())

        self._exchangeMessages()

        protocolVersion = self.getInt()
        if cp:
            serverKey = self.getString()
            salt = self.getString()

        self.__connectionDatabaseUUID = self.getUUID()

        if protocolVersion >= protocol.SEND_CONNID_TO_CLIENT:
            self.__connectionID = self.getInt()

        if protocolVersion >= protocol.SEND_EFFECTIVE_PLATFORM_VERSION_TO_CLIENT:
            self.__effectivePlatformVersion = self.getInt()

        if protocolVersion >= protocol.LAST_COMMIT_INFO:
            self.__connectedNodeID = self.getInt()
            self.__maxNodes = self.getInt()

        self.__sessionVersion = protocolVersion

        if cp:
            self._srp_handshake(params['user'], password, serverKey, salt, cp)

        self._set_timezone()



    def _srp_handshake(self, username, password, serverKey, salt, cp):
        # type: (str, str, str, str, ClientPassword) -> None
        """Authenticate the SRP session."""
        try:
            sessionKey = cp.computeSessionKey(username.upper(), password,
                                              salt, serverKey)
            # We always encrypt the authentication message
            self.setCiphers(RC4Cipher(sessionKey), RC4Cipher(sessionKey))
            self._putMessageId(protocol.AUTHENTICATION)
            self.putString(protocol.AUTH_TEST_STR)

            self._exchangeMessages()

            # If we want an un-encrypted session, disable the cipher now
            if not self.__encryption:
                self._setCiphers(NoCipher(), NoCipher())
        except SessionException as e:
            raise ProgrammingError('Failed to authenticate: ' + str(e))

    def get_version(self):
        # type: () -> int
        """Return the client protocol version."""
        return self.__sessionVersion

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

    def _set_timezone(self):
        """
        Query TE for TimeZone name. This is done because timezone abbreviations
        are allowed in TE but, not handled by ZoneInfo.  If TE gets a TimeZone=EST
        connection property,  it will set TimeZone system connection property to America/
        
        # type: () -> None
        """
        # Create a statement handle
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        handle = self.getInt()

        self._setup_statement(handle, protocol.EXECUTEQUERY)
        self.putString("select value from system.connectionproperties where property='TimeZone'")
        self._exchangeMessages()

        # returns: rsHandle, count, colname, result, fieldValue, r2
        res = [self.getInt(), self.getInt(), self.getString(),
               self.getInt(), self.getString(), self.getInt()]
        self.timezone_name=res[-2]
        self._putMessageId(protocol.CLOSESTATEMENT).putInt(handle)



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
        # type: () -> Statement
        """Create a statement and return a Statement object."""
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        return Statement(self.getInt())

    def execute_statement(self, statement, query):
        # type: (Statement, str) -> ExecutionResult
        """Execute a query using the given statement.

        :param statement: Statement to use for the query.
        :param query: Operation to be executed.
        :returns: The result of the operation execution.
        """
        self._setup_statement(statement.handle, protocol.EXECUTE).putString(query)
        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()

        if self.__sessionVersion >= protocol.TIMESTAMP_WITHOUT_TZ:
            tzChange = self.getBoolean()
            if tzChange:
                tzName = self.getString()
                self.timezone_name = tzName
        return ExecutionResult(statement, result, rowcount)

    def close_statement(self, statement):
        # type: (Statement) -> None
        """Close the statement."""
        self._putMessageId(protocol.CLOSESTATEMENT).putInt(statement.handle)
        self._exchangeMessages(False)

    def close_result_set(self, result_set):
        # type: (ResultSet) -> None
        """Close the result set."""
        self._putMessageId(protocol.CLOSERESULTSET).putInt(result_set.handle)
        self._exchangeMessages(False)

    def create_prepared_statement(self, query):
        # type: (str) -> PreparedStatement
        """Create a prepared statement for the given query."""
        self._putMessageId(protocol.PREPARE).putString(query)
        self._exchangeMessages()

        handle = self.getInt()
        param_count = self.getInt()

        return PreparedStatement(handle, param_count)

    def execute_prepared_statement(self, prepared_statement, parameters):
        # type: (PreparedStatement, Collection[Value]) -> ExecutionResult
        """Execute a prepared statement with the given parameters."""
        self._setup_statement(prepared_statement.handle, protocol.EXECUTEPREPAREDSTATEMENT)

        self.putInt(len(parameters))
        for param in parameters:
            self.putValue(param)

        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()

        if self.__sessionVersion >= protocol.TIMESTAMP_WITHOUT_TZ:
            tzChange = self.getBoolean()
            if tzChange:
                tzName = self.getString()
                self.timezone_name = tzName

        return ExecutionResult(prepared_statement, result, rowcount)

    def execute_batch_prepared_statement(self, prepared_statement, param_lists):
        # type: (PreparedStatement, Collection[Collection[Value]]) -> List[int]
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

        tzChange = self.getBoolean()
        if tzChange:
            tzName = self.getString()
            print(f"New TimeZone: {tzName}")

        # timezone
        # transid (getLong)
        # nodeid  (getInt)
        # commitsequence (getLong)

        return results

    def fetch_result_set(self, statement):
        # type: (Statement) -> ResultSet
        """Get the ResultSet from the previous operation."""
        self._putMessageId(protocol.GETRESULTSET).putInt(statement.handle)
        self._exchangeMessages()

        handle = self.getInt()
        colcount = self.getInt()

        # skip the header labels
        for _ in range(colcount):
            self.getString()

        complete = False
        init_results = []  # type: List[Row]

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

        return ResultSet(handle, colcount, init_results, complete)

    def fetch_result_set_next(self, result_set):
        # type: (ResultSet) -> None
        """Get more rows from this result set."""
        self._putMessageId(protocol.NEXT).putInt(result_set.handle)
        self._exchangeMessages()

        result_set.clear_results()

        while self._hasBytes(1):
            if self.getInt() == 0:
                result_set.complete = True
                break

            row = [None] * result_set.col_count
            for i in range(result_set.col_count):
                row[i] = self.getValue()

            result_set.add_row(tuple(row))

    def fetch_result_set_description(self, result_set):
        # type: (ResultSet) -> List[List[Any]]
        """Return the metadata for this result set."""
        self._putMessageId(protocol.GETMETADATA).putInt(result_set.handle)
        self._exchangeMessages()

        description = [list()] * self.getInt()  # type: List[List[Any]]
        for i in range(result_set.col_count):
            self.getString()    # catalog_name
            self.getString()    # schema_name
            self.getString()    # table_name
            column_name = self.getString()
            self.getString()    # column_label
            self.getValue()     # collation_sequence
            column_type_name = self.getString()
            self.getInt()       # column_type
            column_display_size = self.getInt()
            precision = self.getInt()
            scale = self.getInt()
            self.getInt()       # flags

            # TODO: type information should be derived from the type
            # (column_type) not the typename.
            description[i] = [column_name, TypeObjectFromNuodb(column_type_name),
                              column_display_size, None, precision, scale, None]

        return description

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
                data = toByteString(value)
            else:
                data = toSignedByteString(value)
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
        scale = abs(value.as_tuple()[2])
        data = toSignedByteString(int(value * decimal.Decimal(10**scale)))

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
            lengthStr = toByteString(length)
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
            lenData = toByteString(length)
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
        data = toSignedByteString(value)
        self.__output.append(protocol.MILLISECLEN0 + len(data))
        self.__output += data
        return self

    def putNsSinceEpoch(self, value):
        # type: (int) -> EncodedSession
        """Append the NsSinceEpoch value to the message.

        :type value: int
        """
        data = toSignedByteString(value)
        self.__output.append(protocol.NANOSECLEN0 + len(data))
        self.__output += data
        return self

    def putMsSinceMidnight(self, value):
        # type: (int) -> EncodedSession
        """Append the MsSinceMidnight value to the message.

        :type value: int
        """
        data = toByteString(value)
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
        lenData = toByteString(length)
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
        lenData = toByteString(length)
        self.__output.append(protocol.CLOBLEN0 + len(lenData))
        self.__output += lenData
        self.__output += value
        return self

    def _putScaled(self, base, ticks, scale):
        # type: (int, int, int) -> EncodedSession
        data = toSignedByteString(ticks)
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
                               *datatype.TimeToTicks(value,self.timezone_info))

    def putScaledTimestamp(self, value):
        # type: (datatype.Timestamp) -> EncodedSession
        """Append a Scaled Timestamp value to the message.

        :type value: datetime.datetime
        """
        return self._putScaled(protocol.SCALEDTIMESTAMPLEN0,
                               *datatype.TimestampToTicks(value,self.timezone_info))

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
        scale = abs(value.as_tuple()[2])
        sign = 1 if value.as_tuple()[0] == 0 else -1
        signData = toSignedByteString(sign)
        data = toByteString(int(abs(value) * decimal.Decimal(10**scale)))
        self.__output.append(protocol.SCALEDCOUNT2)
        self.__output += toByteString(scale)
        self.__output += signData
        self.__output.append(len(data))
        self.__output += data
        return self

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
            return fromSignedByteString(self._takeBytes(code - protocol.INTLEN0))

        raise DataError('Not an integer: %d' % (code))

    # Does not preserve E notation
    def getScaledInt(self):
        # type: () -> decimal.Decimal
        """Read the next Scaled Integer value off the session.

        :rtype: decimal.Decimal
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDLEN0 and code <= protocol.SCALEDLEN8:
            scale = fromByteString(self._takeBytes(1))
            value = fromSignedByteString(self._takeBytes(code - protocol.SCALEDLEN0))
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
            return arrayToStr(data)

        if code >= protocol.UTF8COUNT1 and code <= protocol.UTF8COUNT4:
            length = fromByteString(self._takeBytes(code - protocol.UTF8COUNT0))
            data = self._takeBytes(length)
            return arrayToStr(data)

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
            length = fromByteString(self._takeBytes(code - protocol.OPAQUECOUNT0))
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
            return fromSignedByteString(self._takeBytes(code - protocol.MILLISECLEN0))

        if code >= protocol.NANOSECLEN0 and code <= protocol.NANOSECLEN8:
            return fromSignedByteString(self._takeBytes(code - protocol.NANOSECLEN0))

        if code >= protocol.TIMELEN0 and code <= protocol.TIMELEN4:
            return fromByteString(self._takeBytes(code - protocol.TIMELEN0))

        raise DataError('Not a time')

    # Not currently used by Python driver
    def getBlob(self):
        # type: () -> datatype.Binary
        """Read the next Blob(Binary Large OBject) value off the session.

        :rtype: datatype.Binary
        """
        code = self._getTypeCode()

        if code >= protocol.BLOBLEN0 and code <= protocol.BLOBLEN4:
            length = fromByteString(self._takeBytes(code - protocol.BLOBLEN0))
            return datatype.Binary(self._takeBytes(length))

        raise DataError('Not a blob')

    def getClob(self):
        # type: () -> bytes
        """Read the next Clob(Character Large OBject) value off the session.

        :rtype: bytes
        """
        code = self._getTypeCode()

        if code >= protocol.CLOBLEN0 and code <= protocol.CLOBLEN4:
            length = fromByteString(self._takeBytes(code - protocol.CLOBLEN0))
            return self._takeBytes(length)

        raise DataError('Not a clob')

    def getScaledTime(self):
        # type: () -> datatype.Time
        """Read the next Scaled Time value off the session.

        :rtype: datetime.time
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDTIMELEN1 and code <= protocol.SCALEDTIMELEN8:
            scale = fromByteString(self._takeBytes(1))
            time = fromSignedByteString(self._takeBytes(code - protocol.SCALEDTIMELEN0))
            # ticks = decimal.Decimal(time) / decimal.Decimal(10**scale)
            # return datatype.TimeFromTicks(round(int(ticks)),
            #                               int((ticks % 1) * decimal.Decimal(1000000)))
            shiftr = 10**scale
            tick = time//shiftr
            if scale:
                micro = time%shiftr
                micro *= 10**(6-scale)
            return datatype.TimeFromTicks(tick,micro,self.timezone_info)

        raise DataError('Not a scaled time')

    def getScaledTimestamp(self):
        # type: () -> datatype.Timestamp
        """Read the next Scaled Timestamp value off the session.

        :rtype: datetime.datetime
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDTIMESTAMPLEN1 and code <= protocol.SCALEDTIMESTAMPLEN8:
            scale = fromByteString(self._takeBytes(1))
            stamp = fromSignedByteString(self._takeBytes(code - protocol.SCALEDTIMESTAMPLEN0))
            shiftr = 10**scale
            ticks = stamp//shiftr
            if scale:
                micro = stamp%shiftr
                micro *= 10**(6-scale)
            #print(f"getScaledTimestamp: stamp {stamp} to {ticks}.{micro}")
            return datatype.TimestampFromTicks(ticks,micro,self.timezone_info)

        raise DataError('Not a scaled timestamp')

    def getScaledDate(self):
        # type: () -> datatype.Date
        """Read the next Scaled Date value off the session.

        :rtype: datetime.date
        """
        code = self._getTypeCode()

        if code >= protocol.SCALEDDATELEN1 and code <= protocol.SCALEDDATELEN8:
            scale = fromByteString(self._takeBytes(1))
            date = fromSignedByteString(self._takeBytes(code - protocol.SCALEDDATELEN0))
            return datatype.DateFromTicks(date//(10**scale))

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

    def getScaledCount2(self):
        # type: () -> decimal.Decimal
        """Read a scaled and signed decimal from the session.

        :rtype: decimal.Decimal
        """
        code = self._getTypeCode()
        if code is protocol.SCALEDCOUNT2:
            scale = decimal.Decimal(fromByteString(self._takeBytes(1)))
            sign = fromSignedByteString(self._takeBytes(1))
            sign = 1 if sign < 0 else 0
            length = fromByteString(self._takeBytes(1))
            data = fromByteString(self._takeBytes(length))
            value = tuple(int(i) for i in str(abs(data)))
            scaledcount = decimal.Decimal((sign, value, -1 * int(scale)))
            return scaledcount

        raise DataError('Not a Scaled Count 2')

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

        if code == protocol.SCALEDCOUNT2:
            return self.getScaledCount2()

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
            self.__input = bytesToArray(resp)

            error = self.getInt()
            if error != 0:
                db_error_handler(error, self.getString())

    def setCiphers(self, cipherIn, cipherOut):
        # type: (BaseCipher, BaseCipher) -> None
        """Set the incoming and outgoing ciphers for the session.

        :type cipherIn: RC4Cipher , NoCipher
        :type cipherOut: RC4Cipher , NoCipher
        """
        Session._setCiphers(self, cipherIn, cipherOut)

    # Protected utility routines

    def _setup_statement(self, handle, msgId):
        # type: (int, int) -> EncodedSession
        """Set up a new statement.

        :type handle: int
        :type msgId: int
        """
        self._putMessageId(msgId)
        if self.__sessionVersion >= protocol.LAST_COMMIT_INFO:
            self.putInt(self.getCommitInfo(self.__connectedNodeID))
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

    def getCommitInfo(self, nodeID):  # pylint: disable=no-self-use
        # type: (int) -> int
        """Return the last commit info.  Does not support last commit."""
        return 0 * nodeID
