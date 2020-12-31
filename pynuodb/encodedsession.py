"""A module for housing the EncodedSession class.

(C) Copyright 2013-2020 NuoDB, Inc.  All Rights Reserved.

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

from .crypt import toByteString, fromByteString, toSignedByteString
from .crypt import fromSignedByteString, NoCipher
from .session import Session, SessionException
from . import protocol
from . import datatype
from .exception import DataError, EndOfStream, ProgrammingError
from .exception import db_error_handler, BatchError
from .datatype import TypeObjectFromNuodb
from .statement import Statement, PreparedStatement, ExecutionResult
from .result_set import ResultSet

systemVersion = sys.version[0]
REMOVE_FORMAT = 0


class EncodedSession(Session):
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
    getValue -- Determine the datatype of the next value off the session, then call the
                supporting function.
    exchangeMessages -- Exchange the pending message for an optional response from the server.
    setCiphers -- Re-sets the incoming and outgoing ciphers for the session.
    set_encryption -- Takes a value of type boolean. Setting encryption to False will result in disabling encryption after the handshake
    Private Functions:
    __init__ -- Constructor for the EncodedSession class.
    _peekTypeCode -- Looks at the next Type Code off the session. (Does not move inpos)
    _getTypeCode -- Read the next Type Code off the session.
    _takeBytes -- Gets the next length of bytes off the session.

    """

    def __init__(self, host, port, service='SQL2', options=None, **kwargs):
        """Constructor for the EncodedSession class."""
        super(EncodedSession, self).__init__(host, port=port, service=service, options=options, **kwargs)
        (remote_options, _) = self._split_options(options)
        self.doConnect(attributes=remote_options)

        self.__output = None
        """
        Output buffer to be sent to the server
        :type : str
        """

        self.__input = None
        """
        Input buffer recv from the server
        :type : str
        """

        self.__inpos = 0
        """
        Position in the input buffer
        :type : int
        """

        self.closed = False
        """
        Is connection closed
        :type : boolean
        """

        self.__encryption = True
        """
        Is connection encrypted
        :type : boolean
        """

        self.__connectionDatabaseUUID = 0
        """
        Connection Unique Universal ID
        :type : uuid.UUID
        """

        self.__sessionVersion = 0
        """
        Client protocol version for the session
        :type : int
        """

        self.__connectionID = 0
        """
        ID of current connection
        :type : int
        """

        self.__effectivePlatformVersion = 0
        """
        Database protocol version when the session is created (may change!)
        :type : int
        """

        self.__connectedNodeID = 0
        """
        ID of current Node
        :type : int
        """

        self.__maxNodes = 128
        """
        Maximum supported nodes
        :type : int
        """

    # Mostly for connections
    def open_database(self, db_name, parameters, cp):
        """
        :type db_name: str
        :type parameters: dict[str,str]
        :type cp: crypt.ClientPassword
        :rtype protocolVersion: int
        :rtype serverKey: str
        :rtype salt: str
        """

        (remote_options, _) = self._split_options(parameters)

        self._putMessageId(protocol.OPENDATABASE).putInt(protocol.CURRENT_PROTOCOL_VERSION).putString(db_name).putInt(len(remote_options))
        for (k, v) in remote_options.items():
            self.putString(k).putString(v)
        self.putNull().putString(cp.genClientKey())

        self._exchangeMessages()
        protocolVersion = self.getInt()
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

        return protocolVersion, serverKey, salt

    def open_database_on_secure_connection(self, db_name, parameters):
        """
        :type db_name: str
        :type parameters: dict[str,str]
        :rtype protocolVersion: int
        """
        if not self.tls_encrypted:
            raise RuntimeError("Sessions needs to be encrypted")

        (remote_options, _) = self._split_options(parameters)

        self._putMessageId(protocol.OPENDATABASE).putInt(protocol.CURRENT_PROTOCOL_VERSION).putString(db_name).putInt(len(remote_options))
        for (k, v) in remote_options.items():
            self.putString(k).putString(v)

        self._exchangeMessages()
        protocolVersion = self.getInt()

        self.__connectionDatabaseUUID = self.getUUID()

        if protocolVersion >= protocol.SEND_CONNID_TO_CLIENT:
            self.__connectionID = self.getInt()

        if protocolVersion >= protocol.SEND_EFFECTIVE_PLATFORM_VERSION_TO_CLIENT:
            self.__effectivePlatformVersion = self.getInt()

        if protocolVersion >= protocol.LAST_COMMIT_INFO:
            self.__connectedNodeID = self.getInt()
            self.__maxNodes = self.getInt()

        self.__sessionVersion = protocolVersion

        return protocolVersion

    def check_auth(self):
        try:
            self._putMessageId(protocol.AUTHENTICATION).putString(protocol.AUTH_TEST_STR)
            self._exchangeMessages()
            if self.__encryption is False:
                self._setCiphers(NoCipher(), NoCipher())
        except SessionException as e:
            raise ProgrammingError('Failed to authenticate: ' + str(e))

    def get_version(self):
        """
        :rtype sessionVersion: int
        """
        return self.__sessionVersion

    def get_auth_types(self):
        self._putMessageId(protocol.AUTHORIZETYPESREQUEST)
        self._exchangeMessages()
        val = self.getInt()

        return val

    def get_autocommit(self):
        self._putMessageId(protocol.GETAUTOCOMMIT)
        self._exchangeMessages()
        val = self.getValue()

        return val

    def set_autocommit(self, value):
        self._putMessageId(protocol.SETAUTOCOMMIT).putInt(value)
        self._exchangeMessages(False)

    def send_close(self):

        self._putMessageId(protocol.CLOSE)
        self._exchangeMessages()
        self.close()

    def send_commit(self):
        self._putMessageId(protocol.COMMITTRANSACTION)
        self._exchangeMessages()
        val = self.getValue()
        return val

    def send_rollback(self):
        self._putMessageId(protocol.ROLLBACKTRANSACTION)
        self._exchangeMessages()

    def set_encryption(self, value):
        self.__encryption = value

    def test_connection(self):
        # Create a statement handle
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        handle = self.getInt()

        # Use handle to query dual
        self._setup_statement(handle, protocol.EXECUTEQUERY).putString('select 1 as one from dual')
        self._exchangeMessages()

        rsHandle = self.getInt()
        count = self.getInt()
        colname = self.getString()
        result = self.getInt()
        fieldValue = self.getInt()
        r2 = self.getInt()

        if(rsHandle is None or count is None or colname is None or result is None or fieldValue is None or r2 is None):
            raise ProgrammingError('Failed to connect!')

    # Mostly for cursors
    def create_statement(self):
        """
        :rtype: Statement
        """
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        return Statement(self.getInt())

    def execute_statement(self, statement, query):
        """
        :type statement: Statement
        :type query: str
        :rtype: ExecutionResult
        """
        self._setup_statement(statement.handle, protocol.EXECUTE).putString(query)
        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()

        return ExecutionResult(statement, result, rowcount)

    def execute_sql_test(self, statement, query):
        """
        :type statement: Statement
        :type query: str
        :rtype: String
        """
        self._setup_statement(statement.handle, protocol.SQLTEST).putString(query)
        self._exchangeMessages()

        return self.getString()

    def close_statement(self, statement):
        """
        :type statement: Statement
        """
        self._putMessageId(protocol.CLOSESTATEMENT).putInt(statement.handle)
        self._exchangeMessages(False)

    def close_result_set(self, result_set):
        """
        :type result_set: ResultSet
        """
        self._putMessageId(protocol.CLOSERESULTSET).putInt(result_set.handle)
        self._exchangeMessages(False)

    def create_prepared_statement(self, query):
        """
        :type query: str
        :rtype: PreparedStatement
        """
        self._putMessageId(protocol.PREPARE).putString(query)
        self._exchangeMessages()

        handle = self.getInt()
        param_count = self.getInt()

        return PreparedStatement(handle, param_count)

    def execute_prepared_statement(self, prepared_statement, parameters):
        """
        :type prepared_statement: PreparedStatement
        :type parameters: list
        :rtype: ExecutionResult
        """
        self._setup_statement(prepared_statement.handle, protocol.EXECUTEPREPAREDSTATEMENT)

        self.putInt(len(parameters))
        for param in parameters:
            self.putValue(param)

        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()

        return ExecutionResult(prepared_statement, result, rowcount)

    def execute_batch_prepared_statement(self, prepared_statement, param_lists):
        """
        :type prepared_statement: PreparedStatement
        :type param_lists: list[list]

        """
        self._setup_statement(prepared_statement.handle, protocol.EXECUTEBATCHPREPAREDSTATEMENT)

        for parameters in param_lists:
            if prepared_statement.parameter_count != len(parameters):
                raise ProgrammingError("Incorrect number of parameters specified, expected %d, got %d" %
                                       (prepared_statement.parameter_count, len(parameters)))
            self.putInt(len(parameters))
            for param in parameters:
                self.putValue(param)
        self.putInt(-1)
        self.putInt(len(param_lists))
        self._exchangeMessages()

        results = []
        error_code = None
        error_string = None

        for _ in param_lists:
            result = self.getInt()
            results.append(result)
            if result == -3:
                ec = self.getInt()
                es = self.getString()
                # only report first
                if error_code is None:
                    error_code = ec
                    error_string = es

        if error_code is not None:
            raise BatchError(protocol.stringifyError[error_code] + ': ' + error_string, results)

        return results

    def fetch_result_set(self, statement):
        """
        :type statement: Statement
        :rtype: ResultSet
        """
        self._putMessageId(protocol.GETRESULTSET).putInt(statement.handle)
        self._exchangeMessages()

        handle = self.getInt()
        colcount = self.getInt()

        col_num_iter = range(colcount)
        for _ in col_num_iter:
            self.getString()

        complete = False
        init_results = []
        next_row = self.getInt()

        while next_row == 1:
            row = [None] * colcount
            for i in col_num_iter:
                row[i] = self.getValue()

            init_results.append(tuple(row))

            try:
                next_row = self.getInt()
            except EndOfStream:
                break

        # the first chunk might be all of the data
        if next_row == 0:
            complete = True

        return ResultSet(handle, colcount, init_results, complete)

    def fetch_result_set_next(self, result_set):
        """
        :type result_set: ResultSet
        """
        self._putMessageId(protocol.NEXT).putInt(result_set.handle)
        self._exchangeMessages()

        col_num_iter = range(result_set.col_count)

        result_set.clear_results()

        next_row = self.getInt()
        while next_row == 1:
            row = [None] * result_set.col_count
            for i in col_num_iter:
                row[i] = self.getValue()

            result_set.add_row(tuple(row))

            try:
                next_row = self.getInt()
            except EndOfStream:
                break

        if next_row == 0:
            result_set.complete = True

    def fetch_result_set_description(self, result_set):
        """
        :type result_set: ResultSet
        :rtype: ResultSetMetadata
        """
        self._putMessageId(protocol.GETMETADATA).putInt(result_set.handle)
        self._exchangeMessages()

        description = [None] * self.getInt()
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

            """TODO: type information should be derived from the type (column_type) not the
                     typename.  """
            description[i] = [column_name, TypeObjectFromNuodb(column_type_name),
                              column_display_size, None, precision, scale, None]

        return description

    #
    # Methods to put values into the next message

    def _putMessageId(self, messageId):
        """
        Start a message with the messageId.
        :type messageId: int
        """
        self.__output = ''
        self.putInt(messageId, isMessageId=True)
        return self

    def putInt(self, value, isMessageId=False):
        """
        Appends an Integer value to the message.
        :type value: int
        :type isMessageId: bool
        """
        if value < 32 and value > -11:
            packed = chr(protocol.INT0 + value)
        else:
            if isMessageId:
                valueStr = toByteString(value)
            else:
                valueStr = toSignedByteString(value)
            packed = chr(protocol.INTLEN1 - 1 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    # Does not preserve E notation
    def putScaledInt(self, value):
        """
        Appends a Scaled Integer value to the message.
        :type value: decimal.Decimal
        """
        # Convert the decimal's notation into decimal
        value += REMOVE_FORMAT
        scale = abs(value.as_tuple()[2])
        valueStr = toSignedByteString(int(value * decimal.Decimal(10**scale)))

        # If our length is more than 9 bytes we will need to send the data
        # using ScaledCount2
        if len(valueStr) > 8:
            return self.putScaledCount2(value)

        packed = chr(protocol.SCALEDLEN0 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self

    def putString(self, value):
        """
        Appends a String to the message.
        :type value: str
        """
        if systemVersion == '3' and not self.isASCII(value):
            value = value.encode('utf-8').decode('latin-1')
        length = len(value)
        if length < 40:
            packed = chr(protocol.UTF8LEN0 + length) + value
        else:
            lengthStr = toByteString(length)
            packed = chr(protocol.UTF8COUNT1 - 1 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putBoolean(self, value):
        """
        Appends a Boolean value to the message.
        :type value: bool
        """
        if value is True:
            self.__output += chr(protocol.TRUE)
        else:
            self.__output += chr(protocol.FALSE)
        return self

    def putNull(self):
        """Appends a Null value to the message."""
        self.__output += chr(protocol.NULL)
        return self

    def putUUID(self, value):
        """
        Appends a UUID to the message.
        :type value: uuid.UUID
        """
        self.__output += chr(protocol.UUID) + str(value)
        return self

    def putOpaque(self, value):
        """
        Appends an Opaque data value to the message.
        :type value: datatype.Binary
        """
        data = value.string
        length = len(data)
        if systemVersion == '3' and type(data) is bytes:
            data = data.decode('latin-1')
        if length < 40:
            packed = chr(protocol.OPAQUELEN0 + length) + data
        else:
            lengthStr = toByteString(length)
            packed = chr(protocol.OPAQUECOUNT1 - 1 + len(lengthStr)) + lengthStr + data
        self.__output += packed
        return self

    def putDouble(self, value):
        """
        Appends a Double to the message.
        :type value: decimal.Decimal
        """
        valueStr = struct.pack('!d', value)
        if systemVersion == '3':
            valueStr = valueStr.decode('latin-1')
        packed = chr(protocol.DOUBLELEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putMsSinceEpoch(self, value):
        """
        Appends the MsSinceEpoch value to the message.
        :type value: int
        """
        valueStr = toSignedByteString(value)
        packed = chr(protocol.MILLISECLEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putNsSinceEpoch(self, value):
        """
        Appends the NsSinceEpoch value to the message.
        :type value: int
        """
        valueStr = toSignedByteString(value)
        packed = chr(protocol.NANOSECLEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putMsSinceMidnight(self, value):
        """
        Appends the MsSinceMidnight value to the message.
        :type value: int
        """
        valueStr = toByteString(value)
        packed = chr(protocol.TIMELEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    # Not currently used by Python driver
    def putBlob(self, value):
        """
        Appends the Blob(Binary Large OBject) value to the message.
        :type value: datatype.Binary
        """
        data = value.string
        length = len(data)
        lengthStr = toByteString(length)
        lenlengthstr = len(lengthStr)
        packed = chr(protocol.BLOBLEN0 + lenlengthstr) + lengthStr + data
        self.__output += packed
        return self

    def putClob(self, value):
        """
        Appends the Clob(Character Large OBject) value to the message.
        :type value: datatype.Binary
        """
        length = len(value)
        lengthStr = toByteString(length)
        packed = chr(protocol.CLOBLEN0 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putScaledTime(self, value):
        """
        Appends a Scaled Time value to the message.
        :type value: datetime.time
        """
        (ticks, scale) = datatype.TimeToTicks(value)
        valueStr = toSignedByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDTIMELEN1) + chr(0) + chr(0)
        else:
            packed = chr(protocol.SCALEDTIMELEN1 - 1 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self

    def putScaledTimestamp(self, value):
        """
        Appends a Scaled Timestamp value to the message.
        :type value: datetime.datetime
        """
        (ticks, scale) = datatype.TimestampToTicks(value)
        valueStr = toSignedByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDTIMESTAMPLEN1) + chr(0) + chr(0)
        else:
            packed = chr(protocol.SCALEDTIMESTAMPLEN1 - 1 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self

    def putScaledDate(self, value):
        """
        Appends a Scaled Date value to the message.
        :type value: datatime.date
        """
        ticks = datatype.DateToTicks(value)
        valueStr = toSignedByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDDATELEN1) + chr(0) + chr(0)
        else:
            packed = chr(protocol.SCALEDDATELEN1 - 1 + len(valueStr)) + chr(0) + valueStr
        self.__output += packed
        return self

    def putScaledCount2(self, value):
        """
        Appends a scaled and signed decimal to the message
        :type value: decimal.Decimal
        """
        scale = abs(value.as_tuple()[2])
        sign = 1 if value.as_tuple()[0] == 0 else -1
        sign = toSignedByteString(sign)
        value = toByteString(int(abs(value) * decimal.Decimal(10**scale)))
        packed = chr(protocol.SCALEDCOUNT2) + toByteString(scale) + sign + chr(len(value)) + value
        self.__output += packed
        return self

    def putValue(self, value):
        """Determines the probable type of the value and calls the supporting function."""
        if value is None:
            return self.putNull()
        elif type(value) == int:
            return self.putInt(value)
        elif type(value) == float:
            return self.putDouble(value)
        elif isinstance(value, decimal.Decimal):
            return self.putScaledInt(value)
        elif isinstance(value, datatype.Timestamp):
            # Note: Timestamp must be above Date because it inherits from Date
            return self.putScaledTimestamp(value)
        elif isinstance(value, datatype.Date):
            return self.putScaledDate(value)
        elif isinstance(value, datatype.Time):
            return self.putScaledTime(value)
        elif isinstance(value, datatype.Binary):
            return self.putOpaque(value)
        elif value is True or value is False:
            return self.putBoolean(value)
        else:
            return self.putString(str(value))

    #
    # Methods to get values out of the last exchange

    def getInt(self):
        """
        Read the next Integer value off the session.
        :rtype: int
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.INTMINUS10, protocol.INT31 + 1):
            return typeCode - 20

        elif typeCode in range(protocol.INTLEN1, protocol.INTLEN8 + 1):
            return fromSignedByteString(self._takeBytes(typeCode - 51))

        raise DataError('Not an integer')

    # Does not preserve E notation
    def getScaledInt(self):
        """
        Read the next Scaled Integer value off the session.
        :rtype: decimal.Decimal
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDLEN0, protocol.SCALEDLEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            value = fromSignedByteString(self._takeBytes(typeCode - 60))
            # preserves Decimal sign, exp, int...
            sign = 1 if value < 0 else 0
            value = tuple(int(i) for i in str(abs(value)))
            return decimal.Decimal((sign, value, -1 * scale))

        raise DataError('Not a scaled integer')

    def getString(self):
        """
        Read the next String off the session.
        :rtype: str
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.UTF8LEN0, protocol.UTF8LEN39 + 1):
            value = self._takeBytes(typeCode - 109)
            if systemVersion == '3' and not self.isASCII(value):
                value = value.encode('latin-1').decode('utf-8')
            return value

        if typeCode in range(protocol.UTF8COUNT1, protocol.UTF8COUNT4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 68))
            value = self._takeBytes(strLength)
            if systemVersion == '3' and not self.isASCII(value):
                value = value.encode('latin-1').decode('utf-8')
            return value

        raise DataError('Not a string')

    def getBoolean(self):
        """
        Read the next Boolean value off the session.
        :rtype: boolean
        """
        typeCode = self._getTypeCode()

        if typeCode == protocol.TRUE:
            return True
        if typeCode == protocol.FALSE:
            return False

        raise DataError('Not a boolean')

    def getNull(self):
        """
        Read the next Null value off the session.
        :rtype: None
        """
        if self._getTypeCode() != protocol.NULL:
            raise DataError('Not null')

    def getDouble(self):
        """
        Read the next Double off the session.
        :rtype: float
        """
        typeCode = self._getTypeCode()

        if typeCode == protocol.DOUBLELEN0:
            return 0.0

        if typeCode in range(protocol.DOUBLELEN0 + 1, protocol.DOUBLELEN8 + 1):
            test = self._takeBytes(typeCode - 77)
            if typeCode < protocol.DOUBLELEN8:
                for i in range(0, protocol.DOUBLELEN8 - typeCode):
                    test = test + chr(0)
            if systemVersion == '3':
                # Python 3 returns an array: we want the 0th element and
                # remove form
                return struct.unpack('!d', bytes(test, 'latin-1'))[0] + REMOVE_FORMAT
            return struct.unpack('!d', test)[0]

        raise DataError('Not a double')

    def getTime(self):
        """
        Read the next Time value off the session.
        :rtype: int
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.MILLISECLEN0, protocol.MILLISECLEN8 + 1):
            return fromSignedByteString(self._takeBytes(typeCode - 86))

        if typeCode in range(protocol.NANOSECLEN0, protocol.NANOSECLEN8 + 1):
            return fromSignedByteString(self._takeBytes(typeCode - 95))

        if typeCode in range(protocol.TIMELEN0, protocol.TIMELEN4 + 1):
            return fromByteString(self._takeBytes(typeCode - 104))

        raise DataError('Not a time')

    def getOpaque(self):
        """
        Read the next Opaque value off the session.
        :rtype: datatype.Binary
        """

        typeCode = self._getTypeCode()

        if typeCode in range(protocol.OPAQUELEN0, protocol.OPAQUELEN39 + 1):
            value = self._takeBytes(typeCode - 149)
            return datatype.Binary(value)
        if typeCode in range(protocol.OPAQUECOUNT1, protocol.OPAQUECOUNT4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 72))
            value = self._takeBytes(strLength)
            if systemVersion == '3':
                value = value.encode('latin-1')
            return datatype.Binary(value)

        raise DataError('Not an opaque value')

    # Not currently used by Python driver
    def getBlob(self):
        """
        Read the next Blob(Binary Large OBject) value off the session.
        :rtype: datatype.Binary
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.BLOBLEN0, protocol.BLOBLEN4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 189))
            return datatype.Binary(self._takeBytes(strLength))

        raise DataError('Not a blob')

    def getClob(self):
        """
        Read the next Clob(Character Large OBject) value off the session.
        :rtype: bytes
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.CLOBLEN0, protocol.CLOBLEN4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 194))
            return self._takeBytes(strLength)

        raise DataError('Not a clob')

    def getScaledTime(self):
        """
        Read the next Scaled Time value off the session.
        :rtype: datetime.time
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDTIMELEN1, protocol.SCALEDTIMELEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            time = fromSignedByteString(self._takeBytes(typeCode - 208))
            ticks = decimal.Decimal(str(time)) / decimal.Decimal(10**scale)
            return datatype.TimeFromTicks(round(int(ticks)), int((ticks % 1) * decimal.Decimal(1000000)))

        raise DataError('Not a scaled time')

    def getScaledTimestamp(self):
        """
        Read the next Scaled Timestamp value off the session.
        :rtype: datetime.datetime
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDTIMESTAMPLEN1, protocol.SCALEDTIMESTAMPLEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            timestamp = fromSignedByteString(self._takeBytes(typeCode - 216))
            ticks = decimal.Decimal(str(timestamp)) / decimal.Decimal(10**scale)
            return datatype.TimestampFromTicks(round(int(ticks)), int((ticks % 1) * decimal.Decimal(1000000)))

        raise DataError('Not a scaled timestamp')

    def getScaledDate(self):
        """
        Read the next Scaled Date value off the session.
        :rtype: datetime.date
        """
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDDATELEN1, protocol.SCALEDDATELEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            date = fromSignedByteString(self._takeBytes(typeCode - 200))
            return datatype.DateFromTicks(round(date / 10.0 ** scale))

        raise DataError('Not a scaled date')

    def getUUID(self):
        """
        Read the next UUID value off the session.
        :rtype uuid.UUID
        """
        if self._getTypeCode() == protocol.UUID:
            byteString = self._takeBytes(16)
            if systemVersion == '3':
                byteString = byteString.encode('latin-1')
            return uuid.UUID(bytes=byteString)
        if self._getTypeCode() == protocol.SCALEDCOUNT1:
            # before version 11
            pass

        raise DataError('Not a UUID')

    def getScaledCount2(self):
        """
        Read a scaled and signed decimal from the session
        :rtype: decimal.Decimal
        """
        typeCode = self._getTypeCode()
        if typeCode is protocol.SCALEDCOUNT2:
            scale = decimal.Decimal(fromByteString(self._takeBytes(1)))
            sign = fromSignedByteString(self._takeBytes(1))
            sign = 1 if sign < 0 else 0
            length = fromByteString(self._takeBytes(1))
            value = fromByteString(self._takeBytes(length))
            value = tuple(int(i) for i in str(abs(value)))
            scaledcount = decimal.Decimal((sign, value, -1 * int(scale)))
            return scaledcount

        raise DataError('Not a Scaled Count 2')

    def getValue(self):
        """Determine the datatype of the next value off the session, then call
        the supporting function.
        """
        typeCode = self._peekTypeCode()

        # get string type
        if typeCode in range(protocol.UTF8COUNT1, protocol.UTF8COUNT4 + 1) or \
           typeCode in range(protocol.UTF8LEN0, protocol.UTF8LEN39 + 1):
            return self.getString()

        # get integer type
        if typeCode in range(protocol.INTMINUS10, protocol.INTLEN8 + 1):
            return self.getInt()

        # get double precision type
        if typeCode in range(protocol.DOUBLELEN0, protocol.DOUBLELEN8 + 1):
            return self.getDouble()

        # get boolean type
        if typeCode in [protocol.TRUE, protocol.FALSE]:
            return self.getBoolean()

        # get uuid type
        if typeCode is [protocol.UUID, protocol.SCALEDCOUNT1]:
            return self.getUUID()

        # get Scaled Count 2 type
        if typeCode is protocol.SCALEDCOUNT2:
            return self.getScaledCount2()

        # get scaled int type
        if typeCode in range(protocol.SCALEDLEN0, protocol.SCALEDLEN8 + 1):
            return self.getScaledInt()

        # get opaque type
        if typeCode in range(protocol.OPAQUECOUNT1, protocol.OPAQUECOUNT4 + 1) or \
           typeCode in range(protocol.OPAQUELEN0, protocol.OPAQUELEN39 + 1):
            return self.getOpaque()

        # get blob/clob type
        if typeCode in range(protocol.BLOBLEN0, protocol.CLOBLEN4 + 1):
            return self.getBlob()

        # get time type
        if typeCode in range(protocol.MILLISECLEN0, protocol.TIMELEN4 + 1):
            return self.getTime()

        # get scaled time
        if typeCode in range(protocol.SCALEDTIMELEN1, protocol.SCALEDTIMELEN8 + 1):
            return self.getScaledTime()

        # get scaled timestamp
        if typeCode in range(protocol.SCALEDTIMESTAMPLEN1, protocol.SCALEDTIMESTAMPLEN8 + 1):
            return self.getScaledTimestamp()

        # get scaled date
        if typeCode in range(protocol.SCALEDDATELEN1, protocol.SCALEDDATELEN8 + 1):
            return self.getScaledDate()

        # get null type
        if typeCode is protocol.NULL:
            return self.getNull()

        raise NotImplementedError("not implemented")

    def _exchangeMessages(self, getResponse=True):
        """Exchange the pending message for an optional response from the server."""
        try:
            self.send(self.__output)
        finally:
            self.__output = None

        if getResponse is True:
            self.__input = self.recv(False)
            self.__inpos = 0

            if self.__input is None:
                db_error_handler(protocol.OPERATION_TIMEOUT, "timed out")

            error = self.getInt()

            if error != 0:
                db_error_handler(error, self.getString())
        else:
            self.__input = None
            self.__inpos = 0

    def setCiphers(self, cipherIn, cipherOut):
        """
        Re-sets the incoming and outgoing ciphers for the session.
        :type cipherIn: RC4Cipher , NoCipher
        :type cipherOut: RC4Cipher , NoCipher
        """
        Session._setCiphers(self, cipherIn, cipherOut)

    # Protected utility routines

    def _setup_statement(self, handle, msgId):
        """
        :type handle: int
        :type msgId: int
        """
        self._putMessageId(msgId)
        if self.__sessionVersion >= protocol.LAST_COMMIT_INFO:
            self.putInt(self.getCommitInfo(self.__connectedNodeID))
        self.putInt(handle)

        return self

    def _peekTypeCode(self):
        """Looks at the next Type Code off the session. (Does not move inpos)"""
        return ord(self.__input[self.__inpos])

    def _getTypeCode(self):
        """Read the next Type Code off the session."""
        if self.__inpos >= len(self.__input):
            raise EndOfStream('end of stream reached')

        try:
            return ord(self.__input[self.__inpos])
        finally:
            self.__inpos += 1

    def _takeBytes(self, length):
        """
        Gets the next length of bytes off the session.
        :type length: int
        :rtype: bytes
        """
        if self.__inpos + length > len(self.__input):
            raise EndOfStream('end of stream reached')

        try:
            return self.__input[self.__inpos:self.__inpos + length]
        finally:
            self.__inpos += length

    def isASCII(self, data):
        """
        Trys to encode the given string in ascii, if it fails then
        we know the string is unicode
        :type data: str
        :rtype: boolean
        """
        try:
            data.encode('ascii')
        except UnicodeEncodeError:
            return False
        else:
            return True

    def getCommitInfo(self, nodeID):
        """ Currently does not support last commit """

        return 0 * nodeID
