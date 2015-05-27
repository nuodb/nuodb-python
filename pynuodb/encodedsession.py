"""A module for housing the EncodedSession class.

Exported Classes:
EncodedSession -- Class for representing an encoded session with the database.
"""

__all__  = [ 'EncodedSession' ]

from crypt import toByteString, fromByteString, toSignedByteString, fromSignedByteString
from session import Session, SessionException

import uuid
import struct
import protocol
import datatype
import decimal
import sys

from exception import DataError, EndOfStream, ProgrammingError, db_error_handler, BatchError
from datatype import TypeObjectFromNuodb

from statement import Statement, PreparedStatement, ExecutionResult
from result_set import ResultSet

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
    
    Private Functions:
    __init__ -- Constructor for the EncodedSession class.
    _peekTypeCode -- Looks at the next Type Code off the session. (Does not move inpos)
    _getTypeCode -- Read the next Type Code off the session.
    _takeBytes -- Gets the next length of bytes off the session.
 
    """

    def __init__(self, host, port, service='SQL2'):
        """Constructor for the EncodedSession class."""
        Session.__init__(self, host, port=port, service=service)
        self.doConnect()

        self.__output = None
        """ @type : str """
        self.__input = None
        """ @type : str """
        self.__inpos = 0
        """ @type : int """
        self.closed = False

    # Mostly for connections
    def open_database(self, db_name, parameters, cp):
        """
        @type db_name str
        @type parameters dict[str,str]
        @type cp crypt.ClientPassword
        """
        self._putMessageId(protocol.OPENDATABASE).putInt(protocol.CURRENT_PROTOCOL_VERSION).putString(db_name).putInt(len(parameters))
        for (k, v) in parameters.items():
            self.putString(k).putString(v)
        self.putNull().putString(cp.genClientKey())

        self._exchangeMessages()

        version = self.getInt()
        serverKey = self.getString()
        salt = self.getString()

        return version, serverKey, salt

    def check_auth(self):
        try:
            self._putMessageId(protocol.AUTHENTICATION).putString(protocol.AUTH_TEST_STR)
            self._exchangeMessages()
        except SessionException as e:
            raise ProgrammingError('Failed to authenticate: ' + str(e)), None, sys.exc_info()[2]


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

    def test_connection(self):
        # Create a statement handle
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        handle = self.getInt()

        # Use handle to query dual
        self._putMessageId(protocol.EXECUTEQUERY).putInt(handle).putString('select 1 as one from dual')
        self._exchangeMessages()

        rsHandle = self.getInt()
        count = self.getInt()
        colname = self.getString()
        result = self.getInt()
        fieldValue = self.getInt()
        r2 = self.getInt()

    # Mostly for cursors
    def create_statement(self):
        """
        @rtype: Statement
        """
        self._putMessageId(protocol.CREATE)
        self._exchangeMessages()
        return Statement(self.getInt())

    def execute_statement(self, statement, query):
        """
        @type statement Statement
        @type query str
        @rtype: ExecutionResult
        """
        self._putMessageId(protocol.EXECUTE).putInt(statement.handle).putString(query)
        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()

        return ExecutionResult(statement, result, rowcount)

    def close_statement(self, statement):
        """
        @type statement Statement
        """
        self._putMessageId(protocol.CLOSESTATMENT).putInt(statement.handle)
        self._exchangeMessages(False)

    def create_prepared_statement(self, query):
        """
        @type query str
        @rtype: PreparedStatement
        """
        self._putMessageId(protocol.PREPARE).putString(query)
        self._exchangeMessages()

        handle = self.getInt()
        param_count = self.getInt()

        return PreparedStatement(handle, param_count)

    def execute_prepared_statement(self, prepared_statement, parameters):
        """
        @type prepared_statement PreparedStatement
        @type parameters list
        @rtype: ExecutionResult
        """
        self._putMessageId(protocol.EXECUTEPREPAREDSTATEMENT)
        self.putInt(prepared_statement.handle).putInt(len(parameters))

        for param in parameters:
            self.putValue(param)

        self._exchangeMessages()

        result = self.getInt()
        rowcount = self.getInt()

        return ExecutionResult(prepared_statement, result, rowcount)

    def execute_batch_prepared_statement(self, prepared_statement, param_lists):
        """
        @type prepared_statement PreparedStatement
        @type param_lists list[list]

        """
        self._putMessageId(protocol.EXECUTEBATCHPREPAREDSTATEMENT)
        self.putInt(prepared_statement.handle)
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
        @type statement Statement
        @rtype: ResultSet
        """
        self._putMessageId(protocol.GETRESULTSET).putInt(statement.handle)
        self._exchangeMessages()

        handle = self.getInt()
        colcount = self.getInt()

        col_num_iter = xrange(colcount)
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
        @type result_set ResultSet
        """
        self._putMessageId(protocol.NEXT).putInt(result_set.handle)
        self._exchangeMessages()

        col_num_iter = xrange(result_set.col_count)

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
        @type result_set ResultSet
        @rtype: ResultSetMetadata
        """
        self._putMessageId(protocol.GETMETADATA).putInt(result_set.handle)
        self._exchangeMessages()

        description = [None] * self.getInt()
        for i in xrange(result_set.col_count):
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
        @type messageId int
        """
        self.__output = ''
        self.putInt(messageId, isMessageId=True)
        return self

    def putInt(self, value, isMessageId=False):
        """
        Appends an Integer value to the message.
        @type value int
        @type isMessageId bool
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

    def putScaledInt(self, value):
        """
        Appends a Scaled Integer value to the message.
        @type value decimal.Decimal
        """
        scale = abs(value.as_tuple()[2])
        valueStr = toSignedByteString(int(value * decimal.Decimal(10**scale)))
        packed = chr(protocol.SCALEDLEN0 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self

    def putString(self, value):
        """
        Appends a String to the message.
        @type value str
        """
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
        @type value bool
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
        """Appends a UUID to the message."""
        self.__output += chr(protocol.UUID) + str(value)
        return self

    def putOpaque(self, value):
        """Appends an Opaque data value to the message."""
        data = value.string
        length = len(data)
        if length < 40:
            packed = chr(protocol.OPAQUELEN0 + length) + data
        else:
            lengthStr = toByteString(length)
            packed = chr(protocol.OPAQUECOUNT1 - 1 + len(lengthStr)) + lengthStr + data
        self.__output += packed
        return self

    def putDouble(self, value):
        """Appends a Double to the message."""
        valueStr = struct.pack('!d', value)
        packed = chr(protocol.DOUBLELEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putMsSinceEpoch(self, value):
        """Appends the MsSinceEpoch value to the message."""
        valueStr = toSignedByteString(value)
        packed = chr(protocol.MILLISECLEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self
        
    def putNsSinceEpoch(self, value):
        """Appends the NsSinceEpoch value to the message."""
        valueStr = toSignedByteString(value)
        packed = chr(protocol.NANOSECLEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self
        
    def putMsSinceMidnight(self, value):
        """Appends the MsSinceMidnight value to the message."""
        valueStr = toByteString(value)
        packed = chr(protocol.TIMELEN0 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    # Not currently used by NuoDB
    def putBlob(self, value):
        """Appends the Blob(Binary Large OBject) value to the message."""
        data = value.string
        length = len(data)
        lengthStr = toByteString(length)
        lenlengthstr = len(lengthStr)
        packed = chr(protocol.BLOBLEN0 + lenlengthstr) + lengthStr + data
        self.__output += packed
        return self

    def putClob(self, value):
        """Appends the Clob(Character Large OBject) value to the message."""
        length = len(value)
        lengthStr = toByteString(length)
        packed = chr(protocol.CLOBLEN0 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self
        
    def putScaledTime(self, value):
        """Appends a Scaled Time value to the message."""
        (ticks, scale) = datatype.TimeToTicks(value)
        valueStr = toByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDTIMELEN1) + chr(0) + chr(0)
        else:
            packed = chr(protocol.SCALEDTIMELEN1 - 1 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self
    
    def putScaledTimestamp(self, value):
        """Appends a Scaled Timestamp value to the message."""
        (ticks, scale) = datatype.TimestampToTicks(value)
        valueStr = toSignedByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDTIMESTAMPLEN1) + chr(0) + chr(0)
        else:
            packed = chr(protocol.SCALEDTIMESTAMPLEN1 - 1 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self
        
    def putScaledDate(self, value):
        """Appends a Scaled Date value to the message."""
        ticks = datatype.DateToTicks(value)
        valueStr = toSignedByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDDATELEN1) + chr(0) + chr(0)
        else:  
            packed = chr(protocol.SCALEDDATELEN1 - 1 + len(valueStr)) + chr(0) + valueStr
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
        elif isinstance(value, datatype.Timestamp): #Note: Timestamp must be above Date because it inherits from Date
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
        """Read the next Integer value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.INTMINUS10, protocol.INT31 + 1):
            return typeCode - 20

        elif typeCode in range(protocol.INTLEN1, protocol.INTLEN8 + 1):
            return fromSignedByteString(self._takeBytes(typeCode - 51))

        raise DataError('Not an integer')

    def getScaledInt(self):
        """Read the next Scaled Integer value off the session."""
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
        """Read the next String off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.UTF8LEN0, protocol.UTF8LEN39 + 1):
            return self._takeBytes(typeCode - 109)

        if typeCode in range(protocol.UTF8COUNT1, protocol.UTF8COUNT4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 68))
            return self._takeBytes(strLength)

        raise DataError('Not a string')

    def getBoolean(self):
        """Read the next Boolean value off the session."""
        typeCode = self._getTypeCode()

        if typeCode == protocol.TRUE:
            return True
        if typeCode == protocol.FALSE:
            return False

        raise DataError('Not a boolean')

    def getNull(self):
        """Read the next Null value off the session."""
        if self._getTypeCode() != protocol.NULL:
            raise DataError('Not null')

    def getDouble(self):
        """Read the next Double off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode == protocol.DOUBLELEN0:
            return 0.0
        
        if typeCode in range(protocol.DOUBLELEN0 + 1, protocol.DOUBLELEN8 + 1):
            test = self._takeBytes(typeCode - 77)
            if typeCode < protocol.DOUBLELEN8:
                for i in xrange(0, protocol.DOUBLELEN8 - typeCode):
                    test = test + chr(0)
            return struct.unpack('!d', test)[0]
            
        raise DataError('Not a double')

    def getTime(self):
        """Read the next Time value off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode in range(protocol.MILLISECLEN0, protocol.MILLISECLEN8 + 1):
            return fromSignedByteString(self._takeBytes(typeCode - 86))
            
        if typeCode in range(protocol.NANOSECLEN0, protocol.NANOSECLEN8 + 1):
            return fromSignedByteString(self._takeBytes(typeCode - 95))
            
        if typeCode in range(protocol.TIMELEN0, protocol.TIMELEN4 + 1):
            return fromByteString(self._takeBytes(typeCode - 104))
            
        raise DataError('Not a time')
    
    def getOpaque(self):
        """Read the next Opaque value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.OPAQUELEN0, protocol.OPAQUELEN39 + 1):
            return datatype.Binary(self._takeBytes(typeCode - 149))

        if typeCode in range(protocol.OPAQUECOUNT1, protocol.OPAQUECOUNT4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 72))
            return datatype.Binary(self._takeBytes(strLength))

        raise DataError('Not an opaque value')

    # Not currently used by NuoDB
    def getBlob(self):
        """Read the next Blob(Binary Large OBject) value off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode in range(protocol.BLOBLEN0, protocol.BLOBLEN4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 189))
            return datatype.Binary(self._takeBytes(strLength))

        raise DataError('Not a blob')
    
    def getClob(self):
        """Read the next Clob(Character Large OBject) value off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode in range(protocol.CLOBLEN0, protocol.CLOBLEN4 + 1):
            strLength = fromByteString(self._takeBytes(typeCode - 194))
            return self._takeBytes(strLength)

        raise DataError('Not a clob')
    
    def getScaledTime(self):
        """Read the next Scaled Time value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDTIMELEN1, protocol.SCALEDTIMELEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            time = fromByteString(self._takeBytes(typeCode - 208))
            ticks = decimal.Decimal(str(time)) / decimal.Decimal(10**scale)
            return datatype.TimeFromTicks(round(int(ticks)), int((ticks % 1) * decimal.Decimal(1000000)))

        raise DataError('Not a scaled time')
    
    def getScaledTimestamp(self):
        """Read the next Scaled Timestamp value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDTIMESTAMPLEN1, protocol.SCALEDTIMESTAMPLEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            timestamp = fromSignedByteString(self._takeBytes(typeCode - 216))
            ticks = decimal.Decimal(str(timestamp)) / decimal.Decimal(10**scale)
            return datatype.TimestampFromTicks(round(int(ticks)), int((ticks % 1) * decimal.Decimal(1000000)))

        raise DataError('Not a scaled timestamp')
    
    def getScaledDate(self):
        """Read the next Scaled Date value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDDATELEN1, protocol.SCALEDDATELEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            date = fromSignedByteString(self._takeBytes(typeCode - 200))
            return datatype.DateFromTicks(round(date/10.0**scale))

        raise DataError('Not a scaled date')

    def getUUID(self):
        """Read the next UUID value off the session."""
        if self._getTypeCode() == protocol.UUID:
            return uuid.UUID(bytes=self._takeBytes(16))
        if self._getTypeCode() == protocol.SCALEDCOUNT1:
            # before version 11
            pass
        if self._getTypeCode() == protocol.SCALEDCOUNT2:
            # version 11 and later
            pass

        raise DataError('Not a UUID')

    def getValue(self):
        """Determine the datatype of the next value off the session, then call the
        supporting function.
        """
        typeCode = self._peekTypeCode()
        

        # get string type
        if typeCode in range(protocol.UTF8COUNT1, protocol.UTF8COUNT4 + 1) or \
             typeCode in range(protocol.UTF8LEN0, protocol.UTF8LEN39 + 1):
            return self.getString()

        # get integer type
        elif typeCode in range(protocol.INTMINUS10, protocol.INTLEN8 + 1):
            return self.getInt()

        # get double precision type
        elif typeCode in range(protocol.DOUBLELEN0, protocol.DOUBLELEN8 + 1):
            return self.getDouble()
        
        # get boolean type
        elif typeCode in [protocol.TRUE, protocol.FALSE]:
            return self.getBoolean()
        
        # get uuid type
        elif typeCode in [protocol.UUID, protocol.SCALEDCOUNT1, protocol.SCALEDCOUNT2]:
            return self.getUUID()
        
        # get scaled int type
        elif typeCode in range(protocol.SCALEDLEN0, protocol.SCALEDLEN8 + 1):
            return self.getScaledInt()
        
        # get opaque type
        elif typeCode in range(protocol.OPAQUECOUNT1, protocol.OPAQUECOUNT4 + 1) or \
             typeCode in range(protocol.OPAQUELEN0, protocol.OPAQUELEN39 + 1):
            return self.getOpaque()
        
        # get blob/clob type
        elif typeCode in range(protocol.BLOBLEN0, protocol.CLOBLEN4 + 1):
            return self.getBlob()
        
        # get time type
        elif typeCode in range(protocol.MILLISECLEN0, protocol.TIMELEN4 + 1):
            return self.getTime()
        
        # get scaled time
        elif typeCode in range(protocol.SCALEDTIMELEN1, protocol.SCALEDTIMELEN8 + 1):
            return self.getScaledTime()
        
        # get scaled timestamp
        elif typeCode in range(protocol.SCALEDTIMESTAMPLEN1, protocol.SCALEDTIMESTAMPLEN8 + 1):
            return self.getScaledTimestamp()
        
        # get scaled date
        elif typeCode in range(protocol.SCALEDDATELEN1, protocol.SCALEDDATELEN8 + 1):
            return self.getScaledDate()
        
        # get null type
        elif typeCode is protocol.NULL:
            return self.getNull()

        else:
            raise NotImplementedError("not implemented")

    def _exchangeMessages(self, getResponse=True):
        """Exchange the pending message for an optional response from the server."""
        try:
            # print "message to server: %s" %  (self.__output)
            self.send(self.__output)
        finally:
            self.__output = None

        if getResponse is True:
            self.__input = self.recv(False)
            self.__inpos = 0
            
            error = self.getInt()

            if error != 0:
                db_error_handler(error, self.getString())
        else:
            self.__input = None
            self.__inpos = 0

    def setCiphers(self, cipherIn, cipherOut):
        """Re-sets the incoming and outgoing ciphers for the session."""
        Session._setCiphers(self, cipherIn, cipherOut)

    # Protected utility routines

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
        """Gets the next length of bytes off the session."""
        if self.__inpos + length > len(self.__input):
            raise EndOfStream('end of stream reached')
                        
        try:
            return self.__input[self.__inpos:self.__inpos + length]
        finally:
            self.__inpos += length
