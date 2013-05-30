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
import time
import decimal
from exception import DataError, DatabaseError, EndOfStream

# from nuodb.util import getCloudEntry
# (host, port) = getCloudEntry(broker, dbName, connectionKeys)

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
        self.__input = None
        self.__inpos = 0
        self.closed = False

    #
    # Methods to put values into the next message

    def putMessageId(self, messageId):
        """Start a message with the messageId."""
        if self.__output != None:
            raise SessionException('no')
        self.__output = ''
        self.putInt(messageId, isMessageId = True)
        return self

    def putInt(self, value, isMessageId = False):
        """Appends an Integer value to the message."""
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
        """Appends a Scaled Integer value to the message."""
        scale = abs(value.as_tuple()[2])
        valueStr = toSignedByteString(int(value * decimal.Decimal(10**scale)))
        packed = chr(protocol.SCALEDLEN0 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self

    def putString(self, value):
        """Appends a String to the message."""
        length = len(value)
        if length < 40:
            packed = chr(protocol.UTF8LEN0 + length) + value
        else:
            lengthStr = toByteString(length)
            packed = chr(protocol.UTF8COUNT1 - 1 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putBoolean(self, value):
        """Appends a Boolean value to the message."""
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
        valueStr = struct.pack('d', value)
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
        ticks = datatype.TimeToTicks(value)
        valueStr = toByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDTIMELEN1) + chr(0) + chr(0)
        else:
            packed = chr(protocol.SCALEDTIMELEN1 - 1 + len(valueStr)) + chr(0) + valueStr
        self.__output += packed
        return self
    
    def putScaledTimestamp(self, value):
        """Appends a Scaled Timestamp value to the message."""
        ticks = datatype.TimestampToTicks(value)
        valueStr = toSignedByteString(ticks)
        if len(valueStr) == 0:
            packed = chr(protocol.SCALEDTIMESTAMPLEN1) + chr(0) + chr(0)
        else:
            packed = chr(protocol.SCALEDTIMESTAMPLEN1 - 1 + len(valueStr)) + chr(0) + valueStr
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
        if value == None:
            return self.putNull()
        elif type(value) == int:
            return self.putInt(value)
        elif type(value) == float:
            return self.putDouble(value)
        elif isinstance(value, decimal.Decimal):
            return self.putScaledInt(value)
        elif isinstance(value, datatype.Date):
            return self.putScaledDate(value)
        elif isinstance(value, datatype.Time):
            return self.putScaledTime(value)
        elif isinstance(value, datatype.Timestamp):
            return self.putScaledTimestamp(value)
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
            return decimal.Decimal(value) / decimal.Decimal(10**scale)

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
            return struct.unpack('d', self._takeBytes(typeCode - 77))[0]
            
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
            return datatype.TimeFromTicks(round(time/10.0**scale))

        raise DataError('Not a scaled time')
    
    def getScaledTimestamp(self):
        """Read the next Scaled Timestamp value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(protocol.SCALEDTIMESTAMPLEN1, protocol.SCALEDTIMESTAMPLEN8 + 1):
            scale = fromByteString(self._takeBytes(1))
            timestamp = fromSignedByteString(self._takeBytes(typeCode - 216))
            return datatype.TimestampFromTicks(round(timestamp/10.0**scale))

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
            return uuid.UUID(self._takeBytes(16))
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
        
        # get null type
        if typeCode is protocol.NULL:
            return self.getNull()
        
        # get boolean type
        elif typeCode in [protocol.TRUE, protocol.FALSE]:
            return self.getBoolean()
        
        # get uuid type
        elif typeCode in [protocol.UUID, protocol.SCALEDCOUNT1, protocol.SCALEDCOUNT2]:
            return self.getUUID()
        
        # get integer type
        elif typeCode in range(protocol.INTMINUS10, protocol.INTLEN8 + 1):
            return self.getInt()
        
        # get scaled int type
        elif typeCode in range(protocol.SCALEDLEN0, protocol.SCALEDLEN8 + 1):
            return self.getScaledInt()
        
        # get double precision type
        elif typeCode in range(protocol.DOUBLELEN0, protocol.DOUBLELEN8 + 1):
            return self.getDouble()
        
        # get string type
        elif typeCode in range(protocol.UTF8COUNT1, protocol.UTF8COUNT4 + 1) or \
             typeCode in range(protocol.UTF8LEN0, protocol.UTF8LEN39 + 1):
            return self.getString()
        
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
        
        else:
            raise NotImplementedError("not implemented")

    def exchangeMessages(self, getResponse=True):
        """Exchange the pending message for an optional response from the server."""
        try:
            # print "message to server: %s" %  (self.__output)
            self.send(self.__output)
        finally:
            self.__output = None

        if getResponse is True:
            self.__input = self.recv(False)
            self.__inpos = 0

            # TODO: include the actual error message, and use a different type
            if self.getInt() != 0:
                raise DatabaseError('Non-zero status: %s' % self.getString())
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
        if self.__inpos >= len(self.__input):
            raise EndOfStream('end of stream reached')
                        
        try:
            return self.__input[self.__inpos:self.__inpos + length]
        finally:
            self.__inpos += length
