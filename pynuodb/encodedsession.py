"""A module for housing the EncodedSession class.

Exported Classes:
EncodedSession -- Class for representing an encoded session with the database.
"""

__all__  = [ 'EncodedSession' ]

from crypt import toByteString, fromByteString
from session import Session, SessionException

import uuid
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
    putScaledTime -- Currently not supported.
    putScaledDate -- Currently not supported.
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
    getScaledTime -- Currently not supported.
    getScaledDate -- Currently not supported.
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
        self.putInt(messageId)
        return self

    def putInt(self, value):
        """Appends an Integer value to the message."""
        if value < 32 and value > -11:
            packed = chr(20 + value)
        else:
            valueStr = toByteString(value)
            packed = chr(51 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putScaledInt(self, value, scale):
        """Appends a Scaled Integer value to the message."""
        if scale is 0:
            self.putInt(value)
        elif value is 0:
            packed = chr(60) + chr(scale)
        else:
            valueStr = toByteString(value)
            packed = chr(60 + len(valueStr)) + chr(scale) + valueStr
        self.__output += packed
        return self

    def putString(self, value):
        """Appends a String to the message."""
        length = len(value)
        if length < 40:
            packed = chr(109 + length) + value
        else:
            lengthStr = toByteString(length)
            packed = chr(68 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putBoolean(self, value):
        """Appends a Boolean value to the message."""
        if value is True:
            self.__output += chr(2)
        else:
            self.__output += chr(3)
        return self

    def putNull(self):
        """Appends a Null value to the message."""
        self.__output += chr(1)
        return self

    def putUUID(self, value):
        """Appends a UUID to the message."""
        self.__output += chr(202) + str(value)
        return self

    def putOpaque(self, value):
        """Appends an Opaque data value to the message."""
        length = len(value)
        if length < 40:
            packed = chr(150 + length) + value
        else:
            lengthStr = self.toByteSting(length)
            packed = chr(72 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putDouble(self, value):
        """Appends a Double to the message."""
        valueStr = struct.pack('d', value)
        packed = chr(77 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putMsSinceEpoch(self, value):
        """Appends the MsSinceEpoch value to the message."""
        valueStr = toByteString(value)
        packed = chr(86 + len(valueStr)) + valueStr
        self.__output += packed
        return self
        
    def putNsSinceEpoch(self, value):
        """Appends the NsSinceEpoch value to the message."""
        valueStr = toByteString(value)
        packed = chr(95 + len(valueStr)) + valueStr
        self.__output += packed
        return self
        
    def putMsSinceMidnight(self, value):
        """Appends the MsSinceMidnight value to the message."""
        valueStr = toByteString(value)
        packed = chr(104 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putBlob(self, value):
        """Appends the Blob(Binary Large OBject) value to the message."""
        length = len(value)
        if length is 0:
            packed = chr(191)
        else:
            lengthStr = self.toByteSting(length)
            packed = chr(191 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putClob(self, value):
        """Appends the Clob(Character Large OBject) value to the message."""
        length = len(value)
        if length is 0:
            packed = chr(196)
        else:
            lengthStr = self.toByteSting(length)
            packed = chr(196 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self
        
    def putScaledTime(self, value, scale):
        """Currently not supported."""
        pass
        
    def putScaledDate(self, value, scale):
        """Currently not supported."""
        pass

    def putValue(self, value):
        """Determines the probable type of the value and calls the supporting function."""
        if value == None:
            return self.putNull()
        elif value == True or value == False:
            return self.putBoolean(value)
        elif type(value) == int:
            return self.putInt(value)
        elif type(value) == float:
            return self.putDouble(value)
        else:
            return self.putString(value)
        
    #
    # Methods to get values out of the last exchange

    def getInt(self):
        """Read the next Integer value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(10, 52):
            return typeCode - 20

        elif typeCode in range(52, 60):
            return fromByteString(self._takeBytes(typeCode - 51))

        raise DataError('Not an integer')

    def getScaledInt(self):
        """Read the next Scaled Integer value off the session."""
        typeCode = self._getTypeCode()

        if typeCode == 60:
            return (0, self.__takeBytes(1))

        if typeCode in range(61, 69):
            scale = self.__takeBytes(1)
            return (fromByteString(self.__takeBytes(typeCode - 60)), scale)

        raise DataError('Not a scaled integer')

    def getString(self):
        """Read the next String off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(109, 149):
            return self._takeBytes(typeCode - 109)

        if typeCode in range(69, 73):
            strLength = fromByteString(self._takeBytes(typeCode - 68))
            return self._takeBytes(strLength)

        raise DataError('Not a string')

    def getBoolean(self):
        """Read the next Boolean value off the session."""
        typeCode = self._getTypeCode()

        if typeCode == 2:
            return True
        if typeCode == 3:
            return False

        raise DataError('Not a boolean')

    def getNull(self):
        """Read the next Null value off the session."""
        if self._getTypeCode() != 1:
            raise DataError('Not null')

    def getDouble(self):
        """Read the next Double off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode in range(77, 86):
            return struct.unpack('d', self.__takeBytes(typeCode - 77))[0]
            
        raise DataError('Not a double')

    def getTime(self):
        """Read the next Time value off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode in range(86, 95):
            return fromByteString(self._takeBytes(typeCode - 86))
            
        if typeCode in range(95, 104):
            return fromByteString(self._takeBytes(typeCode - 95))
            
        if typeCode in range(104, 109):
            return fromByteString(self._takeBytes(typeCode - 104))
            
        raise DataError('Not a time')
    
    def getOpaque(self):
        """Read the next Opaque value off the session."""
        typeCode = self._getTypeCode()

        if typeCode in range(150, 190):
            return self._takeBytes(typeCode - 150)

        if typeCode in range(73, 77):
            strLength = fromByteString(self._takeBytes(typeCode - 72))
            return self._takeBytes(strLength)

        raise DataError('Not an opaque value')

    def getBlob(self):
        """Read the next Blob(Binary Large OBject) value off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode == 191:
            return None
        
        if typeCode in range(192, 196):
            strLength = fromByteString(self._takeBytes(typeCode - 191))
            return self._takeBytes(strLength)

        raise DataError('Not a blob')
    
    def getClob(self):
        """Read the next Clob(Character Large OBject) value off the session."""
        typeCode = self._getTypeCode()
        
        if typeCode == 196:
            return None
        
        if typeCode in range(197, 201):
            strLength = fromByteString(self._takeBytes(typeCode - 196))
            return self._takeBytes(strLength)

        raise DataError('Not a clob')
    
    def getScaledTime(self):
        """Currently not supported."""
        raise NotImplementedError("not implemented")
    
    def getScaledDate(self):
        """Currently not supported."""
        raise NotImplementedError("not implemented")

    def getUUID(self):
        """Read the next UUID value off the session."""
        if self._getTypeCode() == 202:
            return uuid.UUID(self._takeBytes(16))
        if self._getTypeCode() == 201:
            # before version 11
            pass
        if self._getTypeCode() == 227:
            # version 11 and later
            pass

        raise DataError('Not a UUID')

    def getValue(self):
        """Determine the datatype of the next value off the session, then call the
        supporting function.
        """
        typeCode = self._peekTypeCode()
        
        # get null type
        if typeCode is 1:
            return self.getNull()
        
        # get boolean type
        elif typeCode in [2, 3]:
            return self.getBoolean()
        
        # get uuid type
        elif typeCode in [202, 201, 227]:
            return self.getUUID()
        
        # get integer type
        elif typeCode in range(10, 60):
            return self.getInt()
        
        # get scaled int type
        elif typeCode in range(60, 69):
            return self.getScaledInt()
        
        # get double precision type
        elif typeCode in range(77, 86):
            return self.getDouble()
        
        # get string type
        elif typeCode in range(69, 73) or typeCode in range(109, 150):
            return self.getString()
        
        # get opague type
        elif typeCode in range(73, 77) or typeCode in range(150, 191):
            return self.getOpaque()
        
        # get blob/clob type
        elif typeCode in range(191, 201):
            return self.getBlob()
        
        # get time type
        elif typeCode in range(86, 109):
            return self.getTime()
        
        # get scaled time
        elif typeCode in range(211, 227):
            return self.getScaledTime()
        
        # get scaled date
        elif typeCode in range(203, 211):
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
