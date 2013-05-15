
__all__  = [ 'EncodedSession' ]

from nuodb.crypt import toByteString, fromByteString
from nuodb.session import Session, SessionException

#from . import exception
#from exception import DataError

import struct
import uuid
from exception import DataError

# from nuodb.util import getCloudEntry
# (host, port) = getCloudEntry(broker, dbName, connectionKeys)

class EncodedSession(Session):

    def __init__(self, host, port, service='SQL2'):
        Session.__init__(self, host, port=port, service=service)
        self.doConnect()
        self.typeCode = None
        self.__output = None
        self.__input = None
        self.__inpos = 0
 
    #
    # Methods to put values into the next message

    def putMessageId(self, messageId):
        if self.__output != None:
            raise SessionException('no')
        self.__output = ''
        self.putInt(messageId)
        return self

    def putInt(self, value):
        if value < 32 and value > -11:
            packed = chr(20 + value)
        else:
            valueStr = toByteString(value)
            packed = chr(51 + len(valueStr)) + valueStr
        self.__output += packed
        return self

    def putScaledInt(self, value, scale):
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
        length = len(value)
        if length < 40:
            packed = chr(109 + length) + value
        else:
            lengthStr = toByteString(length)
            packed = chr(68 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putBoolean(self, value):
        if value is True:
            self.__output += chr(2)
        else:
            self.__output += chr(3)
        return self

    def putNull(self):
        self.__output += chr(1)
        return self

    def putUUID(self, value):
        self.__output += chr(202) + str(value)
        return self

    def putOpaque(self, value):
        length = len(value)
        if length < 40:
            packed = chr(150 + length)
        else:
            lengthStr = self.toByteSting(length)
            packed = chr(72 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putDouble(self, value):
        byteString = struct.pack('d', value)
        lengthStr = len(byteString)
        packed = chr(77 + lengthStr) + byteString
        self.__output += packed
        return self

    def putTime(self, value):
        pass

    def putBlob(self, value):
        length = len(value)
        if length is 0:
            packed = chr(191)
        else:
            lengthStr = self.toByteSting(length)
            packed = chr(191 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self

    def putClob(self, value):
        length = len(value)
        if length is 0:
            packed = chr(196)
        else:
            lengthStr = self.toByteSting(length)
            packed = chr(196 + len(lengthStr)) + lengthStr + value
        self.__output += packed
        return self
        
    def putScaledTime(self, value):
        pass
        
    def putScaledDate(self, value):
        pass

    #
    # Methods to get values out of the last exchange

    def getInt(self):

        if self.typeCode in range(10, 51):
            return self.typeCode - 20

        if self.typeCode in range(52, 59):
            return fromByteString(self._takeBytes(self.typeCode - 51))

        if self.typeCode == 1:
            return 0

        raise DataError('Not an integer')

    def getScaledInt(self):

        if self.typeCode is 60:
            return (0, self.__takeBytes(1))

        if self.typeCode in range(61, 68):
            scale = self.__takeBytes(1)
            return (fromByteString(self.__takeBytes(self.typeCode - 60)), scale)

        raise DataError('Not a scaled integer')

    def getString(self):

        if self.typeCode in range(109, 148):
            return self._takeBytes(self.typeCode - 109)

        if self.typeCode in range(69, 72):
            strLength = fromByteString(self._takeBytes(self.typeCode - 68))
            return self._takeBytes(strLength)

        raise DataError('Not a string')

    def getBoolean(self):

        if self.typeCode == 2:
            return True
        if self.typeCode == 3:
            return False

        raise DataError('Not a boolean')

    def getNull(self):
        if self.typeCode != 1:
            raise DataError('Not null')

    def getDouble(self):
        typeCode = self._getTypeCode()
        return struct.unpack('d', self.__takeBytes(typeCode - 76))[0]

    def getTime(self):
        raise NotImplementedError
    
    def getOpaque(self):
        raise NotImplementedError
    
    def getBlob(self):
        raise NotImplementedError
    
    def getClob(self):
        raise NotImplementedError
    
    def getScaledTime(self):
        raise NotImplementedError
    
    def getScaledDate(self):
        raise NotImplementedError

    def getUUID(self):
        if self.typeCode == 202:
            return uuid.UUID(self._takeBytes(16))
        if self.typeCode == 201:
            # before version 11
            pass
        if self.typeCode == 227:
            # version 11 and later
            pass

        raise DataError('Not a UUID')

    def getValue(self):

        self.typeCode = self._getTypeCode()
        
        # get null type
        if self.typeCode is 1:
            return self.getNull()
        
        # get boolean type
        elif self.typeCode in [2, 3]:
            return self.getBoolean()
        
        # get uuid type
        elif self.typeCode in [202, 201, 227]:
            return self.getUUID()
        
        # get integer type
        elif self.typeCode in range(10, 60):
            return self.getInt()
        
        # get scaled int type
        elif self.typeCode in range(60, 69):
            return self.getScaledInt()
        
        # get double precision type
        elif self.typeCode in range(77, 86):
            return self.getDouble()
        
        # get string type
        elif self.typeCode in range(69, 73) or self.typeCode in range(109, 150):
            return self.getString()

        # get opague type
        elif self.typeCode in range(73, 77) or self.typeCode in range(150, 191):
            return self.getOpaque()
        
        # get blob/clob type
        elif self.typeCode in range(191, 201):
            return self.getBlob()
        
        #get clob type
        elif self.typeCode in range(196, 201):
            return self.getClob()
        
        # get time type
        elif self.typeCode in range(86, 109):
            return self.getTime()
        
        # get scaled time
        elif self.typeCode in range(211, 227):
            return self.getScaledTime()
        
        # get scaled date
        elif self.typeCode in range(203, 211):
            return self.getScaledDate()
        
        else:
            raise NotImplementedError

    # Exchange the pending message for an optional response from the server
    def exchangeMessages(self, getResponse=True):
        try:
#             print "message to server: %s" % self.__output
            self.send(self.__output)
        finally:
            self.__output = None

        if getResponse is True:
            self.__input = self.recv()
            self.__inpos = 0

            # TODO: include the actual error message, and use a different type
            if self.getInt() != 0:
                raise SessionException('Non-zero status: %s' % self.getString())
        else:
            self.__input = None

    # Re-sets the incoming and outgoing ciphers for the session
    def setCiphers(self, cipherIn, cipherOut):
        Session._setCiphers(self, cipherIn, cipherOut)

    # Protected utility routines

    def _peekTypeCode(self):
        return ord(self.__input[self.__inpos])

    def _getTypeCode(self):
        try:
            return ord(self.__input[self.__inpos])
        finally:
            self.__inpos += 1

    def _takeBytes(self, length):
        try:
            return self.__input[self.__inpos:self.__inpos + length]
        finally:
            self.__inpos += length
