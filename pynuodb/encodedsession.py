
__all__  = [ 'EncodedSession' ]

from nuodb.crypt import toByteString, fromByteString
from nuodb.session import Session, SessionException

#from . import exception
#from exception import DataError

import uuid
from exception import DataError

# from nuodb.util import getCloudEntry
# (host, port) = getCloudEntry(broker, dbName, connectionKeys)

class EncodedSession(Session):

    def __init__(self, host, port, service='SQL2'):
        Session.__init__(self, host, port=port, service=service)
        self.doConnect()

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
        pass

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

    #
    # Methods to get values out of the last exchange

    def getInt(self):
        typeCode = self._getTypeCode()

        if typeCode in range(10, 51):
            return typeCode - 20

        if typeCode in range(52, 59):
            return fromByteString(self._takeBytes(typeCode - 51))

        if typeCode == 1:
            return 0

        raise DataError('Not an integer')

    def getScaledInt(self):
        typeCode = self._getTypeCode()

        if typeCode is 60:
            return (0, self.__takeBytes(1))

        if typeCode in range(61, 68):
            scale = self.__takeBytes(1)
            return (fromByteString(self.__takeBytes(typeCode - 60)), scale)

        raise DataError('Not a scaled integer')

    def getString(self):
        typeCode = self._getTypeCode()

        if typeCode in range(109, 148):
            return self._takeBytes(typeCode - 109)

        if typeCode in range(69, 72):
            strLength = fromByteString(self._takeBytes(typeCode - 68))
            return self._takeBytes(strLength)

        raise DataError('Not a string')

    def getBoolean(self):
        typeCode = self._getTypeCode()

        if typeCode == 2:
            return True
        if typeCode == 3:
            return False

        raise DataError('Not a boolean')

    def getNull(self):
        if self._getTypeCode() != 1:
            raise DataError('Not null')

    def getUUID(self):
        if self._getTypeCode() == 202:
            return uuid.UUID(self._takeBytes(16))

        raise DataError('Not a UUID')

    def getValue(self):
        typeCode = self.session._getTypeCode()
        
        # get integer
        if typeCode in range(10, 51) or typeCode in range(52, 59):
            return self.getInt()
        
        # get scaled int
        elif typeCode is 60 or typeCode in range(61, 68):
            return self.getScaledInt()
        
        # get string
        elif typeCode in range(109, 148) or typeCode in range(69, 72):
            return self.getString()
        
        # get boolean
        elif typeCode in [2, 3]:
            return self.getBoolean()
        
        # get uuid
        elif typeCode is 202:
            return self.getUUID()
        
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

    #
    # Protected utility routines

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
