
__all__ = [ "testEngine" ]

# NOTE: This is still more of a proof-of-concept module showing that basic
# interaction can be done from python with an engine, but really only serves
# to expose the liveness-testing routine at this point ... assume that all of
# the other components are still in flux.
#
# This module is the starting point for doing direct communications from a
# python client to an engine. It uses Session to communicate, but then opens
# the database and mutually authenticates using the binary protocol. The
# specific message identifiers and header fields are hard-coded for this
# example, but if we decide to flesh this out into a more general utility
# obviously those would be pulled into a separate enumeration.
#
# While the geenric openSQLSession routine returns an open, authenticated
# session ready to handle SQL operations there's not a lot that can be done
# with it at this point without knowing how to encode our protocol directly.
# The one utility that is included is testEngine(), which togther with the
# util module provides the ability to do this:
#
#   (addr, port) = util.getCloudEntry('localhost', 'myDB')
#   testEngine(addr, port, 'myDB', 'user', 'cloud')
#
# If the engine isn't available, isn't supporting the chorus 'myDB', doesn't
# allow the user/cloud account connection or isn't responding to SQL queries
# this raises an exception. The final test is done by issuing the request:
#
#   select 1 as one from dual
#
# and validating that the response is formed as expected. Essentially, the
# testEngine() routine is a very light-weight test that an engine is running
# correctly and responding to queries.

from crypt import ClientPassword, RC4Cipher
from session import Session, SessionException
from util import getCloudEntry

import string
import struct

from xml.etree import ElementTree


def testEngine(address, port, dbName, user, pwd):
    (s, cipherIn, cipherOut) = openSQLSession(address, port, dbName, user, pwd)

    stmtHandle = createStatement(s, cipherIn, cipherOut)

    # Int0+ExecuteQuery, Int0+stmtHandle, query
    queryStr = _encodeString("select 1 as one from dual")
    msg = struct.pack("!BB%is" % len(queryStr), 39, stmtHandle + 20, queryStr)

    s.send(cipherOut.transform(msg))
    msg = cipherIn.transform(s.recv())

    msg = _testStatus(msg, "Failed to execute select test")

    (rsHandle, msg) = _decodeInt(msg)
    (count, msg) = _decodeInt(msg)
    (colname, msg) = _decodeString(msg)

    if count != 1:
        raise SessionException("Too many columns found on select test")
    if colname != "ONE":
        raise SessionException("Wrong column found on select test")

    (result, msg) = _decodeInt(msg)
    if result != 1:
        raise SessionException("Too mamy rows found on select test")

    (fieldValue, msg) = _decodeInt(msg)
    if fieldValue != 1:
        raise SessionException("Expected 1 as value; got " + str(fieldValue))

    (result, msg) = _decodeInt(msg)
    if result != 0:
        raise SessionException("Expected zero as final result")

    s.close()

def resolveSQLSession(broker, dbName, sqlUser, sqlPwd, brokerAttrs=None, engineParams=None):
    (host, port) = getCloudEntry(broker, dbName, brokerAttrs)

    return openSQLSession(host, port, dbName, sqlUser, sqlPwd, engineParams)

def openSQLSession(address, port, dbName, user, pwd, paramaters=None):
    s = Session(address, port=port, service="SQL2")
    s.doConnect()

    cp = ClientPassword()
    clientKey = _encodeString(cp.genClientKey())

    if not paramaters:
        paramaters = []

    paramaters.append(("user", user))
    paramaters.append(("schema", "test"))

    paramStr = ""
    for (key, value) in paramaters:
        paramStr = paramStr + _encodeString(key) + _encodeString(value)

    # IntLen1=52
    # Int0=20

    # Int0+OpenDatabase, Int0+PROTOCOL_VERSION4, connectStr,
    # Int0+parametersLength, parameters, Null (txn id), clientKey
    connectStr = _encodeString(dbName)
    msg = struct.pack("!BB%isB%isB%is" % (len(connectStr), len(paramStr), len(clientKey)), 23, 24, connectStr, 20 + len(paramaters), paramStr, 1, clientKey)

    s.send(msg)
    msg = s.recv()

    # RETURNS: int (status) int (version), string (serverKey), string (salt), 
    #status = ord(msg[0]) - 20
    #version = ord(msg[1]) - 20

    msg = _testStatus(msg, "Unexpected initial server response")

    (version, msg) = _decodeInt(msg)
    (serverKey, msg) = _decodeString(msg)
    (salt, msg) = _decodeString(msg)

    sessionKey = cp.computeSessionKey(string.upper(user), pwd, salt, serverKey)

    cipherIn = RC4Cipher(sessionKey)
    cipherOut = RC4Cipher(sessionKey)

    # IntLen1=Authentication
    verifierStr =  _encodeString("Success!")
    msg = struct.pack("!BB%is" % len(verifierStr), 52, 86, verifierStr)

    s.send(cipherOut.transform(msg))
    msg = cipherIn.transform(s.recv())

    _testStatus(msg, "Engine authentication failed")

    return (s, cipherIn, cipherOut)

def createStatement(s, cipherIn, cipherOut):
    # Int0+CreateStatement
    msg = struct.pack("!B", 31)

    s.send(cipherOut.transform(msg))
    msg = cipherIn.transform(s.recv())

    msg = _testStatus(msg, "Failed to create statement")

    (statementHandle, msg) = _decodeInt(msg)

    return statementHandle

def _testStatus(msg, errorStr):
    (status, msg) = _decodeInt(msg)
    if status != 0:
        raise SessionException("Non-zero status (" + str(status) + "): " + errorStr)

    return msg

def _encodeString(strValue):
    # simplest, if wasteful method to pack a string
    # Utf8Count2=length (2-bytes), string characters
    length = len(strValue)
    return struct.pack("!BH%is" % length, 70, length, strValue)

def _decodeString(encoded):
    firstByte = ord(encoded[0])
    if firstByte > 72:
        length = firstByte - 109
        return (encoded[1:length + 1], encoded[length + 1:])

    strStart = (firstByte - 69) + 2
    length = ord(encoded[1])
    pos = 2
    while pos < strStart:
        length = (length << 8) + ord(encoded[pos])
        pos = pos + 1
    return (encoded[strStart:length + strStart], encoded[length + strStart:])

def _decodeInt(encoded):
    firstByte = ord(encoded[0])
    if firstByte >= 10 and firstByte <= 51:
        return (firstByte - 20, encoded[1:])

    intVal = ord(encoded[1])
    pos = 2
    byteCount = firstByte - 52
    while byteCount > 0:
        intVal = (intVal << 8) + ord(encoded[pos])
        pos = pos + 1
        byteCount = byteCount - 1
    return (intVal, encoded[pos:])
