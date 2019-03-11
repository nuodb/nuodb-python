__all__ = [ "ClientPassword", "ServerPassword", "RC4Cipher" ]

# This module provides the basic cryptographic rouintes (SRP and RC4) used to
# establish authenticated, confidential sessions with agents and engines. Note
# that no protocols are implemented here, just the ability to calculate and
# use session keys. Most users should never need the routines here, since they
# are encapsulated in other classes, but they are available.
#
# For a client, the typical pattern is:
#
#   cp = ClientPassword()
#   clientPub = cp.genClientKey()
#
#   [ send 'clientPub' and get 'salt' and 'serverPub' from the server]
#
#   sessionKey = cp.computeSessionKey('user', 'password', salt, serverKey)
#   cipherIn = RC4Cipher(sessionKey)
#   cipherOut = RC4Cipher(sessionKey)


import hashlib
import random
import binascii
import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

systemVersion = sys.version[0]

def toHex(bigInt):
    #Python 3 will no longer insert an L for type formatting
    if systemVersion is '3':
        hexStr = (hex(bigInt)[2:])
    else:
        hexStr = (hex(bigInt)[2:])[:-1]
    # if the number is the right size then the hex string will be missing one
    # character that some platforms assume, so force an even length encoding
    if len(hexStr) % 2 == 1:
        hexStr = "0" + hexStr
    return hexStr.upper()

def fromHex(hexStr):
    return int(hexStr, 16)

def toSignedByteString(value):
    if value == 0 or value == -1:
        return chr(value & 0xFF)

    resultBytes = []
    while value != 0 and value != -1:
        resultBytes.append(chr(value & 0xFF))
        value >>= 8

        # Zero pad if positive
    if value == 0 and (ord(resultBytes[-1]) & 0x80) == 0x80:
        resultBytes.append(chr(0x00))
    elif value == -1 and (ord(resultBytes[-1]) & 0x80) == 0x00:
        resultBytes.append(chr(0xFF))

    resultBytes.reverse()
    result = ''.join(resultBytes)

    return result


def fromSignedByteString(byteStr):
    if byteStr:
        is_neg = (ord(byteStr[0]) & 0x80) >> 7
    else:
        is_neg = 0
    result = 0
    shiftCount = 0
    for b in reversed(byteStr):
        result = result | (((ord(b) & 0xFF) ^ (is_neg * 0xFF)) << shiftCount)
        shiftCount = shiftCount + 8

    return ((-1)**is_neg) * (result + is_neg)

def toByteString(bigInt):
    resultBytes = []
    if bigInt == -1 or bigInt == 0:
        resultBytes.append(chr(bigInt & 0xFF))
        return ''.join(resultBytes)

    while bigInt != 0 and bigInt != -1:
        resultBytes.append(chr(bigInt & 0xFF))
        bigInt >>= 8

    resultBytes.reverse()
    result = ''.join(resultBytes)

    return result

def fromByteString(byteStr):
    result = 0
    shiftCount = 0
    if systemVersion == '3':
        if type(byteStr) is bytes:
            byteStr = byteStr.decode('latin-1')
    for b in reversed(byteStr):
        result = result | ((ord(b) & 0xff) << shiftCount)
        shiftCount = shiftCount + 8

    return result


class RemoteGroup(object):

    defaultPrime = "EEAF0AB9ADB38DD69C33F80AFA8FC5E86072618775FF3C0B9EA2314C" + \
        "9C256576D674DF7496EA81D3383B4813D692C6E0E0D5D8E250B98BE4" + \
        "8E495C1D6089DAD15DC7D7B46154D6B6CE8EF4AD69B15D4982559B29" + \
        "7BCF1885C529F566660E57EC68EDBC3C05726CC02FD4CBF4976EAA9A" + \
        "FD5138FE8376435B9FC61D2FC0EB06E3"

    defaultGenerator = "2"

    def __init__(self, primeStr=defaultPrime, generatorStr=defaultGenerator):
        self.__primeInt = fromHex(primeStr)
        self.__generatorInt = fromHex(generatorStr)

        primeBytes = toByteString(self.__primeInt)
        generatorBytes = toByteString(self.__generatorInt)
        paddingLength = len(primeBytes) - len(generatorBytes)
        paddingBuffer = chr(0) * paddingLength

        md = hashlib.sha1()
        if systemVersion == '3':
            primeBytes = primeBytes.encode('latin-1')
            generatorBytes = generatorBytes.encode('latin-1')
            paddingBuffer = paddingBuffer.encode('latin-1')

        md.update(primeBytes)
        if paddingLength > 0:
            md.update(paddingBuffer)
        md.update(generatorBytes)

        self.__k = fromByteString(md.digest())

    def getPrime(self):
        return self.__primeInt

    def getGenerator(self):
        return self.__generatorInt

    def getK(self):
        return self.__k

class RemotePassword(object):

    def __init__(self):
        self.__group = RemoteGroup()


    def _getUserHash(self, account, password, salt):
        md = hashlib.sha1()
        userInfo = account + ":" + password
        if systemVersion == '3':
            userInfo = userInfo.encode('latin-1')
        md.update(userInfo)
        hash1 = md.digest()

        md = hashlib.sha1()
        md.update(binascii.a2b_hex(salt))
        md.update(hash1)

        return fromByteString(md.digest())

    def _computeScramble(self, clientPublicKey, serverPublicKey):
        clientBytes = toByteString(clientPublicKey)
        serverBytes = toByteString(serverPublicKey)

        md = hashlib.sha1()

        if systemVersion == '3':
            clientBytes = clientBytes.encode('latin-1')
            serverBytes = serverBytes.encode('latin-1')

        md.update(clientBytes)
        md.update(serverBytes)

        return fromByteString(md.digest())

    def _getGroup(self):
        return self.__group

class ClientPassword(RemotePassword):

    def genClientKey(self):
        group = self._getGroup()

        self.__privateKey = random.getrandbits(256)
        self.__publicKey = pow(group.getGenerator(), self.__privateKey, group.getPrime())

        return toHex(self.__publicKey)

    def computeSessionKey(self, account, password, salt, serverKey):
        serverPubKey = fromHex(serverKey)
        scramble = self._computeScramble(self.__publicKey, serverPubKey)

        group = self._getGroup()
        prime = group.getPrime()

        x = self._getUserHash(account, password, salt)
        gx = pow(group.getGenerator(), x, prime)
        kgx = (group.getK() * gx) % prime
        diff = (serverPubKey - kgx) % prime
        ux = (scramble * x) % prime
        aux = (self.__privateKey + ux) % prime

        sessionSecret = pow(diff, aux, prime)
        secretBytes = toByteString(sessionSecret)

        md = hashlib.sha1()
        if systemVersion == '3':
            secretBytes = secretBytes.encode('latin-1')
        md.update(secretBytes)

        return md.digest()

class ServerPassword(RemotePassword):

    def genSalt(self):
        return toHex(random.getrandbits(256))

    def computeVerifier(self, account, password, salt):
        x = self._getUserHash(account, password, salt)

        group = self._getGroup()
        verifier = pow(group.getGenerator(), x, group.getPrime())

        return toHex(verifier)

    def genServerKey(self, verifier):
        self.__privateKey = random.getrandbits(256)

        group = self._getGroup()

        gb = pow(group.getGenerator(), self.__privateKey, group.getPrime())
        v = fromByteString(verifier)
        kv = (group.getK() * v) % group.getPrime()
        self.__publicKey = (kv + gb) % group.getPrime()

        return toHex(self.__publicKey)

    def computeSessionKey(self, clientKey, verifier):
        clientPubKey = fromHex(clientKey)
        scramble = self._computeScramble(clientPubKey, self.__publicKey)

        prime = self._getGroup().getPrime()

        vu = pow(fromHex(verifier), scramble, prime)
        avu = (clientPubKey * vu) % prime

        sessionSecret = pow(avu, self.__privateKey, prime)
        secretBytes = toByteString(sessionSecret)

        md = hashlib.sha1()
        md.update(secretBytes)

        return md.digest()

class RC4Cipher(object):

    def __init__(self, key):
        if systemVersion == '3' and type(key) == str:
            key = key.encode()
        self.cipher = Cipher(algorithms.ARC4(key), mode=None, backend=default_backend()).encryptor()

    def transform(self, data):
        # Cipher expects bytes
        if systemVersion == '3' and type(data) == str:
            data = data.encode()
        return self.cipher.update(data)

class NoCipher(object):

    def __init__(self):
        """ A class to allow polymorphic cipher streams"""
        pass

    def transform(self, data):
        """ Returns the data as passed in so that it will be sent unencrypted to the server"""
        return data
