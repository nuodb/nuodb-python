
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
import string
import struct


def toHex(bigInt):
    hexStr = (hex(bigInt)[2:])[:-1]
    # if the number is the right size then the hex string will be missing one
    # character that some platforms assume, so force an even length encoding
    if len(hexStr) % 2 == 1:
        hexStr = "0" + hexStr
    return string.upper(hexStr)

def fromHex(hexStr):
    return int(hexStr, 16)

def toByteString(bigInt):
    resultBytes = []

    while bigInt:
        resultBytes.append(chr(bigInt & 0xFF))
        bigInt >>= 8

    resultBytes.reverse();
    result = ''.join(resultBytes)
    
    return result

def fromByteString(byteStr):
    result = 0
    shiftCount = 0

    for b in reversed(byteStr):
        result = result | ((ord(b) & 0xff) << shiftCount)
        shiftCount = shiftCount + 8

    return result


class RemoteGroup:

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

        md = hashlib.sha1()
        md.update(primeBytes)
        if paddingLength > 0:
            md.update(chr(0) * paddingLength)
        md.update(generatorBytes)

        self.__k = fromByteString(md.digest())

    def getPrime(self):
        return self.__primeInt

    def getGenerator(self):
        return self.__generatorInt

    def getK(self):
        return self.__k

class RemotePassword:

    def __init__(self):
        self.__group = RemoteGroup()

    def _getUserHash(self, account, password, salt):
        md = hashlib.sha1()
        md.update(account + ":" + password)
        hash1 = md.digest()

        md = hashlib.sha1()
        md.update(toByteString(fromHex(salt)))
        md.update(hash1)

        return fromByteString(md.digest())

    def _computeScramble(self, clientPublicKey, serverPublicKey):
        clientBytes = toByteString(clientPublicKey)
        serverBytes = toByteString(serverPublicKey)

        md = hashlib.sha1()
        md.update(clientBytes)
        md.update(serverBytes)

        return fromByteString(md.digest())

    def _getGroup(self):
        return self.__group

class ClientPassword(RemotePassword):

    def genClientKey(self):
        group = self._getGroup()

        self.__privateKey = random.getrandbits(256)
        self.__publicKey = pow(group.getGenerator(), self.__privateKey, group.getPrime());

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

class RC4Cipher:

    def __init__(self, key):
        self.__S = range(256)
        self.__s1 = 0
        self.__s2 = 0

        state = self.__S

        j = 0
        for i in range(256):
            j = (j + state[i] + ord(key[i % len(key)])) % 256
            state[i], state[j] = state[j], state[i]

    def transform(self, data):
        transformed = []
        state = self.__S

        for char in data:
            self.__s1 = (self.__s1 + 1) % 256
            self.__s2 = (self.__s2 + state[self.__s1]) % 256
            state[self.__s1], state[self.__s2] = state[self.__s2], state[self.__s1]
            cipherByte = ord(char) ^ state[(state[self.__s1] + state[self.__s2]) % 256]
            transformed.append(chr(cipherByte))

        return ''.join(transformed)

