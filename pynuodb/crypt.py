"""Manage encryption.

(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

# This module provides the basic cryptographic routines (SRP and RC4) used to
# establish authenticated, confidential sessions with engines. Note that no
# protocols are implemented here, just the ability to calculate and use
# session keys. Most users should never need the routines here, since they are
# encapsulated in other classes, but they are available.
#
# For a client, the typical pattern is:
#
#   cp = ClientPassword()
#   clientPub = cp.genClientKey()
#
#   [ send 'clientPub' and get 'salt' and 'serverPub' from the server]
#
#   sessionKey = cp.computeSessionKey('user', 'password', salt, serverKey)
#   cipherIn = RC4Cipher(False, sessionKey)
#   cipherOut = RC4Cipher(True, sessionKey)

# Encodings: We want to convert to/from a full 8-bit character value.  We
# can't use utf-8 here since it reserves some of the 255 values to introduce
# multi-byte values.  And we can't use ASCII because it's a 7-bit code.  The
# 'latin-1' encoding allows all values 0-255 to be mapped to the "standard"
# byte values.

import hashlib
import random
import binascii
import sys

try:
    from typing import Optional  # pylint: disable=unused-import
except ImportError:
    pass

try:
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        AESImported = True
        try:
            # Newer cryptography versions stash ARC4 here
            from cryptography.hazmat.decrepit.ciphers.algorithms import ARC4
        except ImportError:
            # In older cryptography it's still with the regular algorithms
            ARC4 = algorithms.ARC4
    arc4Imported = True
except ImportError:
    arc4Imported = False
    AESImported = False

isP2 = sys.version[0] == '2'


def get_ciphers():
    # type: () -> str
    """Return the list of ciphers supported by this client."""
    return ("AES-256-CTR,AES-128-CTR," if AESImported else '') + "RC4"


# We use a bytearray for our sending buffer because we need to construct it.
# If we were using only Python3, then our received data could be stored in a
# bytes object which might be slightly more efficient.  But in Python2 a bytes
# object is the same as a string so it can't be portable: if use bytes we need
# different code to extract it on P2 vs. P3.
#
# Instead, we'll use a bytearray for the received data as well as the sending
# data for as long as we need to support Python 2.

if isP2:
    def bytesToArray(data):
        # type: (bytes) -> bytearray
        """Convert bytes to a bytearray.

        On Python 2 bytes is a string so we have to ord each character.
        """
        return bytearray([ord(c) for c in data])  # type: ignore

    def arrayToStr(data):
        # type: (bytearray) -> str
        """Convert a bytearray to a str.

        On Python 2 we can just use the str() constructor.  If we use decode
        we get back a unicode string not a string..
        """
        return str(data)

    def hexstrToBytes(hexstr):
        # type: (Optional[str]) -> Optional[bytes]
        """Convert a hex string to bytes."""
        return binascii.unhexlify(hexstr) if hexstr is not None else None
else:
    def bytesToArray(data):
        # type: (bytes) -> bytearray
        """Convert bytes to a bytearray.

        On Python 3 bytes is a binary string so we can just convert it.
        """
        return bytearray(data)

    def arrayToStr(data):
        # type: (bytearray) -> str
        """Convert a bytearray to a str.

        On Python 3 we must decode: assume UTF-8 always.
        """
        return data.decode('utf-8')

    def hexstrToBytes(hexstr):
        # type: (Optional[str]) -> Optional[bytes]
        """Convert a hex string to bytes."""
        return bytes.fromhex(hexstr) if hexstr is not None else None  # pylint: disable=no-member


def toHex(bigInt):
    # type: (int) -> str
    """Convert an integer into a hex string."""
    if isP2:
        hexStr = (hex(bigInt)[2:])[:-1]
    else:
        # Python 3 will no longer insert an L for type formatting
        hexStr = hex(bigInt)[2:]
    # Some platforms assume hex strings are even length: add padding if needed
    if len(hexStr) % 2 == 1:
        hexStr = '0' + hexStr
    return hexStr.upper()


def fromHex(hexStr):
    # type: (str) -> int
    """Convert a hex string into an integer."""
    return int(hexStr, 16)


def toSignedByteString(value):
    # type: (int) -> bytearray
    """Convert an integer into bytes."""
    result = bytearray()
    if value == 0 or value == -1:
        result.append(value & 0xFF)
    else:
        while value != 0 and value != -1:
            result.append(value & 0xFF)
            value >>= 8
        # Zero pad if positive
        if value == 0 and (result[-1] & 0x80) == 0x80:
            result.append(0x00)
        elif value == -1 and (result[-1] & 0x80) == 0x00:
            result.append(0xFF)
        result.reverse()
    return result


def fromSignedByteString(data):
    # type: (bytearray) -> int
    """Convert bytes into a signed integer."""
    if data:
        is_neg = (data[0] & 0x80) >> 7
    else:
        is_neg = 0
    result = 0
    shiftCount = 0
    for b in reversed(data):
        result = result | (((b & 0xFF) ^ (is_neg * 0xFF)) << shiftCount)
        shiftCount += 8

    return ((-1)**is_neg) * (result + is_neg)


def toByteString(bigInt):
    # type: (int) -> bytearray
    """Convert an integer into bytes."""
    result = bytearray()
    if bigInt == -1 or bigInt == 0:
        result.append(bigInt & 0xFF)
    else:
        while bigInt != 0 and bigInt != -1:
            result.append(bigInt & 0xFF)
            bigInt >>= 8
        result.reverse()
    return result


def fromByteString(data):
    # type: (bytearray) -> int
    """Convert bytes into an integer."""
    result = 0
    shiftCount = 0
    for b in reversed(data):
        result = result | ((b & 0xff) << shiftCount)
        shiftCount += 8
    return result


class RemoteGroup(object):
    """A remote group."""

    defaultPrime = ("EEAF0AB9ADB38DD69C33F80AFA8FC5E86072618775FF3C0B9EA2314C"
                    "9C256576D674DF7496EA81D3383B4813D692C6E0E0D5D8E250B98BE4"
                    "8E495C1D6089DAD15DC7D7B46154D6B6CE8EF4AD69B15D4982559B29"
                    "7BCF1885C529F566660E57EC68EDBC3C05726CC02FD4CBF4976EAA9A"
                    "FD5138FE8376435B9FC61D2FC0EB06E3")

    defaultGenerator = "2"

    def __init__(self, prime=defaultPrime, generator=defaultGenerator):
        # type: (str, str) -> None
        """Create a RemoteGroup.

        :param prime: Prime string.
        :param generator: Generator.
        """
        self.__prime = fromHex(prime)
        self.__generator = fromHex(generator)

        primeBytes = toByteString(self.__prime)
        generatorBytes = toByteString(self.__generator)
        paddingLength = len(primeBytes) - len(generatorBytes)
        paddingBuffer = bytearray(paddingLength)

        md = hashlib.sha1()
        md.update(primeBytes)
        if paddingLength > 0:
            md.update(paddingBuffer)
        md.update(generatorBytes)
        self.__k = fromByteString(bytesToArray(md.digest()))

    def getPrime(self):
        # type: () -> int
        """Return the prime."""
        return self.__prime

    def getGenerator(self):
        # type: () -> int
        """Return the generator."""
        return self.__generator

    def getK(self):
        # type: () -> int
        """Return K."""
        return self.__k


class RemotePassword(object):
    """Manage the remote password."""

    def __init__(self):
        # type: () -> None
        self.__group = RemoteGroup()

    @staticmethod
    def _getUserHash(account, password, salt):
        # type: (str, str, str) -> int
        """Compute a hash from the account, password, and salt."""
        md = hashlib.sha1()
        userInfo = '%s:%s' % (account, password)
        md.update(userInfo.encode('latin-1'))
        hash1 = md.digest()
        md = hashlib.sha1()
        md.update(binascii.a2b_hex(salt))
        md.update(hash1)

        return fromByteString(bytesToArray(md.digest()))

    @staticmethod
    def _computeScramble(clientPublicKey, serverPublicKey):
        # type: (int, int) -> int
        """Compute the scramble given the public keys."""
        md = hashlib.sha1()
        md.update(toByteString(clientPublicKey))
        md.update(toByteString(serverPublicKey))

        return fromByteString(bytesToArray(md.digest()))

    def _getGroup(self):
        # type: () -> RemoteGroup
        """Return the RemoteGroup for this password."""
        return self.__group


class ClientPassword(RemotePassword):
    """Manage the client password."""

    __privateKey = 0
    __publicKey = 0

    def genClientKey(self):
        # type: () -> str
        """Return the client key."""
        group = self._getGroup()

        self.__privateKey = random.getrandbits(256)
        self.__publicKey = pow(group.getGenerator(), self.__privateKey, group.getPrime())

        return toHex(self.__publicKey)

    def computeSessionKey(self, account, password, salt, serverKey):
        # type: (str, str, str, str) -> bytes
        """Compute the session key."""
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
        return toByteString(sessionSecret)


class BaseCipher(object):
    """Base class for ciphers."""

    name = 'invalid'
    keysize = 0

    def _convert_key(self, full):
        # type: (bytes) -> bytes
        """Convert a full key to the size needed for a given cipher."""
        if self.keysize == hashlib.sha256().digest_size:
            md = hashlib.sha256()
        elif self.keysize == hashlib.sha1().digest_size:
            md = hashlib.sha1()
        elif self.keysize == hashlib.md5().digest_size:
            md = hashlib.md5()
        else:
            raise Exception("Invalid key size: %d" % (self.keysize))
        md.update(full)
        return md.digest()

    def transform(self, data):
        # type: (bytes) -> bytes
        """Perform a byte by byte cipher transform on the input."""
        raise NotImplementedError("Invalid cipher")


class NoCipher(BaseCipher):
    """No cipher."""

    name = 'None'

    def transform(self, data):
        # type: (bytes) -> bytes
        """:returns: the input data unchanged."""
        return data


class AESBaseCipher(BaseCipher):
    """An AES cipher object using cryptography."""

    def __init__(self, encrypt, key, nonce):
        # type: (bool, bytes, bytes) -> None
        """Create an AES cipher using the given key and nonce.

        :param encrypt: True if encrypting, False if decrypting
        :param key: The key to initialize from.
        :param nonce: The nonce for the cipher or None to create it
        """
        algo = algorithms.AES(self._convert_key(key))
        cipher = Cipher(algo, mode=modes.CTR(nonce), backend=default_backend())
        self.cipher = cipher.encryptor() if encrypt else cipher.decryptor()

    def transform(self, data):
        # type: (bytes) -> bytes
        """:returns: data transformed by the cipher."""
        return self.cipher.update(data)


class AES256Cipher(AESBaseCipher):
    """An AES-256 cipher object using cryptography."""

    name = 'AES-256'
    keysize = int(256 / 8)


class AES128Cipher(AESBaseCipher):
    """An AES-128 cipher object using cryptography."""

    name = 'AES-128'
    keysize = int(128 / 8)


class RC4CipherNuoDB(BaseCipher):
    """An RC4 cipher object using a native Python algorithm."""

    name = 'RC4-local'
    keysize = int(160 / 8)

    def __init__(self, _, key):
        # type: (bool, bytes) -> None
        """Create an RC4 cipher using the given key.

        This uses a native Python implementation which is sloooooow.
        It will be used if you don't have cryptography installed.
        Encryption and decryption use the same algorithm.

        :param encrypt: True if encrypting, False if decrypting
        :param key: The cipher key.
        """
        super(RC4CipherNuoDB, self).__init__()

        self.__state = list(range(256))
        self.__idx1 = 0
        self.__idx2 = 0

        state = self.__state

        data = bytesToArray(self._convert_key(key))
        sz = len(data)
        j = 0
        for i in range(256):
            val = data[i % sz]
            j = (j + state[i] + val) % 256
            state[i], state[j] = state[j], state[i]

    def transform(self, data):
        # type: (bytes) -> bytes
        """Perform a byte by byte RC4 transform on the stream.

        Python 2:
            automatically handles encoding bytes into an extended ASCII
            encoding [0,255] w/ 1 byte per character

        Python 3:
            bytes objects must be converted into extended ASCII, latin-1 uses
            the desired range of [0,255]

        For utf-8 strings (characters consisting of more than 1 byte) the
        values are broken into 1 byte sections and shifted.  The RC4 stream
        cipher processes 1 byte at a time, as does ord when converting
        character values to integers.

        :param data: Data to be transformed.
        :returns: Transformed data.
        """
        transformed = bytearray()
        state = self.__state

        for char in bytesToArray(data):
            self.__idx1 = (self.__idx1 + 1) % 256
            self.__idx2 = (self.__idx2 + state[self.__idx1]) % 256
            state[self.__idx1], state[self.__idx2] = state[self.__idx2], state[self.__idx1]
            cipherByte = char ^ state[(state[self.__idx1] + state[self.__idx2]) % 256]
            transformed.append(cipherByte)
        return bytes(transformed)


class RC4CipherCryptography(BaseCipher):
    """An RC4 cipher object using cryptography."""

    name = 'RC4'
    keysize = int(160 / 8)

    def __init__(self, encrypt, key):
        # type: (bool, bytes) -> None
        """Create an RC4 cipher using the given key.

        :param encrypt: True if encrypting, False if decrypting
        :param key: The key to initialize from.
        """
        algo = ARC4(self._convert_key(key))

        # There's a bug in older versions of mypy where they don't infer the
        # optionality of mode correctly.
        # https://github.com/pyca/cryptography/issues/9464
        cipher = Cipher(algo, mode=None,  # type: ignore
                        backend=default_backend())
        self.cipher = cipher.encryptor() if encrypt else cipher.decryptor()

    def transform(self, data):
        # type: (bytes) -> bytes
        """:returns: data transformed by the cipher."""
        return self.cipher.update(data)


if arc4Imported:
    RC4Cipher = RC4CipherCryptography
else:
    RC4Cipher = RC4CipherNuoDB  # type: ignore
