"""Establish and manage a SQL session with a NuoDB database.

(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

__all__ = ["checkForError", "SessionException", "Session"]

# This module abstracts the common functionaliy needed to establish a session
# with an agent or engine. It separates incoming and outgoing stream handling,
# optionally with encryption, and correctly encodes and re-assembles messages
# based on their legnth header.

import socket
import struct
import sys
from ipaddress import ip_address
import xml.etree.ElementTree as ET

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse  # type: ignore

try:
    from typing import Dict, Generator, Iterable, Mapping  # pylint: disable=unused-import
    from typing import Optional, Tuple, Union  # pylint: disable=unused-import
except ImportError:
    pass

from .exception import Error, OperationalError, InterfaceError
from . import crypt

isP2 = sys.version[0] == '2'

NUODB_PORT = 48004


class SessionException(OperationalError):  # pylint: disable=too-many-ancestors
    """Raised for problems encountered with the network session.

    It's unfortunate that we invented this exception, but it may be widely
    used now.  Make it a subclass of the OperationalError exception.
    """

    pass


def checkForError(message):
    # type: (str) -> None
    """Check a result XML string for errors.

    :param message: The message to be checked.
    :raises ET.ParseError: If the message is invalid XML.
    :raises SessionException: If the message is an error result.
    """
    root = ET.fromstring(message)
    if root.tag == "Error":
        raise SessionException(root.get("text", "Unknown Error"))


def strToBool(s):
    # type: (str) -> bool
    """Convert a database boolean value to a Python boolean.

    :param s: Value to convert
    :returns: True if the value is true, False if it's false
    :raises ValueError: If the value is not a valid boolean string.
    """
    if s.lower() == 'true':
        return True
    elif s.lower() == 'false':
        return False
    raise ValueError('"%s" is not a valid boolean string' % s)


class Session(object):
    """A NuoDB service session (either AP or Engine)."""

    __SERVICE_CONN = "<Connect Service=\"%s\"%s/>"
    __SERVICE_REQ = "<Request Service=\"%s\"%s/>"
    __AUTH_REQ = "<Authorize TargetService=\"%s\" Type=\"SRP\"/>"
    __SRP_REQ = '<SRPRequest ClientKey="%s" Ciphers="%s" Username="%s"/>'

    __isTLSEncrypted = False
    __cipherOut = None   # type: crypt.BaseCipher
    __cipherIn = None    # type: crypt.BaseCipher

    __port = NUODB_PORT  # type: int
    __sock = None        # type: Optional[socket.socket]

    __xml_encoding = 'utf-8' if isP2 else 'unicode'

    @property
    def _sock(self):
        # type: () -> socket.socket
        """Return the socket: raise if it's closed."""
        sock = self.__sock
        if sock is None:
            raise SessionException("Session is closed")
        return sock

    def __init__(self, host,            # type: str
                 port=None,             # type: Optional[int]
                 service="SQL2",        # type: str
                 timeout=None,          # type: Optional[float]
                 connect_timeout=None,  # type: Optional[float]
                 read_timeout=None,     # type: Optional[float]
                 options=None           # type: Optional[Mapping[str, str]]
                 ):
        # type: (...) -> None
        if options is None:
            options = {}

        self.__address, _port, ver = self._parse_addr(host, options.get('ipVersion'))
        if port is not None:
            self.__port = port
        elif _port is not None:
            self.__port = _port

        af = socket.AF_INET
        if ver == 6:
            af = socket.AF_INET6

        # for backwards-compatibility, set connect and read timeout to
        # `timeout` if either is not specified
        if connect_timeout is None:
            connect_timeout = timeout
        if read_timeout is None:
            read_timeout = timeout

        self.__service = service

        self._open_socket(connect_timeout, self.__address, self.__port, af, read_timeout)

        if options.get('trustStore') is not None:
            # We have to have a trustStore parameter to enable TLS
            try:
                self.establish_secure_tls_connection(options)
            except socket.error:
                if strToBool(options.get('allowSRPFallback', "False")):
                    # fall back to SRP, do not attempt to TLS handshake
                    self.close()
                    self._open_socket(connect_timeout, self.__address,
                                      self.__port, af, read_timeout)
                else:
                    raise

    @staticmethod
    def session_options(options):
        # type: (Optional[Mapping[str, str]]) -> Tuple[Dict[str, str], Dict[str, str]]
        """Split into connection parameters and session options.

        Connection parameters are passed to the SQL server to control the
        connection.  Session options are not sent to the SQL server, and
        instead control the local session.

        :return: A tuple of (connection parameters, session options).
        """
        opts = ['password', 'user', 'ipVersion', 'direct', 'allowSRPFallback',
                'trustStore', 'sslVersion', 'verifyHostname']
        session = {}
        parameters = {}
        if options:
            for key, val in options.items():
                if key in opts:
                    session[key] = val
                else:
                    parameters[key] = val
        return parameters, session

    @staticmethod
    def _to_ipaddr(addr):
        # type: (str) -> Tuple[str, int]
        if isP2 and not isinstance(addr, unicode):  # type: ignore
            ipaddr = ip_address(unicode(addr, 'utf_8'))  # type: ignore
        else:
            ipaddr = ip_address(addr)
        return (str(ipaddr), ipaddr.version)

    def _parse_addr(self, addr, ipver):
        # type: (str, Optional[str]) -> Tuple[str, Optional[int], int]
        port = None
        try:
            # v4/v6 addr w/o port e.g. 192.168.1.1, 2001:3200:3200::10
            ip, ver = self._to_ipaddr(addr)
        except ValueError:
            # v4/v6 addr w/port e.g. 192.168.1.1:53, [2001::10]:53
            parsed = urlparse('//{}'.format(addr))
            if parsed.hostname is None:
                raise InterfaceError("Invalid Host/IP Address format: %s" % (addr))
            try:
                ip, ver = self._to_ipaddr(parsed.hostname)
                port = parsed.port
            except ValueError:
                parts = addr.split(":")
                if len(parts) == 1:
                    # hostname w/o port e.g. ad0
                    ip = addr
                elif len(parts) == 2:
                    # hostname with port e.g. ad0:53
                    ip = parts[0]
                    try:
                        port = int(parts[1])
                    except ValueError:
                        raise InterfaceError("Invalid Host/IP Address Format %s" % addr)
                else:
                    # failed
                    raise InterfaceError("Invalid Host/IP Address Format %s" % addr)

                # select v6/v4 for hostname based on user option
                ver = 4
                if ipver == 'v6':
                    ver = 6

        return ip, port, ver

    def _open_socket(self, connect_timeout, host, port, af, read_timeout):
        # type: (Optional[float], str, int, int, Optional[float]) -> None
        assert self.__sock is None, "Open called with already open socket"
        self.__sock = socket.socket(af, socket.SOCK_STREAM)
        # disable Nagle's algorithm
        self.__sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # separate connect and read timeout; we do not necessarily want to
        # close out connection if reads block for a long time, because it could
        # take a while for the server to generate data to send
        self.__sock.settimeout(connect_timeout)
        self.__sock.connect((host, port))
        self.__sock.settimeout(read_timeout)

    def establish_secure_tls_connection(self, options):
        # type: (Mapping[str, str]) -> None
        """Establish a TLS connection to the service.

        :raises: RuntimeError if the Python SSL module is not available.
        """
        try:
            import ssl
        except ImportError:
            raise RuntimeError("SSL requested but the ssl module not available")

        sslcontext = ssl.SSLContext(int(options.get('sslVersion', ssl.PROTOCOL_TLSv1_2)))
        sslcontext.options |= ssl.OP_NO_SSLv2
        sslcontext.options |= ssl.OP_NO_SSLv3
        sslcontext.verify_mode = ssl.CERT_REQUIRED
        sslcontext.check_hostname = strToBool(options.get('verifyHostname', "True"))

        sslcontext.load_verify_locations(options['trustStore'])

        self.__sock = sslcontext.wrap_socket(self._sock, server_hostname=self.__address)
        self.__isTLSEncrypted = True

    @property
    def tls_encrypted(self):
        # type: () -> bool
        """Return True if the session is encrypted with TLS."""
        return self.__isTLSEncrypted

    @property
    def address(self):
        # type: () -> str
        """Return the address of the service."""
        return self.__address

    @property
    def port(self):
        # type: () -> int
        """Return the port of the service."""
        return self.__port

    def _setCiphers(self, cipherIn, cipherOut):
        # type: (crypt.BaseCipher, crypt.BaseCipher) -> None
        """Set the input and output cipher implementations."""
        self.__cipherIn = cipherIn
        self.__cipherOut = cipherOut

    def authorize(self, account, dbpassword, cipher='RC4'):
        # type: (str, str, str) -> None
        """Authorize this session.

        You can only use this if you know the database password, and this is
        only available from the AP.
        """
        req = Session.__AUTH_REQ % (self.__service)
        self.send(req.encode())

        cp = crypt.ClientPassword()
        key = cp.genClientKey()
        req = Session.__SRP_REQ % (key, cipher, account)
        response = self.__sendAndReceive(req.encode())

        try:
            root = ET.fromstring(response.decode())
            if root.tag != "SRPResponse":
                raise InterfaceError("Request for authorization was denied")

            salt = root.get("Salt")
            if salt is None:
                raise SessionException("Malformed authorization response (salt)")
            serverKey = root.get("ServerKey")
            if serverKey is None:
                raise SessionException("Malformed authorization response (server key)")

            sessionKey = cp.computeSessionKey(account, dbpassword, salt, serverKey)

            serverCipher = root.get("Cipher")
            if serverCipher == 'None':
                self._setCiphers(crypt.NoCipher(), crypt.NoCipher())
            else:
                self._setCiphers(crypt.RC4Cipher(sessionKey),
                                 crypt.RC4Cipher(sessionKey))

            verifyMessage = self.recv()
            if verifyMessage is None:
                raise SessionException("Failed to establish session (no verification)")
            try:
                root = ET.fromstring(verifyMessage.decode())
            except Exception as e:
                raise SessionException("Failed to establish session with password: " + str(e))

            if root.tag != "PasswordVerify":
                raise SessionException("Unexpected verification response: " + root.tag)
        except Error:
            self.close()
            raise

        self.send(verifyMessage)

    def doConnect(self, attributes=None,  # type: Optional[Mapping[str, str]]
                  text=None,              # type: Optional[str]
                  children=None           # type: Optional[Iterable[ET.Element]]
                  ):
        # type: (...) -> None
        """Connect to the service."""
        connectStr = self.__constructServiceMessage(
            Session.__SERVICE_CONN, attributes, text, children)

        try:
            self.send(connectStr.encode())
        except Exception:
            self.close()
            raise

    def doRequest(self, attributes=None,  # type: Optional[Dict[str, str]]
                  text=None,              # type: Optional[str]
                  children=None           # type: Optional[Iterable[ET.Element]]
                  ):
        # type: (...) -> str
        """Ask the service to execute a request and return the response.

        Issues the request, closes the session and returns the response
        string, or raises an exeption if the session fails or the response is
        an error.
        """
        requestStr = self.__constructServiceMessage(
            Session.__SERVICE_REQ, attributes, text, children)

        try:
            response = self.__sendAndReceive(requestStr.encode()).decode()
            checkForError(response)
            return response
        finally:
            self.close()

    def __constructServiceMessage(self,
                                  template,  # type: str
                                  attrs,     # type: Optional[Mapping[str, str]]
                                  text,      # type: Optional[str]
                                  children   # type: Optional[Iterable[ET.Element]]
                                  ):
        # type: (...) -> str
        """Create an XML service message and return it."""
        attributeString = ""
        if attrs:
            for (key, value) in attrs.items():
                attributeString += ' %s="%s"' % (key, value)

        message = template % (self.__service, attributeString)

        if children or text:
            root = ET.fromstring(message)

            if text:
                root.text = text

            if children:
                for child in children:
                    root.append(child)

            message = ET.tostring(root, encoding=self.__xml_encoding)

        return message

    def send(self, message):
        # type: (Union[str, bytes, bytearray]) -> None
        """Send an encoded message to the server over the socket.

        The message to be sent is either already-encoded bytes or bytearray,
        or it's a UTF-8 str.
        """
        sock = self._sock

        if isinstance(message, bytearray):
            data = bytes(message)
        elif isinstance(message, bytes) or isP2:
            data = message  # type: ignore
        elif isinstance(message, str):
            data = message.encode('utf-8')
        else:
            raise SessionException("Invalid message type: %s" % (type(message)))

        if self.__cipherOut:
            data = self.__cipherOut.transform(data)

        lenbuf = struct.pack("!I", len(data))
        # It should be possible to send the length followed by the data so we
        # don't have to reallocate this entire buffer, but it is unreliable.
        buf = lenbuf + data
        view = memoryview(buf)
        start = 0
        left = len(buf)

        try:
            while left > 0:
                sent = sock.send(view[start:left])
                start += sent
                left -= sent
        except Exception:
            self.close()
            raise

    def recv(self, timeout=None):
        # type: (Optional[float]) -> Optional[bytes]
        """Pull the next message from the socket.

        If timeout is None, wait forever (until read_timeout, if set).
        If timeout is a float, then set this timeout for this recv().
        On timeout, return None but do not close the connection.
        """
        try:
            # We only wait on timeout to read the header.  Once we read
            # a header we'll wait as long as it takes to read the data.
            lengthHeader = self.__readFully(4, timeout=timeout)
            if lengthHeader is None:
                # This can only happen if the recv timed out
                return None
            msgLength = int(struct.unpack("!I", lengthHeader)[0])
            msg = self.__readFully(msgLength)
        except Exception:
            self.close()
            raise

        if msg is None:
            # This can't happen because we don't set a timeout above
            raise RuntimeError("Session.recv read no data!")

        if self.__cipherIn:
            msg = self.__cipherIn.transform(msg)

        return msg

    def __readFully(self, msgLength, timeout=None):
        # type: (int, Optional[float]) -> Optional[bytes]
        """Pull the entire next raw bytes message from the socket."""
        sock = self._sock
        msg = bytearray()
        old_tmout = sock.gettimeout()
        while msgLength > 0:
            if timeout is not None:
                # It's a little wrong that this timeout applies to each recv()
                # instead of to the entire operation; however we only use this
                # when reading the header which will always be read in one
                # pass anyway.
                sock.settimeout(timeout)
            try:
                received = sock.recv(msgLength)
            except socket.timeout:
                return None
            except IOError as e:
                raise SessionException(
                    "Session closed while receiving: network error %s: %s" %
                    (str(e.errno), e.strerror if e.strerror else str(e.args)))
            finally:
                if timeout is not None:
                    sock.settimeout(old_tmout)

            if not received:
                raise SessionException(
                    "Session closed waiting for data: wanted length=%d,"
                    " received length=%d"
                    % (msgLength, len(msg)))
            msg += received
            msgLength -= len(received)

        return bytes(msg)

    def stream_recv(self, blocksz=4096, timeout=None):
        # type: (int, Optional[float]) -> Generator[bytes, None, None]
        """Read data from the socket in blocksz increments.

        Will yield bytes buffers of blocksz for as long as the sender is
        sending.  After this function completes the socket has been closed.
        Note it's best if blocksz is a multiple of 32, to ensure that block
        ciphers will work.  This code doesn't manage block sizes or padding.

        If timeout is not None, raises a socket.timeout exception on timeout.
        The socket is still closed.
        """
        sock = self._sock
        try:
            sock.settimeout(timeout)
            while True:
                msg = sock.recv(blocksz)
                if not msg:
                    break
                if self.__cipherIn:
                    msg = self.__cipherIn.transform(msg)
                yield msg
        finally:
            self.close()

    def close(self, force=False):
        # type: (bool) -> None
        """Close the current socket connection with the server."""
        sock = self.__sock
        if sock is None:
            return
        try:
            if force:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except (OSError, socket.error):
                    # On MacOS this can raise "Socket is not connected"
                    pass
            sock.close()
        finally:
            self.__sock = None

    def __sendAndReceive(self, message):
        # type: (bytes) -> bytes
        """Send one message and return the response."""
        self.send(message)
        resp = self.recv()
        if resp is None:
            # This can't actually happen since we have no timeout on recv()
            raise RuntimeError("Session.recv() returned None!")
        return resp
