"""Establish and manage a session with an agent or engine.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

__all__ = ["checkForError", "SessionException", "Session", "SessionMonitor", "BaseListener"]

# This module abstracts the common functionaliy needed to establish a session
# with an agent or engine. It separates incoming and outgoing stream handling,
# optionally with encryption, and correctly encodes and re-assembles messages
# based on their legnth header.
#
# A Session can either be constructed and then called directly to send or
# receieve messages, or can negotiate with a service using the utility
# connect/request routines. For exchanges of more than single request and
# response the SessionMonitor class is provided to notify the user when
# messages arrive.
#
# Getting the local agent identity:
#
#   print 'Local agent identity: ' + Session('localhost').doRequest()
#
# Getting the local agent state:
#
#   s = Session('localhost', service='State')
#   s.authorize('admin', 'bird')
#   print s.doRequest()
#
# For both doRequest() and doConnect() the attributes parameter is a map from
# attribute key name to value (its a map so that the same attribute key isn't
# used more than once).  The children parameter is a (possibly empty) list of
# ElementTree instances.
#
# For more examples of how to use this module, see the functions in the util
# module which use this directly. For an example of how to communicate with
# directly with an engine see the sql module.


from .crypt import ClientPassword, RC4Cipher, NoCipher

from ipaddress import ip_address

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import socket
import struct
import threading
import sys
import xml.etree.ElementTree as ElementTree


# A simple but commonly-used routine that raises a "useful" exception if the
# message is invalid XML, or is valid XML and has the root element "Error"
def checkForError(message):
    root = ElementTree.fromstring(message)
    if root.tag == "Error":
        raise SessionException(root.get("text"))


class SessionException(Exception):
    def __init__(self, value):
        self.__value = value

    def __str__(self):
        return repr(self.__value)


def strToBool(s):
    if s.lower() == 'true':
        return True
    elif s.lower() == 'false':
        return False
    else:
        raise ValueError('"%s" is not a valid boolean string' % s)


class Session(object):

    __AUTH_REQ = "<Authorize TargetService=\"%s\" Type=\"SRP\"/>"
    __SRP_REQ = '<SRPRequest ClientKey="%s" Ciphers="%s" Username="%s"/>'

    __SERVICE_REQ = "<Request Service=\"%s\"%s/>"
    __SERVICE_CONN = "<Connect Service=\"%s\"%s/>"

    def __init__(self, host, port=None, service="Identity", timeout=None,
                 connect_timeout=None, read_timeout=None, options=None):

        self.__address, prt, ver = self._parse_addr(host, options)

        if port is None:
            if prt is None:
                self.__port = 48004
            else:
                self.__port = prt
            port = self.__port
        else:
            self.__port = port

        af = socket.AF_INET
        if ver == 6:
            af = socket.AF_INET6

        self.__isTLSEncrypted = False

        # for backwards-compatibility, set connect and read timeout to
        # `timeout` if either is not specified
        if connect_timeout is None:
            connect_timeout = timeout
        if read_timeout is None:
            read_timeout = timeout

        self.__cipherOut = None
        self.__cipherIn = None

        self.__service = service

        self.__pyversion = sys.version[0]

        self.__sock = None

        self._open_socket(connect_timeout, self.__address, self.__port, af, read_timeout)

        (_, tls_options) = self._split_options(options)

        if tls_options:
            try:
                self.establish_secure_tls_connection(tls_options)
            except socket.error:
                if strToBool(tls_options.get('allowSRPFallback', "False")):
                    # fall back to SRP, do not attempt to TLS handshake
                    self.close()
                    self._open_socket(connect_timeout, self.__address,
                                      self.__port, af, read_timeout)
                else:
                    raise

    @staticmethod
    def _extract_options(options):
        expected = ['password', 'user', 'trustStore', 'verifyHostname',
                    'allowSRPFallback', 'ciphers', 'ipVersion', 'direct']
        remote_options = {}
        extracted_options = {}
        if options:
            for (k, v) in options.items():
                if k in expected:
                    extracted_options[k] = v
                else:
                    remote_options[k] = v

        return remote_options, extracted_options

    @staticmethod
    def _split_options(options):
        expected = ['trustStore', 'verifyHostname', 'allowSRPFallback']
        remote_options = {}
        tls_options = {}
        if options:
            for (k, v) in options.items():
                if k in expected:
                    tls_options[k] = v
                else:
                    remote_options[k] = v

        return remote_options, tls_options

    @staticmethod
    def _to_ipaddr(addr):
        if sys.version_info >= (3, 0):
            ip = ip_address(str(addr))
        else:
            ip = ip_address(unicode(addr, 'utf_8'))
        return ip

    def _parse_addr(self, addr, options):
        try:
            # v4/v6 addr w/o port e.g. 192.168.1.1, 2001:3200:3200::10
            ip = self._to_ipaddr(addr)
            port = None
            ver = ip.version
        except ValueError:
            # v4/v6 addr w/port e.g. 192.168.1.1:53, [2001::10]:53
            parsed = urlparse('//{}'.format(addr))
            try:
                ip = self._to_ipaddr(parsed.hostname)
                port = parsed.port
                ver = ip.version
            except ValueError:
                parts = addr.split(":")
                if len(parts) == 1:
                    # hostname w/o port e.g. ad0
                    ip = addr
                    port = None
                elif len(parts) == 2:
                    # hostname with port e.g. ad0:53
                    ip = parts[0]
                    try:
                        port = int(parts[1])
                    except ValueError:
                        raise SessionException("Invalid Host/IP Address Format %s" % addr)
                else:
                    # failed
                    raise SessionException("Invalid Host/IP Address Format %s" % addr)

                # select v6/v4 for hostname based on user option
                ver = 4
                if options is not None and 'ipVersion' in options:
                    if options['ipVersion'] == 'v6':
                        ver = 6

        return str(ip), port, ver

    def _open_socket(self, connect_timeout, host, port, af, read_timeout):
        self.__sock = socket.socket(af, socket.SOCK_STREAM)
        # disable Nagle's algorithm
        self.__sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # separate connect and read timeout; we do not necessarily want to
        # close out connection if reads block for a long time, because it could
        # take a while for the server to generate data to send
        self.__sock.settimeout(connect_timeout)
        self.__sock.connect((host, port))
        self.__sock.settimeout(read_timeout)

    def establish_secure_tls_connection(self, tls_options):
        try:
            import ssl
            sslcontext = ssl.SSLContext(tls_options.get('sslVersion', ssl.PROTOCOL_TLSv1_2))
            sslcontext.options |= ssl.OP_NO_SSLv2
            sslcontext.options |= ssl.OP_NO_SSLv3
            sslcontext.verify_mode = ssl.CERT_REQUIRED
            sslcontext.check_hostname = strToBool(tls_options.get('verifyHostname', "True"))

            sslcontext.load_verify_locations(tls_options['trustStore'])

            self.__sock = sslcontext.wrap_socket(self.__sock, server_hostname=self.__address)
            self.__isTLSEncrypted = True

        except ImportError:
            raise RuntimeError("SSL required but ssl module not available in this python installation")

    @property
    def tls_encrypted(self):
        return self.__isTLSEncrypted

    @property
    def address(self):
        return self.__address

    @property
    def port(self):
        return self.__port

    # NOTE: This routine works only for agents ... see the sql module for a
    # still-in-progress example of opening an authorized engine session
    def authorize(self, account="domain", password=None, cipher='RC4'):
        if not password:
            raise SessionException("A password is required for authorization")

        cp = ClientPassword()
        key = cp.genClientKey()
        self.send(Session.__AUTH_REQ % self.__service)
        response = self.__sendAndReceive(Session.__SRP_REQ % (key, cipher, account))

        root = ElementTree.fromstring(response)
        if root.tag != "SRPResponse":
            self.close()
            raise SessionException("Request for authorization was denied")

        salt = root.get("Salt")
        serverKey = root.get("ServerKey")
        sessionKey = cp.computeSessionKey(account, password, salt, serverKey)

        cipher = root.get("Cipher")
        if cipher == 'None':
            self._setCiphers(NoCipher(), NoCipher())
        else:
            self._setCiphers(RC4Cipher(sessionKey), RC4Cipher(sessionKey))

        verifyMessage = self.recv()
        try:
            root = ElementTree.fromstring(verifyMessage)
        except Exception as e:
            self.close()
            raise SessionException("Failed to establish session with password: " + str(e))

        if root.tag != "PasswordVerify":
            self.close()
            raise SessionException("Unexpected verification response: " + root.tag)

        self.send(verifyMessage)

    def _setCiphers(self, cipherIn, cipherOut):
        self.__cipherIn = cipherIn
        self.__cipherOut = cipherOut

    # Issues the request, closes the session and returns the response string,
    # or raises an exeption if the session fails or the response is an error.
    def doRequest(self, attributes=None, text=None, children=None):
        requestStr = self.__constructServiceMessage(Session.__SERVICE_REQ, attributes, text, children)

        try:
            response = self.__sendAndReceive(requestStr)
            checkForError(response)

            return response
        finally:
            self.close()

    def doConnect(self, attributes=None, text=None, children=None):
        connectStr = self.__constructServiceMessage(Session.__SERVICE_CONN, attributes, text, children)

        try:
            self.send(connectStr)
        except Exception:
            self.close()
            raise

    def __constructServiceMessage(self, template, attributes, text, children):
        attributeString = ""
        if attributes:
            for (key, value) in attributes.items():
                attributeString += " " + key + "=\"" + value + "\""

        message = template % (self.__service, attributeString)

        if children or text:
            root = ElementTree.fromstring(message)

            if text:
                root.text = text

            if children:
                for child in children:
                    root.append(child)

            message = ElementTree.tostring(root)

        return message

    def send(self, message):
        """ Send an encoded message to the server over the socket """
        if not self.__sock:
            raise SessionException("Session is not open to send")

        if self.__cipherOut:
            message = self.__cipherOut.transform(message)

        if self.__pyversion == '3':
            message = bytes(message, 'latin-1')

        lenStr = struct.pack("!I", len(message))

        try:
            self.__sock.send(lenStr + message)
        except Exception:
            self.close()
            raise

    def recv(self, doStrip=True, timeout=None):
        """Pull the next message from the socket and decode/trim it if needed

        If timeout is None, wait forever (until read_timeout, if set).
        If timeout is a float, then set this timeout for this recv().
        If the timeout is hit return None and do not close the connection.
        """
        if not self.__sock:
            raise SessionException("Session is not open to receive")

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

        if self.__cipherIn:
            if doStrip:
                msg = self.__cipherIn.transform(msg).lstrip()
            else:
                msg = self.__cipherIn.transform(msg)

        if type(msg) is bytes and self.__pyversion == '3':
            msg = msg.decode("latin-1")
        return msg

    def __readFully(self, msgLength, timeout=None):
        """ Pull the entire next raw bytes message from the socket """
        msg = b''
        old_tmout = self.__sock.gettimeout()
        while msgLength > 0:
            if timeout is not None:
                # It's a little wrong that this timeout applies to each recv()
                # instead of to the entire operation; however we only use this
                # when reading the header which will always be read in one
                # pass anyway.
                self.__sock.settimeout(timeout)
            try:
                received = self.__sock.recv(msgLength)
            except socket.timeout:
                return None
            except IOError as e:
                raise SessionException("Session was closed while receiving: network error %s: %s" %
                                       (str(e.errno), e.strerror if e.strerror else str(e.args)))
            finally:
                if timeout is not None:
                    self.__sock.settimeout(old_tmout)

            if not received:
                raise SessionException("Session was closed while receiving msgLength=[%d] "
                                       "len(msg)=[%d] len(received)=[%d]" %
                                       (msgLength, len(msg), len(received)))
            if self.__pyversion == '3':
                msg = b''.join([msg, received])
                msgLength = msgLength - len(received.decode('latin-1'))
            else:
                msg = msg + received
                msgLength = msgLength - len(received)
        return msg

    def close(self, force=False):
        """ Close the current socket connection with the server """
        # The SessionMonitor thread also calls close() which resets
        # self.__sock to None, so cache the value here.
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
        self.send(message)
        return self.recv()


class SessionMonitor(threading.Thread):

    def __init__(self, session, listener=None):
        threading.Thread.__init__(self)

        self.__session = session
        self.__listener = listener

    def run(self):
        while True:
            try:
                message = self.__session.recv()
            except Exception:
                # the session was closed out from under us
                break

            try:
                root = ElementTree.fromstring(message)
            except Exception:
                if self.__listener:
                    try:
                        self.__listener.invalid_message(message)
                    except Exception:
                        pass
            else:
                if self.__listener:
                    try:
                        self.__listener.message_received(root)
                    except Exception:
                        pass

        try:
            self.close()
        except Exception:
            pass

    def close(self):
        if self.__listener:
            try:
                self.__listener.closed()
            except Exception:
                pass
            self.__listener = None
        self.__session.close(force=True)


class BaseListener(object):

    def message_received(self, root):
        pass

    def invalid_message(self, message):
        pass

    def closed(self):
        pass
