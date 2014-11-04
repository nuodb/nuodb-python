
__all__ = [ "checkForError", "SessionException", "Session", "SessionMonitor", "BaseListener" ]

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
# Quiescing a database:
#
#   s = Session('localhost', service='ChorusManagement')
#   s.authorize('admin', 'bird')
#   s.doConnect(attributes={"Database" : 'db', "Action" : DatabaseAction.Quiesce})
#   checkForError(s.recv())
#   s.close()
#
# For both doRequest() and doConnect() the attributes parameter is a map from
# attribute key name to value (its a map so that the same attribute key isn't
# used more than once).  The children parameter is a (possibly empty) list of
# ElementTree instances.
#
# For more examples of how to use this module, see the functions in the util
# module which use this directly. For an example of how to communicate with
# directly with an engine see the sql module.

from crypt import ClientPassword, RC4Cipher

import socket
import string
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


class Session:

    __AUTH_REQ = "<Authorize TargetService=\"%s\" Type=\"SRP\"/>"
    __SRP_REQ = "<SRPRequest ClientKey=\"%s\" Cipher=\"RC4\" Username=\"%s\"/>"

    __SERVICE_REQ = "<Request Service=\"%s\"%s/>"
    __SERVICE_CONN = "<Connect Service=\"%s\"%s/>"

    def __init__(self, host, port=None, service="Identity"):
        if not port:
            hostElements = host.split(":")
            if len(hostElements) == 2:
                host = hostElements[0]
                port = int(hostElements[1])
            else:
                port = 48004

        self.__address = host
        self.__port = port

        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.__sock.connect((host, port))

        self.__cipherOut = None
        self.__cipherIn = None

        self.__service = service

    @property
    def address(self):
        return self.__address

    @property
    def port(self):
        return self.__port

    # NOTE: This routine works only for agents ... see the sql module for a
    # still-in-progress example of opening an authorized engine session
    def authorize(self, account="domain", password=None):
        if not password:
            raise SessionException("A password is required for authorization")

        cp = ClientPassword()
        key = cp.genClientKey()

        self.send(Session.__AUTH_REQ % self.__service)
        response = self.__sendAndReceive(Session.__SRP_REQ % (key, account))

        root = ElementTree.fromstring(response)
        if root.tag != "SRPResponse":
            self.close()
            raise SessionException("Request for authorization was denied")

        salt = root.get("Salt")
        serverKey = root.get("ServerKey")
        sessionKey = cp.computeSessionKey(account, password, salt, serverKey)

        self._setCiphers(RC4Cipher(sessionKey), RC4Cipher(sessionKey))

        verifyMessage = self.recv()
        try:
            root = ElementTree.fromstring(verifyMessage)
        except Exception as e:
            self.close()
            raise SessionException("Failed to establish session with password: " + str(e)), None, sys.exc_info()[2]

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
        if not self.__sock:
            raise SessionException("Session is not open to send")

        if self.__cipherOut:
            message = self.__cipherOut.transform(message)

        lenStr = struct.pack("!I", len(message))

        try:
            self.__sock.send(lenStr + message)
        except Exception:
            self.close()
            raise

    def recv(self, doStrip=True):
        if not self.__sock:
            raise SessionException("Session is not open to receive")

        try:
            lengthHeader = self.__readFully(4)
            msgLength = int(struct.unpack("!I", lengthHeader)[0])
            
            msg = self.__readFully(msgLength)

        except Exception:
            self.close()
            raise

        if self.__cipherIn:
            if doStrip:
                msg = string.strip(self.__cipherIn.transform(msg))
            else:
                msg = self.__cipherIn.transform(msg)

        return msg


    def __readFully(self, msgLength):
        msg = ""
        
        while msgLength > 0:
            received = self.__sock.recv(msgLength)

            if not received:
                raise SessionException("Session was closed while receiving msgLength=[%d] len(msg)=[%d] "
                                       "len(received)=[%d]" % (msgLength, len(msg), len(received)))

            msg = msg + received
            msgLength = msgLength - len(received)

        return msg

    def close(self, force=False):
        if not self.__sock:
            return

        try:
            if force:
                self.__sock.shutdown(socket.SHUT_RDWR)

            if self.__sock:
                self.__sock.close()
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
            except:
                # the session was closed out from under us
                break

            try:
                root = ElementTree.fromstring(message)
            except:
                if self.__listener:
                    try:
                        self.__listener.invalid_message(message)
                    except:
                        pass
            else:
                if self.__listener:
                    try:
                        self.__listener.message_received(root)
                    except:
                        pass

        try:
            self.close()
        except:
            pass

    def close(self):
        if self.__listener:
            try:
                self.__listener.closed()
            except:
                pass
            self.__listener = None
        self.__session.close(force=True)


class BaseListener:

    def message_received(self, root):
        pass

    def invalid_message(self, message):
        pass

    def closed(self):
        pass
