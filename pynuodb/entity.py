
__all__ = [ "Domain", "Peer", "Database", "Process" ]

""" This module provides basic "entity" support, similar to what's available
in the com.nuodb.entity Java package. A Domain instance provides entry into
a domain, and optionally a hook for getting called back when domain-level
events happen. The Domain provides access to Peers, Databases and Processes.
"""

from session import BaseListener, Session, SessionMonitor, SessionException
from util import ChorusAction, startProcess, killProcess, doChorusAction, queryEngine

import time
import warnings

from threading import Event, Lock
import xml.etree.ElementTree as ElementTree


""" To create a Domain 'connection' you need to give a broker address (a string
which may end in ':PORT') and domain password. You can also supply a class
to notify on domain events. That class may implement any of the methods:
   
    peerJoined(self, peer)
    peerLeft(self, peer)
    processJoined(self, process)
    processLeft(self, process)
    processFailed(self, peer, reason)
    processStatusChanged(self, process, status)
    databaseJoined(self, database)
    databaseLeft(self, database)
    closed(self)
    
The following methods may be used for backwards compatibility, but are 
deprecated. They are identical to the process*() methods. A node*() method
will not be called if it's process*() counterpart is defined. 
    
    nodeJoined(self, node)
    nodeLeft(self, node)
    nodeFailed(self, peer, reason)
    nodeStatusChanged(self, node, status)
    

For instance, a valid listener could be formed like this:

class MyListener():
    def databaseJoined(self, database):
        pass
    def closed(self):
        pass

It would then get used when joining a domain:

 domain = Domain('localhost:48004', 'admin', 'bird', MyListener())

When finished with a Domain, all users must call disconnect() to ensure that
connections are closed and that the listening thread shuts down.

TODO: This class doesn't handle entry broker failure by trying to connect
to another broker. Either this should be added, or some clear exception
should be raised to help the caller make this happen.
"""

def deprecated(func):
    warnings.warn("Function %s is deprecated" % (func.__name__), DeprecationWarning, 2)
    return func


class Domain(BaseListener):

    def __init__(self, brokerAddr, domainUser, domainPwd, listener=None):
        if not domainPwd:
            raise Exception("A password is required to join a domain")

        self.__session = Session(brokerAddr, service="Monitor")
        self.__session.authorize(domainUser, domainPwd)

        self.__user = domainUser
        self.__password = domainPwd
        self.__listener = listener
        self.__peers = dict()
        self.__databases = dict()

        self.__monitor = SessionMonitor(self.__session, self)

        try:
            self.__session.doConnect()
            self.__handleStatus(self.__session.recv())
        except Exception as x:
            self.__monitor.close()
            raise x

        self.__monitor.start()

    def __str__(self):
        return self.getDomainName() + " [Entered through: " + self.getEntryPeer().getConnectStr() + "]"

    def disconnect(self):
        self.__monitor.close()

    def getUser(self):
        return self.__user

    def getPassword(self):
        return self.__password

    def getDomainName(self):
        return self.__domainName

    def getPeer(self, agentId):
        return self.__peers.get(agentId)

    def getPeers(self):
        return self.__peers.itervalues()

    def getEntryPeer(self):
        return self.__entryPeer

    def getDatabase(self, name):
        return self.__databases.get(name)

    def getDatabases(self):
        return self.__databases.itervalues()

    def getDatabaseCount(self):
        return len(self.__databases)

    def shutdown(self, graceful=True):
        for (dbName, db) in self.__databases.items():
            db.shutdown(graceful)

    def messageReceived(self, root):
        if root.tag == "Event":
            eventType = root.get("Type")
            if eventType == "NewBroker":
                self.__peerJoined(Peer.fromMessage(self, root.find("Broker")))
            elif eventType == "BrokerExit":
                self.__peerLeft(Peer.fromMessage(self, root.find("Broker")))
            elif eventType == "StatusChanged":
                status = root.get("Status")

                processElement = root.find("Process")
                db = self.__databases[processElement.get("Database")]
                process = Process.fromMessage(db, processElement)

                self.__processStatusChanged(process, status)
            elif eventType == "ProcessFailed":
                peer = Peer.fromMessage(self, root.find("Broker"))
                peer = self.getPeer(peer.getId())

                reason = root.get("Reason")
                startId = root.get("StartId")

                self.__processFailed(peer, startId, reason)
            elif eventType == "NewProcess" or eventType == "ProcessExit":
                processElement = root.find("Process")
                dbName = processElement.get("Database")

                if dbName not in self.__databases:
                    self.__databases[dbName] = Database(self, dbName)
                    if self.__listener:
                        try:
                            self.__listener.databaseJoined(self.__databases[dbName])
                        except:
                            pass

                if eventType == "NewProcess":
                    startId = processElement.get("StartId")
                    self.__processJoined(Process.fromMessage(self.__databases[dbName],
                                                       processElement), startId)
                else:
                    self.__processLeft(Process.fromMessage(self.__databases[dbName],
                                                     processElement))

    def closed(self):
        if self.__listener:
            try:
                self.__listener.closed()
            except:
                pass

    # NOTE: this is the status provided on initial broker-connection, and not
    # per-process status updates
    def __handleStatus(self, message):
        root = ElementTree.fromstring(message)
        if root.tag != "Status":
            raise Exception("Expected status message; got " + root.tag)

        self.__domainName = root.get("Domain")

        self.__entryPeer = Peer(self, self.__session.getAddress(), root.get("AgentId"),
                                (root.get("Role") == "Broker"), self.__session.getPort(),
                                root.get("Hostname"), root.get("Version"))
        self.__peerJoined(self.__entryPeer)

        for child in list(root):
            if child.tag == "Broker":
                self.__peerJoined(Peer.fromMessage(self, child))

        for child in list(root):
            if child.tag == "Database":
                name = child.get("Name")
                self.__databases[name] = Database(self, name)
                if self.__listener:
                    try:
                        self.__listener.databaseJoined(self.__databases[name])
                    except:
                        pass

                for processElement in list(child):
                    if processElement.tag == "Process":
                        self.__processJoined(Process.fromMessage(self.__databases[name],
                                                           processElement), None)

    def __peerJoined(self, peer):
        self.__peers[peer.getId()] = peer
        if self.__listener:
            try:
                self.__listener.peerJoined(peer)
            except:
                pass

    def __peerLeft(self, peer):
        del self.__peers[peer.getId()]
        if self.__listener:
            try:
                self.__listener.peerLeft(peer)
            except:
                pass

    def __processJoined(self, process, startId):
        process.getDatabase()._addProcess(process)
        process.getPeer()._notifyStartId(startId, process)
        if self.__listener:
            try:
                self.__listener.processJoined(process)
            except:
                try:
                    self.__listener.nodeJoined(process)
                except:
                    pass
    
    def __processLeft(self, process):
        database = process.getDatabase()
        database._removeProcess(process)
        process.getPeer()._removeProcess(process)
        if self.__listener:
            try:
                self.__listener.processLeft(process)
            except:
                try:
                    self.__listener.nodeLeft(process)
                except:
                    pass

        if database.getProcessCount() == 0:
            del self.__databases[database.getName()]
            if self.__listener:
                try:
                    self.__listener.databaseLeft(database)
                except:
                    pass

    def __processFailed(self, peer, startId, reason):
        peer._notifyStartId(startId, reason)
        if self.__listener:
            try:
                self.__listener.processFailed(peer, reason)
            except:
                try:
                    self.__listener.nodeFailed(peer, reason)
                except:
                    pass

    def __processStatusChanged(self, process, status):
        process._setStatus(status)
        if self.__listener:
            try:
                self.__listener.processStatusChanged(process, status)
            except:
                try:
                    self.__listener.nodeStatusChanged(process, status)
                except:
                    pass

    # an initial verison only to support the shutdown routine that doesn't
    # need to watch for return messages ... right now this module is only
    # supporting the tests, which don't need the other management routines
    # at this point, so we'll flesh this out (as in the Java implementation)
    # in the second round when other utilites get updated as well
    def _sendManagementMessage(self, message, peer, process):
        root = ElementTree.fromstring("<ManagementRequest AgentId=\"%s\" ProcessId=\"%i\"/>" % (peer.getId(), process.getPid()))
        root.append(message)

        self.__session.send(ElementTree.tostring(root))

class Peer:

    def __init__(self, domain, address, agentId, broker=False, port=48004, hostname=None, version=None):
        self.__domain = domain
        self.__address = address
        self.__id = agentId
        self.__isBroker = broker
        self.__port = port
        self.__hostname = hostname
        self.__lock = Lock()
        self.__processes = dict()
        self.__version = version
        self.__startIdSlots = dict()

    @staticmethod
    def fromMessage(domain, peerElement):
        return Peer(domain, peerElement.get("Address"), peerElement.get("AgentId"),
                    peerElement.get("Role") == "Broker", peerElement.get("Port"),
                    peerElement.get("Hostname"), peerElement.get("Version"))

    def __hash__(self):
        return self.__id.hash()

    def __eq__(self, other):
        if not other:
            return False
        return self.__id == other.__id

    def __ne__(self, other):
        return self.__eq__(other) != True

    def __str__(self):
        role = "broker" if self.isBroker() else "agent"
        return self.getConnectStr() + " [role=" + role + "]"

    def getDomain(self):
        return self.__domain

    def getAddress(self):
        return self.__address

    def getConnectStr(self):
        return self.__address + ":" + str(self.__port)

    def getPort(self):
        return self.__port

    def getId(self):
        return self.__id

    def getHostname(self):
        return self.__hostname
    
    def getVersion(self):
        return self.__version

    def isBroker(self):
        return self.__isBroker

    def startTransactionEngine(self, db_name, options=None, waitSeconds=None):
        return self.__startProcess(db_name, options, waitSeconds)

    def startStorageManager(self, db_name, archive, initialize, options=None, waitSeconds=None):
        if not options:
            options = []

        options.append(("--archive", archive))

        if initialize:
            options.append(("--initialize", None))
            options.append(("--force", None))

        return self.__startProcess(db_name, options, waitSeconds)


    def __startProcess(self, db_name, options, waitSeconds):
        if waitSeconds == None:
            startProcess(self.getConnectStr(), self.__domain.getUser(), self.__domain.getPassword(), db_name, options)
            return

        e = Event()
        # acquire the lock to avoid _notifyStartId reading the __startIdSlots map before we put the event inside it
        self.__lock.acquire()
        try:
            startResponse = startProcess(self.getConnectStr(), self.__domain.getUser(), self.__domain.getPassword(), db_name, options)

            startId = ElementTree.fromstring(startResponse).get("StartId")
            if not startId:
                return

            self.__startIdSlots[startId] = e
        finally:
            self.__lock.release()

        if waitSeconds == 0:
            e.wait()
        else:
            e.wait(waitSeconds)

        if not e.isSet():
            del self.__startIdSlots[startId]
            raise SessionException("Timed out waiting for process start")

        result = self.__startIdSlots[startId]
        del self.__startIdSlots[startId]

        # if the process failed to start in some known way then what's in the
        # "slot"  will be some meaningful error message, not a process instance
        if not isinstance(result, Process):
            raise SessionException(str(result))

        return result

    # NOTE: the "result" parameter should be an instance of Process or, in the
    # case that startup failed, anything that can be evaluated as str(result)
    # where the string is a meaningful description of the failure
    def _notifyStartId(self, startId, result):
        self.__lock.acquire()
        try:
            if startId in self.__startIdSlots:
                e = self.__startIdSlots[startId]
                self.__startIdSlots[startId] = result
                e.set()
        finally:
            self.__lock.release()

    def getLocalProcesses(self, db_name=None):
        if db_name == None:
            return self.__processes.values()

        processes = []
        for process in self.__processes.values():
            if process.getDatabase().getName() == db_name:
                processes.append(process)

        return processes
    
    @deprecated
    def getLocalNodes(self, db_name=None):
        return self.getLocalProcesses(db_name)

    def _getProcess(self, pid):
        return self.__processes.get(pid)

    def _addProcess(self, process):
        self.__processes[process.getPid()] = process

    def _removeProcess(self, process):
        try:
            del self.__processes[process.getPid()]
        except:
            pass


class Database:

    def __init__(self, domain, name):
        self.__domain = domain
        self.__name = name

        self.__processes = dict()

    def __hash__(self):
        return self.__name.hash()

    def __eq__(self, other):
        if not other:
            return False
        return self.__name == other.__name and self.__domain == other.__domain

    def __ne__(self, other):
        return self.__eq__(other) != True

    def __str__(self):
        return self.getName()

    def getDomain(self):
        return self.__domain

    def getName(self):
        return self.__name

    def _addProcess(self, process):
        self.__processes[self.__processId(process)] = process

    def _removeProcess(self, process):
        del self.__processes[self.__processId(process)]

    def getProcesses(self):
        return self.__processes.itervalues()

    def getProcessCount(self):
        return len(self.__processes)
    
    @deprecated    
    def getNodes(self):
        return self.getProcesses()

    @deprecated
    def getNodeCount(self):
        return self.getProcessCount()

    def __processId(self, process):
        return process.getPeer().getId() + ":" + str(process.getPid())

    def shutdown(self, graceful=True):
        if len(self.__processes) == 0:
            return

        if graceful:
            self.quiesce()

        processes = self.__processes.items()
        failureCount = 0
        failureText = ""

        for (processId, process) in self.__processes.items():
            if process.isTransactional():
                try:
                    if graceful:
                        process.shutdown()
                    else:
                        process.kill()
                    #del processes[processId]
                except Exception, e:
                    failureCount = failureCount + 1
                    failureText = failureText + str(e) + "\n"

        for (processId, process) in self.__processes.items():
            if not process.isTransactional():
                try:
                    if graceful:
                        process.shutdown()
                    else:
                        process.kill()
                except Exception, e:
                    failureCount = failureCount + 1
                    failureText = failureText + str(e) + "\n"

        if failureCount > 0:
            raise SessionException("Failed to shutdown " + str(failureCount) + " process(es)\n" + failureText)

    def quiesce(self, waitSeconds=0):
        doChorusAction(self.__domain.getEntryPeer().getConnectStr(),
                       self.__domain.getUser(), self.__domain.getPassword(),
                       self.__name, ChorusAction.Quiesce)
        if waitSeconds == 0:
            return

        if not self.__waitForStatus("QUIESCED", waitSeconds):
            raise SessionException("Timed out waiting to quiesce database")

    def unquiesce(self, waitSeconds=0):
        doChorusAction(self.__domain.getEntryPeer().getConnectStr(),
                       self.__domain.getUser(), self.__domain.getPassword(),
                       self.__name, ChorusAction.Unquiesce)
        if waitSeconds == 0:
            return

        if not self.__waitForStatus("RUNNING", waitSeconds):
            raise SessionException("Timed out waiting to unquiesce database")

    def updateConfiguration(self, name, value=None):
        optionElement = ElementTree.fromstring("<Option Name=\"%s\">%s</Option>" %
                                               (name, value if value is not None else ""))
        doChorusAction(self.__domain.getEntryPeer().getConnectStr(),
                       self.__domain.getUser(), self.__domain.getPassword(),
                       self.__name, ChorusAction.UpdateConfiguration,
                       child=optionElement)

    def __waitForStatus(self, status, waitSeconds):
        remainingProcesses = list(self.__processes.values())

        while waitSeconds >= 0:
            for process in remainingProcesses:
                if process.getStatus() == status:
                    remainingProcesses.remove(process)

            if len(remainingProcesses) == 0:
                return True

            if waitSeconds > 0:
                time.sleep(1)
            waitSeconds = waitSeconds - 1

        return False

class Process:

    def __init__(self, peer, database, port, pid, transactional, status, hostname=None, version=None):
        self.__peer = peer
        self.__database = database
        self.__port = port
        self.__pid = pid
        self.__transactional = transactional
        self.__hostname = hostname
        self.__version = version
        peer._addProcess(self)
        if status != None:
            self.__status = status
        else:
            self.__status = "UNKNOWN"

    @staticmethod
    def fromMessage(database, processElement):
        peer = database.getDomain().getPeer(processElement.get("AgentId"))
        if peer == None:
            raise Exception("Process is for an unknown peer")

        pid = int(processElement.get("ProcessId"))
        process = peer._getProcess(pid)
        if process != None:
            return process

        return Process(peer, database, int(processElement.get("Port")),
                    pid, int(processElement.get("NodeType")) == 1,
                     processElement.get("State"), processElement.get("Hostname"), processElement.get("Version"))

    def __hash__(self):
        return self.__pid

    def __eq__(self, other):
        if not other:
            return False
        return self.__port == other.__port and self.__peer == other.__peer

    def __ne__(self, other):
        return self.__eq__(other) != True

    def __str__(self):
        processType = "(TE)" if self.isTransactional() else "(SM)"
        return self.getAddress() + ":" + str(self.getPort()) + " [pid=" + str(self.getPid())+ "] " + processType

    def getPeer(self):
        return self.__peer

    def getDatabase(self):
        return self.__database

    def getAddress(self):
        return self.__peer.getAddress()

    def getPort(self):
        return self.__port

    def getPid(self):
        return self.__pid

    def getDbName(self):
        return self.__database.getName()

    def isTransactional(self):
        return self.__transactional

    def getHostname(self):
        return self.__hostname
    
    def getVersion(self):
        return self.__version

    def shutdown(self, waitTime=0):
        msg = ElementTree.fromstring("<Request Service=\"Admin\" Type=\"Shutdown\" WaitTime=\"%i\"/>" % waitTime)
        self.__peer.getDomain()._sendManagementMessage(msg, self.__peer, self)

    def kill(self):
        d = self.__peer.getDomain()
        killProcess(self.__peer.getConnectStr(), d.getUser(), d.getPassword(), self.getPid())

    def getStatus(self):
        return self.__status

    def waitForStatus(self, status, waitSeconds):

        while waitSeconds >= 0:
            if self.getStatus() == status:
                return True
            
            if waitSeconds > 0:
                time.sleep(1)

            waitSeconds = waitSeconds - 1

        return False

    def _setStatus(self, status):
        self.__status = status

    # to start, this is just a simple routine that asks for the db password and
    # uses that to establish the same direct connection that we've been using
    # to this point ... eventually we will support the async request/response
    # to send this over the existing connection, but for RC1 that's one too
    # many moving pieces to implement and test
    def query(self, type, msgBody=None):
        s = Session(self.getPeer().getConnectStr(), service="Manager")
        s.authorize(self.getPeer().getDomain().getUser(),
                    self.getPeer().getDomain().getPassword())
        pwdResponse = s.doRequest(attributes={ "Type" : "GetDatabaseCredentials",
                                               "Database" : self.getDbName() })

        pwdXml = ElementTree.fromstring(pwdResponse)
        pwd = pwdXml.find("Password").text.strip()

        return queryEngine(self.getAddress(), self.getPort(), type, pwd, msgBody)
    
