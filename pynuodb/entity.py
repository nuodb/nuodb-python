__all__ = [ "Domain", "Peer", "Database", "Process" ]

""" This module provides basic "entity" support, similar to what's available
in the com.nuodb.entity Java package. A Domain instance provides entry into
a domain, and optionally a hook for getting called back when domain-level
events happen. The Domain provides access to Peers, Databases and Processes.
"""

from session import BaseListener, Session, SessionMonitor, SessionException
from util import DatabaseAction, startProcess, killProcess, doDatabaseAction, queryEngine

import time, json
from threading import Event, Lock
import xml.etree.ElementTree as ElementTree


""" To create a Domain 'connection' you need to give a broker address (a string
which may end in ':PORT') and domain password. You can also supply a class
to notify on domain events. That class may implement any of the methods:
   
    peer_joined(self, peer)
    peer_left(self, peer)
    process_joined(self, process)
    process_left(self, process)
    process_failed(self, peer, reason)
    process_status_changed(self, process, status)
    database_joined(self, database)
    database_left(self, database)
    closed(self)
    

For instance, a valid listener could be formed like this:

class MyListener():
    def database_joined(self, database):
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

class Domain(BaseListener):
    
    """Represents the NuoDB domain.
    
    The domain is the top level NuoDB management object. The domain object 
    provides access to the peers and databases that are contained within.
    """

    def __init__(self, broker_addr, domain_user, domain_pwd, listener=None):
        if not domain_pwd:
            raise Exception("A password is required to join a domain")

        self.__session = Session(broker_addr, service="Monitor")
        self.__session.authorize(domain_user, domain_pwd)

        self.__user = domain_user
        self.__password = domain_pwd
        self.__listener = listener
        self.__peers = dict()
        self.__databases = dict()

        self.__monitor = SessionMonitor(self.__session, self)
        
        # These will be set in handle status after joining the domain 
        self.__domain_name = None
        self.__entry_peer = None

        try:
            self.__session.doConnect()
            self.__handle_status(self.__session.recv())
        except Exception:
            self.__monitor.close()
            raise

        self.__monitor.start()

    def __str__(self):
        return self.domain_name + " [Entered through: " + self.entry_peer.connect_str + "]"

    def disconnect(self):
        """Disconnect from the domain."""
        self.__monitor.close()
        
    def _send_domain_message(self, service, attributes=None, text=None, children=None):
        session = Session(self.__entry_peer.address, port=self.__entry_peer.port, service=service)
        session.authorize(self.__user, self.__password)
        return session.doRequest(attributes, text, children)

    @property
    def user(self):
        """Return the domain user."""
        return self.__user

    @property
    def password(self):
        """Return the domain password."""
        return self.__password
    
    @property
    def domain_name(self):
        """Return the domain name."""
        return self.__domain_name

    def get_peer(self, agent_id):
        """Return a peer for a given agent_id."""
        return self.__peers.get(agent_id)

    @property
    def peers(self):
        """Return a list of all peers in the domain."""
        return self.__peers.values()

    @property
    def entry_peer(self):
        """Return the peer that was used to enter the domain."""
        return self.__entry_peer

    def get_database(self, name):
        """Return a database by name"""
        return self.__databases.get(name)

    @property
    def databases(self):
        """Return a list of databases in the domain"""
        return self.__databases.values()

    def set_template_description(self, template, description):
        """Set template by name"""
        return self._send_domain_message("DescriptionService", {'Action': 'SetTemplateDescription', 'TemplateName': template}, text=description)

    def get_template(self, name):
        """Return a template by name"""
        message = self._send_domain_message("DescriptionService", {'Action': 'GetTemplateDescription', 'TemplateName': name})
        if ElementTree.fromstring(message).text:
            return json.loads(ElementTree.fromstring(message).text)
        return {}

    @property
    def templates(self):
        """Return a list of templates in the domain"""
        message = self._send_domain_message("DescriptionService", {'Action': 'ListTemplates'})
        templates = ElementTree.fromstring(message)
        data = []
        for template in templates:
            data.append(dict((k.lower(), v) for k, v in template.attrib.iteritems()))
        return data

    def set_description(self, database, description):
        """Sets description of specified database. If the database does not exist, then create one"""
        inner_text = str({'template': description})
#         inner_text = "{\"name\": \"%s\", 'description': '%s'}" % (database, description) 
        return self._send_domain_message("DescriptionService", {'Action': 'SetDatabaseDescription', 'DatabaseName': database, 'DbaUser': self.user, 'DbaPassword': self.password}, text=inner_text)

    def shutdown(self, graceful=True):
        """Shutdown all databases in the domain.
        
        graceful -- (default True) means that the database will first
        be quiesced and then shutdown.
        """
        for database in self.__databases.itervalues():
            database.shutdown(graceful)

    def message_received(self, root):
        """Process a management message from the broker.
        
        Override from session.BaseListener.
        """
        if root.tag == "Event":
            event_type = root.get("Type")
            if event_type == "NewBroker":
                self.__peer_joined(Peer.from_message(self, root.find("Broker")))
            elif event_type == "BrokerExit":
                self.__peer_left(Peer.from_message(self, root.find("Broker")))
            elif event_type == "StatusChanged":
                status = root.get("Status")

                process_element = root.find("Process")
                db = self.__databases[process_element.get("Database")]
                process = Process.from_message(db, process_element)

                self.__process_status_changed(process, status)
            elif event_type == "ProcessFailed":
                peer = Peer.from_message(self, root.find("Broker"))
                peer = self.get_peer(peer.id)

                reason = root.get("Reason")
                start_id = root.get("StartId")

                self.__process_failed(peer, start_id, reason)
            elif event_type == "NewProcess" or event_type == "ProcessExit":
                process_element = root.find("Process")
                db_name = process_element.get("Database")

                if db_name not in self.__databases:
                    self.__databases[db_name] = Database(self, db_name)
                    if self.__listener:
                        try:
                            self.__listener.database_joined(self.__databases[db_name])
                        except AttributeError:
                            pass
                            

                if event_type == "NewProcess":
                    start_id = process_element.get("StartId")
                    self.__process_joined(Process.from_message(self.__databases[db_name],
                                                       process_element), start_id)
                else:
                    self.__process_left(Process.from_message(self.__databases[db_name],
                                                     process_element))

    def closed(self):
        """Called when the session is closed.
        
        Override from session.BaseListener.
        """
        if self.__listener:
            try:
                self.__listener.closed()
            except AttributeError:
                pass

    def __handle_status(self, message):
        """Handle initial domain status on domain connection.
        
        Note that this is ONLY for processing the initial status message. All
        further update messages are processed by message_received()."""
        root = ElementTree.fromstring(message)
        if root.tag != "Status":
            raise Exception("Expected status message; got " + root.tag)

        self.__domain_name = root.get("Domain")

        self.__entry_peer = Peer(self, self.__session.address, root.get("AgentId"),
                                (root.get("Role") == "Broker"), self.__session.port,
                                root.get("Hostname"), root.get("Version"))
        self.__peer_joined(self.__entry_peer)

        for child in list(root):
            if child.tag == "Broker":
                self.__peer_joined(Peer.from_message(self, child))

        for child in list(root):
            if child.tag == "Database":
                name = child.get("Name")
                if self.__listener:
                    try:
                        self.__listener.database_joined(self.__databases[name])
                    except AttributeError:
                        pass

                for process_element in list(child):
                    if process_element.tag == "Process":
                        if name not in self.__databases:
                            self.__databases[name] = Database(self, name)
                        self.__process_joined(Process.from_message(self.__databases[name], process_element), None)

    def __peer_joined(self, peer):
        """Called when a peer joins the domain."""
        self.__peers[peer.id] = peer
        if self.__listener:
            try:
                self.__listener.peer_joined(peer)
            except AttributeError:
                pass

    def __peer_left(self, peer):
        """Called when a peer leaves the domain."""
        del self.__peers[peer.id]
        if self.__listener:
            try:
                self.__listener.peer_left(peer)
            except AttributeError:
                pass

    def __process_joined(self, process, start_id):
        """Called when a process joins the domain."""
        process.database._add_process(process)
        process.peer._notify_start_id(start_id, process)
        if self.__listener:
            try:
                self.__listener.process_joined(process)
            except AttributeError:
                pass
    
    def __process_left(self, process):
        """Called when a process leaves the domain."""
        database = process.database
        database._remove_process(process)
        process.peer._remove_process(process)
        if self.__listener:
            try:
                self.__listener.process_left(process)
            except AttributeError:
                pass

        if len(database.processes) == 0:
            del self.__databases[database.name]
            if self.__listener:
                try:
                    self.__listener.database_left(database)
                except AttributeError:
                    pass

    def __process_failed(self, peer, start_id, reason):
        """Called when a process in the domain fails."""
        peer._notify_start_id(start_id, reason)
        if self.__listener:
            try:
                self.__listener.process_failed(peer, reason)
            except AttributeError:
                pass

    def __process_status_changed(self, process, status):
        """Called when a process in the domain changes status."""
        process._set_status(status)
        if self.__listener:
            try:
                self.__listener.process_status_changed(process, status)
            except AttributeError:
                pass


    def _send_management_message(self, message, peer, process):
        """Send a management message.
        
        Note that this is an initial verison only to support the shutdown 
        routine that doesn't need to watch for return messages ... right now 
        this module is only supporting the tests, which don't need the other 
        management routines at this point, so we'll flesh this out (as in the 
        Java implementation) in the second round when other utilites get 
        updated as well
        """
        root = ElementTree.fromstring("<ManagementRequest AgentId=\"%s\" ProcessId=\"%i\"/>" % (peer.id, process.pid))
        root.append(message)

        self.__session.send(ElementTree.tostring(root))

class Peer:
    
    """Represents a peer (or host) in the domain."""

    def __init__(self, domain, address, agent_id, broker=False, port=48004, hostname=None, version=None):
        self.__domain = domain
        self.__address = address
        self.__id = agent_id
        self.__is_broker = broker
        self.__port = port
        self.__hostname = hostname
        self.__lock = Lock()
        self.__processes = dict()
        self.__version = version
        self.__start_id_slots = dict()

    @staticmethod
    def from_message(domain, peer_element):
        """"Construct a new peer object from an XML message."""
        return Peer(domain, peer_element.get("Address"), peer_element.get("AgentId"),
                    peer_element.get("Role") == "Broker", peer_element.get("Port"),
                    peer_element.get("Hostname"), peer_element.get("Version"))

    def __hash__(self):
        return self.__id.hash()

    def __eq__(self, other):
        if not other:
            return False
        return self.id == other.id

    def __ne__(self, other):
        return self.__eq__(other) != True

    def __str__(self):
        role = "broker" if self.is_broker else "agent"
        return self.connect_str + " [role=" + role + "]"

    @property
    def domain(self):
        """Return the domain that contains this peer."""
        return self.__domain

    @property
    def address(self):
        """Return the address of this peer."""
        return self.__address

    @property
    def connect_str(self):
        """Return the connect string for this peer."""
        return self.__address + ":" + str(self.__port)

    @property
    def port(self):
        """Return the port that this peer is using."""
        return self.__port

    @property
    def id(self):
        """Return the id of this peer (agent_id)."""
        return self.__id

    @property
    def hostname(self):
        """Return the hostname of this peer."""
        return self.__hostname
    
    @property
    def version(self):
        """Return the NuoDB release version of this peer."""
        return self.__version

    @property
    def is_broker(self):
        """Return True if this peer is a broker."""
        return self.__is_broker

    @property
    def tags(self):
        """Return all host tags"""
        message = self.__domain._send_domain_message("DescriptionService", {'Action': 'GetHostTags', 'AgentId': self.id})
        tags = ElementTree.fromstring(message)
        data = {}
        for tag in tags:
            data[tag.get('Key')] = tag.get('Value')

        return data        
    
    def get_tag(self, tag):
        """Return host tag"""
        return self.tags[tag]

    def create_tag(self, key, value):
        """Set host tag"""
        element = ElementTree.fromstring("<Tag Key=\"%s\" Value=\"%s\"/>" % (key, value))
        self.__domain._send_domain_message("DescriptionService", {'Action': 'CreateHostTags', 'AgentId': self.id}, children=[element])
        
    def update_tag(self, key, value):
        """Set host tag"""
        element = ElementTree.fromstring("<Tag Key=\"%s\" Value=\"%s\"/>" % (key, value))
        self.__domain._send_domain_message("DescriptionService", {'Action': 'UpdateHostTags', 'AgentId': self.id}, children=[element])

    def start_transaction_engine(self, db_name, options=None, wait_seconds=None):
        """Start a transaction engine on this peer for a given database.
        
        options -- accepts a list of two element tuples, where the first element
        is a nuodb option flag and the second is the value. For options that
        do not accept a value, pass None as the value.
         
        If this is the first transaction engine to be started for a database
        you must include --dba-user and --dba-password in the options.
        
        wait_seconds -- defines how long to wait for the transaction engine to 
        start. The default is None, which does not wait for a response. 
        Specifying a wait_seconds value will cause this function to block 
        until a response is received indicating success or failure. If the
        time elapses without a response a SessionException will be raised.
        
        """
        return self.__start_process(db_name, options, wait_seconds)

    def start_storage_manager(self, db_name, archive, initialize, options=None, wait_seconds=None):
        """Start a storage manager on this peer for a given database.
              
        archive -- the archive location for the new storage manager. 
        
        initialize -- should be set to True if this storage manager is being
        started with a new archive. 
        
        options -- accepts a list of two element tuples, where the first 
        element is a nuodb option flag and the second is the value. For 
        options that do not accept a value, pass None as the value.
        
        wait_seconds -- defines how long to wait for the storage manager to 
        start. The default is None, which does not wait for a response. 
        Specifying a wait_seconds value will cause this function to block 
        until a response is received indicating success or failure. If the
        time elapses without a response, a SessionException will be raised.
        """
        if not options:
            options = []

        options.append(("--archive", archive))

        if initialize:
            options.append(("--initialize", None))

        return self.__start_process(db_name, options, wait_seconds)


    def __start_process(self, db_name, options, wait_seconds):
        if wait_seconds == None:
            startProcess(self.connect_str, self.__domain.user, self.__domain.password, db_name, options)
            return

        e = Event()
        # acquire the lock to avoid _notify_start_id reading the __start_id_slots map before we put the event inside it
        self.__lock.acquire()
        try:
            start_response = startProcess(self.connect_str, self.__domain.user, self.__domain.password, db_name, options)

            start_id = ElementTree.fromstring(start_response).get("StartId")
            if not start_id:
                return

            self.__start_id_slots[start_id] = e
        finally:
            self.__lock.release()

        if wait_seconds == 0:
            e.wait()
        else:
            e.wait(wait_seconds)

        if not e.isSet():
            del self.__start_id_slots[start_id]
            raise SessionException("Timed out waiting for process start")

        result = self.__start_id_slots[start_id]
        del self.__start_id_slots[start_id]

        # if the process failed to start in some known way then what's in the
        # "slot"  will be some meaningful error message, not a process instance
        if not isinstance(result, Process):
            raise SessionException(str(result))

        return result

    # NOTE: the "result" parameter should be an instance of Process or, in the
    # case that startup failed, anything that can be evaluated as str(result)
    # where the string is a meaningful description of the failure
    def _notify_start_id(self, start_id, result):
        self.__lock.acquire()
        try:
            if start_id in self.__start_id_slots:
                e = self.__start_id_slots[start_id]
                self.__start_id_slots[start_id] = result
                e.set()
        finally:
            self.__lock.release()

    def get_local_processes(self, db_name=None):
        """Return a list of the NuoDB processes on this host.
        
        db_name -- (default None) if not None, only return processes on this peer that belong
        to a given database. Note that if the database spans multiple peers
        this method will only return the subset of processes that are on this 
        peer. 
        """
        if db_name == None:
            return self.__processes.values()

        processes = []
        for process in self.__processes.values():
            if process.database.name == db_name:
                processes.append(process)

        return processes

    def _get_process(self, pid):
        return self.__processes.get(pid)

    def _add_process(self, process):
        self.__processes[process.pid] = process

    def _remove_process(self, process):
        try:
            del self.__processes[process.pid]
        except:
            pass


class Database:
    
    """Represents a NuoDB database."""

    def __init__(self, domain, name):
        self.__domain = domain
        self.__name = name

        self.__processes = dict()

    def __hash__(self):
        return self.__name.hash()

    def __eq__(self, other):
        if not other:
            return False
        return self.name == other.name and self.domain == other.domain

    def __ne__(self, other):
        return self.__eq__(other) != True

    def __str__(self):
        return self.name

    @property
    def domain(self):
        """Return the domain that contains this database."""
        return self.__domain

    @property
    def name(self):
        """Return the name of this database."""
        return self.__name

    @property
    def description(self):
        """Return the description of this database."""
        message = self.__domain._send_domain_message("DescriptionService", {'Action': 'GetDatabaseDescription', 'DatabaseName': self.__name})
        return json.loads(ElementTree.fromstring(message).text)

    @property
    def status(self):
        """Return the status of the database."""
        #TODO: hack to determine database state
        data = {'RUNNING': 0, 'QUIESCED': 0}
        for process in self.processes:
            if process.status == "RUNNING":
                data['RUNNING'] = data['RUNNING'] + 1
            if process.status == "QUIESCED":
                data['QUIESCED'] = data['QUIESCED'] + 1
        if data['RUNNING'] > data['QUIESCED']:
            return "RUNNING"
        if data['QUIESCED'] > data['RUNNING']:
            return "QUIESCED"

    @property
    def storage_managers(self):
        """Return storage managers."""
        return [process for process in self.__processes if not process.is_transactional]
      
    @property
    def transaction_engines(self):
        """Return transaction engines."""
        return [process for process in self.__processes if process.is_transactional]

    def _add_process(self, process):
        self.__processes[self.__process_id(process)] = process

    def _remove_process(self, process):
        del self.__processes[self.__process_id(process)]

    @property
    def processes(self):
        """Return a list of all processes in this database."""
        return self.__processes.values()

    def __process_id(self, process):
        return process.peer.id + ":" + str(process.pid)

    def shutdown(self, graceful=True):
        """Shutdown this database.
        
        graceful -- (default True) if True, the database will first
        be quiesced and then shutdown.
        """
        if len(self.__processes) == 0:
            return

        if graceful:
            self.quiesce()

        failure_count = 0
        failure_text = ""

        for process in self.__processes.itervalues():
            if process.is_transactional:
                try:
                    if graceful:
                        process.shutdown()
                    else:
                        process.kill()
                    
                except Exception, e:
                    failure_count = failure_count + 1
                    failure_text = failure_text + str(e) + "\n"

        for process in self.__processes.itervalues():
            if not process.is_transactional:
                try:
                    if graceful:
                        process.shutdown()
                    else:
                        process.kill()
                except Exception, e:
                    failure_count = failure_count + 1
                    failure_text = failure_text + str(e) + "\n"

        if failure_count > 0:
            raise SessionException("Failed to shutdown " + str(failure_count) + " process(es)\n" + failure_text)

    def quiesce(self, wait_seconds=0):
        """Quiesce the database.
        
        wait_seconds -- (default 0) defines how long to wait for the database 
        to quiesce. If wait_seconds is 0 quiesce will not wait for a response.
        If wait_seconds is not 0 quiesce will block until the database is 
        quiesced or wait_seconds seconds pass. If the database does not 
        respond with a status of QUIESCED within the timeout, a 
        SessionException will be raised.
        """
        doDatabaseAction(self.__domain.entry_peer.connect_str,
                       self.__domain.user, self.__domain.password,
                       self.__name, DatabaseAction.Quiesce)
        if wait_seconds == 0:
            return

        if not self.__wait_for_status("QUIESCED", wait_seconds):
            raise SessionException("Timed out waiting to quiesce database")

    def unquiesce(self, wait_seconds=0):
        """Unquiesce the database.
        
        wait_seconds -- (default 0) defines how long to wait for the database 
        to unquiesce. If wait_seconds is 0 unquiesce will not wait for a response.
        If wait_seconds is not 0 unquiesce will block until the database is 
        running or wait_seconds seconds pass. If the database does not 
        respond with a status of RUNNING within the timeout, a 
        SessionException will be raised.
        """
        doDatabaseAction(self.__domain.entry_peer.connect_str,
                       self.__domain.user, self.__domain.password,
                       self.__name, DatabaseAction.Unquiesce)
        if wait_seconds == 0:
            return

        if not self.__wait_for_status("RUNNING", wait_seconds):
            raise SessionException("Timed out waiting to unquiesce database")

    def update_configuration(self, name, value=None):
        option_element = ElementTree.fromstring("<Option Name=\"%s\">%s</Option>" %
                                               (name, value if value is not None else ""))
        doDatabaseAction(self.__domain.entry_peer.connect_str,
                       self.__domain.user, self.__domain.password,
                       self.__name, DatabaseAction.UpdateConfiguration,
                       child=option_element)

    def __wait_for_status(self, status, wait_seconds):
        remaining_processes = list(self.__processes.values())

        while wait_seconds >= 0:
            for process in remaining_processes:
                if process.status == status:
                    remaining_processes.remove(process)

            if len(remaining_processes) == 0:
                return True

            if wait_seconds > 0:
                time.sleep(1)
            wait_seconds = wait_seconds - 1

        return False

class Process:
    
    """Represents a NuoDB process (TE or SM)"""

    def __init__(self, peer, database, port, pid, transactional, status, hostname=None, version=None):
        self.__peer = peer
        self.__database = database
        self.__port = port
        self.__pid = pid
        self.__transactional = transactional
        self.__hostname = hostname
        self.__version = version
        peer._add_process(self)
        if status != None:
            self.__status = status
        else:
            self.__status = "UNKNOWN"

    @staticmethod
    def from_message(database, process_element):
        """Construct a new process from an XML message."""
        peer = database.domain.get_peer(process_element.get("AgentId"))
        if peer == None:
            raise Exception("Process is for an unknown peer")

        pid = int(process_element.get("ProcessId"))
        process = peer._get_process(pid)
        if process != None:
            return process

        return Process(peer, database, int(process_element.get("Port")),
                    pid, int(process_element.get("NodeType")) == 1,
                     process_element.get("State"), process_element.get("Hostname"), process_element.get("Version"))

    def __hash__(self):
        return self.__pid

    def __eq__(self, other):
        if not other:
            return False
        return self.port == other.port and self.peer == other.peer

    def __ne__(self, other):
        return self.__eq__(other) != True

    def __str__(self):
        process_type = "(TE)" if self.is_transactional else "(SM)"
        return self.address + ":" + str(self.port) + " [pid=" + str(self.pid)+ "] " + process_type

    @property
    def peer(self):
        """Return the peer on which this process is running."""
        return self.__peer

    @property
    def database(self):
        """Return the database that contains this process."""
        return self.__database

    @property
    def address(self):
        """Return the address of this process."""
        return self.__peer.address

    @property
    def port(self):
        """Return the port that this process is using."""
        return self.__port

    @property
    def pid(self):
        """Return the process id of this process."""
        return self.__pid
    
    @property
    def is_transactional(self):
        """Return True if this process is a Transaction Engine.
        
        Return False if it is a Storage Manager.
        """
        return self.__transactional

    @property
    def hostname(self):
        """Return the hostname of this process."""
        return self.__hostname
    
    @property
    def version(self):
        """Return the NuoDB release version of this process."""
        return self.__version

    def shutdown(self, wait_time=0):
        """Shutdown this process.
        
        This is used in a graceful=True database shutdown.
        """
        msg = ElementTree.fromstring("<Request Service=\"Admin\" Type=\"Shutdown\" WaitTime=\"%i\"/>" % wait_time)
        self.__peer.domain._send_management_message(msg, self.__peer, self)

    def kill(self):
        """Kill this process.
        
        This is used in a graceful=False database shutdown. 
        """
        domain = self.__peer.domain
        killProcess(self.__peer.connect_str, domain.user, domain.password, self.pid)

    @property
    def status(self):
        """Return the status of this process.
        
        Possible statuses are:
        ACTIVE - The process has reported that it's ready for database participation.
        RUNNING - The process is in its running/active state.
        SYNCING - The process is currently synchronizing with the database state.
        QUIESCING - The process is starting to quiesce.
        UNQUIESCING - The process is moving from being quiesced to running.
        QUIESCED - The process is quiesced and will not service transactions.
        DIED - The process is recognized as having left the database.
        QUIESCING2 - An internal state change in the process of quiescing.
        SHUTTING_DOWN - The process is in the process of a soft shutdown.
        UNKNOWN - Any unknown state ... this should always be last in this enum 
                  to protect against skew between this enum and the C++ constants.
        """
        return self.__status

    def wait_for_status(self, status, wait_seconds):
        """Block until this process has a specified status.
        
        If the status is not reached within wait_seconds seconds this method
        will return False. If the status is reached it will immediately return 
        True.
        """
        while wait_seconds >= 0:
            if self.status == status:
                return True
            
            if wait_seconds > 0:
                time.sleep(1)

            wait_seconds = wait_seconds - 1

        return False

    def _set_status(self, status):
        self.__status = status

    # to start, this is just a simple routine that asks for the db password and
    # uses that to establish the same direct connection that we've been using
    # to this point ... eventually we will support the async request/response
    # to send this over the existing connection, but for RC1 that's one too
    # many moving pieces to implement and test
    def query(self, query_type, msg_body=None):
        session = Session(self.peer.connect_str, service="Manager")
        session.authorize(self.peer.domain.user,
                    self.peer.domain.password)
        pwd_response = session.doRequest(attributes={ "Type" : "GetDatabaseCredentials",
                                               "Database" : self.database.name })

        pwd_xml = ElementTree.fromstring(pwd_response)
        pwd = pwd_xml.find("Password").text.strip()

        return queryEngine(self.address, self.port, query_type, pwd, msg_body)
    
