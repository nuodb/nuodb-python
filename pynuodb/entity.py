__all__ = [ "Domain", "Peer", "Database", "Process" ]

""" This module provides basic "entity" support, similar to what's available
in the com.nuodb.entity Java package. A Domain instance provides entry into
a domain, and optionally a hook for getting called back when domain-level
events happen. The Domain provides access to Peers, Databases and Processes.

To create a Domain 'connection' you need to give a broker address (a string
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

from session import BaseListener, Session, SessionMonitor, SessionException
from util import DatabaseAction, startProcess, killProcess, doDatabaseAction, queryEngine

import time, json, socket
from threading import Event, Lock
import xml.etree.ElementTree as ElementTree


class Domain(BaseListener):
    """Represents the NuoDB domain.
    
    The domain is the top level NuoDB management object. The domain object 
    provides access to the peers and databases that are contained within.
    """

    def __init__(self, broker_addr, domain_user, domain_pwd, listener=None):
        """
        @type broker_addr str
        @type domain_user str
        @type domain_pwd str
        @type listener
        """
        if not domain_pwd:
            raise Exception("A password is required to join a domain")

        self.__session = Session(broker_addr, service="Monitor")
        self.__session.authorize(domain_user, domain_pwd)

        self.__user = domain_user
        self.__password = domain_pwd
        self.__listener = listener
        self.__peers = dict()
        """ @type : dict[str, Peer] """
        self.__peers_by_addr = dict()
        """ @type : dict[str, Peer] """
        self.__databases = dict()
        """ @type : dict[str, Database] """

        self.__monitor = SessionMonitor(self.__session, self)
        
        # These will be set in handle status after joining the domain 
        self.__domain_name = None
        """ @type : str """
        self.__entry_peer = None
        """ @type : Peer """

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

    def find_peer(self, address, port=None):
        """
        Find a peer by address
        @type: address str
        @type: port int or str
        @rtype: Peer
        """
        if port is None:
            if ":" in address:
                address, port = address.split(':', 2)
            else:
                port = self.__entry_peer.port
        else:
            if ":" in address:
                address, _ = address.split(':', 2)

        ip = socket.gethostbyname(address)
        inet_sock_addr = ":".join([ip, str(port)])
        try:
            return self.__peers_by_addr[inet_sock_addr]
        except:
            pass

        session = Session(address, port=port, service="Identity")
        session.authorize(self.__user, self.__password)
        response = session.doRequest()
        try:
            root = ElementTree.fromstring(response)
            if self.__domain_name != root.get("Domain"):
                return None
            peer =  self.get_peer(root.get("AgentId"))
            if peer:
                self.__peers_by_addr[peer._get_normalized_addr()] = peer
            return peer
        except:
            return None

    def get_peer(self, agent_id):
        """
        Return a peer for a given agent_id.
        @type agent_id str
        @rtype: Peer
        """
        return self.__peers.get(agent_id)

    @property
    def peers(self):
        """
        Return a list of all peers in the domain.
        @rtype: list[Peer]
        """
        return self.__peers.values()

    @property
    def entry_peer(self):
        """
        Return the peer that was used to enter the domain.
        @rtype: Peer
        """
        return self.__entry_peer

    def get_database(self, name):
        """
        Return a database by name
        @type name str
        @rtype: Database
        """
        return self.__databases.get(name)

    @property
    def databases(self):
        """
        Return a list of databases in the domain
        @rtype: list[Database]
        """
        return self.__databases.values()

    def create_template(self, template_name, summary, requirements):
        """Create template by name"""
        response = self._send_domain_message(**Template.build_create_request(template_name, summary, requirements))
        return ElementTree.fromstring(response).tag == Template.success_message

    def update_template(self, template_name, summary, requirements):
        """Update template by name"""
        response = self._send_domain_message(**Template.build_update_request(template_name, summary, requirements))
        return ElementTree.fromstring(response).tag == Template.success_message

    def delete_template(self, template_name):
        """Delete template by name"""
        response = self._send_domain_message(**Template.build_delete_request(template_name))
        return ElementTree.fromstring(response).tag == Template.success_message

    def get_template(self, template_name):
        """Return a template by name"""
        response = self._send_domain_message(**Template.build_get_request(template_name))
        return Template.from_message(response)

    @property
    def templates(self):
        """Return a list of templates in the domain"""
        response = self._send_domain_message(**Template.build_list_request())
        return Template.from_list_message(response)

    def create_description(self, name, template_name, variables, dba_user, dba_password):
        response = self._send_domain_message(**Description.build_create_request(name, template_name, variables, dba_user, dba_password))
        return ElementTree.fromstring(response).tag == Description.success_message

    def update_description(self, name, template_name, variables):
        response = self._send_domain_message(**Description.build_update_request(name, template_name, variables))
        return ElementTree.fromstring(response).tag == Description.success_message

    def delete_description(self, name):
        response = self._send_domain_message(**Description.build_delete_request(name))
        return ElementTree.fromstring(response).tag == Description.success_message

    def get_description(self, name):
        response = self._send_domain_message(**Description.build_get_request(name))
        return Description.from_message(response)

    def start_description(self, name):
        response = self._send_domain_message(**Description.build_start_request(name))
        return ElementTree.fromstring(response).tag == Description.success_message

    def stop_description(self, name):
        response = self._send_domain_message(**Description.build_stop_request(name))
        return ElementTree.fromstring(response).tag == Description.success_message

    @property
    def descriptions(self):
        response = self._send_domain_message(**Description.build_list_request())
        return Description.from_list_message(response)

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
        self.__peers_by_addr[peer._get_normalized_addr()] = peer
        if self.__listener:
            try:
                self.__listener.peer_joined(peer)
            except AttributeError:
                pass

    def __peer_left(self, peer):
        """Called when a peer leaves the domain."""
        del self.__peers[peer.id]
        del self.__peers_by_addr[peer._get_normalized_addr()]
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
        """
        @type domain Domain
        @type address str
        @type agent_id str
        @type broker bool
        @type port int
        @type hostname str
        @type version str
        """
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
        self.__inet_sock_addr = None

    @staticmethod
    def from_message(domain, peer_element):
        """"Construct a new peer object from an XML message."""
        return Peer(domain, peer_element.get("Address"), peer_element.get("AgentId"),
                    peer_element.get("Role") == "Broker", peer_element.get("Port"),
                    peer_element.get("Hostname"), peer_element.get("Version"))

    def __hash__(self):
        return hash(self.__id)

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
        """
        Return the domain that contains this peer.
        @rtype: Domain
        """
        return self.__domain

    @property
    def address(self):
        """
        Return the address of this peer.
        @rtype: str
        """
        return self.__address

    @property
    def connect_str(self):
        """
        Return the connect string for this peer.
        @rtype: str
        """
        return self.__address + ":" + str(self.__port)

    @property
    def port(self):
        """
        Return the port that this peer is using.
        @rtype: int
        """
        return self.__port

    @property
    def id(self):
        """
        Return the id of this peer (agent_id).
        @rtype: str
        """
        return self.__id

    @property
    def hostname(self):
        """
        Return the hostname of this peer.
        @rtype: str
        """
        return self.__hostname
    
    @property
    def version(self):
        """
        Return the NuoDB release version of this peer.
        @rtype: str
        """
        return self.__version

    @property
    def is_broker(self):
        """
        Return True if this peer is a broker.
        @rtype: bool
        """
        return self.__is_broker

    @property
    def tags(self):
        """
        Return all host tags
        @rtype: dict[str,str]
        """
        message = self.__domain._send_domain_message("Tag", {'Action': 'GetHostTags', 'AgentId': self.id})
        tags = ElementTree.fromstring(message)
        data = {}
        for tag in tags:
            data[tag.get('Key')] = tag.get('Value')

        return data        
    
    def get_tag(self, tag):
        """
        Return host tag
        @rtype: str
        """
        return self.tags[tag]

    def set_tag(self, key, value):
        """
        Set host tag
        @type key str
        @type value str
        """
        element = ElementTree.fromstring("<Tag Key=\"%s\" Value=\"%s\"/>" % (key, value))
        self.__domain._send_domain_message("Tag", {'Action': 'SetHostTags', 'AgentId': self.id}, children=[element])
        
    def delete_tag(self, key):
        """
        Delete host tag
        @type key str
        """
        element = ElementTree.fromstring("<Tag Key=\"%s\"/>" % (key))
        self.__domain._send_domain_message("Tag", {'Action': 'DeleteHostTags', 'AgentId': self.id}, children=[element])

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

        @type db_name str
        @type options list[tuple[str]]
        @type wait_seconds int
        @rtype: Process
        """
        return self.__start_process(db_name, options, wait_seconds)

    def start_storage_manager(self, db_name, archive, initialize, options=None, wait_seconds=None, snapshot_archive=None):
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

        snapshot_archive -- if not None, this storage manager will be a snapshot
        storage manager with specified snapshot archive location.

        @type db_name str
        @type archive str
        @type initialize bool
        @type options list[tuple[str]]
        @type wait_seconds int
        @type snapshot_archive str
        @rtype: Process
        """
        if not options:
            options = []
        else:
            options = list(options)

        options.append(("--archive", archive))

        if initialize:
            options.append(("--initialize", None))

        if snapshot_archive:
            options.append(("--snapshot", None))
            options.append(("--snapshot-archive", snapshot_archive))

        return self.__start_process(db_name, options, wait_seconds)

    def __start_process(self, db_name, options, wait_seconds):
        """
        @type db_name str
        @type options list[tuple[str]]
        @type wait_seconds int
        @rtype: Process | None
        """
        if wait_seconds is None:
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

        @rtype: list[Process]
        """
        if db_name is None:
            return self.__processes.values()

        processes = []
        for process in self.__processes.values():
            if process.database.name == db_name:
                processes.append(process)

        return processes

    def _get_process(self, pid):
        """
        @type pid int
        @rtype: Process
        """
        return self.__processes.get(pid)

    def _add_process(self, process):
        """
        @type process Process
        """
        self.__processes[process.pid] = process

    def _remove_process(self, process):
        """
        @type process Process
        """
        try:
            del self.__processes[process.pid]
        except:
            pass

    def _get_normalized_addr(self):
        """
        Return ip_address:port
        @rtype: str
        """
        if self.__inet_sock_addr is None:
            ip = socket.gethostbyname(self.__address)
            inet_sock_addr = ":".join([ip, str(self.__port)])
            self.__inet_sock_addr = inet_sock_addr
        return self.__inet_sock_addr

class Database:
    
    """Represents a NuoDB database."""

    def __init__(self, domain, name):
        """
        @type domain Domain
        @type name str
        """
        self.__domain = domain
        self.__name = name

        self.__processes = dict()
        """ @type : dict[str, Process] """

    def __hash__(self):
        return hash(self.__name)

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
        """
        Return the domain that contains this database.
        @rtype: Domain
        """
        return self.__domain

    @property
    def name(self):
        """
        Return the name of this database.
        @rtype: str
        """
        return self.__name

    @property
    def description(self):
        """Return the description of this database."""
        message = self.__domain._send_domain_message("Description", {'Action': 'GetDatabaseDescription', 'DatabaseName': self.__name})
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
        return [process for process in self.__processes.values() if not process.is_transactional]
      
    @property
    def transaction_engines(self):
        """Return transaction engines."""
        return [process for process in self.__processes.values() if process.is_transactional]

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
        
        graceful -- (default True) if True, the database processes will be shutdown gracefully.
        """
        if len(self.__processes) == 0:
            return

        failure_count = 0
        failure_text = ""

        for process in self.__processes.values():
            if process.is_transactional:
                try:
                    if graceful:
                        process.shutdown()
                    else:
                        process.kill()
                    
                except Exception, e:
                    failure_count = failure_count + 1
                    failure_text = failure_text + str(e) + "\n"

        for process in self.__processes.values():
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
    TE_NODE_TYPE = 1
    SM_NODE_TYPE = 2
    SSM_NODE_TYPE = 6
    TYPE_NAMES = {TE_NODE_TYPE: "TE", SM_NODE_TYPE: "SM", SSM_NODE_TYPE: "SSM"}

    """Represents a NuoDB process (TE, SM, or SSM)"""

    def __init__(self, peer, database, port, pid, type_num, status, hostname, version, node_id):
        """
        @type peer Peer
        @type database Database
        @type port int
        @type pid int
        @type type_num int
        @type status str
        @type hostname str
        @type version str
        @type node_id int
        """
        self.__peer = peer
        self.__database = database
        self.__port = port
        self.__pid = pid

        if type_num not in Process.TYPE_NAMES:
            raise ValueError("Unknown NodeType: {}".format(type_num))

        self.__type_num = type_num
        self.__hostname = hostname
        self.__version = version

        if node_id is not None:
            self.__node_id = int(node_id)
        else:
            self.__node_id = None

        peer._add_process(self)
        if status is not None:
            self.__status = status
        else:
            self.__status = "UNKNOWN"

    @staticmethod
    def from_message(database, process_element):
        """Construct a new process from an XML message."""
        peer = database.domain.get_peer(process_element.get("AgentId"))
        if peer is None:
            raise Exception("Process is for an unknown peer")

        pid = int(process_element.get("ProcessId"))
        process = peer._get_process(pid)
        if process is not None:
            return process

        return Process(peer, database, int(process_element.get("Port")),
                    pid, int(process_element.get("NodeType")),
                     process_element.get("State"), process_element.get("Hostname"),
                     process_element.get("Version"), process_element.get("NodeId"))

    def __hash__(self):
        return self.__pid

    def __eq__(self, other):
        if not other:
            return False
        return self.port == other.port and self.peer == other.peer

    def __ne__(self, other):
        return self.__eq__(other) != True

    def __str__(self):
        process_type = "({})".format(self.get_type())
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
    def node_id(self):
        """Return the NodeId of this process."""
        return self.__node_id
    
    @property
    def is_transactional(self):
        """Return True if this process is a Transaction Engine.
        
        Return False if it is a Storage Manager or Snapshot Storage Manager.
        """
        return self.__type_num == Process.TE_NODE_TYPE

    @property
    def hostname(self):
        """Return the hostname of this process."""
        return self.__hostname
    
    @property
    def version(self):
        """Return the NuoDB release version of this process."""
        return self.__version

    def get_type(self):
        """Return the type name (TE, SM, SSM)"""
        return Process.TYPE_NAMES[self.__type_num]

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
        session.authorize(self.peer.domain.user, self.peer.domain.password)
        pwd_response = session.doRequest(attributes={"Type": "GetDatabaseCredentials",
                                                     "Database": self.database.name})

        pwd_xml = ElementTree.fromstring(pwd_response)
        pwd = pwd_xml.find("Password").text.strip()

        return queryEngine(self.address, self.port, query_type, pwd, msg_body)


class Template:
    success_message = "Success"

    @staticmethod
    def build_create_request(name, summary, requirements):
        summary_element = ElementTree.Element("Summary")
        summary_element.text = summary
        requirements_element = ElementTree.Element("Requirements")
        requirements_element.text = requirements
        return {"service": "Description",
                "attributes": {'Action': 'CreateTemplate', 'TemplateName': name},
                "children": [summary_element, requirements_element]}

    @staticmethod
    def build_update_request(name, summary, requirements):
        summary_element = ElementTree.Element("Summary")
        summary_element.text = summary
        requirements_element = ElementTree.Element("Requirements")
        requirements_element.text = requirements
        return {"service": "Description",
                "attributes": {'Action': 'UpdateTemplate', 'TemplateName': name},
                "children": [summary_element, requirements_element]}

    @staticmethod
    def build_delete_request(name):
        return {"service": "Description", "attributes": {'Action': 'DeleteTemplate', 'TemplateName': name}}

    @staticmethod
    def build_get_request(name):
        return {"service": "Description", "attributes": {'Action': 'GetTemplate', 'TemplateName': name}}

    @staticmethod
    def build_list_request():
        return {"service": "Description", "attributes": {'Action': 'ListTemplates'}}

    @staticmethod
    def from_message(message):
        root = ElementTree.fromstring(message)
        name = root.get("TemplateName")

        summary = ""
        summary_element = root.find("Summary")
        if summary_element is not None:
            summary = summary_element.text

        requirements = ""
        requirements_element = root.find("Requirements")
        if requirements_element is not None:
            requirements = requirements_element.text

        return Template(name, summary, requirements)

    @staticmethod
    def from_list_message(message):
        names = list()
        root = ElementTree.fromstring(message)
        for child in root:
            names.append(child.get("TemplateName"))

        return names

    def __init__(self, name, summary, requirements):
        """
        @type name str
        @type summary str
        @type requirements str
        """
        self._name = name
        self._summary = summary
        self._requirements = requirements

    @property
    def name(self):
        return self._name

    @property
    def summary(self):
        return self._summary

    @property
    def requirements(self):
        return self._requirements


class Description:
    success_message = "Success"

    @staticmethod
    def build_create_request(name, template_name, variables, dba_user, dba_password):
        template_element = ElementTree.Element("Template")
        template_element.text = template_name
        variables_element = ElementTree.Element("Variables")
        for key in variables:
            variable_child = ElementTree.SubElement(variables_element, "Variable")
            variable_child.set("Key", key)
            variable_child.text = variables[key]

        return {"service": "Description",
                "attributes": {'Action': 'CreateDescription',
                               'DatabaseName': name,
                               'DbaUser': dba_user,
                               'DbaPassword': dba_password},
                "children": [template_element, variables_element]}

    @staticmethod
    def build_update_request(name, template_name, variables):
        template_element = ElementTree.Element("Template")
        template_element.text = template_name
        variables_element = ElementTree.Element("Variables")
        for key in variables:
            variable_child = ElementTree.SubElement(variables_element, "Variable")
            variable_child.set("Key", key)
            variable_child.text = variables[key]

        return {"service": "Description",
                "attributes": {'Action': 'UpdateDescription',
                               'DatabaseName': name},
                "children": [template_element, variables_element]}

    @staticmethod
    def build_delete_request(name):
        return {"service": "Description", "attributes": {'Action': 'DeleteDescription', 'DatabaseName': name}}

    @staticmethod
    def build_get_request(name):
        return {"service": "Description", "attributes": {'Action': 'GetDescription', 'DatabaseName': name}}

    @staticmethod
    def build_list_request():
        return {"service": "Description", "attributes": {'Action': 'ListDescriptions'}}

    @staticmethod
    def build_start_request(name):
        return {"service": "Description", "attributes": {'Action': 'StartDescription', 'DatabaseName': name}}

    @staticmethod
    def build_stop_request(name):
        return {"service": "Description", "attributes": {'Action': 'StopDescription', 'DatabaseName': name}}

    @staticmethod
    def from_message(message):
        root = ElementTree.fromstring(message)
        name = root.get("DatabaseName")

        template_name = ""
        template_element = root.find("Template")
        if template_element is not None:
            template_name = template_element.text

        variables = {}
        variables_element = root.find("Variables")
        if variables_element is not None:
            for var in variables_element:
                key = var.get("Key")
                value = var.text
                variables[key] = value

        status = ""
        status_element = root.find("Status")
        if status_element is not None:
            status = status_element.text

        live_status = ""
        live_status_element = root.find("LiveStatus")
        if live_status_element is not None:
            live_status = live_status_element.text

        return Description(name, template_name, variables, status, live_status)

    @staticmethod
    def from_list_message(message):
        names = list()
        root = ElementTree.fromstring(message)
        for child in root:
            names.append(child.get("DatabaseName"))

        return names

    def __init__(self, name, template_name, variables, status, live_status=""):
        """
        @type name str
        @type template_name str
        @type variables dict[str,str]
        @type status str
        @type live_status str
        """
        self._name = name
        self._template_name = template_name
        self._variables = variables
        self._status = status
        self._live_status = live_status

    @property
    def name(self):
        return self._name

    @property
    def template_name(self):
        return self._template_name

    @property
    def variables(self):
        return self._variables

    @property
    def status(self):
        return self._status

    @property
    def live_status(self):
        return self._live_status
