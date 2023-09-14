"""Utilities for the NuoDB Python driver

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

__all__ = ["DatabaseAction", "EngineMonitor",
           "getLicense", "setLicense", "getIdentity", "getState",
           "doDatabaseAction", "startProcess", "stopProcess", "killProcess",
           "monitorDomainStats", "monitorEngine",
           "getCloudEntry"]

# This module contains a collection of static routines useful for interacting
# with an agent, domain or engine. It is the module that the Peer, Database
# and Node classes uses to issue their own requests. These are designed to
# provide one-line domain management operation. For instance:
#
#   print "Current license: " + getLicense('localhost', 'admin', 'bird')
#
#   print "Local agent state: " + getState('localhost', 'admin', 'bird')
#
#   print "Unresponsive exit: " + str(killProcess('localhost', 'admin', 'bird', 5498))
#
# While most of the routines here are used to manage at a domain-level, there
# are also routines to monitor and (optionally) capture logging from an engine.
# This example queries a broker for the next available engine and then prints
# the engine's raw stats and info/warn/error log messages to standard out:
#
#   (addr, port) = getCloudEntry('localhost', 'myDB')
#   monitor = monitorEngine(addr, port)
#   monitor.changeLogMask(14)
#   [...]
#   monitor.close()
#
# TODO: the log mask values should be included with friendly names in some
# python module along with an easy way to describe which combinations you want
# to monitor (for now, see Platform/Log.h for the values). There should also
# be a utility listener that parses the stat/log XML and pretty-prints it.

from .session import Session, SessionMonitor, SessionException, checkForError, BaseListener

from xml.etree import ElementTree


class _DatabaseActions(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError


DatabaseAction = _DatabaseActions(["Quiesce", "Unquiesce", "Validate",
                                   "UpdateConfiguration", "EvictNodes"])


class _StandardOutListener(BaseListener):
    def message_received(self, root):
        print(ElementTree.tostring(root))


class EngineMonitor(object):

    def __init__(self, monitor, session):
        self.__monitor = monitor
        self.__session = session

    def changeLogMask(self, mask):
        self.__session.send("<Request Action=\"log\" Mask=\"%s\"/>" % str(mask))

    def disableLogMessages(self):
        self.changeLogMask(0)

    def close(self):
        self.__monitor.close()


_OPTION_FLAG_STR = "<Option option=\"%s\"/>"
_OPTION_VALUE_STR = "<Option option=\"%s\" value=\"%s\"/>"


def logString(broker, user, password, message):
    s = Session(broker, service="DebugConsole")
    s.authorize(user, password)

    return s.doRequest(attributes={"RequestType": "LogString"}, text=message)


def getLicense(broker, user, password):
    s = Session(broker, service="License")
    s.authorize(user, password)

    return s.doRequest(attributes={"Action": "GetCurrentLicense"})


def setLicense(broker, user, password, licenseText):
    s = Session(broker, service="License")
    s.authorize(user, password)

    s.doRequest(attributes={"Action": "ApplyLicense"}, text=licenseText)


def getIdentity(agent):
    s = Session(agent, service="Identity")

    return s.doRequest()


def getState(broker, user, password):
    s = Session(broker, service="State")
    s.authorize(user, password)

    return s.doRequest()


def doDatabaseAction(broker, user, password, db_name, action, child=None):
    s = Session(broker, service="ChorusManagement")
    s.authorize(user, password)

    if child is not None:
        child = [child]
    s.doConnect(attributes={"Database": db_name, "Action": action}, children=child)
    response = s.recv()
    checkForError(response)

    s.close()

    return response


def startProcess(agent, user, password, db_name, options=None, waitSeconds=-1):
    s = Session(agent, service="ProcessStart")
    s.authorize(user, password)

    if not options:
        options = []

    options.append(("--database", db_name))

    opts = []
    for (k, v) in options:
        if v:
            optStr = _OPTION_VALUE_STR % (k, v)
        else:
            optStr = _OPTION_FLAG_STR % k
        opts.append(ElementTree.fromstring(optStr))

    return s.doRequest(attributes={"Process": "server",
                                   "StartBarrierTimeout": str(waitSeconds * 1000) if waitSeconds > 0 else "-1"},
                       children=opts)


# NOTE: this is the *old* method for process stop that attaches directly to a
# nuodb instance and does the non-soft shutdown ... most (all?) invocations
# should now be made on the entity.Node interface
def stopProcess(address, port, dbPassword):
    s = Session(address, port=port, service="Monitor")
    s.authorize("Cloud", dbPassword)
    s.doConnect()

    s.send("<Request Action=\"shutdown\"/>")

    s.close()


def killProcess(agent, user, password, pid):
    s = Session(agent, service="ProcessStop")
    s.authorize(user, password)

    response = s.doRequest(attributes={"PID": str(pid)})

    return int(ElementTree.fromstring(response).get("ExitCode"))


def monitorDomainStats(broker, user, password, listener=None):
    if not listener:
        listener = _StandardOutListener()

    s = Session(broker, service="HostStats")
    s.authorize(user, password)

    s.doConnect()
    checkForError(s.recv())

    monitor = SessionMonitor(s, listener=listener)
    monitor.start()

    return monitor


def monitorEngine(address, port, dbPassword, listener=None):
    if not listener:
        listener = _StandardOutListener()

    s = Session(address, port=port, service="Monitor")
    s.authorize("Cloud", dbPassword)

    monitor = SessionMonitor(s, listener=listener)
    monitor.start()

    s.doConnect()

    return EngineMonitor(monitor, s)


# Note: msgBody here is an ElementTree Element to include directly in the query
def queryEngine(address, port, target, dbPassword, msgBody=None):
    s = Session(address, port=port, service="Query")
    s.authorize("Cloud", dbPassword)
    s.doConnect()

    msg = "<Query Type=\"%s\"/>" % target
    if msgBody is not None:
        xml = ElementTree.fromstring(msg)
        xml.append(msgBody)
        msg = ElementTree.tostring(xml)

    s.send(msg)
    response = s.recv()

    checkForError(response)

    s.close()

    return response


def getCloudEntry(broker, db_name, options=None):
    attributes, session_options = Session._extract_options(options)
    attributes["Database"] = db_name

    s = Session(broker, service="SQL2", options=session_options)

    s.doConnect(attributes=attributes)
    connectDetail = s.recv()
    s.close()

    checkForError(connectDetail)

    root = ElementTree.fromstring(connectDetail)
    if root.tag != "Cloud":
        raise SessionException("Unexpected response type: " + root.tag)

    return (root.get("Address"), int(root.get("Port")))


def getArchiveHistory(agent, user, password, archive, options=None):
    s = Session(agent, service="ProcessStart")
    s.authorize(user, password)

    opts = []

    if archive:
        option = _OPTION_VALUE_STR % ("--archive", archive)
        opts.append(ElementTree.fromstring(option))

    if options:
        for (k, v) in options:
            if v:
                option = _OPTION_VALUE_STR % (k, v)
            else:
                option = _OPTION_FLAG_STR % k
            opts.append(ElementTree.fromstring(option))

    return s.doRequest(attributes={"Process": "archiveHistory"}, children=opts)
