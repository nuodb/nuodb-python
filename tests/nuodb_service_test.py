"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import os
import tempfile
import gzip
import xml.etree.ElementTree as ET
import pytest
import logging

try:
    from typing import Optional  # pylint: disable=unused-import
except ImportError:
    pass

try:
    from pynuoadmin import nuodb_mgmt
    __have_pynuoadmin = True
except ImportError:
    __have_pynuoadmin = False

import pynuodb

from . import nuodb_base

_log = logging.getLogger('pynuodbtest')


@pytest.fixture(scope="session")
def ap_conn():
    # type: () -> Optional[nuodb_mgmt.AdminConnection]
    global __have_pynuoadmin
    if not __have_pynuoadmin:
        _log.info("Cannot load NuoDB pynuoadmin Python module")
        return None

    # Use the same method of locating the AP REST service as nuocmd.
    key = os.environ.get('NUOCMD_CLIENT_KEY')
    verify = os.environ.get('NUOCMD_VERIFY_SERVER')
    api = os.environ.get('NUOCMD_API_SERVER', 'localhost:8888')
    if not api.startswith('http://') and not api.startswith('https://'):
        if not key:
            api = 'http://' + api
        else:
            api = 'https://' + api

    _log.info("Creating AP connection to %s (client_key=%s verify=%s)",
              api, str(key), str(verify))
    return nuodb_mgmt.AdminConnection(api, client_key=key, verify=verify)


class TestNuoDBService(nuodb_base.NuoBase):
    """Test using the Session object to connect directly to an Engine."""

    def test_query_memory(self, ap_conn):
        """Test query of process memory."""
        if ap_conn is None:
            pytest.skip("No AP available")

        dbname = self.connect_args['database']
        procs = ap_conn.get_processes(db_name=dbname)
        dbpasswd = ap_conn._get_db_password(dbname)

        def try_message(msg):
            session = pynuodb.session.Session(
                procs[0].address, service='Query',
                options={'verifyHostname': 'False'})
            session.authorize('Cloud', dbpasswd)
            session.send(msg)
            res = session.recv()
            root = ET.fromstring(res)
            assert root.tag == 'MemoryInfo'
            info = root.findall('HeapInformation')
            assert len(info) == 1
            assert info[0].tag == 'HeapInformation'

        # Send with different types of buffers
        msg = '<Request Service="Query" Type="Memory"/>'
        try_message(msg)
        try_message(msg.encode('utf-8'))
        try_message(pynuodb.crypt.bytesToArray(msg.encode('utf-8')))

    def test_request_gc(self, ap_conn):
        """Test a request operation."""
        if ap_conn is None:
            pytest.skip("No AP available")

        dbname = self.connect_args['database']
        procs = ap_conn.get_processes(db_name=dbname)
        dbpasswd = ap_conn._get_db_password(dbname)

        session = pynuodb.session.Session(
            procs[0].address, service='Admin',
            options={'verifyHostname': 'False'})
        session.authorize('Cloud', dbpasswd)

        req = ET.fromstring('''<Request Type="RequestGarbageCollection">
                                 <MinMemory>100000</MinMemory>
                               </Request>''')
        res = session.doRequest(children=[req])
        root = ET.fromstring(res)
        assert root.tag == 'Response'
        info = root.findall('ChorusActionStarted')
        assert len(info) == 1
        assert info[0].get('Action') == 'RequestGarbageCollection'

    def test_stream_recv(self, ap_conn):
        """Test the stream_recv() facility."""
        if ap_conn is None:
            pytest.skip("No AP available")

        dbname = self.connect_args['database']
        procs = ap_conn.get_processes(db_name=dbname)
        dbpasswd = ap_conn._get_db_password(dbname)

        session = pynuodb.session.Session(
            procs[0].address, service='Admin',
            options={'verifyHostname': 'False'})
        session.authorize('Cloud', dbpasswd)

        session.send('''<Request Service="Admin">
                          <Request Type="GetSysDepends"/>
                        </Request>''')
        resp = session.recv()
        xml = ET.fromstring(resp)
        assert xml.find('Success') is not None, "Failed: %s" % (resp)

        deppath = os.path.join(tempfile.gettempdir(), 'deps.tar.gz')
        with open(deppath, 'wb') as of:
            for data in session.stream_recv():
                of.write(data)

        # The socket should be closed now: this will raise
        with pytest.raises(pynuodb.session.SessionException):
            session._sock

        # Now make sure that what we read is uncompressable
        with gzip.GzipFile(deppath, 'rb') as gz:
            # We don't really care we just want to make sure it works
            assert gz.read() is not None, "Failed to unzip %s" % (deppath)
