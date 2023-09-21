#!/usr/bin/env python

import os
import unittest
import tempfile
import gzip
import xml.etree.ElementTree as ET

import pynuodb

from . import DATABASE_NAME, get_ap_conn
from .nuodb_base import NuoBase


class NuoDBServiceTest(NuoBase):
    """Test using the Session object to connect directly to an Engine."""

    def test_query_memory(self):
        """Test query of process memory."""
        ap_conn = get_ap_conn()
        if ap_conn is None:
            self.skipTest("No AP available")

        procs = ap_conn.get_processes(db_name=DATABASE_NAME)
        dbpasswd = ap_conn._get_db_password(DATABASE_NAME)

        def try_message(msg):
            session = pynuodb.session.Session(
                procs[0].address, service='Query',
                options={'verifyHostname': 'False'})
            session.authorize('Cloud', dbpasswd)
            session.send(msg)
            res = session.recv()
            root = ET.fromstring(res)
            self.assertEqual(root.tag, 'MemoryInfo')
            info = root.findall('HeapInformation')
            self.assertEqual(len(info), 1)
            self.assertEqual(info[0].tag, 'HeapInformation')

        # Send with different types of buffers
        msg = '<Request Service="Query" Type="Memory"/>'
        try_message(msg)
        try_message(msg.encode('utf-8'))
        try_message(pynuodb.crypt.bytesToArray(msg.encode('utf-8')))

    def test_request_gc(self):
        """Test a request operation."""
        ap_conn = get_ap_conn()
        if ap_conn is None:
            self.skipTest("No AP available")

        procs = ap_conn.get_processes(db_name=DATABASE_NAME)
        dbpasswd = ap_conn._get_db_password(DATABASE_NAME)

        session = pynuodb.session.Session(
            procs[0].address, service='Admin',
            options={'verifyHostname': 'False'})
        session.authorize('Cloud', dbpasswd)

        req = ET.fromstring('''<Request Type="RequestGarbageCollection">
                                 <MinMemory>100000</MinMemory>
                               </Request>''')
        res = session.doRequest(children=[req])
        root = ET.fromstring(res)
        self.assertEqual(root.tag, 'Response')
        info = root.findall('ChorusActionStarted')
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0].get('Action'), 'RequestGarbageCollection')

    def test_stream_recv(self):
        """Test the stream_recv() facility."""
        ap_conn = get_ap_conn()
        if ap_conn is None:
            self.skipTest("No AP available")

        procs = ap_conn.get_processes(db_name=DATABASE_NAME)
        dbpasswd = ap_conn._get_db_password(DATABASE_NAME)

        session = pynuodb.session.Session(
            procs[0].address, service='Admin',
            options={'verifyHostname': 'False'})
        session.authorize('Cloud', dbpasswd)

        session.send('''<Request Service="Admin">
                          <Request Type="GetSysDepends"/>
                        </Request>''')
        resp = session.recv()
        xml = ET.fromstring(resp)
        self.assertIsNotNone(xml.find('Success'), "Failed: %s" % (resp))

        deppath = os.path.join(tempfile.gettempdir(), 'deps.tar.gz')
        with open(deppath, 'wb') as of:
            for data in session.stream_recv():
                of.write(data)

        # The socket should be closed now: this will raise
        self.assertRaises(pynuodb.session.SessionException,
                          lambda: session._sock)

        # Now make sure that what we read is uncompressable
        with gzip.GzipFile(deppath, 'rb') as gz:
            # We don't really care we just want to make sure it works
            self.assertIsNotNone(gz.read(), "Failed to unzip %s" % (deppath))


if __name__ == '__main__':
    unittest.main()
