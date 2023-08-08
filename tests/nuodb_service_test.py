#!/usr/bin/env python

import unittest
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

        session = pynuodb.session.Session(
            procs[0].address, service='Query',
            options={'verifyHostname': 'False'})
        session.authorize('Cloud', dbpasswd)

        session.send('<Request Service="Query" Type="Memory"/>'.encode('utf-8'))
        res = session.recv()
        root = ET.fromstring(res)
        self.assertEqual(root.tag, 'MemoryInfo')
        info = root.findall('HeapInformation')
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0].tag, 'HeapInformation')

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


if __name__ == '__main__':
    unittest.main()
