"""
(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import unittest

import pynuodb

from . import DATABASE_NAME, DBA_USER, DBA_PASSWORD
from . import get_ap_conn, nuocmd, cvtjson, get_sqlhost


class NuoBase(unittest.TestCase):
    # Set the driver module for the imported test suites
    driver = pynuodb

    connect_args = ()
    host = None

    lower_func = 'lower'  # For stored procedure test

    @classmethod
    def setUpClass(cls):
        super(NuoBase, cls).setUpClass()
        cls.host = get_sqlhost()
        cls.longMessage = True

    @classmethod
    def _connect(cls, options=None):
        if options is None:
            options = {'schema': 'test'}
        elif 'schema' not in options:
            options['schema'] = 'test'

        return pynuodb.connect(database=DATABASE_NAME, host=cls.host,
                               user=DBA_USER, password=DBA_PASSWORD,
                               options=options)

    def setUp(self):
        super(NuoBase, self).setUp()
        self.verifydb()

    def tearDown(self):
        self.verifydb()
        super(NuoBase, self).tearDown()

    def verifydb(self):
        stat = ''
        sms = 0
        tes = 0
        ap_conn = get_ap_conn()
        if ap_conn:
            db = ap_conn.get_database(DATABASE_NAME)
            self.assertEqual(db.state, 'RUNNING')
            for proc in ap_conn.get_processes(db_name=DATABASE_NAME):
                self.assertEqual(proc.engine_state, 'RUNNING')
                if proc.engine_type == 'SM':
                    sms += 1
                elif proc.engine_type == 'TE':
                    tes += 1
        else:
            (ret, out) = nuocmd(['--show-json', 'get', 'processes',
                                 '--db-name', DATABASE_NAME])
            self.assertEqual(ret, 0, "DB not running: %s" % (out))
            for proc in cvtjson(out):
                sid = proc.get('startId', '<unk>')
                state = proc.get('state', '<unk>')
                self.assertEqual(state, 'RUNNING',
                                 "Process sid %s state is %s" % (sid, state))
                typ = proc.get('type', '<unk>')
                if typ == 'SM':
                    sms += 1
                elif typ == 'TE':
                    tes += 1
                else:
                    self.fail("Invalid proc sid %s type %s" % (sid, typ))

            if sms == 0 or tes == 0:
                (ret, stat) = nuocmd(['show', 'domain'])

        self.assertGreater(sms, 0, "No SMs found:\n%s" % (stat))
        self.assertGreater(tes, 0, "No TEs found:\n%s" % (stat))


class TestDomainListener(object):
    def __init__(self):
        self.db_left = False

    def database_left(self, database):
        self.db_left = True
