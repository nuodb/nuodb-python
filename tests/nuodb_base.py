#!/usr/bin/env python

import pynuodb.entity
import tempfile
import unittest
import time
import os
import string
import random

HOST            = "localhost"
DOMAIN_USER     = "domain"
DOMAIN_PASSWORD = "bird"

DBA_USER        = 'dba'
DBA_PASSWORD    = 'dba_password'
DATABASE_NAME   = 'pynuodb_test'

class NuoBase(unittest.TestCase):
    driver = pynuodb
    connect_args = ()
    options = {"schema": "test"}
    host = HOST + (':'+os.environ['NUODB_PORT'] if 'NUODB_PORT' in os.environ else '')
    connect_kw_args = {'database': DATABASE_NAME, 'host': host, 'user': DBA_USER, 'password': DBA_PASSWORD, 'options': options }

    lower_func = 'lower' # For stored procedure test

    @classmethod
    def setUpClass(cls):
        domain = pynuodb.entity.Domain(cls.host, DOMAIN_USER, DOMAIN_PASSWORD)
        try:
            if DATABASE_NAME not in [db.name for db in domain.databases]:
                peer = domain.entry_peer
                archive = os.path.join(tempfile.gettempdir(), ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)))
                peer.start_storage_manager(DATABASE_NAME, archive, True, wait_seconds=10)
                peer.start_transaction_engine(DATABASE_NAME,  [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], wait_seconds=10)

        finally:
            domain.disconnect()


    @classmethod
    def tearDownClass(cls):
        listener = TestDomainListener()
        domain = pynuodb.entity.Domain(cls.host, DOMAIN_USER, DOMAIN_PASSWORD, listener)
        try:
            database = domain.get_database(DATABASE_NAME)
            if database is not None:
                for process in database.processes:
                    process.shutdown()

                for i in xrange(1, 20):
                    time.sleep(0.25)
                    if listener.db_left:
                        time.sleep(1)
                        break

        finally:
            domain.disconnect()


    def _connect(self):
        return pynuodb.connect(**NuoBase.connect_kw_args)

class TestDomainListener(object):
    def __init__(self):
        self.db_left = False

    def database_left(self, database):
        self.db_left = True
