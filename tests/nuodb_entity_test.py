""" These tests require a broker to be running on the local host with default
credentials. Update the variables below to run in an alternate configuration.

Note that these tests include shutting down the domain and all databases
inside, so DO NOT run these tests against a production domain.

"""

import unittest
import tempfile
import time
import random
import string
import os
import warnings

from pynuodb.entity import Domain

BROKER_HOST = 'localhost'
DOMAIN_USER = 'domain'
DOMAIN_PASSWORD = 'bird'
TEST_DB_NAME = 'entity_test_database'
TEST_DB_NAME2 = 'entity_test_database2'
DBA_USER = 'dba'
DBA_PASSWORD = 'dba'


class NuoDBEntityTest(unittest.TestCase):
    def setUp(self):
        self.host = BROKER_HOST + (':' + os.environ['NUODB_PORT'] if 'NUODB_PORT' in os.environ else '')

        domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)
        self._cleanup(domain)

    def tearDown(self):
        pass

    def test_connectDomain(self):
        domain = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            self.assertIsNotNone(domain)
            self.assertFalse(domain.closed())

            self.assertEqual(domain.user, DOMAIN_USER)
            self.assertEqual(domain.password, DOMAIN_PASSWORD)
            self.assertIsNotNone(domain.domain_name)
            self.assertIsNotNone(domain.entry_peer)
            self.assertIn(domain.entry_peer, domain.peers)

        finally:
            self._cleanup(domain)

    def test_shutdownDomain(self):
        domain = None
        database = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            num_dbs_before = len(domain.databases)
            peer = domain.entry_peer
            sm = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            te = peer.start_transaction_engine(TEST_DB_NAME,
                                               [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                               wait_seconds=10)
            database = domain.get_database(TEST_DB_NAME)
            self.assertIsNotNone(database)
            domain.shutdown()
            for _ in range(0, 10):
                time.sleep(1)
                database = domain.get_database(TEST_DB_NAME)
                if database is None:
                    break

            database = domain.get_database(TEST_DB_NAME)
            self.assertIsNone(database)

        finally:
            self._cleanup(domain)

    def test_listenDomain(self):
        domain = None
        database = None
        dl = NuoTestListener()
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD, dl)
            peer = domain.entry_peer

            self.assertIs(peer, dl.pJoined)
            self.assertIsNone(dl.pLeft)
            self.assertIsNone(dl.nJoined)
            self.assertIsNone(dl.nLeft)
            self.assertIsNone(dl.nFailed)
            self.assertIsNone(dl.nStatusChanged[0])
            self.assertIsNone(dl.nStatusChanged[1])
            self.assertIsNone(dl.dJoined)
            self.assertIsNone(dl.dLeft)
            self.assertFalse(dl.c)

            sm = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            i = 0
            while dl.nJoined is not sm and i < 10:
                time.sleep(1)
                i += 1
            self.assertIs(sm, dl.nJoined)
            i = 0
            while dl.nStatusChanged[0] is not sm and i < 10:
                time.sleep(1)
                i += 1
            self.assertIs(sm, dl.nStatusChanged[0])
            self.assertEqual("RUNNING", dl.nStatusChanged[1])

            te = peer.start_transaction_engine(TEST_DB_NAME,
                                               [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                               wait_seconds=10)
            i = 0
            while dl.nJoined is not te and i < 10:
                time.sleep(1)
                i += 1
            self.assertIs(te, dl.nJoined)
            i = 0
            while dl.nStatusChanged[0] is not te and i < 10:
                time.sleep(1)
                i += 1
            self.assertIs(te, dl.nStatusChanged[0])
            self.assertEqual("RUNNING", dl.nStatusChanged[1])

            database = domain.get_database(TEST_DB_NAME)
            self.assertIs(database, dl.dJoined)

            self.assertIsNone(dl.pLeft)
            self.assertIsNone(dl.nLeft)
            self.assertIsNone(dl.nFailed)
            self.assertIsNone(dl.dLeft)

            te.shutdown()
            i = 0
            while dl.nLeft is not te and i < 10:
                time.sleep(1)
                i += 1
            self.assertIs(te, dl.nLeft)
            self.assertIsNone(dl.dLeft)

            sm.shutdown()
            i = 0
            while dl.nLeft is not sm and i < 10:
                time.sleep(1)
                i += 1
            self.assertIs(sm, dl.nLeft)

            self.assertIs(database, dl.dLeft)

            self.assertFalse(dl.c)
            domain.disconnect()
            self.assertTrue(dl.c)

        finally:
            self._cleanup(domain)

    def test_entryPeer(self):
        domain = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)
            peer = domain.entry_peer
            self.assertIsNotNone(peer)

            self.assertIs(peer.domain, domain)
            self.assertIsNotNone(peer.address)
            self.assertIsNotNone(peer.connect_str)
            self.assertIsNotNone(peer.port)
            self.assertIsNotNone(peer.id)
            self.assertIsNotNone(peer.hostname)
            self.assertIsNotNone(peer.version)
            self.assertTrue(peer.is_broker)

        finally:
            self._cleanup(domain)

    def test_find_peer(self):
        domain = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)
            found_peer = domain.find_peer('localhost')
            self.assertIsNotNone(found_peer)
            self.assertIs(domain.entry_peer, found_peer)
            found_peer = domain.find_peer('localhost', os.environ[
                'NUODB_PORT'] if 'NUODB_PORT' in os.environ else domain.entry_peer.port)
            self.assertIsNotNone(found_peer)
            self.assertIs(domain.entry_peer, found_peer)
        finally:
            self._cleanup(domain)

    def test_startDatabase(self):
        """Starts a TE and SM for a new database on a single host"""
        domain = None
        database = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            num_dbs_before = len(domain.databases)
            peer = domain.entry_peer

            sm = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            self.assertIsNotNone(sm)
            self.assertIs(sm.peer, peer)
            self.assertIsNotNone(sm.address)
            self.assertIsNotNone(sm.port)
            self.assertIsNotNone(sm.pid)
            self.assertIsNotNone(sm.hostname)
            self.assertIsNotNone(sm.version)
            self.assertEqual(len(domain.databases), num_dbs_before + 1)
            self.assertIn(sm, peer.get_local_processes())
            self.assertFalse(sm.is_transactional)

            database = domain.get_database(TEST_DB_NAME)
            self.assertIsNotNone(database)
            self.assertIn(database, domain.databases)
            self.assertIs(database.domain, domain)
            self.assertEqual(database.name, TEST_DB_NAME)
            self.assertEqual(len(database.processes), 1)
            self.assertIn(sm, database.processes)

            te = peer.start_transaction_engine(TEST_DB_NAME,
                                               [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                               wait_seconds=10)
            self.assertIsNotNone(te)
            self.assertIs(te.peer, peer)
            self.assertIsNotNone(te.address)
            self.assertIsNotNone(te.port)
            self.assertIsNotNone(te.pid)
            self.assertIsNotNone(te.hostname)
            self.assertIsNotNone(te.version)
            self.assertEqual(len(domain.databases), num_dbs_before + 1)
            self.assertIn(te, peer.get_local_processes())
            self.assertTrue(te.is_transactional)

            self.assertEqual(len(database.processes), 2)
            self.assertIn(te, database.processes)
            self.assertEqual(sm.version, te.version)
            self.assertIs(sm.database, database)
            self.assertIs(te.database, database)


        finally:
            self._cleanup(domain)

    def test_existingDatabase(self):
        domain = None
        database = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            peer = domain.entry_peer
            sm = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            te = peer.start_transaction_engine(TEST_DB_NAME,
                                               [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                               wait_seconds=10)
            database = domain.get_database(TEST_DB_NAME)
            self.assertIsNotNone(database)
            domain.disconnect()
            time.sleep(1)

            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)
            database = domain.get_database(TEST_DB_NAME)
            self.assertIsNotNone(database)

            self.assertEqual(len(database.processes), 2)


        finally:
            self._cleanup(domain)

    def test_twoDatabase(self):
        domain = None
        database1 = None
        database2 = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            peer = domain.entry_peer
            sm1 = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            te1 = peer.start_transaction_engine(TEST_DB_NAME,
                                                [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                                wait_seconds=10)
            database1 = domain.get_database(TEST_DB_NAME)
            self.assertIsNotNone(database1)

            sm2 = peer.start_storage_manager(TEST_DB_NAME2, gen_archive_path(), True, wait_seconds=10)
            te2 = peer.start_transaction_engine(TEST_DB_NAME2,
                                                [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                                wait_seconds=10)
            database2 = domain.get_database(TEST_DB_NAME2)
            self.assertIsNotNone(database2)
            self.assertNotEqual(database1, database2)

            self.assertIn(sm1, peer.get_local_processes())
            self.assertIn(te1, peer.get_local_processes())
            self.assertIn(sm2, peer.get_local_processes())
            self.assertIn(te2, peer.get_local_processes())

            self.assertIn(sm1, peer.get_local_processes(database1.name))
            self.assertIn(te1, peer.get_local_processes(database1.name))
            self.assertIn(sm2, peer.get_local_processes(database2.name))
            self.assertIn(te2, peer.get_local_processes(database2.name))

            self.assertNotIn(sm1, peer.get_local_processes(database2.name))
            self.assertNotIn(te1, peer.get_local_processes(database2.name))
            self.assertNotIn(sm2, peer.get_local_processes(database1.name))
            self.assertNotIn(te2, peer.get_local_processes(database1.name))

        finally:
            self._cleanup(domain)

    def test_addProcess(self):
        domain = None
        database = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            num_dbs_before = len(domain.databases)
            peer = domain.entry_peer
            sm = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            te = peer.start_transaction_engine(TEST_DB_NAME,
                                               [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                               wait_seconds=10)
            database = domain.get_database(TEST_DB_NAME)
            self.assertEqual(len(database.processes), 2)

            new_te = peer.start_transaction_engine(TEST_DB_NAME, wait_seconds=10)
            self.assertTrue(new_te.wait_for_status('RUNNING', 10))
            self.assertEqual(len(database.processes), 3)

        finally:
            self._cleanup(domain)

    def test_shutdownProcess(self):
        domain = None
        database = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            num_dbs_before = len(domain.databases)
            peer = domain.entry_peer
            sm = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            te = peer.start_transaction_engine(TEST_DB_NAME,
                                               [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                               wait_seconds=10)
            new_te = peer.start_transaction_engine(TEST_DB_NAME, wait_seconds=10)
            database = domain.get_database(TEST_DB_NAME)
            self.assertEqual(len(database.processes), 3)
            new_te.shutdown()
            for _ in range(0, 10):
                time.sleep(1)
                if len(database.processes) == 2:
                    break

            self.assertEqual(len(database.processes), 2)

        finally:
            self._cleanup(domain)

    def test_killProcess(self):
        domain = None
        database = None
        try:
            domain = Domain(self.host, DOMAIN_USER, DOMAIN_PASSWORD)

            num_dbs_before = len(domain.databases)
            peer = domain.entry_peer
            sm = peer.start_storage_manager(TEST_DB_NAME, gen_archive_path(), True, wait_seconds=10)
            te = peer.start_transaction_engine(TEST_DB_NAME,
                                               [('--dba-user', DBA_USER), ('--dba-password', DBA_PASSWORD)],
                                               wait_seconds=10)
            new_te = peer.start_transaction_engine(TEST_DB_NAME, wait_seconds=10)
            database = domain.get_database(TEST_DB_NAME)
            self.assertEqual(len(database.processes), 3)
            new_te.kill()
            time.sleep(1)

            self.assertEqual(len(database.processes), 2)

        finally:
            self._cleanup(domain)

    def _cleanup(self, domain):
        if domain is not None:
            try:
                db_names = [TEST_DB_NAME, TEST_DB_NAME2]
                for name in db_names:
                    db = domain.get_database(name)
                    if db is not None:
                        db.shutdown()
                        i = 0
                        while len(db.processes) > 0 and i < 10:
                            time.sleep(1)
                            i += 1
                    if domain.get_database(name) is not None:
                        raise Exception("Could not shutdown existing test database %s" % (name))

            finally:
                domain.disconnect()


class NuoTestListener(object):
    def __init__(self):
        self.pJoined = None
        self.pLeft = None
        self.nJoined = None
        self.nLeft = None
        self.nFailed = None
        self.nStatusChanged = [None, None]
        self.dJoined = None
        self.dLeft = None
        self.c = False

    def peer_joined(self, peer):
        self.pJoined = peer

    def peer_left(self, peer):
        self.pLeft = peer

    def process_joined(self, process):
        self.nJoined = process

    def process_left(self, process):
        self.nLeft = process

    def process_failed(self, process):
        self.nFailed = process

    def process_status_changed(self, process, status):
        self.nStatusChanged[0] = process
        self.nStatusChanged[1] = status

    def database_joined(self, database):
        self.dJoined = database

    def database_left(self, database):
        self.dLeft = database

    def closed(self):
        self.c = True


def gen_archive_path():
    return os.path.join(tempfile.gettempdir(), ''.join([random.choice(string.ascii_lowercase) for _ in range(10)]))


if __name__ == '__main__':
    warnings.simplefilter("always")
    unittest.main()
