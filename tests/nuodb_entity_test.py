import unittest
import tempfile
import time
from pynuodb.entity import Domain
import warnings

BROKER_HOST = 'localhost'
DOMAIN_USER = 'domain'
DOMAIN_PASSWORD = 'bird'
TEST_DB_NAME = 'entity_test_database'
TEST_DB_NAME2 = 'entity_test_database2'
DBA_USER = 'dba'
DBA_PASSWORD = 'dba'


class NuoDBEntityTest(unittest.TestCase):
    def setUp(self):
        domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
        self._cleanup(domain)
        
    def tearDown(self):
        pass
        
    def test_connectDomain(self):
        domain = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            
            self.assertIsNotNone(domain)
            self.assertFalse(domain.closed())
            
            self.assertEqual(domain.getUser(), DOMAIN_USER)
            self.assertEqual(domain.getPassword(), DOMAIN_PASSWORD)     
            self.assertIsNotNone(domain.getDomainName())
            self.assertIsNotNone(domain.getEntryPeer())
            self.assertIn(domain.getEntryPeer(), domain.getPeers())
            
        finally:
            self._cleanup(domain)
                
    def test_shutdownDomain(self):
        domain = None
        database = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
                        
            num_dbs_before = domain.getDatabaseCount()
            peer = domain.getEntryPeer()
            sm = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            te = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertIsNotNone(database)
            domain.shutdown()
            time.sleep(1)
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertIsNone(database)
            
        finally:
            self._cleanup(domain)
            
    def test_listenDomain(self):
        domain = None
        database = None
        dl = TestListener()
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD, dl)
            peer = domain.getEntryPeer()
            
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
            
            sm = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            i = 0
            while dl.nJoined is not sm and i < 10:
                time.sleep(1)
            self.assertIs(sm, dl.nJoined)
            i = 0
            while dl.nStatusChanged[0] is not sm and i < 10:
                time.sleep(1)
            self.assertIs(sm, dl.nStatusChanged[0])
            self.assertEqual("RUNNING", dl.nStatusChanged[1])
            
            te = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            i = 0
            while dl.nJoined is not te and i < 10:
                time.sleep(1)
            self.assertIs(te, dl.nJoined)
            self.assertIs(te, dl.nStatusChanged[0])
            self.assertEqual("RUNNING", dl.nStatusChanged[1])
            
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertIs(database, dl.dJoined)
            
            self.assertIsNone(dl.pLeft)
            self.assertIsNone(dl.nLeft)
            self.assertIsNone(dl.nFailed)
            self.assertIsNone(dl.dLeft)
            
            te.shutdown()
            i = 0
            while dl.nLeft is not te and i < 10:
                time.sleep(1)
            self.assertIs(te, dl.nLeft)
            self.assertIsNone(dl.dLeft)  
            
            sm.shutdown()
            i = 0
            while dl.nLeft is not sm and i < 10:
                time.sleep(1)
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
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            peer = domain.getEntryPeer()
            self.assertIsNotNone(peer)
            
            self.assertIs(peer.getDomain(), domain)
            self.assertIsNotNone(peer.getAddress())
            self.assertIsNotNone(peer.getConnectStr())
            self.assertIsNotNone(peer.getPort())
            self.assertIsNotNone(peer.getId())
            self.assertIsNotNone(peer.getHostname())
            self.assertIsNotNone(peer.getVersion())
            self.assertTrue(peer.isBroker())
            
        finally:
            self._cleanup(domain)          
                
    def test_startDatabase(self):
        """Starts a TE and SM for a new database on a single host"""
        domain = None
        database = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            
            num_dbs_before = domain.getDatabaseCount()
            peer = domain.getEntryPeer()
            
            sm = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            self.assertIsNotNone(sm)
            self.assertIs(sm.getPeer(), peer)
            self.assertIsNotNone(sm.getAddress())
            self.assertIsNotNone(sm.getPort())
            self.assertIsNotNone(sm.getPid())
            self.assertIsNotNone(sm.getHostname())
            self.assertIsNotNone(sm.getVersion())
            self.assertEqual(domain.getDatabaseCount(), num_dbs_before + 1)
            self.assertIn(sm, peer.getLocalProcesses())
            self.assertFalse(sm.isTransactional())
            
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertIsNotNone(database)
            self.assertIn(database, domain.getDatabases())
            self.assertIs(database.getDomain(), domain)
            self.assertEqual(database.getName(), TEST_DB_NAME)
            self.assertEqual(database.getProcessCount(), 1)
            self.assertIn(sm, database.getProcesses())
            
            te = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            self.assertIsNotNone(te)
            self.assertIs(te.getPeer(), peer)
            self.assertIsNotNone(te.getAddress())
            self.assertIsNotNone(te.getPort())
            self.assertIsNotNone(te.getPid())
            self.assertIsNotNone(te.getHostname())
            self.assertIsNotNone(te.getVersion())
            self.assertEqual(domain.getDatabaseCount(), num_dbs_before + 1)
            self.assertIn(te, peer.getLocalProcesses())
            self.assertTrue(te.isTransactional())
            
            self.assertEqual(database.getProcessCount(), 2)
            self.assertIn(te, database.getProcesses())
            self.assertEqual(sm.getVersion(), te.getVersion())
            self.assertIs(sm.getDatabase(), database)
            self.assertIs(te.getDatabase(), database)
            
            
        finally:
            self._cleanup(domain)
                
    def test_existingDatabase(self):
        domain = None
        database = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            
            peer = domain.getEntryPeer()
            sm = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            te = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertIsNotNone(database)
            domain.disconnect()
            time.sleep(1)
            
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertIsNotNone(database)
            
            self.assertEqual(database.getProcessCount(), 2)            
            
            
        finally:
            self._cleanup(domain)
                
                
    def test_twoDatabase(self):
        domain = None
        database1 = None
        database2 = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            
            peer = domain.getEntryPeer()
            sm1 = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            te1 = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            database1 = domain.getDatabase(TEST_DB_NAME)
            self.assertIsNotNone(database1)
            
            sm2 = peer.startStorageManager(TEST_DB_NAME2, tempfile.mkdtemp(), True, waitSeconds=10)
            te2 = peer.startTransactionEngine(TEST_DB_NAME2, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            database2 = domain.getDatabase(TEST_DB_NAME2)
            self.assertIsNotNone(database2)
            self.assertNotEqual(database1, database2)
            
            self.assertIn(sm1, peer.getLocalProcesses())
            self.assertIn(te1, peer.getLocalProcesses())
            self.assertIn(sm2, peer.getLocalProcesses())
            self.assertIn(te2, peer.getLocalProcesses())
            
            self.assertIn(sm1, peer.getLocalProcesses(database1.getName()))
            self.assertIn(te1, peer.getLocalProcesses(database1.getName()))
            self.assertIn(sm2, peer.getLocalProcesses(database2.getName()))
            self.assertIn(te2, peer.getLocalProcesses(database2.getName()))
            
            self.assertNotIn(sm1, peer.getLocalProcesses(database2.getName()))
            self.assertNotIn(te1, peer.getLocalProcesses(database2.getName()))
            self.assertNotIn(sm2, peer.getLocalProcesses(database1.getName()))
            self.assertNotIn(te2, peer.getLocalProcesses(database1.getName()))
            
        finally:
            self._cleanup(domain)
                
    def test_addProcess(self):
        domain = None
        database = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            
            num_dbs_before = domain.getDatabaseCount()
            peer = domain.getEntryPeer()
            sm = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            te = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertEqual(database.getProcessCount(), 2)
            
            new_te = peer.startTransactionEngine(TEST_DB_NAME, waitSeconds=10)
            self.assertTrue(new_te.waitForStatus('RUNNING', 10))
            self.assertEqual(database.getProcessCount(), 3)
            
        finally:
            self._cleanup(domain)
            
    def test_shutdownProcess(self):
        domain = None
        database = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            
            num_dbs_before = domain.getDatabaseCount()
            peer = domain.getEntryPeer()
            sm = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            te = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            new_te = peer.startTransactionEngine(TEST_DB_NAME, waitSeconds=10)
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertEqual(database.getProcessCount(), 3)
            new_te.shutdown()
            time.sleep(1)
            
            self.assertEqual(database.getProcessCount(), 2)
            
        finally:
            self._cleanup(domain)
            
    def test_killProcess(self):
        domain = None
        database = None
        try:
            domain = Domain(BROKER_HOST, DOMAIN_USER, DOMAIN_PASSWORD)
            
            num_dbs_before = domain.getDatabaseCount()
            peer = domain.getEntryPeer()
            sm = peer.startStorageManager(TEST_DB_NAME, tempfile.mkdtemp(), True, waitSeconds=10)
            te = peer.startTransactionEngine(TEST_DB_NAME, [('--dba-user', DBA_USER),('--dba-password', DBA_PASSWORD)], waitSeconds=10)
            new_te = peer.startTransactionEngine(TEST_DB_NAME, waitSeconds=10)
            database = domain.getDatabase(TEST_DB_NAME)
            self.assertEqual(database.getProcessCount(), 3)
            new_te.kill()
            time.sleep(1)
            
            self.assertEqual(database.getProcessCount(), 2)
            
        finally:
            self._cleanup(domain)
            
    def _cleanup(self, domain):
        if domain is not None:
            try:
                db_names = [db.getName() for db in domain.getDatabases()]
                for name in db_names:
                    db = domain.getDatabase(name)
                    db.shutdown()
                    i = 0
                    while db.getProcessCount() > 0 and i < 10:
                        time.sleep(1)
                        i += 1
                    if domain.getDatabase(name) is not None:
                        raise Exception("Could not shutdown existing test database %s" % (name))
                        
            finally:
                domain.disconnect()
                
class TestListener(object):
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
    
    def peerJoined(self, peer):
        self.pJoined = peer
        
    def peerLeft(self, peer):
        self.pLeft = peer
        
    def processJoined(self, process):
        self.nJoined = process
        
    def processLeft(self, process):
        self.nLeft = process
        
    def processFailed(self, process):
        self.nFailed = process
        
    def processStatusChanged(self, process, status):
        self.nStatusChanged[0] = process
        self.nStatusChanged[1] = status
        
    def databaseJoined(self, database):
        self.dJoined = database
        
    def databaseLeft(self, database):
        self.dLeft = database
        
    def closed(self):
        self.c = True


if __name__ == '__main__':
    warnings.simplefilter("always")
    unittest.main()