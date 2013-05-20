#!/usr/bin/env python

import popen2
import pynuodb
import tempfile
import unittest

HOST            = "localhost"
DOMAIN_USER     = "domain"
DOMAIN_PASSWORD = "bird"

DBA_USER        = 'dba'
DBA_PASSWORD    = 'dba'
DATABASE_NAME   = 'dbapi20_test'

NUODB_HOME      = "/opt/nuodb"

NUODB_MGR       = "java -jar %s/jar/nuodbmanager.jar --broker %s --user %s --password %s --command \"%%s\"" % (NUODB_HOME, HOST, DOMAIN_USER, DOMAIN_PASSWORD)
START_SM        = "start process sm host %s database %s archive %s initialize yes" % (HOST, DATABASE_NAME, tempfile.mkdtemp())
START_TE        = "start process te host %s database %s options '--dba-user %s --dba-password %s'" % (HOST, DATABASE_NAME, DBA_USER, DBA_PASSWORD)

class NuoBase(unittest.TestCase):
    driver = pynuodb
    connect_args = ()
    connect_kw_args = {'database': DATABASE_NAME, 'host': HOST, 'user': DBA_USER, 'password': DBA_PASSWORD}

    lower_func = 'lower' # For stored procedure test

    def setUp(self):
        try:
            con = self._connect()
            con.close()
        except:
            cmd = NUODB_MGR % (START_SM)
            cout,cin = popen2.popen2(cmd)
            cin.close()
            cout.read()
            
            cmd = NUODB_MGR % (START_TE)
            cout,cin = popen2.popen2(cmd)
            cin.close()
            cout.read()

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
