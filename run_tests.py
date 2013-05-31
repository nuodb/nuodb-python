from tests.nuodb_basic_test import *
from tests.nuodb_dbapi20_test import *

import unittest
import os

os.system('echo "quit" | /opt/nuodb/run-quickstart')
path = os.getcwd()
os.chdir(path + '/tests')
if __name__ == '__main__':
    unittest.main()
