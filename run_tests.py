from tests.nuodb_basic_test import *
from tests.nuodb_executionflow_test import *
from tests.nuodb_dbapi20_test import *
from tests.nuodb_transaction_test import *
from tests.nuodb_globals_test import *
from tests.nuodb_cursor_test import *
from tests.nuodb_blob_test import *
from tests.nuodb_huge_test import *

import unittest
import os

path = os.getcwd()
os.chdir(path + '/tests')
if __name__ == '__main__':
    unittest.main()
