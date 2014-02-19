import unittest

suite = unittest.TestLoader().discover('tests', pattern="*_test.py")

if __name__ == '__main__':
    unittest.TextTestRunner().run(suite)
