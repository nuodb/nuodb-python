import unittest

suite = unittest.TestLoader().discover('tests', pattern="*_test.py")

if __name__ == '__main__':
    res = unittest.TextTestRunner().run(suite)

    if len(res.errors) == 0 and len(res.failures) == 0:
        exit(0)

    exit(1)
