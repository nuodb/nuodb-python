import unittest

import pynuodb.util
from .nuodb_base import NuoBase

HOST = "localhost"
DOMAIN_USER = "domain"
DOMAIN_PASSWORD = "bird"

DBA_USER = 'dba'
DBA_PASSWORD = 'dba_password'
DATABASE_NAME = 'pynuodb_test'


class NuoDBStressTest(NuoBase):
    @unittest.skip("Too long for Bamboo")
    def test_million_reads(self):
        connection = pynuodb.connect(DATABASE_NAME, HOST, DBA_USER, DBA_PASSWORD, options={'schema': 'hockey'})
        cursor = connection.cursor()
        alphabet = "abcdefghijklmnopqrstuwxyz"

        cursor.execute("USE test")
        cursor.execute("DROP TABLE hundred IF EXISTS")
        cursor.execute("CREATE TABLE hundred (f1 INTEGER)")

        for i in range(100):
            cursor.execute("INSERT INTO hundred VALUES (?)", [i])
        connection.commit();

        cursor.execute("SELECT ? FROM hundred AS a1, hundred AS a2, hundred AS a3", [alphabet])

        # Fetch the next 1,000,000 results
        for i in range(1000000):
            rowArray = cursor.fetchone()

        cursor.close()
        connection.close()

    @unittest.skip("Too long for Bamboo")
    def test_thousand_connection(self):
        for i in range(0, 1000):
            connection = pynuodb.connect(DATABASE_NAME, HOST, DBA_USER, DBA_PASSWORD, options={'schema': 'hockey'})
            connection.close()
