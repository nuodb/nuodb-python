""" This assumes that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart
"""

import os

import pynuodb

port = os.environ.get('NUODB_PORT')
host = 'localhost' + (':' + port if port else '')

connection = pynuodb.connect("test", host, "dba", "goalie", options={"schema": "hockey"})
cursor = connection.cursor()
cursor.arraysize = 3
cursor.execute("select * from hockey")
print(cursor.fetchone())
