"""This assumes that you have the quickstart database running (test@localhost).

If you don't, start it by running:  /opt/nuodb/samples/nuoadmin-quickstart
"""

import os

import pynuodb

options = {"schema": "hockey"}

port = os.environ.get('NUODB_PORT')
host = 'localhost' + (':' + port if port else '')

trustStore = os.environ.get('NUOCMD_VERIFY_SERVER')
if trustStore:
    options['trustStore'] = trustStore
    options['verifyHostname'] = 'false'

connection = pynuodb.connect("test", host, "dba", "goalie", options=options)

cursor = connection.cursor()
cursor.arraysize = 3
cursor.execute("select * from hockey")
print(cursor.fetchone())
