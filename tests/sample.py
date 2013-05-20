""" This assumes that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart
"""
import pynuodb
connection = pynuodb.connect("test", "localhost", "dba", "goalie", schema='hockey')
cursor = connection.cursor()
cursor.arraysize = 3
cursor.execute("select * from hockey")
cursor.fetchmany()
cursor.arraysize = 5
cursor.execute("select * from hockey")
cursor.fetchmany()
cursor.arraysize = 7
cursor.execute("select * from hockey")
cursor.fetchmany()
cursor.arraysize = 10
cursor.execute("select * from hockey")
cursor.fetchmany()
print cursor.fetchone()
