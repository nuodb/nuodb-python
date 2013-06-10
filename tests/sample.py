""" This assumes that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart
"""
import pynuodb

connection = pynuodb.connect("test", "localhost", "dba", "goalie",  options = {"schema":"hockey"})
cursor = connection.cursor()
cursor.arraysize = 3
cursor.execute("select * from hockey")
print cursor.fetchone()
