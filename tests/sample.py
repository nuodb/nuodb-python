""" This assumes that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart
"""
import pynuodb
connection = pynuodb.connect("test", "dba", "goalie", "localhost")
cursor = connection.cursor()
cursor.execute("select * from hockey.hockey")
print cursor.fetchone()
