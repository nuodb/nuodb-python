import pynuodb

connection = pynuodb.connect("test", "cloud", "user", "localhost", 48004)
cursor = connection.cursor()

# works
# result = cursor.execute('select 1 as one from dual')

# does not work with events table in user schema
result = cursor.execute('select name, type from events')

print ">> %s" % result
