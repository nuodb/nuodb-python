import pynuodb

connection = pynuodb.connect("test", "cloud", "user", "localhost", 48004)
cursor = connection.cursor()
cursor.execute('select name, type, id from events')
row = cursor.fetchone()

print ">> %s" % row
