import pynuodb
connection = pynuodb.connect("test", "cloud", "user", "localhost", 48004)
cursor = connection.cursor()
cursor.execute("select name, type, id from event")
row = cursor.fetchone()
print row

