import pynuodb
connection = pynuodb.connect("test", "cloud", "user", "localhost", 48004)
cursor = connection.cursor()
cursor.execute("select name, type, id from events")
print cursor.description
row = cursor.fetchone()
print row

