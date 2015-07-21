import time
import pynuodb

smallIterations = 100
largeIterations = smallIterations * 1000

dropTable = "drop table perf_test cascade if exists"
createTable = "create table perf_test (a int,b char)"
#A database named test with user dba and password dba must be created before running
connection = pynuodb.connect("dba", "localhost", "dba", "dba")
cursor = connection.cursor()

cursor.execute("use test")
cursor.execute(dropTable)
cursor.execute(createTable)

""" Begin SMALL_INSERT_ITERATIONS test"""

smallIterationsInsertTime = time.clock()
for i in range(smallIterations):
	cursor.execute("INSERT INTO perf_test (a,b ) VALUES (%d,'A')" % i)
connection.commit()
smallIterationsInsertTime = time.clock() - smallIterationsInsertTime

print("\n Elapse time of SMALL_INSERT_ITERATIONS   =   " + str(smallIterationsInsertTime) + "s")

""" Begin SMALL_SELECT_ITERATIONS test"""

smallIterationsSelectTime = time.clock()
cursor.execute("select * from perf_test")
cursor.fetchall()
smallIterationsSelectTime = time.clock() - smallIterationsSelectTime
print("\n Elapse time of SMALL_SELECT_ITERATIONS   =   " + str(smallIterationsSelectTime) + "s")

""" Begin LARGE_INSERT_ITERATIONS test"""
cursor.execute(dropTable)
cursor.execute(createTable)

largeIterationsInsertTime = time.clock()
for i in range(largeIterations):
	cursor.execute("INSERT INTO perf_test (a,b ) VALUES (%d,'A')" % i)
connection.commit()
largeIterationsInsertTime = time.clock() - largeIterationsInsertTime

print("\n Elapse time of LARGE_INSERT_ITERATIONS   =   " + str(largeIterationsInsertTime) + "s")

""" Begin LARGE_SELECT_ITERATIONS test"""

largeIterationsSelectTime = time.clock()
cursor.execute("select * from perf_test")
cursor.fetchall()
largeIterationsSelectTime = time.clock() - largeIterationsSelectTime 

print("\n Elapse time of LARGE_SELECT_ITERATIONS   =   " + str(largeIterationsSelectTime) + "s")

if largeIterationsInsertTime > smallIterationsInsertTime * 1000 :
	print("Insert is too slow!")

if largeIterationsSelectTime > smallIterationsSelectTime * 1000 :
	print("Select is too slow!")
