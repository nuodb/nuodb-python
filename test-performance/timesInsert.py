# A database named test with user dba / password dba must be created first

import os
import time

import pynuodb

smallIterations = 100
largeIterations = smallIterations * 1000


def gettime():
    return time.time()


def insert(count):
    for i in range(count):
        cursor.execute("INSERT INTO perf_test (a,b ) VALUES (%d,'A')" % i)
    connection.commit()


def select():
    cursor.execute("select * from perf_test")
    cursor.fetchall()


dropTable = "drop table perf_test cascade if exists"
createTable = "create table perf_test (a int,b char)"

port = os.environ.get('NUODB_PORT')
if not port:
    port = '48004'

options = {}
trustStore = os.environ.get('NUOCMD_VERIFY_SERVER')
if trustStore:
    options = {'trustStore': trustStore, 'verifyHostname': 'False'}

connection = pynuodb.connect("test", "localhost:" + port, "dba", "dba",
                             options=options)
cursor = connection.cursor()
cursor.execute("use test")

# Begin SMALL_INSERT_ITERATIONS test
cursor.execute(dropTable)
cursor.execute(createTable)
start = gettime()
insert(smallIterations)
smallInsertElapsed = gettime() - start

print("Elapse time of SMALL_INSERT_ITERATIONS = %.4fs" % (smallInsertElapsed))

# Begin SMALL_SELECT_ITERATIONS test
start = gettime()
select()
smallSelectElapsed = gettime() - start
print("Elapse time of SMALL_SELECT_ITERATIONS = %.4fs" % (smallSelectElapsed))

# Begin LARGE_INSERT_ITERATIONS test
cursor.execute(dropTable)
cursor.execute(createTable)

start = gettime()
insert(largeIterations)
largeInsertElapsed = gettime() - start

print("Elapse time of LARGE_INSERT_ITERATIONS = %.4fs" % (largeInsertElapsed))

# Begin LARGE_SELECT_ITERATIONS test
start = gettime()
select()
largeSelectElapsed = gettime() - start

print("Elapse time of LARGE_SELECT_ITERATIONS = %.4fs" % (largeSelectElapsed))

if largeInsertElapsed > smallInsertElapsed * 1000:
    print("Insert is too slow!")

if largeSelectElapsed > smallSelectElapsed * 1000:
    print("Select is too slow!")

print("\n")
