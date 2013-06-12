NuoDB - Python
==============

[![Build Status](https://travis-ci.org/nuodb/nuodb-python.png?branch=master)](https://travis-ci.org/nuodb/nuodb-python)

This is the official Python package for [NuoDB](http://www.nuodb.com). Please note that this driver is currently in beta.

### Requirements

If you haven't already, [Download and Install NuoDB](http://nuodb.com/download-nuodb/). Currently the driver supports Python version 2.7 only.

### Install

Install from source by running

```bash
git clone git://github.com/nuodb/nuodb-python.git
cd nuodb-python
sudo python setup.py install
```

### Example

Simple example for connecting and reading from an existing table:

```python
""" This assumes that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart
"""
import pynuodb

connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
cursor = connection.cursor()
cursor.arraysize = 3
cursor.execute("select * from hockey")
print cursor.fetchone()
```

Data can be inserted into a table either explicitly within the execute method...

```python
import pynuodb

connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
cursor = connection.cursor()

cursor.execute("create table typetest (bool_col boolean, date_col date, " +
               "string_col string, integer_col integer)")
               
cursor.execute("insert into typetest values ('False', '2012-10-03', 'hello world', 42)")
cursor.execute("select * from typetest")
print cursor.fetchone()
```

or using variables...

```python
import pynuodb

connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
cursor = connection.cursor()

cursor.execute("create table variabletest (bool_col boolean, date_col date, " +
               "string_col string, integer_col integer)")

test_vals = (False, pynuodb.Date(2012,10,3), "hello world", 42)
cursor.execute("insert into variabletest values (?, ?, ?, ?)", test_vals)
cursor.execute("select * from variabletest")
print cursor.fetchone()
```

For further information on getting started with NuoDB, please refer to the [NuoDB wiki](http://doc.nuodb.com/display/DOC/Getting+Started) 


### License

[NuoDB License](https://github.com/nuodb/nuodb-drivers/blob/master/LICENSE)

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/b9c6afe4ffa75ebbb78f07ac04c750a7 "githalytics.com")](http://githalytics.com/nuodb/nuodb-python)

