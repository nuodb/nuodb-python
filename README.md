NuoDB - Python
==============

[![Build Status](https://travis-ci.org/nuodb/nuodb-python.png?branch=master)](https://travis-ci.org/nuodb/nuodb-python)

This is the official Python pip package for [NuoDB](http://www.nuodb.com). It implements the NuoDB [SQL Protocol](https://github.com/nuodb/nuodb-python/blob/master/SQL_Protocol.md)

### Requirements

If you haven't already, [Download and Install NuoDB](http://nuodb.com/download-nuodb/)

### Install

	$ pip install pynuodb

### Example

```python
""" This assumes that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart
"""
import pynuodb

connection = pynuodb.connect("test", "localhost", "dba", "goalie", schema='hockey')
cursor = connection.cursor()
cursor.arraysize = 3
cursor.execute("select * from hockey")
print cursor.fetchone()
```

### License

[NuoDB License](https://github.com/nuodb/nuodb-drivers/blob/master/LICENSE)
