NuoDB - Python
==============

.. image:: https://travis-ci.org/nuodb/nuodb-python.svg?branch=master
    :target: https://travis-ci.org/nuodb/nuodb-python

This is the official Python package for `NuoDB <http://www.nuodb.com>`_.

Requirements
------------

If you haven't already, `Download and Install NuoDB <http://nuodb.com/download-nuodb/>`_.
Currently the driver supports Python version 2.7 only.

To run the tests, you will also need `pytz <http://pytz.sourceforge.net/>`_::

    pip install pytz

Install
-------

Install from source by running::

    git clone git://github.com/nuodb/nuodb-python.git
    cd nuodb-python
    sudo python setup.py install

Or install from pip::

    pip install pynuodb

Example
-------

The following examples assume that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart.

Simple example for connecting and reading from an existing table::

    import pynuodb

    connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
    cursor = connection.cursor()
    cursor.arraysize = 3
    cursor.execute("select * from hockey")
    print cursor.fetchone()

Data can be inserted into a table either explicitly within the execute method::

    import pynuodb

    connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
    cursor = connection.cursor()

    cursor.execute("create table typetest (bool_col boolean, date_col date, " +
                   "string_col string, integer_col integer)")

    cursor.execute("insert into typetest values ('False', '2012-10-03', 'hello world', 42)")
    cursor.execute("select * from typetest")
    print cursor.fetchone()

or using variables::

    import pynuodb

    connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
    cursor = connection.cursor()

    cursor.execute("create table variabletest (bool_col boolean, date_col date, " +
                   "string_col string, integer_col integer)")

    test_vals = (False, pynuodb.Date(2012,10,3), "hello world", 42)
    cursor.execute("insert into variabletest values (?, ?, ?, ?)", test_vals)
    cursor.execute("select * from variabletest")
    print cursor.fetchone()

For further information on getting started with NuoDB, please refer to the
`NuoDB Documentation <http://doc.nuodb.com/display/doc/NuoDB+at+a+Glance>`_.

License
-------

PyNuoDB is licensed under a `BSD 3-Clause License <https://github.com/nuodb/nuodb-python/blob/master/LICENSE>`_.
