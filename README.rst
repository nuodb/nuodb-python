==============
NuoDB - Python
==============

.. image:: https://travis-ci.org/nuodb/nuodb-python.svg?branch=master
    :target: https://travis-ci.org/nuodb/nuodb-python
.. image:: https://gemnasium.com/nuodb/nuodb-python.svg
    :target: https://gemnasium.com/nuodb/nuodb-python
.. image:: https://landscape.io/github/nuodb/nuodb-python/master/landscape.svg?style=flat
   :target: https://landscape.io/github/nuodb/nuodb-python/master
   :alt: Code Health

.. contents::

This package contains the official pure-Python NuoDB_ client library that
provides both a standard `PEP 249`_ SQL API, a NuoDB administration API.

Requirements
------------

* Python -- one of the following:

  - CPython_ >= 2.7 or <= 3.4

* NuoDB -- one of the following:

  - NuoDB_ >= 2.0.4

If you haven't done so already, `Download and Install NuoDB <http://dev.nuodb.com/download-nuodb/request/download/>`_.

Installation
------------

The last stable release is available on PyPI and can be installed with ``pip``::

    $ pip install pynuodb

Alternatively (e.g. if ``pip`` is not available), a tarball can be downloaded
from GitHub and installed with Setuptools::

    $ curl -L https://github.com/nuodb/nuodb-python/archive/master.tar.gz | tar xz
    $ cd nuodb-python*
    $ python setup.py install
    $ # The folder nuodb-python* can be safely removed now.

Example
-------

The following examples assume that you have the quickstart database running (test@localhost).
If you don't, you can start it by running /opt/nuodb/run-quickstart.

Simple example for connecting and reading from an existing table:

.. code:: python

    import pynuodb

    connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
    cursor = connection.cursor()
    cursor.arraysize = 3
    cursor.execute("select * from hockey")
    print cursor.fetchone()

Data can be inserted into a table either explicitly within the execute method:

.. code:: python

    import pynuodb

    connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
    cursor = connection.cursor()

    cursor.execute("create table typetest (bool_col boolean, date_col date, " +
                   "string_col string, integer_col integer)")

    cursor.execute("insert into typetest values ('False', '2012-10-03', 'hello world', 42)")
    cursor.execute("select * from typetest")
    print cursor.fetchone()

or using variables:

.. code:: python

    import pynuodb

    connection = pynuodb.connect("test", "localhost", "dba", "goalie", options={'schema':'hockey'})
    cursor = connection.cursor()

    cursor.execute("create table variabletest (bool_col boolean, date_col date, " +
                   "string_col string, integer_col integer)")

    test_vals = (False, pynuodb.Date(2012,10,3), "hello world", 42)
    cursor.execute("insert into variabletest values (?, ?, ?, ?)", test_vals)
    cursor.execute("select * from variabletest")
    print cursor.fetchone()

For further information on getting started with NuoDB, please refer to the Documentation_.

Resources
---------

DB-API 2.0: http://www.python.org/dev/peps/pep-0249/
NuoDB Documentation: http://doc.nuodb.com/display/DOC/Getting+Started

License
-------

PyNuoDB is licensed under a `BSD 3-Clause License <https://github.com/nuodb/nuodb-python/blob/master/LICENSE>`_.

.. _Documentation: http://doc.nuodb.com/display/DOC/Getting+Started
.. _NuoDB: http://www.nuodb.com/
.. _CPython: http://www.python.org/
.. _PEP 249: https://www.python.org/dev/peps/pep-0249/
