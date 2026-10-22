==============
NuoDB - Python
==============

.. image:: https://dl.circleci.com/status-badge/img/gh/nuodb/nuodb-python/tree/master.svg?style=svg
    :target: https://dl.circleci.com/status-badge/redirect/gh/nuodb/nuodb-python/tree/master

.. contents::

This package contains the community driven NuoDB_ client library that provides
a standard `PEP 249`_ SQL API. It includes optional Cython acceleration and a
pure-Python fallback. This is a community driven driver with limited support
and testing from NuoDB.

Requirements
------------

* Python >= 3.9

  - Tested with CPython_ 3.9

* NuoDB_ >= 6.0.2
* Runtime dependencies: ``tzlocal``, ``jdcal``, and ``cryptography>=36.0``.
  These are installed automatically by pip. Cryptography is required in 5.0;
  the former ``[crypto]`` extra is no longer needed.

| If you don't have a NuoDB domain available you can create one using the Docker
  image on DockerHub.
| See `Quick Start Guides / Docker`_.

Installation
------------

The current stable release is available on PyPI and can be installed with
``pip``::

    $ pip install pynuodb

To install the development version from a GitHub source archive::

    $ curl -L https://github.com/nuodb/nuodb-python/archive/master.tar.gz | tar xz
    $ cd nuodb-python*
    $ python -m pip install .
    # The extracted source directory can be safely removed after installation.

Cython acceleration
~~~~~~~~~~~~~~~~~~~

When available, the optional native extension accelerates result decoding and
batch parameter encoding for ``executemany()``. Installing a compatible binary
wheel does not require a compiler. Building the extension from source requires
a C compiler and the development headers for the Python interpreter being used.
Pip installs the build dependencies, including Cython, from ``pyproject.toml``.
If native compilation fails, installation can continue with the pure-Python
implementation.

To check whether the extension is available::

    $ python -c "from pynuodb import _fetch; print(_fetch.__file__)"

An import error means that the pure-Python fallback is being used. The extension
uses the CPython C API; PyPy is untested.

See `Release notes`_ for changes since 4.1.0 and `CONTRIBUTING.rst`_ for development
and testing instructions.

Example
-------

Here is an example using the `PEP 249`_ API that creates some tables, inserts
some data, runs a query, and cleans up after itself:

.. code:: python

    import pynuodb

    options = {"schema": "test"}
    connect_kw_args = {'database': "test", 'host': "localhost", 'user': "dba", 'password': "dba", 'options': options}

    connection = pynuodb.connect(**connect_kw_args)
    cursor = connection.cursor()
    try:
        stmt_drop = "DROP TABLE IF EXISTS names"
        cursor.execute(stmt_drop)

        stmt_create = """
        CREATE TABLE names (
            id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name VARCHAR(30) DEFAULT '' NOT NULL,
            age INTEGER DEFAULT 0
        )"""
        cursor.execute(stmt_create)

        names = (('Greg', 17,), ('Marsha', 16,), ('Jan', 14,))
        stmt_insert = "INSERT INTO names (name, age) VALUES (?, ?)"
        cursor.executemany(stmt_insert, names)

        connection.commit()

        age_limit = 15
        stmt_select = "SELECT id, name FROM names where age > ? ORDER BY id"
        cursor.execute(stmt_select, (age_limit,))
        print("Results:")
        for row in cursor.fetchall():
            print("%d | %s" % (row[0], row[1]))

    finally:
        cursor.execute(stmt_drop)
        cursor.close()
        connection.close()

For further information on getting started with NuoDB, please refer to the Documentation_.

Resources
---------

DB-API 2.0: https://www.python.org/dev/peps/pep-0249/

NuoDB Documentation: https://doc.nuodb.com/nuodb/latest/introduction-to-nuodb/

License
-------

PyNuoDB is licensed under a `BSD 3-Clause License`_.

.. _Documentation: https://doc.nuodb.com/nuodb/latest/introduction-to-nuodb/
.. _NuoDB: https://www.nuodb.com/
.. _CPython: https://www.python.org/
.. _PEP 249: https://www.python.org/dev/peps/pep-0249/
.. _Quick Start Guides / Docker: https://doc.nuodb.com/nuodb/latest/quick-start-guide/docker/
.. _BSD 3-Clause License: https://github.com/nuodb/nuodb-python/blob/master/LICENSE
.. _Release notes: https://github.com/nuodb/nuodb-python/blob/master/CHANGES.rst
.. _CONTRIBUTING.rst: https://github.com/nuodb/nuodb-python/blob/master/CONTRIBUTING.rst
