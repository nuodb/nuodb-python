==============
NuoDB - Python
==============

.. image:: https://dl.circleci.com/status-badge/img/gh/nuodb/nuodb-python/tree/master.svg?style=svg
    :target: https://dl.circleci.com/status-badge/redirect/gh/nuodb/nuodb-python/tree/master

.. contents::

This package contains the community driven NuoDB_ client library that provides
a standard `PEP 249`_ SQL API. It includes a pure-Python implementation and an
optional Cython-based accelerator for encoding parameters and decoding results.
The same Python API is used with or without the accelerator. This is a
community driven driver with limited support and testing from NuoDB.

Requirements
------------

* Python >= 3.9

  - The CI wheel matrix builds and tests standard (GIL-enabled) CPython_
    3.9, 3.10, 3.11, 3.12, and 3.13 on Linux x86-64/ARM64, macOS Intel/Apple
    Silicon, and Windows x86-64.
  - Each wheel runs database-independent accelerator, encoding, and date/time
    tests. Full database regressions and performance tests run separately on
    Linux using the Python version in the NuoDB CI image; they do not run for
    every wheel combination.

* NuoDB_ >= 6.0.2

| If you don't have a NuoDB domain available you can create one using the Docker
  image on DockerHub.
| See `Quick Start Guides / Docker`_.

Installation
------------

The current stable release is available on PyPI and can be installed with
``pip``::

    $ python -m pip install pynuodb

For a release with compatible wheels on PyPI, ``pip`` automatically selects
the wheel matching your Python version, operating system, and CPU architecture.
There is no separate package name or manual binary selection. Installing a
matching wheel requires neither Cython nor a C compiler; the accelerator is
already compiled into the wheel.

If no compatible wheel is available and the release provides a source archive,
``pip`` attempts a local build. A C compiler is needed to build the optional
accelerator. If extension compilation is unavailable or fails, installation
can continue with the pure-Python implementation, without acceleration. This
fallback applies to the driver's optional extension: build requirements and
runtime dependencies, including ``cryptography``, must still be installable.

The CI workflow in this repository generates wheel candidates and a source
archive. Publishing them to PyPI is a separate release step; a successful CI
build does not imply that its wheels are already available on PyPI. See the
`Contributor guide`_ for the build matrix, wheel filenames, and release process.

To install a development snapshot from GitHub instead of a released version::

    $ curl -L https://github.com/nuodb/nuodb-python/archive/master.tar.gz | tar xz
    $ cd nuodb-python*
    $ python -m pip install .
    # The extracted source folder can be removed after installation.

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
.. _Contributor guide: https://github.com/nuodb/nuodb-python/blob/master/CONTRIBUTING.rst
