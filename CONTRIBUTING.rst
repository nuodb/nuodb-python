Development
===========

Requirements and installation
-----------------------------

Use Python >= 3.9 and a virtual environment. A C compiler and matching Python
development headers are required to build and test the native extension.

From the project directory::

    python3 -m venv .venv
    . .venv/bin/activate
    python -m pip install --upgrade pip
    python -m pip install -e .
    python -m pip install pytest pytest-benchmark pytest-cov mock nose

``requirements.txt`` lists runtime dependencies, not test dependencies.
``test_requirements.txt`` also lists optional Admin and coverage-reporting tools.
Some ``pynuoadmin`` releases require ``pynuodb<5``; check those constraints before
installing the full test requirements into the driver development environment.

Verify that the checkout and its compiled extension are loaded::

    python -c "import pynuodb; print(pynuodb.__file__)"
    python -c "from pynuodb import _fetch; print(_fetch.__file__)"

An extension import error means native compilation did not succeed. The driver
can run with the pure-Python fallback, but the Cython tests require the extension.
Re-run ``python -m pip install -e .`` after changing ``.pyx``, ``.pxi``, or native
header files to rebuild it.

Database-independent tests
--------------------------

The encoder/decoder unit tests and tick-conversion tests can run without NuoDB::

    python -m pytest tests/nuodb_cython_test.py tests/nuodb_datatype_ticks_test.py \
        -k "not TestNuoDBCython"

Database-backed tests
---------------------

Use a disposable NuoDB environment. Tests create and drop database objects;
do not point them at a production database.

Default local setup
~~~~~~~~~~~~~~~~~~~

Install NuoDB and start a licensed local Admin service before running the tests.
Set ``NUODB_HOME`` to the installation directory and put its tools on ``PATH``::

    export NUODB_HOME=/path/to/nuodb
    export PATH="$NUODB_HOME/bin:$PATH"
    nuocmd show domain
    python -m pytest tests

``nuocmd`` must be configured for the Admin service's endpoint, authentication,
and TLS settings. The fixtures locate a local Admin Process, create or reuse
``pynuodb_test``, provision an archive and engines, and clean up resources they
created. An existing test database must use ``dba`` / ``dba_password``. Use a
dedicated domain: fixtures can reset database options and remove archive data.

CI uses ``America/New_York`` as the local timezone. Use the same timezone for
reproducible date/time testing. A TAR-based single-host setup is described in the
`NuoDB quick-start guide`_.

Three engine-service tests also require ``pynuoadmin`` to be importable in the
test interpreter; they otherwise skip with "No AP available". Keep the Admin
client separate from the driver under test. In particular, do not add the entire
NuoDB distribution's bundled ``site-packages`` directory to ``PYTHONPATH``: it
can shadow the checkout's dependencies. Expose only the ``pynuoadmin`` package
and install its required dependencies in the test environment.

Existing database
~~~~~~~~~~~~~~~~~

To bypass local ``nuocmd`` discovery and lifecycle management::

    python -m pytest tests --use-existing-db \
        --db-host localhost:48004 --db-name pynuodb_test \
        --db-user dba --db-password '<test-password>' --db-schema test

The user needs permission to create and drop test objects. The client must be
able to reach the connection endpoint and any TE addresses returned by NuoDB.
This mode does not create or delete the database, but the tests still modify its
contents. Engine-service tests additionally need access to the Admin API.

Test options
~~~~~~~~~~~~

Use ``-s`` to display captured output, ``-k`` to select tests, or ``--pdb`` to
enter the debugger on failure. Generate coverage with::

    python -m pytest tests --cov=pynuodb --cov-report=html --cov-report=term-missing

The pytest session header reports whether the native extension is loaded.

Performance tests
-----------------

Performance tests are skipped unless ``--run-perf`` is supplied. With a test
database configured, run::

    python -m pytest tests/perf --run-perf --benchmark-only \
        --benchmark-json=benchmark.json

The same ``--use-existing-db`` and ``--db-*`` options apply. Compare two runs
performed on the same hardware and database configuration with::

    python tests/perf/compare.py baseline.json branch.json

Release
-------

Maintain the changes and known defects for each release in ``CHANGES.rst``.

.. _NuoDB quick-start guide: https://doc.nuodb.com/nuodb/latest/quick-start-guide/linux/tar/
