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

Building distribution artifacts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``.circleci/config.yml`` owns the complete build, test, and artifact workflow.
It runs on branch builds and version tags matching ``v*``, subject to the
project's CircleCI trigger settings, and can be triggered manually in CircleCI.
CircleCI does **not** publish to PyPI or use PyPI credentials. Publishing is
done by GitHub Actions on version tags, as described under `Release bundle`_.

The initial wheel matrix is CPython 3.9 through 3.13 (standard, GIL-enabled
builds) on:

* Linux x86-64 and ARM64, using ``manylinux2014`` (glibc >= 2.17).
* macOS Intel and Apple Silicon, using separate architecture-specific wheels.
  Both jobs use Apple Silicon runners; Intel wheel tests execute under Rosetta.
  Rosetta installation and x86-64 execution are checked explicitly, not skipped.
* Windows x86-64.

PyPy, free-threaded Python, 32-bit targets, Windows ARM64, and Alpine/musl
wheels are not included. A source distribution is provided for other systems;
its extension build is optional, allowing the pure-Python fallback when a C
compiler is unavailable. The workflow also checks that no-compiler source
installation path. Package dependencies must still be installable.

The source archive is built once. Every platform builds its wheels from that
same archive, which includes the Cython sources, generated C, headers, and test
sources. ``cibuildwheel`` installs each wheel into an isolated test environment
and runs the database-independent accelerator, encoding, and date/time tests.
The tests import the installed wheel, not the source checkout, and fail if the
compiled extension is missing. Windows tests install ``tzdata`` for IANA time
zones.

The database regression job builds and installs a wheel from that same source
archive and runs the full suite outside the checkout. It saves its native wheel
and source archive under ``artifacts/dist`` for diagnosis; that native Linux
wheel is **not** a portable manylinux release artifact. The existing performance
comparison also runs in the same workflow.

Linux wheel jobs use x86-64 and ARM64 machine executors with Docker for
manylinux builds. macOS jobs use ``m4pro.medium`` and Windows uses
``windows.medium``. These resource classes must be enabled for the CircleCI
organization and consume the corresponding credits. Only the existing database
and performance jobs use the ``common-config`` context; source/wheel building
and bundle assembly do not use a secret context.

Understanding wheel filenames
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A wheel is a pre-built, installable Python distribution. For this driver it
contains both Python modules and the compiled accelerator, so users installing
a compatible wheel do not need to compile the extension themselves.

The current driver wheels follow this filename pattern::

    pynuodb-<version>-<python_tag>-<abi_tag>-<platform_tag>.whl

For example::

    pynuodb-5.0.0-cp39-cp39-win_amd64.whl

.. list-table:: Reading the example filename
   :header-rows: 1
   :widths: 30 70

   * - Component
     - Meaning
   * - ``pynuodb``
     - Package name.
   * - ``5.0.0``
     - Driver version.
   * - First ``cp39``
     - Python interpreter: CPython 3.9.
   * - Second ``cp39``
     - Application binary interface (ABI): the CPython 3.9 interface that
       the compiled extension must match.
   * - ``win_amd64``
     - Windows on x86-64 CPUs, including both Intel and AMD processors.

The Python tags ``cp39``, ``cp310``, ``cp311``, ``cp312``, and ``cp313`` identify
CPython 3.9, 3.10, 3.11, 3.12, and 3.13 respectively. Cython is the build-time
tool that translates the extension sources into C; its version is not encoded
in a wheel filename.

The current extension uses a CPython-version-specific ABI, rather than the
cross-version ``abi3`` tag. Its CPython 3.9 wheel cannot be reused for CPython
3.13, and a Windows x86-64 binary cannot be installed on Linux ARM64. Therefore,
the five Python versions and five OS/CPU targets listed above require
**5 x 5 = 25 wheels**, plus one source archive. Linux uses ``manylinux2014``
compatibility tags rather than a separate wheel for each compatible glibc-based
Linux distribution.

All release wheels share the same package name and driver version on PyPI.
Users still run ``python -m pip install pynuodb``; ``pip`` selects a compatible
wheel automatically when one has been published. CI artifacts are build
candidates, not evidence that a version has already been published.

Release bundle
^^^^^^^^^^^^^^

Only after all five wheel jobs, database regressions, and performance tests
pass does ``Validate and assemble release bundle`` run. CircleCI workspaces
carry the source archive and each target's wheels between jobs, using distinct
paths for parallel outputs. The final job stores one downloadable artifact,
``release-bundle.tar.gz``. Extracting it produces::

    dist/
        pynuodb-<version>.tar.gz
        pynuodb-<version>-<python>-<abi>-<platform>.whl
        ...
    manifest.json
    SHA256SUMS

``ci/release_bundle.py`` requires exactly one source archive and one compiled
wheel for each of the 25 Python/platform combinations. It checks package names,
versions, wheel ABIs/platforms, extension presence, and required source-archive
contents. On tag runs, the tag version must match the package version
(``v5.0`` and ``v5.0.0`` are equivalent). ``twine check --strict`` checks all
distribution metadata.

The package version is defined only in ``pynuodb/__init__.py``
(``__version__``); ``setup.py`` reads it from there. To release, change that one
line, commit, and push a matching tag. A tag that does not match the package
version fails the release bundle job.

Pre-releases are optional. A general availability release can be tagged
directly (for example ``v5.0.0`` with ``__version__ = '5.0.0'``). To publish a
release candidate first, use a Git tag such as ``v5.0.0-rc1`` with the PEP 440
package version ``5.0.0rc1``. Only ``-rcN`` pre-release tags are accepted,
numbered from ``-rc1`` without leading zeros; alpha
and beta tags, and tags without the hyphen (such as ``v5.0.0rc1``), are
rejected. The release manifest records ``prerelease`` for
every build, derived from the package version. PyPI lists pre-release versions
as such, so ``pip install pynuodb`` does not move users to them.

GitHub Actions can publish tag builds to PyPI using trusted publishing (OIDC).
The workflow ``release-publish.yml`` runs on ``v*`` tags and marks the GitHub
Release as a pre-release when the manifest's ``prerelease`` is true. CircleCI
remains the comparison build until the team completes the migration assessment.

The manifest records distribution sizes and SHA-256 checksums, the source
commit/ref, repository, CircleCI pipeline ID/number, workflow ID, and the exact
artifact-producing job number and URL. A rerun produces a new job identity;
do not select artifacts by version or branch name alone. A pull-request URL is
recorded when CircleCI supplies one, but its absence is not release approval.
Artifacts are subject to CircleCI retention settings; archive approved bundles
internally before they expire. Dependency resolution and builder images can
change, so a rebuild is not guaranteed to be byte-identical.

For internal publishing:

#. Select a successful CircleCI workflow for an approved release commit or
   protected version tag. Use its exact workflow ID and final job number,
   not "latest". Bundle assembly is automatically gated on builds and tests.
#. Download ``release-bundle.tar.gz`` from that workflow's final assembly job,
   not the intermediate wheel artifacts or diagnostic distributions. Extract
   it and check the manifest against the approved repository, commit, version,
   pipeline, workflow, and job identity. Branch/PR test artifacts must not be
   treated as approved releases merely because their tests passed.
#. Verify the downloaded distributions from the extracted bundle directory::

       sha256sum --check SHA256SUMS

#. Preserve the approved bundle internally and pass only ``dist/*`` to the
   separate publishing job. Do not rebuild distributions during publishing.

Checksums detect corruption; they do not authenticate an untrusted build or
replace release approval. No inbound connection to internal GitLab is required:
the internal runner can fetch the selected CircleCI artifact using outbound
access (with a read-only CircleCI credential if required for artifact access).
The PyPI token must never be supplied to this workflow or the wheel tests.

When adding Python versions or targets, update ``pyproject.toml``, the CircleCI
workflow jobs, and ``PYTHONS``/``TARGETS`` in ``ci/release_bundle.py`` together.
Test the bundle validator and workflow invariants locally with::

    python -m pip install packaging 'pytest>=8,<9' 'PyYAML>=6,<7'
    python -m pytest ci
    circleci config validate .circleci/config.yml

.. _NuoDB quick-start guide: https://doc.nuodb.com/nuodb/latest/quick-start-guide/linux/tar/
