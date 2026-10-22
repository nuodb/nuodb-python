Development
-----------

The following sections are intended for developers.

Requirements
~~~~~~~~~~~~

Developers should use virtualenv to maintain multiple side-by-side
environments to test with. Specifically, all contributions must be
tested with Python >= 3.9 to ensure the library remains compatible.

Dependencies
~~~~~~~~~~~~

Here was my basic setup on Mac OS X:

    | virtualenv --python=/usr/bin/python3 ~/.venv/pynuodb
    | source ~/.venv/pynuodb/bin/activate
    | pip install mock
    | pip install nose
    | pip install pytest
    | pip install coverage

There are some commonly used libraries with this:

    | pip install sqlalchemy
    | pip install sqlalchemy-nuodb

Or simply:

    | pip install -r requirements.txt

Then once those are setup...

Developer Testing
~~~~~~~~~~~~~~~~~

Prerequisite for testing is to unset LC_CTYPE:

    | unset LC_CTYPE

My basic means of testing has been:

    | source ~/.venv/pynuodb/bin/activate
    | cd <project directory>
    | py.test

First and foremost, painful py.test less, it captures STDOUT and will not
display your log messages. To disable this behavior and enable sane logging
the following option is suggested:

    | --capture=no

To stop on first failure you could augment that with the pdb option:

    | py.test --pdb

To run a specific test you could do something like this:

    | py.test -k "SomeTest and test_something"

Or any combination of the above, if you like.

To gather coverage information you can run the following command:

    | py.test --cov=pynuodb --cov-report html --cov-report term-missing

Developer Installation
~~~~~~~~~~~~~~~~~~~~~~

With pip installed, you can install this project via:

    | pip install -e .

Release
-------

Maintain the list of changes per release in CHANGES.rst. Also note the known defects.

Building distribution artifacts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``.circleci/config.yml`` owns the complete build, test, and artifact workflow.
It runs on branch builds and version tags matching ``v*``, subject to the
project's CircleCI trigger settings, and can be triggered manually in CircleCI.
It does **not** publish to PyPI or use PyPI credentials. The internal deployment
process remains responsible for authorizing a release and publishing it.
There is no separate GitHub Actions build workflow.

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
