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

``.github/workflows/wheels.yml`` builds distribution artifacts on pull requests,
pushes to ``master``, version tags matching ``v*``, and manual workflow runs.
It does **not** publish to PyPI, request an OIDC token, or use PyPI credentials.
The internal deployment process remains responsible for authorizing a release
and publishing it.

The initial wheel matrix is CPython 3.9 through 3.13 (standard, GIL-enabled
builds) on:

* Linux x86-64 and ARM64, using ``manylinux2014`` (glibc >= 2.17).
* macOS Intel and Apple Silicon, using separate architecture-specific wheels.
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

CircleCI continues to run the full database regression suite and performance
comparison. Its regression job now builds and installs a wheel from an sdist
and runs the tests outside the checkout. The job saves its native wheel and
source archive under ``artifacts/dist`` for diagnosis; that native Linux wheel
is **not** a portable manylinux release artifact.

Release bundle
^^^^^^^^^^^^^^

Only after all GitHub wheel jobs pass does the workflow assemble the artifact
``release-bundle-<run-id>-<run-attempt>``. It contains::

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
commit/ref, repository, workflow ref, event, run ID, and run attempt. Artifacts
request 90-day retention (subject to repository/organization limits); archive
approved bundles internally before they expire. Dependency resolution and
builder images can change, so a rebuild is not guaranteed to be byte-identical.

For internal publishing:

#. Select a successful run of this workflow for an approved release commit or
   protected version tag. Use its exact run ID and attempt, not "latest".
#. Confirm that CircleCI database regressions passed for the same release
   commit. GitHub's bundle job does not automatically gate on CircleCI status.
#. Download the **final release bundle**, not the intermediate wheel artifacts
   or CircleCI's diagnostic distributions. Check its manifest against the
   approved repository, commit, version, and run identity. Pull-request runs
   build a merge commit for testing and must not be used for publication.
#. Verify the downloaded distributions from the bundle directory::

       sha256sum --check SHA256SUMS

#. Preserve the approved bundle internally and pass only ``dist/*`` to the
   separate publishing job. Do not rebuild distributions during publishing.

Checksums detect corruption; they do not authenticate an untrusted build or
replace release approval. No GitHub-to-internal-GitLab connection is required:
the internal runner can fetch the selected artifact using outbound access.
The PyPI token must never be supplied to this workflow or the wheel tests.

When adding Python versions or targets, update ``pyproject.toml``, the workflow
matrix, and ``PYTHONS``/``TARGETS`` in ``ci/release_bundle.py`` together. Test the
bundle validator locally with::

    python -m pip install packaging 'pytest>=8,<9'
    python -m pytest ci/test_release_bundle.py
