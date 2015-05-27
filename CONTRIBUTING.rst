Development
-----------

The following sections are intended for developers.

Requirements
~~~~~~~~~~~~

Developers should use virtualenv to maintain multiple side-by-side
environments to test with. Specifically, all contributions must be
tested with both 2.7.6 and 3.4.3 to ensure the library is syntax
compatible between the two versions.

Dependencies
~~~~~~~~~~~~

Here was my basic setup on Mac OS X:

    | virtualenv --python=/usr/bin/python2.7 ~/.venv/pynuodb
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
