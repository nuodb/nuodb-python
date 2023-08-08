# (C) Copyright 2015-2023 Dassault Systemes SE.  All Rights Reserved.
#
# This software is licensed under a BSD 3-Clause License.
# See the LICENSE file provided with this software.

PYTHON_VERSION ?= 3

PYTHON ?= python$(PYTHON_VERSION)
PIP ?= pip$(PYTHON_VERSION)

VIRTUALENV ?= virtualenv
MYPY ?= mypy
PYLINT ?= pylint

MKDIR ?= mkdir -p
RMDIR ?= rm -rf

TMPDIR ?= ./.testtemp
VIRTDIR ?= ./.virttemp

PYTEST ?= pytest
PYTEST_ARGS ?=
PYTEST_OPTS ?= --junitxml=test_results/result.xml
PYTEST_COV ?= --cov=pynuodb --cov-report html --cov-report term-missing

SUDO ?= sudo -n
NUODB_HOME ?= /opt/nuodb
NUO_CONFIG ?= /etc/nuodb/nuoadmin.conf


_PYTEST_CMD = TMPDIR='$(TMPDIR)' PATH="$(NUODB_HOME)/bin:$$PATH" \
		$(PYTEST) $(PYTEST_OPTS) $(PYTEST_ARGS)

all:
	$(MAKE) install
	$(MAKE) test

install:
	$(PIP) install '.[crypto]'

check: mypy pylint fulltest

fulltest: verify
	$(PIP) install '.[crypto]'
	$(PIP) install -r test_requirements.txt
	$(_PYTEST_CMD)

test: verify
	$(_PYTEST_CMD)

test-coverage: verify
	$(_PYTEST_CMD) $(PYTEST_COV)

verify:
	$(NUODB_HOME)/bin/nuocmd show domain

mypy:
	$(MYPY) --ignore-missing-imports pynuodb

pylint:
	$(PYLINT) --rcfile=.pylint-rc pynuodb

virtual-%:
	$(RMDIR) '$(VIRTDIR)'
	$(VIRTUALENV) -p $(PYTHON) '$(VIRTDIR)'
	. '$(VIRTDIR)/bin/activate' && $(MAKE) '$*'

deploy:
	$(PYTHON) setup.py register
	$(PYTHON) setup.py sdist upload

clean:
	$(RMDIR) build/ dist/ *.egg-info htmlcov/

doc:
	$(PIP) install epydoc
	epydoc --html --name PyNuoDB pynuodb/
	cp epydoc.css html/

.PHONY: all install check fulltest test verify deploy clean doc
