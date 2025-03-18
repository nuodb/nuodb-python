# (C) Copyright 2015-2025 Dassault Systemes SE.  All Rights Reserved.
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

ARTIFACTDIR ?= artifacts
RESULTSDIR ?= results

PYTEST ?= pytest
PYTEST_ARGS ?=
PYTEST_LOG ?= --show-capture=stdout --log-file=$(ARTIFACTDIR)/testlog.out --log-file-level=INFO
PYTEST_OPTS ?= --junitxml=$(RESULTSDIR)/result.xml
PYTEST_COV ?= --cov=pynuodb --cov-report=html:$(ARTIFACTDIR) --cov-report=term-missing

SUDO ?= sudo -n
NUODB_HOME ?= /opt/nuodb

_INSTALL_CMD =	$(PIP) install '.[crypto]'
_VERIFY_CMD =	$(NUODB_HOME)/bin/nuocmd show domain
_PYTEST_CMD =	$(MKDIR) $(ARTIFACTDIR) $(RESULTSDIR) \
		&& TMPDIR='$(TMPDIR)' PATH="$(NUODB_HOME)/bin:$$PATH" \
			$(PYTEST) $(PYTEST_LOG) $(PYTEST_OPTS) $(PYTEST_ARGS)

all:
	$(MAKE) install
	$(MAKE) test

install:
	$(_INSTALL_CMD)

check: mypy pylint fulltest

fulltest:
	$(_INSTALL_CMD)
	$(PIP) install -r test_requirements.txt
	$(_VERIFY_CMD)
	$(_PYTEST_CMD)

test:
	$(_PYTEST_CMD)

test-coverage:
	$(_PYTEST_CMD) $(PYTEST_COV)

verify:
	$(_VERIFY_CMD)

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
