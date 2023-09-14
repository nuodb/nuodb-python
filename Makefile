# (C) Copyright 2015-2023 Dassault Systemes SE.  All rights reserved.
#
# This software is licensed under a BSD 3-Clause License.
# See the LICENSE file provided with this software.

PYTHON ?= python
VIRTUALENV ?= virtualenv
PIP ?= pip3

MKDIR ?= mkdir -p
RMDIR ?= rm -rf

TMPDIR ?= ./.testtemp
VIRTDIR ?= ./.virttemp

PYTEST_ARGS ?=

PYTEST_OPTS ?= --junitxml=test_results/result.xml
PYTEST_COV ?= --cov=pynuodb --cov-report html --cov-report term-missing

SUDO ?= sudo -n
NUODB_HOME ?= /opt/nuodb
NUO_CONFIG ?= /etc/nuodb/nuoadmin.conf

all:
	$(MAKE) install
	$(MAKE) test

install:
	$(PIP) install '.[crypto]'

test:
	$(PIP) install '.[crypto]'
	$(PIP) install -r test_requirements.txt
	TMPDIR='$(TMPDIR)' PATH="$(NUODB_HOME)/bin:$$PATH" \
	    pytest $(PYTEST_COV) $(PYTEST_OPTS) $(PYTEST_ARGS)

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

.PHONY: all install test deploy clean doc
