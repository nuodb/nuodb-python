#
# Copyright (c) 2015-2021, NuoDB, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of NuoDB, Inc. nor the names of its contributors may
#       be used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

PYTHON ?= python
VIRTUALENV ?= virtualenv
PIP ?= pip

MKDIR ?= mkdir -p
RMDIR ?= rm -rf

TMPDIR ?= ./.testtemp
VIRTDIR ?= ./.virttemp

PYTEST_ARGS ?=

PYTEST_OPTS ?=
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
