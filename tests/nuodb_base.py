"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import pytest
import copy

try:
    from typing import Any  # pylint: disable=unused-import
except ImportError:
    pass

import pynuodb

from . import nuocmd, cvtjson


class NuoBase(object):
    longMessage = True

    # Set the driver module for the imported test suites
    driver = pynuodb  # type: Any

    connect_args = ()
    system_information = ()
    host = None

    lower_func = 'lower'  # For stored procedure test

    @pytest.fixture(autouse=True)
    def _setup(self, database):
        # Preserve the options we'll need to create a connection to the DB
        self.connect_args = database['connect_args']
        self.system_information = database['system_information']

        # Verify the database is up and has a running TE
        dbname = database['connect_args']['database']
        (ret, out) = nuocmd(['--show-json', 'get', 'processes',
                             '--db-name', dbname], logout=False)
        assert ret == 0, "DB not running: %s" % (out)

        for proc in cvtjson(out):
            if proc.get('type') == 'TE' and proc.get('state') == 'RUNNING':
                break
        else:
            (ret, out) = nuocmd(['show', 'domain'])
            assert ret == 0, "Failed to show domain: %s" % (out)
            pytest.fail("No running TEs found:\n%s" % (out))

    def _connect(self, options=None):
        connect_args = copy.deepcopy(self.connect_args)
        if options:
            if 'options' not in connect_args:
                connect_args['options'] = {}
            for k, v in options.items():
                if v is not None:
                    connect_args['options'][k] = v
                elif k in connect_args['options']:
                    del connect_args['options'][k]
            if not connect_args['options']:
                del connect_args['options']
        return pynuodb.connect(**connect_args)
