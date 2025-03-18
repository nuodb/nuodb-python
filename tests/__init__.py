"""
(C) Copyright 2013-2025 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

import os
import logging
import copy
import subprocess
import json

try:
    from typing import Any, List, Tuple  # pylint: disable=unused-import
except ImportError:
    pass

_log = logging.getLogger("pynuodbtest")


def cvtjson(jstr):
    # type: (str) -> Any
    """Given a string return a valid JSON object.

    Unfortunately the output of nuocmd is not always a valid JSON object;
    sometimes it's a dump of one or more JSON objects concatenated together.
    As a result this function will always return a JSON list object, even
    if the actual output was a single value!
    """
    return json.loads(jstr if jstr.startswith('[') else
                      '[' + jstr.replace('\n}\n{', '\n},\n{') + ']')


# Python coverage's subprocess support breaks tests: nuocmd is a Python 2
# script which doesn't have access to the virtenv or whatever pynuodb is
# using.  So, nuocmd generates error messages related to coverage then the
# parsing of the JSON output fails.  Get rid of the coverage environment
# variables.
env_nocov = copy.copy(os.environ)
env_nocov.pop('COV_CORE_SOURCE', None)
env_nocov.pop('COV_CORE_CONFIG', None)


def nuocmd(args, logout=True):
    # type: (List[str], bool) -> Tuple[int, str]
    _log.info('$ nuocmd ' + ' '.join(args))
    proc = subprocess.Popen(['nuocmd'] + args, env=env_nocov,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (raw, _) = proc.communicate()
    ret = proc.wait()
    out = raw.decode('UTF-8')
    msg = '>exit: %d' % (ret)
    if logout or ret != 0:
        msg += '\n' + out
    _log.info(msg)
    return (ret, out)
