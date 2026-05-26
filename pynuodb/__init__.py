"""An implementation of Python PEP 249 for NuoDB.

(C) Copyright 2013-2026 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.
"""

__version__ = '4.1.0'

from .connection import *  # pylint: disable=wildcard-import
from .datatype import *    # pylint: disable=wildcard-import
from .exception import *   # pylint: disable=wildcard-import, redefined-builtin
