#!/usr/bin/env python

"""Set up the NuoDB Python Driver package.

(C) Copyright 2013-2023 Dassault Systemes SE.  All Rights Reserved.

This software is licensed under a BSD 3-Clause License.
See the LICENSE file provided with this software.

This package can be installed using pip as follows:

    pip install pynuodb

To install with cryptography:

    pip install 'pynuodb[crypto]'

Note cryptography improves performance, but sessions are encrypted even if it
is not intalled.
"""

import os
import re

from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'pynuodb', '__init__.py')) as v:
    m = re.search(r"^ *__version__ *= *'(.*?)'", v.read(), re.M)
    if m is None:
        raise RuntimeError("Cannot detect version in pynuodb/__init__.py")
    VERSION = m.group(1)

readme = os.path.join(os.path.dirname(__file__), 'README.rst')

setup(
    name='pynuodb',
    version=VERSION,
    author='NuoDB',
    author_email='drivers@nuodb.com',
    description='NuoDB Python driver',
    keywords='nuodb scalable cloud database',
    packages=['pynuodb'],
    url='https://github.com/nuodb/nuodb-python',
    license='BSD License',
    long_description=open(readme).read(),
    install_requires=['pytz>=2015.4', 'ipaddress'],
    extras_require=dict(crypto='cryptography>=2.6.1'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: SQL',
        'Topic :: Database :: Front-Ends',
    ],
)
