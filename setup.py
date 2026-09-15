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

import glob
import os
import re

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

# When building a wheel from a checkout we compile .pyx via Cython.  End-user
# installs from an sdist do not need Cython: MANIFEST.in ships the generated
# .c files so the fallback branch compiles those directly.
try:
    from Cython.Build import cythonize
    HAS_CYTHON = True
except ImportError:
    HAS_CYTHON = False


class _OptionalBuildExt(build_ext):
    """build_ext, but the --inplace copy-back step does not abort the
    install for an extension whose compile was skipped (optional=True on
    a missing/broken C toolchain -- see _find_extensions()). The base
    class's copy_extensions_to_source() copies every extension in
    self.extensions unconditionally and errors if the built artifact
    isn't there; this filters down to extensions that actually built
    before delegating to it.
    """

    def copy_extensions_to_source(self):
        all_exts = self.extensions
        built = []
        skipped = []
        for ext in all_exts:
            filename = self.get_ext_filename(self.get_ext_fullname(ext.name))
            if os.path.exists(os.path.join(self.build_lib, filename)):
                built.append(ext)
            else:
                skipped.append(ext.name)
        for name in skipped:
            self.warn('not copying "%s": build was skipped' % name)
        self.extensions = built
        try:
            build_ext.copy_extensions_to_source(self)
        finally:
            self.extensions = all_exts


def _find_extensions():
    suffix = '.pyx' if HAS_CYTHON else '.c'
    sources = sorted(glob.glob(os.path.join('pynuodb', '*' + suffix)))
    exts = [
        Extension(src[:-len(suffix)].replace(os.sep, '.'), [src])
        for src in sources
    ]
    if HAS_CYTHON and exts:
        exts = cythonize(exts, compiler_directives={"language_level": "3"})
    for ext in exts:
        # optional=True: a missing/broken C toolchain (no gcc/clang/MSVC)
        # must not abort the whole install. encodedsession.py already
        # falls back to a pure Python decode loop when `import _fetch`
        # fails, so skipping the extension here just means that fallback
        # is what runs. Set after cythonize(), not on the Extension above:
        # cythonize() rebuilds its own Extension objects and does not
        # carry this flag over from the ones it was given.
        ext.optional = True
    return exts


_ext_modules = _find_extensions()

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
    ext_modules=_ext_modules,
    cmdclass={'build_ext': _OptionalBuildExt},
    url='https://github.com/nuodb/nuodb-python',
    license='BSD License',
    long_description=open(readme).read(),
    python_requires='>=3.9',
    install_requires=['tzlocal', 'jdcal'],
    extras_require=dict(crypto='cryptography>=36.0'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: SQL',
        'Topic :: Database :: Front-Ends',
    ],
)
