# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
    name='pynuodb',
    version='0.0.1',
    author='NuoDB',
    author_email='info@nuodb.com',
    description='NuoDB Python driver',
    keywords='scalable cloud database',
    packages=['pynuodb', 'tests'],
    url='https://github.com/nuodb/nuodb-python',
    license='BSD licence, see LICENCE.txt',
    long_description=open('README.md').read()
)
