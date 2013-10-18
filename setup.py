# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
    name='pynuodb',
    version='2.0',
    author='NuoDB',
    author_email='info@nuodb.com',
    description='NuoDB Python driver',
    keywords='nuodb scalable cloud database',
    packages=['pynuodb'],
    package_dir={'pynuodb': 'pynuodb'},
    url='https://github.com/nuodb/nuodb-python',
    license='BSD licence, see LICENCE.txt',
    long_description=open('README.md').read(),
)

