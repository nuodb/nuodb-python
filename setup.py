import os
import re

from setuptools import setup, find_packages

v = open(os.path.join(os.path.dirname(__file__), 'pynuodb', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.rst')

setup(
    name='pynuodb',
    version=VERSION,
    author='NuoDB',
    author_email='info@nuodb.com',
    description='NuoDB Python driver',
    keywords='nuodb scalable cloud database',
    packages=find_packages(),
    package_dir={'pynuodb': 'pynuodb'},
    url='https://github.com/nuodb/nuodb-python',
    license='BSD License',
    long_description=open(readme).read(),
    install_requires=['pytz'],
)
