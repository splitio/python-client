"""Setup module."""
# !/usr/bin/env python

from os import path
from setuptools import setup, find_packages

TESTS_REQUIRES = [
    'flake8',
    'pytest==7.1.0',
    'pytest-mock==3.11.1',
    'coverage==7.2.7',
    'pytest-cov',
    'importlib-metadata==6.7',
    'tomli',
    'iniconfig',
    'attrs'
]

INSTALL_REQUIRES = [
    'requests',
    'pyyaml',
    'docopt>=0.6.2',
    'bloom-filter2>=2.0.0'
]

with open(path.join(path.abspath(path.dirname(__file__)), 'splitio', 'version.py')) as f:
    exec(f.read())  # pylint: disable=exec-used

setup(
    name='splitio_client',
    version=__version__,  # pylint: disable=undefined-variable
    description='Split.io Python Client',
    author='Patricio Echague, Sebastian Arrubia',
    author_email='pato@split.io, sebastian@split.io',
    url='https://github.com/splitio/python-client',
    download_url=('https://github.com/splitio/python-client/tarball/' + __version__),  # pylint: disable=undefined-variable
    license='Apache License 2.0',
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRES,
    extras_require={
        'test': TESTS_REQUIRES,
        'redis': ['redis>=2.10.5'],
        'uwsgi': ['uwsgi>=2.0.0'],
        'cpphash': ['mmh3cffi==0.2.1'],
    },
    setup_requires=['pytest-runner', 'pluggy==1.0.0;python_version<"3.7"'],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries'
    ],
    packages=find_packages(exclude=('tests', 'tests.*'))
)
