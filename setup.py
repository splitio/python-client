"""Setup module."""
#!/usr/bin/env python

from os import path
from setuptools import setup, find_packages

TESTS_REQUIRES = [
    'flake8',
    'pytest<=4.6',  # for deprecated python versions: https://docs.pytest.org/en/latest/py27-py34-deprecation.html
    'pytest-mock==2.0.0',
    'coverage',
    'pytest-cov',
    'mock;python_version<"3"'
]

INSTALL_REQUIRES = [
    'requests>=2.9.1',
    'pyyaml>=5.1',
    'future>=0.15.2',
    'docopt>=0.6.2',
    'six>=1.10.0',
    'enum34;python_version<"3.4"',
    'futures>=3.0.5;python_version<"3"'
]

with open(path.join(path.abspath(path.dirname(__file__)), 'splitio', 'version.py')) as f:
    exec(f.read())  # pylint: disable=exec-used

setup(
    name='splitio_client',
    version=__version__,  #  pylint: disable=undefined-variable
    description='Split.io Python Client',
    author='Patricio Echague, Sebastian Arrubia',
    author_email='pato@split.io, sebastian@split.io',
    url='https://github.com/splitio/python-client',
    download_url=('https://github.com/splitio/python-client/tarball/' + __version__), #  pylint: disable=undefined-variable
    license='Apache License 2.0',
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRES,
    extras_require={
        'test': TESTS_REQUIRES,
        'redis': ['redis>=2.10.5'],
        'uwsgi': ['uwsgi>=2.0.0'],
        'cpphash': ['mmh3cffi==0.2.0'],
    },
    setup_requires=['pytest-runner'],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries'
    ],
    packages=find_packages()
)
