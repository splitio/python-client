#!/usr/bin/env python

from setuptools import setup
from os import path
from sys import version_info

tests_require = ['flake8', 'nose', 'coverage']
install_requires = [
    'arrow>=0.7.0',
    'requests>=2.9.1',
    'future>=0.15.2',
    'docopt>=0.6.2',
]

if version_info < (3,):
    tests_require += ['mock']
    install_requires += ['six>=1.10.0', 'futures>=3.0.5', 'enum34>=1.1.5']

with open(path.join(path.abspath(path.dirname(__file__)),
                    'splitio', 'version.py')) as f:
    exec(f.read())

setup(name='splitio_client',
      version=__version__,  # noqa
      description='Split.io Python Client',
      author='Patricio Echague, Sebastian Arrubia',
      author_email='pato@split.io, sebastian@split.io',
      url='https://github.com/splitio/python-client',
      download_url=('https://github.com/splitio/python-client/tarball/' +
                    __version__),
      license='Apache License 2.0',
      install_requires=install_requires,
      tests_require=tests_require,
      extras_require={
          'test': tests_require,
          'redis': ['redis>=2.10.5', 'jsonpickle>=0.9.3'],
          'uwsgi': ['uwsgi>=2.0.0', 'jsonpickle>=0.9.3'],
          'cpphash': ['splitmmh3']
      },
      setup_requires=['nose'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
          'Topic :: Software Development :: Libraries'
      ],
      packages=['splitio', 'splitio.update_scripts', 'splitio.bin', 'splitio.hashfns'])
