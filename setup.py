#!/usr/bin/env python

from setuptools import setup
from sys import version_info
from splitio.settings import PACKAGE_VERSION

tests_require = []
install_requires = ['arrow>=0.7.0', 'requests>=2.9.1', 'future>=0.15.2']

if version_info < (3,):
    tests_require += ['mock']
    install_requires += ['six>=1.10.0', 'futures>=3.0.5', 'enum34>=1.1.5']

setup(name='splitio-client',
      version=PACKAGE_VERSION,
      description='Split.io Python Client',
      author='Patricio Echague',
      author_email='pato@split.io',
      url='https://github.com/splitio/python-client',
      download_url='https://github.com/splitio/python-client/tarball/{}'.format(PACKAGE_VERSION),
      license='Apache License 2.0',
      install_requires=install_requires,
      tests_require=tests_require,
      extras_require={'test': tests_require},
      setup_requires=['flake8', 'nose', 'coverage'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
          'Topic :: Software Development :: Libraries'
      ],
      packages=['splitio'])
