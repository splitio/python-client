#!/usr/bin/env python

from setuptools import setup
from sys import version_info

tests_require = ['nose', 'coverage']
install_requires = ['arrow>=0.7.0', 'requests>=2.9.1', 'future>=0.15.2']

if version_info < (3,):
    tests_require += ['mock']
    install_requires += ['six>=1.10.0', 'futures>=3.0.5']

setup(name='splitio-client',
      version='0.0.1',
      description='Split.io Python Client',
      author='Patricio Echague',
      author_email='pato@split.io',
      url='https://github.com/splitio/python-client',
      license='Apache License 2.0',
      install_requires=install_requires,
      tests_require=tests_require,
      extras_require={'test': tests_require},
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
          'Topic :: Software Development :: Libraries'
      ],
      packages=['splitio'])
