#!/usr/bin/env python

from distutils.core import setup

setup(name='splitio-client',
      version='0.0.1',
      description='Split.io Python Client',
      author='Patricio Echague',
      author_email='pato@split.io',
      url='https://github.com/splitio/python-client',
      license='Apache License 2.0',
      install_requires=['arrow>=0.7.0',
                        'requests>=2.9.1',
                        'six>=1.10.0'],
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
