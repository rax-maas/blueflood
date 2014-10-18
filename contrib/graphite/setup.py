# coding: utf-8
from setuptools import setup

version = '0.0.1'

setup(
  name='blueflood',
  version=version,
  url='https://github.com/rackerlabs/blueflood',
  license='APL2',
  keywords='blueflood graphite metrics',
  author='Gary Dusbabek',
  author_email='gdusbabek@gmail.com',
  description=('A plugin for using graphite-web with the cassandra-based '
               'Blueflood storage backend'),
  py_modules=('blueflood','auth','rax_auth'),
  zip_safe=False,
  include_package_data=True,
  platforms='any',
  classifiers=(
      'Intended Audience :: Developers',
      'Intended Audience :: System Administrators',
      'License :: OSI Approved :: BSD License',
      'Operating System :: OS Independent',
      'Programming Language :: Python',
      'Programming Language :: Python :: 2',
      'Topic :: System :: Monitoring',
  ),
  install_requires=(
      'requests',
  ),
  test_suite='tests',
)
