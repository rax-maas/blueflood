# coding: utf-8
from setuptools import setup

version = '0.0.1'

setup(
  name='blueflood-graphite-finder',
  version=version,
  url='https://github.com/rackerlabs/blueflood/tree/master/contrib/graphite',
  license='APL2',
  keywords='blueflood graphite finder metrics',
  author='Rackspace Metrics',
  author_email='cloudMetrics-dev@lists.rackspace.com',
  description=('A plugin for using graphite-web and graphite-api with Blueflood'),
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
      'graphite_api',
      'python-dateutil'
  ),
  test_suite='tests',
)
