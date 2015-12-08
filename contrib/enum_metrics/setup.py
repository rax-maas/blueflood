from distutils.core import setup
from setuptools import setup, find_packages

setup(
    name='enum_metrics',
    version='1.0.0',
    packages=find_packages(exclude=['test']),
    url='https://github.com/rackerlabs/blueflood',
    license='Apache License',
    author='Chandra Addala',
    author_email='chandra.addala@rackspace.com',
    description='Utilities to work on enum metrics',
    install_requires=[
        'cassandra-driver',
        'elasticsearch>=1.0.0,<2.0.0',
        'mock',
        'nose',
        'nose-cover3'
    ]
)
