#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='Origin',
    version='1.0',
    description='Origin Server',
    author='Orthogonal Systems, LLC',
    author_email='admin@orthogonalsystems.com',
    url='https://github.com/Orthogonal-Systems/Origin',
    packages=find_packages(exclude=("tests",)),
    license='GPL3',
    install_requires=[
        'pyzmq',
        'numpy'
    ],
    dependency_links=['http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-2.1.7.zip'],
    extras_require={
        'hdf5': ['h5py'],
        'mongo': ['pymongo']
    }
)
