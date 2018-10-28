#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'boto3',
    'botocore',
]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Mohan Kishore",
    author_email='mohankishore@yahoo.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Python library that emulates the java-based dynamo-db-client from awslabs",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='python_dynamodb_lock',
    name='python_dynamodb_lock',
    packages=find_packages(include=['python_dynamodb_lock']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/mohankishore/python_dynamodb_lock',
    version='0.9.1',
    zip_safe=False,
)
