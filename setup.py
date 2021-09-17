#!/usr/bin/env python3
# coding: utf-8

# from setup.py.jinja2

import os
import re

import setuptools
from setuptools import find_packages, find_namespace_packages

# CAUTION:
# Do NOT import your package from your setup.py

_nsp = 'joker'
_pkg = 'relational'
_desc = ''
_names = [_nsp, _pkg]
_names = [s for s in _names if s]


def read(filename):
    with open(filename) as f:
        return f.read()


def _find_version():
    names = _names + ['__init__.py']
    path = os.path.join(*names)
    root = os.path.dirname(__file__)
    path = os.path.join(root, path)
    regex = re.compile(
        r'''^__version__\s*=\s*('|"|'{3}|"{3})([.\w]+)\1\s*(#|$)''')
    with open(path) as fin:
        for line in fin:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            mat = regex.match(line)
            if mat:
                return mat.groups()[1]
    raise ValueError('__version__ definition not found')


def _find_packages():
    if _nsp:
        return find_namespace_packages(include=['joker.*'])
    else:
        return find_packages(exclude=['test_*', 'tests'])


config = {
    'name': 'joker-relational',
    'version': _find_version(),
    'description': '' + _desc,
    'keywords': '',
    'url': 'example.com',
    'author': 'anonym',
    'author_email': 'anonym@example.com',
    'license': "GNU General Public License (GPL)",
    'packages': _find_packages(),
    'zip_safe': False,
    'install_requires': read("requirements.txt"),
    # 'entry_points': {'console_scripts': ['joker-relational = joker.relational.__main__:run']},
    'classifiers': [
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    # ensure copy static file to runtime directory
    'include_package_data': True,
    # 'long_description': read('README.md'),
    # 'long_description_content_type': "text/markdown",
}

if _nsp:
    config['namespace_packages'] = [_nsp]


setuptools.setup(**config)
