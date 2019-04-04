#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import setuptools

NAME = 'beam_bigtable'
DESCRIPTION = 'Beam Bigtable'
URL = 'https://github.com/juan-rael/beam_bigtable'
EMAIL = 'juan@qlogic.io'
AUTHOR = 'Juan Rael Corrales'
REQUIRES_PYTHON = '>=2.7.0'
VERSION = None
REQUIRED = [
    'google-cloud-bigtable==0.32.1',
    'google-cloud-core==0.29.1',
]
EXTRAS = {
}

here = os.path.abspath(os.path.dirname(__file__))
try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION
about = {}
if not VERSION:
    with open(os.path.join(here, NAME, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION
setuptools.setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=setuptools.find_packages(exclude=('tests',)),
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ]
)
