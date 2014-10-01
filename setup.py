#!/usr/bin/env python
import sys
import os

from distutils.core import setup

import versioneer

versioneer.versionfile_source = 'brilws/_version.py'
versioneer.versionfile_build = 'brilws/_version.py'
versioneer.tag_prefix = '' # tags are like 1.2.0
versioneer.parentdir_prefix = 'brilws' # dirname like 'myproject-1.2.0'

kwds = {'scripts':[]}
kwds['scripts'].append('bin/briltag')
kwds['scripts'].append('bin/brilcalc')
kwds['scripts'].append('bin/brilschema')

setup(
    name = "brilws",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author = "Zhen Xie, CERN/Princeton University",
    author_email = "Zhen.Xie@cern.ch",
    url = "https://github.com/xiezhen/brilws",
    license = "MIT",
    classifiers = [
        "Development Status :: 1 - Beta",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7"
    ],
    description = "bril analysis workspace",
    long_description = 'bril data analysis tools',
    packages = ['brilws', 'brilws.cli'],
    install_requires = [''],
    **kwds
)

