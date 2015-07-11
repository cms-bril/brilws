#!/usr/bin/env python
import sys
import os
import re

from distutils.core import setup
from setuptools import setup
with open("README.md", "rb") as f:
    long_desc = f.read().decode('utf-8')
version = re.search(
                '^__version__\s*=\s*"(.*)"',
                open('brilws/_version.py').read(),
                re.M
                ).group(1)
print version

setup(
    name = "brilws",
    author = "Zhen Xie",
    url = "https://github.com/xiezhen/brilws",
    download_url = 'https://github.com/xiezhen/brilws/tarball/'+version,
    license = "MIT",
    version = version,
    description = "bril worksuite",
    long_description = long_desc,
    packages = ['brilws', 'brilws.cli'],
    entry_points = {
        "console_scripts" : ['brilcalc = brilws.cli.brilcalc_main:brilcalc_main','brilschema = brilws.cli.brilschema_main:brilschema_main','briltag = brilws.cli.briltag_main:briltag_main']
        },
    package_data = {'data':['brilws/data/'],'dbschema':['brilws/dbschema/*.yaml']},
    include_package_data=True,
)

