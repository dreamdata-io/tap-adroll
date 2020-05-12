#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-adroll",
    version="0.0.1",
    description="Singer.io tap for extracting data",
    author="Dreamdata",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_adroll"],
    install_requires=["singer-python==5.8.1", "requests==2.22.0", "backoff==1.8.0"],
    entry_points="""
    [console_scripts]
    tap-adroll=tap_adroll:main
    """,
    packages=["tap_adroll"],
    package_data={"schemas": ["tap_adroll/schemas/*.json"]},
    include_package_data=True,
)
