# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import setuptools

name = "google-pso-data-validator"
description = "A package to enable easy data validation"
version = "1.2.0"
release_status = "Development Status :: 3 - Alpha"

with open("README.md", "r") as fh:
    long_description = fh.read()

dependencies = open("requirements.txt").read().strip().split("\n")
dependencies = [v for v in dependencies if not v.startswith("#")]  # Remove comments

extras_require = {
    "apache-airflow": "1.10.11",
    "pyspark": "3.0.0",
}

packages = [
    "data_validation",
    "data_validation.query_builder",
    "data_validation.result_handlers",
]
packages += [
    "third_party.ibis.{}".format(path)
    for path in setuptools.find_packages(where=os.path.join("third_party", "ibis"))
]

setuptools.setup(
    name=name,
    description=description,
    version=version,
    author="Dylan Hercher",
    author_email="dhercher@google.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=packages,
    classifiers=[
        release_status,
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=dependencies,
    extras_require=extras_require,
    entry_points={
        "console_scripts": ["data-validation=data_validation.__main__:main",]
    },
)
