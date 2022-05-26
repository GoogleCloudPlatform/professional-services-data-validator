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
version = "2.0.0"
release_status = "Development Status :: 3 - Alpha"

with open("README.md", "r") as fh:
    long_description = fh.read()

dependencies = [
    # Dependency corrections from our requirements
    "attrs==20.3.0",
    "grpcio==1.35.0",
    "lazy-object-proxy==1.4.3",
    "marshmallow==3.10.0",
    # Core dependencies
    "google-api-python-client==1.12.8",
    "ibis-framework==1.4.0",
    "ibis-bigquery==0.1.1",
    "impyla==0.17.0",
    "SQLAlchemy==1.3.22",
    "PyMySQL==1.0.2",
    "psycopg2-binary==2.8.6",
    "PyYAML==5.4.1",
    "pandas==1.2.3",
    "proto-plus==1.13.0",
    "pyarrow==3.0.0",
    "pydata-google-auth==1.1.0",
    "google-cloud-bigquery==2.11.0",
    "google-cloud-bigquery-storage==2.3.0",
    "google-cloud-spanner==3.1.0",
    "google-cloud-storage==1.42.2",
    "setuptools>=34.0.0",
    "jellyfish==0.8.2",
    "tabulate==0.8.9",
    "Flask==2.0.2",
]

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
        "console_scripts": [
            "data-validation=data_validation.__main__:main",
        ]
    },
)
