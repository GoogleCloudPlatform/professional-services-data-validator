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
version = "7.1.0"
release_status = "Development Status :: 4 - Beta"

with open("README.md", "r") as fh:
    long_description = fh.read()

dependencies = [
    "Flask>=2.2",  # Some versions of airflow such as 2.9.1 depend on flask<2.3 and >=2.2
    "fsspec>=2024.9.0",
    "google-api-python-client>=2.144.0",
    "google-cloud-bigquery>=3.25.0",
    "google-cloud-bigquery-storage>=2.26.0",
    "google-cloud-secret-manager>=2.20.2",
    "google-cloud-spanner>=3.49.1",
    "google-cloud-storage>=2.18.2",
    "ibis-framework==5.1.0",  # Pinned to 5.1.0, significant work to bump to 7.1.0
    "impyla>=0.19.0",
    "jellyfish>=1.1.0",
    "pandas==2.0.3",  # 2.03 is the highest version that still supports python 3.8
    "parsy>=2.1",
    "psycopg2-binary>=2.9.9",
    "pyarrow==14.0.1",  # ibis-framework 7.1.0 depends on pyarrow<15 and >=2
    "pydata-google-auth>=1.8.2",
    "PyMySQL>=1.1.1",
    "PyYAML>=6.0.2",
    "SQLAlchemy==1.4.49",  # Pinned to 1.4.49, encountered SQL syntax errors above this version
    "tabulate>=0.9.0",
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
    author="PSO DVT Engineering team",
    author_email="data-validator-eng@google.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=packages,
    classifiers=[
        release_status,
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=dependencies,
    extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "data-validation=data_validation.__main__:main",
        ]
    },
)
