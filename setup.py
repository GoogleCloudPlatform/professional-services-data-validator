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


with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = open("requirements.txt").read().strip().split("\n")
install_requires = [
    v for v in install_requires if not v.startswith("#")
]  # Remove comments

packages = [
    "data_validation",
    "data_validation.query_builder",
    "data_validation.result_handlers",
    "ibis_addon",
]
packages += [
    "third_party.ibis.{}".format(path)
    for path in setuptools.find_packages(where=os.path.join("third_party", "ibis"))
]

setuptools.setup(
    name="google-pso-data-validator",
    version="0.0.1",
    author="Dylan Hercher",
    author_email="dhercher@google.com",
    description="A package to enable easy data validation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=packages,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=install_requires,
    entry_points={
        "console_scripts": ["data-validation=data_validation.__main__:main",]
    },
)
