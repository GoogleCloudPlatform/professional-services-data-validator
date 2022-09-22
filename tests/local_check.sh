#!/bin/bash

# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Step 1: After finishing your development, activate your virtual environment
# Step 2: Execute this script only after the virtual env activation

# check unit tests and coverage
pytest --quiet --cov=data_validation --cov=tests.unit --cov-append --cov-config=.coveragerc --cov-report=term tests/unit

# check code style
flake8 data_validation tests

# check code formatting and reformat, if needed
black data_validation samples tests noxfile.py setup.py

# execute all previous checks in a virtual env the same way that is run by our build tool
python3 -m nox --envdir ~/dvt/envs/ -s unit_small blacken lint

