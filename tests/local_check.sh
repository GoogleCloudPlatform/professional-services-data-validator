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

# - After finishing your development, activate your virtual environment
# - Execute this script only after the virtual env activation

# confirm you have all the libraries needed for the full check
echo "Starting LIBRARIES INSTALLATION..."
pip install pytest pytest-cov pyfakefs flake8 black==22.3.0
echo "Finished LIBRARIES INSTALLATION."

# check unit tests and coverage
echo "Starting TEST COVERAGE..."
pytest --quiet --cov=data_validation --cov=tests.unit --cov-append --cov-config=.coveragerc --cov-report=term tests/unit
echo "Finished TEST COVERAGE."

# check code style
echo "Starting CODE STYLE (FLAKE8)..."
flake8 data_validation tests
echo "Finished CODE STYLE (FLAKE8)."

# check code formatting and reformat, if needed
echo "Starting CODE FORMATTING (BLACK)..."
black data_validation samples tests noxfile.py setup.py
echo "Finished CODE FORMATTING (BLACK)."

# execute all previous checks in a virtual env the same way that is run by our build tool
echo "Starting ALL CHECKS IN VENV (NOX)..."
python3 -m nox --envdir ~/dvt/envs/ -s unit_small blacken lint
echo "Finished ALL CHECKS IN VENV (NOX)."
