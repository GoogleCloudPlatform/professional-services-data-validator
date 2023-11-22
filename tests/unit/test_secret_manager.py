# Copyright 2023 Google LLC
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

from data_validation import secret_manager
import pytest

TEST_CONN_NAME = "dvt-project-id"
TEST_PROJECT_NAME = "pso-kokoro-resources"
CONN_CONFIG = "pso-kokoro-resources"
NON_EXIST_NAME = "nona"

def test_retrieve_connection_config():
    manager = secret_manager.SecretManagerBuilder().build("gcp")
    config = manager.maybe_secret(TEST_PROJECT_NAME, TEST_CONN_NAME)
    assert config == CONN_CONFIG

def test_retrieve_nonexistent_secret():
    manager = secret_manager.SecretManagerBuilder().build("gcp")
    with pytest.raises(ValueError):
        config = manager.maybe_secret(TEST_PROJECT_NAME, NON_EXIST_NAME)
