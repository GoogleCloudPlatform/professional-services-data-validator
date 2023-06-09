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

import pytest

@pytest.fixture
def module_under_test():
    import data_validation.query_builder.random_row_builder

    return data_validation.query_builder.random_row_builder


def test_import(module_under_test):
    assert module_under_test is not None


def test_init(module_under_test):
    primary_keys = ["abc", "def"]
    builder = module_under_test.RandomRowBuilder(primary_keys, 100)

    assert builder.primary_keys == primary_keys
    assert builder.batch_size == 100