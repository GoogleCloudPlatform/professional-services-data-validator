# Copyright 2026 Google LLC
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


def get_module_under_test():
    try:
        from third_party.ibis import ibis_sybase
    except ModuleNotFoundError:
        # We don't install Sybase client for all unit tests.
        # These tests will only complete when executed in an environment with sqlalchemy_sybase.
        ibis_sybase = None

    return ibis_sybase


@pytest.fixture
def module_under_test():
    return get_module_under_test()


@pytest.mark.skipif(not get_module_under_test(), reason="No Sybase driver")
def test_import(module_under_test):
    """Check that importing ibis_sybase does not throw exceptions (aside from ModuleNotFoundError)"""
    assert module_under_test is not None
