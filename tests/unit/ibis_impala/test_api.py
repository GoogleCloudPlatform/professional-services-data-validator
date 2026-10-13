# Copyright 2025 Google LLC
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

from unittest import mock

import pytest


@pytest.fixture
def module_under_test():
    from third_party.ibis.ibis_impala import api

    return api


def test_impala_connect_string_options(module_under_test):
    """Options from "connections add" are strings, check they reach impyla in their real type."""
    with mock.patch("ibis.impala.connect") as mock_connect:
        module_under_test.impala_connect(
            host="localhost",
            port="10000",
            timeout="30",
            pool_size="4",
            use_ssl="False",
            use_http_transport="true",
        )
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["port"] == 10000
    assert kwargs["timeout"] == 30
    assert kwargs["pool_size"] == 4
    assert kwargs["use_ssl"] is False
    assert kwargs["use_http_transport"] is True


def test_impala_connect_typed_options(module_under_test):
    """Options that already have the right type are passed through unchanged."""
    with mock.patch("ibis.impala.connect") as mock_connect:
        module_under_test.impala_connect(timeout=30, use_ssl=True)
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["port"] == 10000
    assert kwargs["timeout"] == 30
    assert kwargs["pool_size"] == 8
    assert kwargs["use_ssl"] is True
    assert kwargs["use_http_transport"] is False


@pytest.mark.parametrize(
    "value,expected",
    [
        ("true", True),
        ("True", True),
        ("1", True),
        ("false", False),
        ("False", False),
        ("0", False),
        ("", False),
        (True, True),
        (False, False),
    ],
)
def test_to_bool(module_under_test, value, expected):
    assert module_under_test._to_bool(value) is expected
