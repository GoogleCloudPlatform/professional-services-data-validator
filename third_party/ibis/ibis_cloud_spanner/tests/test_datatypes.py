# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from multipledispatch.conflict import ambiguities
from pytest import param

import ibis.expr.datatypes as dt
from third_party.ibis.ibis_cloud_spanner.datatypes import (
    TypeTranslationContext,
    ibis_type_to_cloud_spanner_type,
)

pytestmark = pytest.mark.cloud_spanner


def test_no_ambiguities():
    ambs = ambiguities(ibis_type_to_cloud_spanner_type.funcs)
    assert not ambs


@pytest.mark.parametrize(
    ("datatype", "expected"),
    [
        (dt.float32, "FLOAT64"),
        (dt.float64, "FLOAT64"),
        (dt.uint8, "INT64"),
        (dt.uint16, "INT64"),
        (dt.uint32, "INT64"),
        (dt.int8, "INT64"),
        (dt.int16, "INT64"),
        (dt.int32, "INT64"),
        (dt.int64, "INT64"),
        (dt.string, "STRING"),
        (dt.Array(dt.int64), "ARRAY<INT64>"),
        (dt.Array(dt.string), "ARRAY<STRING>"),
        (dt.date, "DATE"),
        (dt.timestamp, "TIMESTAMP"),
        param(dt.Timestamp(timezone="US/Eastern"), "TIMESTAMP",),
    ],
)
def test_simple(datatype, expected):
    context = TypeTranslationContext()
    assert ibis_type_to_cloud_spanner_type(datatype, context) == expected


@pytest.mark.parametrize("datatype", [dt.uint64, dt.Decimal(8, 3)])
def test_simple_failure_mode(datatype):
    with pytest.raises(TypeError):
        ibis_type_to_cloud_spanner_type(datatype)
