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

from multipledispatch import Dispatcher

import ibis.expr.datatypes as dt


class TypeTranslationContext:
    """A tag class to allow alteration of the way a particular type is
    translated."""

    __slots__ = ()


ibis_type_to_cloud_spanner_type = Dispatcher("ibis_type_to_cloud_spanner_type")


@ibis_type_to_cloud_spanner_type.register(str)
def trans_string_default(datatype):
    return ibis_type_to_cloud_spanner_type(dt.dtype(datatype))


@ibis_type_to_cloud_spanner_type.register(dt.DataType)
def trans_default(t):
    return ibis_type_to_cloud_spanner_type(t, TypeTranslationContext())


@ibis_type_to_cloud_spanner_type.register(str, TypeTranslationContext)
def trans_string_context(datatype, context):
    return ibis_type_to_cloud_spanner_type(dt.dtype(datatype), context)


@ibis_type_to_cloud_spanner_type.register(dt.Floating, TypeTranslationContext)
def trans_float64(t, context):
    return "FLOAT64"


@ibis_type_to_cloud_spanner_type.register(dt.Integer, TypeTranslationContext)
def trans_integer(t, context):
    return "INT64"


@ibis_type_to_cloud_spanner_type.register(dt.Array, TypeTranslationContext)
def trans_array(t, context):
    return "ARRAY<{}>".format(ibis_type_to_cloud_spanner_type(t.value_type, context))


@ibis_type_to_cloud_spanner_type.register(dt.Date, TypeTranslationContext)
def trans_date(t, context):
    return "DATE"


@ibis_type_to_cloud_spanner_type.register(dt.Timestamp, TypeTranslationContext)
def trans_timestamp(t, context):
    return "TIMESTAMP"


@ibis_type_to_cloud_spanner_type.register(dt.DataType, TypeTranslationContext)
def trans_type(t, context):
    return str(t).upper()


@ibis_type_to_cloud_spanner_type.register(dt.UInt64, TypeTranslationContext)
def trans_lossy_integer(t, context):
    raise TypeError(
        "Conversion from uint64 to Cloud Spanner integer type (int64) is lossy"
    )


@ibis_type_to_cloud_spanner_type.register(dt.Decimal, TypeTranslationContext)
def trans_numeric(t, context):
    if (t.precision, t.scale) != (38, 9):
        raise TypeError(
            "Cloud Spanner only supports decimal types with precision of 38 and "
            "scale of 9"
        )
    return "NUMERIC"
