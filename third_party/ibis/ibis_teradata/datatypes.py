# Copyright 2023 Google Inc.
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
import datetime, decimal

# Mapping https://pypi.org/project/teradatasql/#DataTypes to Ibis
_teradatasql_to_ibis_type = {
    datetime.date: dt.date,
    int: dt.int64,
    float: dt.float64,
    datetime.datetime: dt.timestamp,
    decimal.Decimal: dt.decimal,
    str: dt.string,
    bytes: dt.binary
}

_teradata_native_to_ibis = {
    'DA': 'date',
    'TS': 'timestamp',
    'I': 'int64',
    'I1':'int8',
    'I2': 'int8',
    'I8': 'int16',
    'F': 'float64',
    'D': 'decimal',
    'N': 'decimal',
    'CV': 'string',
}

def _get_type(t):
    if t in _teradata_native_to_ibis:
        return _teradata_native_to_ibis[t]
    else:
        raise Exception(f"Teradata data type {t} not supported.")


class TypeTranslationContext:
    """A tag class to allow alteration of the way a particular type is
    translated.
    Notes
    -----
    This is used to raise an exception when INT64 types are encountered to
    avoid suprising results due to BigQuery's handling of INT64 types in
    JavaScript UDFs.
    """

    __slots__ = ()


class UDFContext(TypeTranslationContext):
    __slots__ = ()


ibis_type_to_teradata_type = Dispatcher("ibis_type_to_teradata_type")

@ibis_type_to_teradata_type.register(dt.DataType)
def trans_default(t):
    return ibis_type_to_teradata_type(t, TypeTranslationContext())


@ibis_type_to_teradata_type.register(str, TypeTranslationContext)
def trans_string_context(datatype, context):
    return "VARCHAR(255)"


@ibis_type_to_teradata_type.register(dt.String, TypeTranslationContext)
def trans_string(t, context):
    return "VARCHAR(255)"


@ibis_type_to_teradata_type.register(dt.Floating, TypeTranslationContext)
def trans_float64(t, context):
    return "FLOAT64"


@ibis_type_to_teradata_type.register(dt.Integer, TypeTranslationContext)
def trans_integer(t, context):
    return "INT64"


@ibis_type_to_teradata_type.register(dt.UInt64, (TypeTranslationContext, UDFContext))
def trans_lossy_integer(t, context):
    raise TypeError("Conversion from uint64 to BigQuery integer type (int64) is lossy")


@ibis_type_to_teradata_type.register(dt.Date, TypeTranslationContext)
def trans_date(t, context):
    return "DATE"


@ibis_type_to_teradata_type.register(dt.Timestamp, TypeTranslationContext)
def trans_timestamp(t, context):
    if t.timezone is not None:
        return f"TIMESTAMP AT TIME ZONE '{t.timezone}'"
    return "TIMESTAMP"


@ibis_type_to_teradata_type.register(dt.DataType, TypeTranslationContext)
def trans_type(t, context):
    return str(t).upper()


@ibis_type_to_teradata_type.register(dt.Decimal, TypeTranslationContext)
def trans_numeric(t, context):
    return "NUMERIC"
