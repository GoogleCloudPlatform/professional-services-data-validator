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
    datetime.time: dt.Time,
    decimal.Decimal: dt.decimal,
    str: dt.string,
    bytes: dt.binary,
}


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


class TeradataTypeTranslator(object):
    def __init__(self):
        pass

    @classmethod
    def _col_data_nullable(cls, col_data: dict) -> bool:
        return bool(col_data.get("Nullable", "Y ").startswith("Y"))

    @classmethod
    def to_ibis(cls, col_data):
        td_type = col_data["Type"].strip()

        to_ibis_func_name = "to_ibis_from_{}".format(td_type)
        if hasattr(cls, to_ibis_func_name):
            return getattr(cls, to_ibis_func_name)(col_data, return_ibis_type=True)

        return cls.to_ibis_from_other(col_data, return_ibis_type=True)

    @classmethod
    def to_ibis_from_other(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.string(nullable=cls._col_data_nullable(col_data))

        print('Unsupported Date Type Seen: "%s"' % col_data["Type"])
        return "VARCHAR"

    @classmethod
    def to_ibis_from_BV(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.binary(nullable=cls._col_data_nullable(col_data))

        return "VARBYTE"

    @classmethod
    def to_ibis_from_CV(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.string(nullable=cls._col_data_nullable(col_data))

        return "VARCHAR"

    @classmethod
    def to_ibis_from_N(cls, col_data, return_ibis_type=True):
        precision = int(
            col_data.get("DecimalTotalDigits", col_data.get("Decimal Total Digits", 38))
        )
        scale = int(
            col_data.get(
                "DecimalFractionalDigits", col_data.get("Decimal Fractional Digits", 38)
            )
        )
        if return_ibis_type:
            # No precision or scale specified
            if precision == -128 or scale == -128:
                return dt.Decimal()
            return dt.Decimal(
                precision, scale, nullable=cls._col_data_nullable(col_data)
            )

        if precision == -128 or scale == -128:
            return "DECIMAL"

        return "DECIMAL(%d, %d)" % (precision, scale)

    @classmethod
    def to_ibis_from_D(cls, col_data, return_ibis_type=True):
        precision = int(
            col_data.get("DecimalTotalDigits", col_data.get("Decimal Total Digits", 5))
        )
        scale = int(
            col_data.get(
                "DecimalFractionalDigits", col_data.get("Decimal Fractional Digits", 0)
            )
        )
        if return_ibis_type:
            return dt.Decimal(
                precision, scale, nullable=cls._col_data_nullable(col_data)
            )
        value_type = "DECIMAL(%d, %d)" % (precision, scale)
        return value_type

    @classmethod
    def to_ibis_from_F(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.float64(nullable=cls._col_data_nullable(col_data))
        return "FLOAT"

    @classmethod
    def to_ibis_from_I(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int32(nullable=cls._col_data_nullable(col_data))
        return "INT"

    @classmethod
    def to_ibis_from_I1(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int8(nullable=cls._col_data_nullable(col_data))
        return "INT"

    @classmethod
    def to_ibis_from_I2(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int16(nullable=cls._col_data_nullable(col_data))
        return "INT"

    @classmethod
    def to_ibis_from_I8(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int64(nullable=cls._col_data_nullable(col_data))
        return "INT"

    @classmethod
    def to_ibis_from_DA(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.date(nullable=cls._col_data_nullable(col_data))

        return "DATE"

    @classmethod
    def to_ibis_from_TS(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.timestamp(
                nullable=cls._col_data_nullable(col_data), timestamp="UTC"
            )
        return "TIMESTAMP"

    @classmethod
    def to_ibis_from_SZ(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.timestamp(
                timezone="UTC", nullable=cls._col_data_nullable(col_data)
            )

        return "TIMESTAMP WITH TIME ZONE"

    @classmethod
    def to_ibis_from_AT(cls, col_data, return_ibis_type=True):  # issue 1189
        if return_ibis_type:
            return dt.time(nullable=cls._col_data_nullable(col_data))
        return "TIME WITH TIME ZONE"


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
    return "FLOAT"


@ibis_type_to_teradata_type.register(dt.Integer, TypeTranslationContext)
def trans_integer(t, context):
    return "BIGINT"


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
    return "DECIMAL"
