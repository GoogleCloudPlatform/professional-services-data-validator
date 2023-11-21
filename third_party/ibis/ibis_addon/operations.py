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

""" The Ibis Addons Operations are intended to help facilitate new expressions
when required before they can be pushed upstream to Ibis.

Raw SQL Filters:
The ability to inject RawSQL into a query DNE in Ibis.  It must be built out
and applied to each Ibis Data Source directly as each has
extended its own registry.  Eventually this can potentially be pushed to
Ibis as an override, though it would not apply for Pandas and other
non-textual languages.
"""
import google.cloud.bigquery as bq
import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import sqlalchemy as sa
from ibis.backends.base.sql.alchemy.registry import fixed_arity as sa_fixed_arity
from ibis.backends.base.sql.alchemy.translator import AlchemyExprTranslator
from ibis.backends.base.sql.compiler.translator import ExprTranslator
from ibis.backends.base.sql.registry import fixed_arity
from ibis.backends.bigquery.client import (
    _DTYPE_TO_IBIS_TYPE as _BQ_DTYPE_TO_IBIS_TYPE,
    _LEGACY_TO_STANDARD as _BQ_LEGACY_TO_STANDARD,
)
from ibis.backends.bigquery.compiler import BigQueryExprTranslator
from ibis.backends.bigquery.registry import (
    STRFTIME_FORMAT_FUNCTIONS as BQ_STRFTIME_FORMAT_FUNCTIONS,
)
from ibis.backends.impala.compiler import ImpalaExprTranslator
from ibis.backends.mssql.compiler import MsSqlExprTranslator
from ibis.backends.mysql.compiler import MySQLExprTranslator
from ibis.backends.postgres.compiler import PostgreSQLExprTranslator
from ibis.expr.operations import (
    Cast,
    Comparison,
    HashBytes,
    IfNull,
    RandomScalar,
    Strftime,
    StringJoin,
    Value,
    ExtractEpochSeconds,
)
from ibis.expr.types import BinaryValue, NumericValue, TemporalValue

import third_party.ibis.ibis_mysql.compiler
import third_party.ibis.ibis_postgres.client
from third_party.ibis.ibis_db2.compiler import Db2ExprTranslator
from third_party.ibis.ibis_oracle.compiler import OracleExprTranslator
from third_party.ibis.ibis_redshift.compiler import RedShiftExprTranslator
from third_party.ibis.ibis_cloud_spanner.compiler import SpannerExprTranslator

# TD requires teradatasql
try:
    from third_party.ibis.ibis_teradata.compiler import TeradataExprTranslator
except Exception:
    TeradataExprTranslator = None

# Snowflake requires snowflake-connector-python and snowflake-sqlalchemy
try:
    from ibis.backends.snowflake import SnowflakeExprTranslator
except Exception:
    SnowflakeExprTranslator = None


class BinaryLength(Value):
    arg = rlz.one_of([rlz.value(dt.Binary)])
    output_dtype = dt.int32
    output_shape = rlz.shape_like("arg")


class ToChar(Value):
    arg = rlz.one_of(
        [
            rlz.value(dt.Decimal),
            rlz.value(dt.float64),
            rlz.value(dt.Date),
            rlz.value(dt.Time),
            rlz.value(dt.Timestamp),
        ]
    )
    fmt = rlz.string
    output_type = rlz.shape_like("arg")


class RawSQL(Comparison):
    pass


def compile_binary_length(binary_value):
    return BinaryLength(binary_value).to_expr()


def compile_to_char(numeric_value, fmt):
    return ToChar(numeric_value, fmt=fmt).to_expr()


def format_hash_bigquery(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({rg})"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")


def format_hashbytes_bigquery(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"TO_HEX(SHA256({arg}))"
    elif how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")


def format_hashbytes_teradata(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"rtrim(hash_sha256({arg}))"
    elif op.how == "sha512":
        return f"rtrim(hash_sha512({arg}))"
    elif op.how == "md5":
        return f"rtrim(hash_md5({arg}))"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")


def strftime_bigquery(translator, op):
    """Timestamp formatting."""
    arg = op.arg
    format_str = op.format_str
    arg_type = arg.output_dtype
    strftime_format_func_name = BQ_STRFTIME_FORMAT_FUNCTIONS[type(arg_type)]
    fmt_string = translator.translate(format_str)
    arg_formatted = translator.translate(arg)
    if isinstance(arg_type, dt.Timestamp):
        return "FORMAT_{}({}, {}({}), {!r})".format(
            strftime_format_func_name,
            fmt_string,
            strftime_format_func_name,
            arg_formatted,
            arg_type.timezone if arg_type.timezone is not None else "UTC",
        )
    return "FORMAT_{}({}, {})".format(
        strftime_format_func_name, fmt_string, arg_formatted
    )


def strftime_mysql(translator, op):
    arg = op.arg
    format_string = op.format_str
    arg_formatted = translator.translate(arg)
    arg_type = arg.output_dtype
    fmt_string = translator.translate(format_string)
    if isinstance(arg_type, dt.Timestamp):
        fmt_string = "%Y-%m-%d %H:%i:%S"
    return sa.func.date_format(arg_formatted, fmt_string)


def strftime_mssql(translator, op):
    """Use MS SQL CONVERT() in place of STRFTIME().

    This is pretty restrictive due to the limited styles offered by SQL Server,
    we've just covered off the generic formats used when casting date based columns
    to string in order to complete row data comparison."""
    arg, pattern = map(translator.translate, op.args)
    supported_convert_styles = {
        "%Y-%m-%d": 23,  # ISO8601
        "%Y-%m-%d %H:%M:%S": 20,  # ODBC canonical
        "%Y-%m-%d %H:%M:%S.%f": 21,  # ODBC canonical (with milliseconds)
    }
    try:
        convert_style = supported_convert_styles[pattern.value]
    except KeyError:
        raise NotImplementedError(
            f"strftime format {pattern.value} not supported for SQL Server."
        )
    result = sa.func.convert(sa.text("VARCHAR(32)"), arg, convert_style)
    return result


def strftime_impala(t, op):
    import sqlglot as sg

    hive_dialect = sg.dialects.hive.Hive
    if (time_mapping := getattr(hive_dialect, "TIME_MAPPING", None)) is None:
        time_mapping = hive_dialect.time_mapping
    reverse_hive_mapping = {v: k for k, v in time_mapping.items()}
    format_str = sg.time.format_time(op.format_str.value, reverse_hive_mapping)
    targ = t.translate(ops.Cast(op.arg, to=dt.string))
    return f"from_unixtime(unix_timestamp({targ}, {format_str!r}), {format_str!r})"


def format_hashbytes_hive(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"sha2({arg}, 256)"
    elif op.how == "md5":
        return f"md5({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def format_hashbytes_alchemy(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"sha2({arg}, 256)"
    elif op.how == "md5":
        return f"md5({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def format_hashbytes_base(translator, op):
    arg = translator.translate(op.arg)
    return f"sha2({arg}, 256)"


def compile_raw_sql(table, sql):
    op = RawSQL(table[table.columns[0]].cast(dt.string), ibis.literal(sql))
    return op.to_expr()


def format_raw_sql(translator, op):
    rand_col, raw_sql = op.args
    return raw_sql.args[0]


def sa_format_raw_sql(translator, op):
    rand_col, raw_sql = op.args
    return sa.text(raw_sql.args[0])


def sa_format_hashbytes_mssql(translator, op):
    arg = translator.translate(op.arg)
    cast_arg = sa.func.convert(sa.sql.literal_column("VARCHAR(MAX)"), arg)
    hash_func = sa.func.hashbytes(sa.sql.literal_column("'SHA2_256'"), cast_arg)
    hash_to_string = sa.func.convert(
        sa.sql.literal_column("CHAR(64)"), hash_func, sa.sql.literal_column("2")
    )
    return sa.func.lower(hash_to_string)


def sa_format_hashbytes_oracle(translator, op):
    arg = translator.translate(op.arg)
    convert = sa.func.convert(arg, sa.sql.literal_column("'UTF8'"))
    hash_func = sa.func.standard_hash(convert, sa.sql.literal_column("'SHA256'"))
    return sa.func.lower(hash_func)


def sa_format_hashbytes_mysql(translator, op):
    arg = translator.translate(op.arg)
    hash_func = sa.func.sha2(arg, sa.sql.literal_column("'256'"))
    return hash_func


def sa_format_hashbytes_db2(translator, op):
    compiled_arg = translator.translate(op.arg)
    hashfunc = sa.func.hash(compiled_arg, sa.sql.literal_column("2"))
    hex = sa.func.hex(hashfunc)
    return sa.func.lower(hex)


def sa_format_hashbytes_redshift(translator, op):
    arg = translator.translate(op.arg)
    return sa.sql.literal_column(f"sha2({arg}, 256)")


def sa_format_hashbytes_postgres(translator, op):
    arg = translator.translate(op.arg)
    convert = sa.func.convert_to(arg, sa.sql.literal_column("'UTF8'"))
    hash_func = sa.func.sha256(convert)
    return sa.func.encode(hash_func, sa.sql.literal_column("'hex'"))


def sa_format_hashbytes_snowflake(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.sha2(arg)


def sa_epoch_time_snowflake(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.date_part(sa.sql.literal_column("epoch_seconds"), arg)


def sa_format_to_char(translator, op):
    arg = translator.translate(op.arg)
    fmt = translator.translate(op.fmt)
    return sa.func.to_char(arg, fmt)


def sa_format_binary_length(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.length(arg)


def sa_format_binary_length_mssql(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.datalength(arg)


def sa_format_binary_length_oracle(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.dbms_lob.getlength(arg)


def sa_cast_postgres(t, op):
    # Add cast from numeric to string
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)

    # Specialize going from numeric(p,s>0) to string
    if (
        arg_dtype.is_decimal()
        and arg_dtype.scale
        and arg_dtype.scale > 0
        and typ.is_string()
    ):
        # When casting a number to string PostgreSQL includes the full scale, e.g.:
        #   SELECT CAST(CAST(100 AS DECIMAL(5,2)) AS VARCHAR(10));
        #     100.00
        # This doesn't match most engines which would return "100".
        # Using to_char() function instead of cast to return a more typical value.
        # We've wrapped to_char in rtrim(".") due to whole numbers having a trailing ".".
        # Would have liked to use trim_scale but this is only available in PostgreSQL 13+
        #     return (sa.cast(sa.func.trim_scale(arg), typ))
        precision = arg_dtype.precision or 38
        fmt = (
            "FM" + ("9" * (precision - arg_dtype.scale)) + "." + ("9" * arg_dtype.scale)
        )
        return sa.func.rtrim(sa.func.to_char(sa_arg, fmt), ".")

    # specialize going from an integer type to a timestamp
    if arg_dtype.is_integer() and typ.is_timestamp():
        return t.integer_to_timestamp(sa_arg, tz=typ.timezone)

    if arg_dtype.is_binary() and typ.is_string():
        return sa.func.encode(sa_arg, "escape")

    if typ.is_binary():
        #  decode yields a column of memoryview which is annoying to deal with
        # in pandas. CAST(expr AS BYTEA) is correct and returns byte strings.
        return sa.cast(sa_arg, sa.LargeBinary())

    if typ.is_json() and not t.native_json_type:
        return sa_arg

    return sa.cast(sa_arg, t.get_sqla_type(typ))


def _sa_string_join(t, op):
    return sa.func.concat(*map(t.translate, op.arg))


def sa_format_new_id(t, op):
    return sa.func.NEWID()


def sa_format_random(t, op):
    return sa.func.RANDOM()


_BQ_DTYPE_TO_IBIS_TYPE["TIMESTAMP"] = dt.Timestamp(timezone="UTC")


@dt.dtype.register(bq.schema.SchemaField)
def _bigquery_field_to_ibis_dtype(field):
    """Convert BigQuery `field` to an ibis type.
    Taken from ibis.backends.bigquery.client.py for issue:
        https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/926
    """
    typ = field.field_type
    if typ == "RECORD":
        fields = field.fields
        assert fields, "RECORD fields are empty"
        names = [el.name for el in fields]
        ibis_types = list(map(dt.dtype, fields))
        ibis_type = dt.Struct(dict(zip(names, ibis_types)))
    elif typ == "NUMERIC":
        if not field.precision and not field.scale:
            return dt.Decimal(precision=38, scale=9, nullable=field.is_nullable)
        return dt.Decimal(
            precision=field.precision,
            scale=field.scale or 0,
            nullable=field.is_nullable,
        )
    elif typ == "BIGNUMERIC":
        if not field.precision and not field.scale:
            return dt.Decimal(precision=76, scale=38, nullable=field.is_nullable)
        return dt.Decimal(
            precision=field.precision,
            scale=field.scale or 0,
            nullable=field.is_nullable,
        )
    else:
        ibis_type = _BQ_LEGACY_TO_STANDARD.get(typ, typ)
        if ibis_type in _BQ_DTYPE_TO_IBIS_TYPE:
            ibis_type = _BQ_DTYPE_TO_IBIS_TYPE[ibis_type](nullable=field.is_nullable)
        else:
            ibis_type = ibis_type
    if field.mode == "REPEATED":
        ibis_type = dt.Array(ibis_type)
    return ibis_type


BinaryValue.byte_length = compile_binary_length

NumericValue.to_char = compile_to_char
TemporalValue.to_char = compile_to_char

BigQueryExprTranslator._registry[HashBytes] = format_hashbytes_bigquery
BigQueryExprTranslator._registry[RawSQL] = format_raw_sql
BigQueryExprTranslator._registry[Strftime] = strftime_bigquery
BigQueryExprTranslator._registry[BinaryLength] = sa_format_binary_length

AlchemyExprTranslator._registry[RawSQL] = format_raw_sql
AlchemyExprTranslator._registry[HashBytes] = format_hashbytes_alchemy
ExprTranslator._registry[RawSQL] = format_raw_sql
ExprTranslator._registry[HashBytes] = format_hashbytes_base

ImpalaExprTranslator._registry[RawSQL] = format_raw_sql
ImpalaExprTranslator._registry[HashBytes] = format_hashbytes_hive
ImpalaExprTranslator._registry[RandomScalar] = fixed_arity("RAND", 0)
ImpalaExprTranslator._registry[Strftime] = strftime_impala
ImpalaExprTranslator._registry[BinaryLength] = sa_format_binary_length

OracleExprTranslator._registry[RawSQL] = sa_format_raw_sql
OracleExprTranslator._registry[HashBytes] = sa_format_hashbytes_oracle
OracleExprTranslator._registry[ToChar] = sa_format_to_char
OracleExprTranslator._registry[BinaryLength] = sa_format_binary_length_oracle

PostgreSQLExprTranslator._registry[HashBytes] = sa_format_hashbytes_postgres
PostgreSQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
PostgreSQLExprTranslator._registry[ToChar] = sa_format_to_char
PostgreSQLExprTranslator._registry[Cast] = sa_cast_postgres
PostgreSQLExprTranslator._registry[BinaryLength] = sa_format_binary_length

MsSqlExprTranslator._registry[HashBytes] = sa_format_hashbytes_mssql
MsSqlExprTranslator._registry[RawSQL] = sa_format_raw_sql
MsSqlExprTranslator._registry[IfNull] = sa_fixed_arity(sa.func.isnull, 2)
MsSqlExprTranslator._registry[StringJoin] = _sa_string_join
MsSqlExprTranslator._registry[RandomScalar] = sa_format_new_id
MsSqlExprTranslator._registry[Strftime] = strftime_mssql
MsSqlExprTranslator._registry[BinaryLength] = sa_format_binary_length_mssql

MySQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
MySQLExprTranslator._registry[HashBytes] = sa_format_hashbytes_mysql
MySQLExprTranslator._registry[Strftime] = strftime_mysql
MySQLExprTranslator._registry[BinaryLength] = sa_format_binary_length

RedShiftExprTranslator._registry[HashBytes] = sa_format_hashbytes_redshift
RedShiftExprTranslator._registry[RawSQL] = sa_format_raw_sql
RedShiftExprTranslator._registry[BinaryLength] = sa_format_binary_length

Db2ExprTranslator._registry[HashBytes] = sa_format_hashbytes_db2
Db2ExprTranslator._registry[RawSQL] = sa_format_raw_sql
Db2ExprTranslator._registry[BinaryLength] = sa_format_binary_length

SpannerExprTranslator._registry[RawSQL] = format_raw_sql

if TeradataExprTranslator:
    TeradataExprTranslator._registry[RawSQL] = format_raw_sql
    TeradataExprTranslator._registry[HashBytes] = format_hashbytes_teradata
    TeradataExprTranslator._registry[BinaryLength] = sa_format_binary_length

if SnowflakeExprTranslator:
    SnowflakeExprTranslator._registry[HashBytes] = sa_format_hashbytes_snowflake
    SnowflakeExprTranslator._registry[RawSQL] = sa_format_raw_sql
    SnowflakeExprTranslator._registry[IfNull] = sa_fixed_arity(sa.func.ifnull, 2)
    SnowflakeExprTranslator._registry[ExtractEpochSeconds] = sa_epoch_time_snowflake
    SnowflakeExprTranslator._registry[RandomScalar] = sa_format_random
    SnowflakeExprTranslator._registry[BinaryLength] = sa_format_binary_length
