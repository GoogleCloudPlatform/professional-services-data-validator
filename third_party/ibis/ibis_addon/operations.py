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

"""The Ibis Addons Operations are intended to help facilitate new expressions
when required before they can be pushed upstream to Ibis.

Raw SQL Filters:
The ability to inject RawSQL into a query DNE in Ibis.  It must be built out
and applied to each Ibis Data Source directly as each has
extended its own registry.  Eventually this can potentially be pushed to
Ibis as an override, though it would not apply for Pandas and other
non-textual languages.
"""

import datetime
import dateutil
import numpy as np
import string

import google.cloud.bigquery as bq
import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import pandas as pd
import sqlalchemy as sa
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend, get_sqla_table
from ibis.backends.base.sql.alchemy.registry import _cast as sa_fixed_cast
from ibis.backends.base.sql.alchemy.registry import fixed_arity as sa_fixed_arity
from ibis.backends.base.sql.alchemy.translator import AlchemyExprTranslator
from ibis.backends.base.sql.compiler.translator import ExprTranslator
from ibis.backends.base.sql.registry import fixed_arity

# In Ibis 7.1.0, BigQueryType handles type conversion natively.
from ibis.backends.bigquery.compiler import BigQueryExprTranslator
from ibis.backends.bigquery.registry import bigquery_cast
from ibis.backends.impala.compiler import ImpalaExprTranslator
from ibis.backends.mssql.compiler import MsSqlExprTranslator
from ibis.backends.mysql.compiler import MySQLExprTranslator
from ibis.backends.pandas.dispatch import execute_node
from ibis.backends.pandas.execution.temporal import execute_epoch_seconds
from ibis.backends.postgres.compiler import PostgreSQLExprTranslator
from ibis.expr.types import (
    BinaryValue,
    NumericValue,
    StringValue,
    DateValue,
    TimeValue,
    TimestampValue,
)

# Do not remove these lines, they trigger patching of Ibis code.
# We patch Ibis native compilers/backends directly.
import third_party.ibis.ibis_mysql.compiler  # noqa
import third_party.ibis.ibis_postgres  # noqa

from third_party.ibis.ibis_cloud_spanner.compiler import SpannerExprTranslator
from third_party.ibis.ibis_redshift.compiler import RedShiftExprTranslator

from third_party.ibis.ibis_mssql import registry as mssql_registry

# DB2 requires ibm_db_dbi
try:
    from third_party.ibis.ibis_db2.compiler import Db2ExprTranslator
    from third_party.ibis.ibis_db2_zos.compiler import Db2zOSExprTranslator
except Exception:
    Db2ExprTranslator = None
    Db2zOSExprTranslator = None

# Oracle requires oracledb
try:
    from third_party.ibis.ibis_oracle.compiler import OracleExprTranslator
except Exception:
    OracleExprTranslator = None

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

# Sybase requires sqlalchemy_sybase package.
try:
    from third_party.ibis.ibis_sybase.compiler import SybaseExprTranslator
except Exception:
    SybaseExprTranslator = None


import ibis.backends.pandas.execution.constants as pandas_constants


class PandasTypeMapping(dict):
    def __getitem__(self, key):
        if isinstance(key, dt.Decimal):
            return object
        if isinstance(key, dt.Date):
            return object
        try:
            return super().__getitem__(key)
        except KeyError:
            if isinstance(key, dt.DataType):
                for k, v in self.items():
                    if isinstance(key, type(k)) or key == k:
                        return v
            raise

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        if isinstance(key, (dt.Decimal, dt.Date)):
            return True
        if super().__contains__(key):
            return True
        if isinstance(key, dt.DataType):
            for k in self.keys():
                if isinstance(key, type(k)) or key == k:
                    return True
        return False


pandas_constants.IBIS_TYPE_TO_PANDAS_TYPE = PandasTypeMapping(
    pandas_constants.IBIS_TYPE_TO_PANDAS_TYPE
)


# Cast of datetime64 NaT to int64 and then in seconds results in the value below.
# We need to use this value in the datetime.date simulation of the datetime64 behaviour.
NAT_INT64_MIN_IN_SECONDS = np.iinfo(np.int64).min // 1_000_000_000


class BinaryLength(ops.Value):
    arg: ops.Value[dt.Binary | dt.String]
    dtype = dt.int32
    shape = rlz.shape_like("arg")


class PaddedCharLength(ops.Value):
    arg: ops.Value[dt.String]
    dtype = dt.int32
    shape = rlz.shape_like("arg")


class ToChar(ops.Value):
    arg: ops.Value[dt.Decimal | dt.Float64 | dt.Date | dt.Time | dt.Timestamp]
    fmt: ops.Value[dt.String]
    dtype = dt.string
    shape = rlz.shape_like("arg")


class RawSQL(ops.Comparison):
    left: ops.Value[dt.String]
    right: ops.Value[dt.String]


def compile_binary_length(binary_value):
    return BinaryLength(binary_value).to_expr()


def compile_padded_char_length(char_value):
    return PaddedCharLength(char_value).to_expr()


def compile_to_char(numeric_value, fmt):
    return ToChar(numeric_value, fmt=fmt).to_expr()


@bigquery_cast.register(str, dt.Binary, dt.String)
def bigquery_cast_from_binary_generate(compiled_arg, from_, to):
    """Cast of binary to string should be hex conversion."""
    return f"TO_HEX({compiled_arg})"


@bigquery_cast.register(str, dt.String, dt.Binary)
def bigquery_cast_to_binary_generate(compiled_arg, from_, to):
    """Cast of binary to string should be hex conversion."""
    return f"FROM_HEX({compiled_arg})"


def format_hashbytes_teradata(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"rtrim(hash_sha256(TransUnicodeToUTF8({arg})))"
    elif op.how == "sha512":
        return f"rtrim(hash_sha512({arg}))"
    elif op.how == "md5":
        return f"rtrim(hash_md5({arg}))"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def strftime_mysql(translator, op):
    arg = op.arg
    format_string = op.format_str
    arg_formatted = translator.translate(arg)
    arg_type = arg.dtype
    fmt_string = translator.translate(format_string)
    if isinstance(arg_type, dt.Timestamp):
        fmt_string = "%Y-%m-%d %H:%i:%S"
    return sa.func.date_format(arg_formatted, fmt_string)


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


def sa_format_hashbytes_oracle(translator, op):
    arg = translator.translate(op.arg)
    convert = sa.func.convert(arg, sa.sql.literal_column("'UTF8'"))
    hash_func = sa.func.standard_hash(convert, sa.sql.literal_column("'SHA256'"))
    return sa.func.lower(hash_func)


def sa_format_hashbytes_mysql(translator, op):
    arg = translator.translate(op.arg)
    hash_func = sa.func.sha2(arg, sa.sql.literal_column("'256'"))
    return hash_func


def sa_format_hashbytes_redshift(translator, op):
    arg = translator.translate(op.arg)
    return sa.sql.literal_column(f"sha2({arg}, 256)")


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


def sa_format_binary_length_oracle(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.dbms_lob.getlength(arg)


def sa_cast_mysql(t, op):
    # Add cast from numeric to string
    arg = op.arg
    typ = op.to
    arg_dtype = arg.dtype

    sa_arg = t.translate(arg)
    # Specialize going from numeric(p,s>0) to string
    if (
        arg_dtype.is_decimal()
        and arg_dtype.scale
        and arg_dtype.scale > 0
        and typ.is_string()
    ):
        # When casting a number to string MySQL includes the full scale, e.g.:
        #   SELECT CAST(CAST(100 AS DECIMAL(5,2)) AS CHAR);
        #     100.00
        # This doesn't match most engines which would return "100".
        # We've used a workaround from StackOverflow:
        #   https://stackoverflow.com/a/20111398
        return sa_fixed_cast(t, op) + sa.literal(0)
    elif arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.lower(sa.func.hex(sa_arg))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.unhex(sa_arg)

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


def sa_cast_snowflake(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.dtype
    sa_arg = t.translate(arg)

    # Specialize going from numeric(p,s>0) to string
    if (
        arg_dtype.is_decimal()
        and arg_dtype.scale
        and arg_dtype.scale > 0
        and typ.is_string()
    ):
        # When casting a number to string Snowflake includes the full scale, e.g.:
        #   SELECT CAST(CAST(100 AS DECIMAL(5,2)) AS VARCHAR(10));
        #     100.00
        # This doesn't match most engines which would return "100".
        # Using to_char() function instead of cast to return a more typical value.
        # We've wrapped to_char in rtrim(".") due to whole numbers having a trailing ".".
        precision = arg_dtype.precision or 38
        fmt = (
            "FM"
            + ("9" * (precision - arg_dtype.scale - 1))
            + "0."
            + ("9" * arg_dtype.scale)
        )
        return sa.func.rtrim(sa.func.to_char(sa_arg, fmt), ".")

    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.hex_encode(sa_arg, sa.literal(0))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.hex_decode_binary(sa_arg)

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


def sa_format_random(t, op):
    return sa.func.RANDOM()


# --- BigQuery Custom Handlers ---
def bigquery_format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"TO_HEX(SHA256({arg}))"
    elif op.how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def bigquery_strftime(translator, op):
    arg = op.arg
    format_str = op.format_str
    arg_type = arg.dtype
    if arg_type.is_date():
        strftime_format_func_name = "DATE"
    elif arg_type.is_time():
        strftime_format_func_name = "TIME"
    elif arg_type.is_timestamp():
        if arg_type.timezone is None:
            strftime_format_func_name = "DATETIME"
        else:
            strftime_format_func_name = "TIMESTAMP"
    else:
        raise TypeError(f"Unsupported strftime argument type: {arg_type}")

    fmt_string = translator.translate(format_str)
    if format_str.value.startswith("%Y"):
        fmt_string = fmt_string.replace("%Y", "%E4Y", 1)
    arg_formatted = translator.translate(arg)
    if strftime_format_func_name == "TIMESTAMP":
        return "FORMAT_TIMESTAMP({}, {}, {!r})".format(
            fmt_string,
            arg_formatted,
            arg_type.timezone if arg_type.timezone is not None else "UTC",
        )
    return "FORMAT_{}({}, {})".format(
        strftime_format_func_name, fmt_string, arg_formatted
    )


def bigquery_format_binary_length(translator, op):
    arg = translator.translate(op.arg)
    return f"LENGTH({arg})"


# --- Impala Custom Handlers ---
def impala_sa_cast(t, op):
    from ibis.backends.base.sql.registry import (
        type_to_sql_string as base_type_to_sql_string,
    )

    arg = op.arg
    typ = op.to
    arg_dtype = arg.dtype
    arg_formatted = t.translate(arg)
    if arg_dtype.is_binary() and typ.is_string():
        return f"lower(hex({arg_formatted}))"
    elif arg_dtype.is_string() and typ.is_binary():
        return f"unhex({arg_formatted})"
    sql_type = base_type_to_sql_string(typ)
    cast_expr = "CAST({} AS {})".format(arg_formatted, sql_type)
    if arg_dtype.is_boolean() and typ.is_string():
        return f"LOWER({cast_expr})"
    else:
        return cast_expr


def impala_sa_ifnull(t, op):
    arg_formatted = t.translate(op.arg)
    return f"coalesce({arg_formatted},'{op.ifnull_expr.value}')"


def impala_sa_format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"sha2({arg}, 256)"
    elif op.how == "md5":
        return f"md5({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def impala_sa_strftime(t, op):
    import sqlglot as sg

    hive_dialect = sg.dialects.hive.Hive
    if (time_mapping := getattr(hive_dialect, "TIME_MAPPING", None)) is None:
        time_mapping = hive_dialect.time_mapping
    reverse_hive_mapping = {v: k for k, v in time_mapping.items()}
    format_str = sg.time.format_time(op.format_str.value, reverse_hive_mapping)
    targ = t.translate(ops.Cast(op.arg, to=dt.string))
    return f"from_unixtime(unix_timestamp({targ}, {format_str!r}), {format_str!r})"


# --- Postgres Custom Handlers ---
def postgres_sa_format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    convert = sa.func.convert_to(arg, sa.sql.literal_column("'UTF8'"))
    hash_func = sa.func.sha256(convert)
    return sa.func.encode(hash_func, sa.sql.literal_column("'hex'"))


def postgres_sa_epoch_seconds(translator, op):
    arg = translator.translate(op.arg)
    return sa.cast(
        sa.extract("epoch", sa.func.date_trunc(sa.sql.literal_column("'second'"), arg)),
        sa.BIGINT,
    )


def postgres_sa_cast(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.dtype
    sa_arg = t.translate(arg)
    if arg_dtype.is_decimal() and typ.is_string():
        if arg_dtype.scale is None:
            return sa.cast(sa.func.trim_scale(sa_arg), t.get_sqla_type(typ))
        elif arg_dtype.scale > 0:
            precision = arg_dtype.precision or 38
            fmt = (
                "FM"
                + ("9" * (precision - arg_dtype.scale - 1))
                + "0."
                + ("9" * arg_dtype.scale)
            )
            return sa.func.rtrim(sa.func.to_char(sa_arg, fmt), ".")
    elif arg_dtype.is_binary() and typ.is_string():
        return sa.func.encode(sa_arg, sa.literal("hex"))
    elif arg_dtype.is_string() and typ.is_binary():
        return sa.func.decode(sa_arg, sa.literal("hex"))
    return sa_fixed_cast(t, op)


def postgres_sa_format_padded_char_length(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.char_length(
        sa.case(
            (arg.is_(None), sa.literal_column("NULL")),
            else_=sa.func.concat(arg, sa.text("''")),
        )
    )




# Native BigQueryType/BigQuerySchema mapping is used in Ibis 7.1.0


def string_to_epoch(ts: str) -> int:
    """Function to convert string timestamp to epoch seconds"""
    try:
        if pd.isna(ts):
            # Casting datetime64 to int64 uses the minimum possible int64 when it
            # encounters NaT. Simulating the same here for when auto cast fails.
            return NAT_INT64_MIN_IN_SECONDS
        parsed_ts = dateutil.parser.isoparse(ts)
        if parsed_ts.tzinfo is None:
            parsed_ts = parsed_ts.replace(tzinfo=datetime.timezone.utc)
        parsed_ts = parsed_ts.astimezone(datetime.timezone.utc)
        return (
            parsed_ts - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        ).total_seconds()
    except (ValueError, OSError):
        # Support DATE '0001-01-01' which throws error when converted to UTC
        # Catching OSError above because all dates prior to 1970 fail in astimezone on Windows.
        parsed_ts = dateutil.parser.isoparse(ts)
        return (parsed_ts - datetime.datetime(1970, 1, 1)).total_seconds()


@execute_node.register(ops.ExtractEpochSeconds, (datetime.datetime, pd.Series))
def execute_epoch_seconds_new(op, data, **kwargs):
    convert = data.astype
    try:
        series = convert(np.int64)
        # We need int64 below because NaT overflows int32.
        return (series // 1_000_000_000).astype(np.int64)
    except (TypeError, ValueError):
        # Catch 'TypeError' or 'ValueError' for large timestamps beyond max datetime64[ns] as per Issue #1053
        # Cast to string instead to work around datetime64[ns] limitation
        series = data.astype("string")
        epoch_series = series.map(string_to_epoch)
        return epoch_series


def _dvt_list_tables(self, like=None, database=None) -> list:
    """Alternative to BaseAlchemyBackend.list_tables that does not include views in the result."""
    tables = self.inspector.get_table_names(schema=database)
    return self._filter_with_like(tables, like)


def _sa_whitespace_rstrip(t, op):
    sa_arg = t.translate(op.arg)
    return sa.func.rtrim(sa_arg, string.whitespace)


execute_epoch_seconds = execute_epoch_seconds_new

BinaryValue.byte_length = compile_binary_length

StringValue.padded_char_length = compile_padded_char_length

NumericValue.to_char = compile_to_char
DateValue.to_char = compile_to_char
TimeValue.to_char = compile_to_char
TimestampValue.to_char = compile_to_char

# This is an additional DVT only method. We tag this onto BaseAlchemyBackend
# so we can piggy back Ibis code rather than writing metadata queries for all engines.
BaseAlchemyBackend.dvt_list_tables = _dvt_list_tables

BigQueryExprTranslator._registry[ops.HashBytes] = bigquery_format_hashbytes
BigQueryExprTranslator._registry[RawSQL] = format_raw_sql
BigQueryExprTranslator._registry[ops.Strftime] = bigquery_strftime
BigQueryExprTranslator._registry[BinaryLength] = bigquery_format_binary_length

AlchemyExprTranslator._registry[RawSQL] = format_raw_sql
AlchemyExprTranslator._registry[ops.HashBytes] = format_hashbytes_alchemy
AlchemyExprTranslator._registry[PaddedCharLength] = AlchemyExprTranslator._registry[
    ops.StringLength
]
ExprTranslator._registry[RawSQL] = format_raw_sql
ExprTranslator._registry[ops.HashBytes] = format_hashbytes_base
# Base length of padded string is the same as for a standard string.
ExprTranslator._registry[PaddedCharLength] = ExprTranslator._registry[ops.StringLength]

ImpalaExprTranslator._registry[ops.Cast] = impala_sa_cast
ImpalaExprTranslator._registry[RawSQL] = format_raw_sql
ImpalaExprTranslator._registry[ops.HashBytes] = impala_sa_format_hashbytes
ImpalaExprTranslator._registry[ops.RandomScalar] = fixed_arity("RAND", 0)
ImpalaExprTranslator._registry[ops.Strftime] = impala_sa_strftime
ImpalaExprTranslator._registry[BinaryLength] = fixed_arity("length", 1)

if OracleExprTranslator:
    OracleExprTranslator._registry[RawSQL] = sa_format_raw_sql
    OracleExprTranslator._registry[ops.HashBytes] = sa_format_hashbytes_oracle
    OracleExprTranslator._registry[ToChar] = sa_format_to_char
    OracleExprTranslator._registry[BinaryLength] = sa_format_binary_length_oracle
    OracleExprTranslator._registry[ops.RStrip] = _sa_whitespace_rstrip
    OracleExprTranslator._registry[PaddedCharLength] = OracleExprTranslator._registry[
        ops.StringLength
    ]

PostgreSQLExprTranslator._registry[ops.HashBytes] = postgres_sa_format_hashbytes
PostgreSQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
PostgreSQLExprTranslator._registry[ToChar] = sa_format_to_char
PostgreSQLExprTranslator._registry[ops.Cast] = postgres_sa_cast
PostgreSQLExprTranslator._registry[BinaryLength] = sa_format_binary_length
PostgreSQLExprTranslator._registry[ops.ExtractEpochSeconds] = postgres_sa_epoch_seconds
PostgreSQLExprTranslator._registry[PaddedCharLength] = (
    postgres_sa_format_padded_char_length
)


MsSqlExprTranslator._registry[ops.HashBytes] = mssql_registry.sa_format_hashbytes
MsSqlExprTranslator._registry[RawSQL] = sa_format_raw_sql
MsSqlExprTranslator._registry[ops.StringJoin] = mssql_registry.sa_string_join
MsSqlExprTranslator._registry[ops.RandomScalar] = mssql_registry.sa_format_new_id
MsSqlExprTranslator._registry[ops.StringLength] = mssql_registry.sa_format_string_length
MsSqlExprTranslator._registry[ops.Strftime] = mssql_registry.strftime
MsSqlExprTranslator._registry[ops.Cast] = mssql_registry.sa_cast_mssql
MsSqlExprTranslator._registry[BinaryLength] = mssql_registry.sa_format_binary_length
MsSqlExprTranslator._registry[ops.TableColumn] = mssql_registry.sa_table_column
MsSqlExprTranslator._registry[ops.ExtractEpochSeconds] = mssql_registry.sa_epoch_seconds
MsSqlExprTranslator._registry[ops.RStrip] = mssql_registry.sa_whitespace_rstrip
MsSqlExprTranslator._registry[ops.Mean] = mssql_registry.sa_format_mean
MsSqlExprTranslator._registry[PaddedCharLength] = MsSqlExprTranslator._registry[
    ops.StringLength
]

MySQLExprTranslator._registry[ops.Cast] = sa_cast_mysql
MySQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
MySQLExprTranslator._registry[ops.HashBytes] = sa_format_hashbytes_mysql
MySQLExprTranslator._registry[ops.Strftime] = strftime_mysql
MySQLExprTranslator._registry[BinaryLength] = sa_format_binary_length

RedShiftExprTranslator._registry[ops.HashBytes] = sa_format_hashbytes_redshift
RedShiftExprTranslator._registry[RawSQL] = sa_format_raw_sql
RedShiftExprTranslator._registry[BinaryLength] = sa_format_binary_length
RedShiftExprTranslator._registry[PaddedCharLength] = RedShiftExprTranslator._registry[
    ops.StringLength
]

if Db2ExprTranslator:
    Db2ExprTranslator._registry[RawSQL] = sa_format_raw_sql
    Db2ExprTranslator._registry[BinaryLength] = sa_format_binary_length
    Db2ExprTranslator._registry[ops.RStrip] = _sa_whitespace_rstrip
    Db2ExprTranslator._registry[PaddedCharLength] = Db2ExprTranslator._registry[
        ops.StringLength
    ]

if Db2zOSExprTranslator:
    Db2zOSExprTranslator._registry[RawSQL] = sa_format_raw_sql
    Db2zOSExprTranslator._registry[BinaryLength] = sa_format_binary_length
    Db2zOSExprTranslator._registry[PaddedCharLength] = Db2zOSExprTranslator._registry[
        ops.StringLength
    ]

SpannerExprTranslator._registry[RawSQL] = format_raw_sql
SpannerExprTranslator._registry[ops.HashBytes] = bigquery_format_hashbytes
SpannerExprTranslator._registry[BinaryLength] = fixed_arity("length", 1)

if TeradataExprTranslator:
    TeradataExprTranslator._registry[RawSQL] = format_raw_sql
    TeradataExprTranslator._registry[ops.HashBytes] = format_hashbytes_teradata
    TeradataExprTranslator._registry[BinaryLength] = fixed_arity("length", 1)
    TeradataExprTranslator._registry[PaddedCharLength] = (
        TeradataExprTranslator._registry[ops.StringLength]
    )

if SnowflakeExprTranslator:
    SnowflakeExprTranslator._registry[ops.Cast] = sa_cast_snowflake
    SnowflakeExprTranslator._registry[ops.HashBytes] = sa_format_hashbytes_snowflake
    SnowflakeExprTranslator._registry[RawSQL] = sa_format_raw_sql
    SnowflakeExprTranslator._registry[ops.ExtractEpochSeconds] = sa_epoch_time_snowflake
    SnowflakeExprTranslator._registry[ops.RandomScalar] = sa_format_random
    SnowflakeExprTranslator._registry[BinaryLength] = sa_format_binary_length
    SnowflakeExprTranslator._registry[ops.RStrip] = _sa_whitespace_rstrip

if SybaseExprTranslator:
    SybaseExprTranslator._registry[BinaryLength] = mssql_registry.sa_format_binary_length
    SybaseExprTranslator._registry[RawSQL] = sa_format_raw_sql
    SybaseExprTranslator._registry[PaddedCharLength] = mssql_registry.sa_format_string_length

# Patch TemporalValue to support strftime in custom calculations
import ibis.expr.types as et


class TemporalValue:
    @staticmethod
    def strftime(expr, format_str):
        return expr.strftime(format_str)


et.TemporalValue = TemporalValue


# Monkey-patch pandas backend compute_row_reduction to handle string/bytes/dict scalars correctly
try:
    import ibis.backends.pandas.execution.generic as gp
    from collections.abc import Sized
    import pandas as pd
    import numpy as np

    orig_compute_row_reduction = gp.compute_row_reduction

    def dvt_compute_row_reduction(func, values, **kwargs):
        final_sizes = {
            len(x)
            for x in values
            if isinstance(x, Sized) and not isinstance(x, (str, bytes, dict))
        }
        if not final_sizes:
            return func(values)
        (final_size,) = final_sizes
        raw = func(list(map(gp.promote_to_sequence(final_size), values)), **kwargs)
        return pd.Series(raw).squeeze()

    gp.compute_row_reduction = dvt_compute_row_reduction
except Exception:
    pass


# Monkey-patch BigQuery backend to support converting INTERVAL columns to Ibis types
try:
    from ibis.backends.bigquery.datatypes import BigQueryType

    orig_bq_to_ibis = BigQueryType.to_ibis

    @classmethod
    def dvt_bq_to_ibis(cls, typ: str, nullable: bool = True) -> dt.DataType:
        if typ == "INTERVAL":
            return dt.Interval(unit="s", nullable=nullable)
        return orig_bq_to_ibis(typ, nullable=nullable)

    BigQueryType.to_ibis = dvt_bq_to_ibis
except Exception:
    pass


# Patch BigQuery translation of ExtractEpochSeconds to handle DATETIME and DATE correctly
def bq_extract_epoch_seconds(translator, op):
    arg = op.arg
    arg_formatted = translator.translate(arg)
    if arg.dtype.is_date() or (arg.dtype.is_timestamp() and arg.dtype.timezone is None):
        return f"UNIX_SECONDS(CAST({arg_formatted} AS TIMESTAMP))"
    return f"UNIX_SECONDS({arg_formatted})"


BigQueryExprTranslator._registry[ops.ExtractEpochSeconds] = bq_extract_epoch_seconds


# Monkey-patch PandasData.convert_Date to handle out-of-bounds / tricky dates safely
try:
    import ibis.formats.pandas as fp
    import pandas as pd
    from datetime import date

    orig_convert_Date = fp.PandasData.convert_Date

    def dvt_convert_Date(s, dtype, pandas_type):
        if isinstance(s.dtype, pd.DatetimeTZDtype):
            s = s.dt.tz_convert("UTC").dt.tz_localize(None)
        try:
            return s.astype(pandas_type).dt.normalize()
        except Exception:

            def to_date_safe(x):
                if isinstance(x, date):
                    return x
                elif isinstance(x, str):
                    try:
                        parts = list(map(int, x.split("-")))
                        return date(*parts)
                    except Exception:
                        try:
                            import dateutil.parser

                            return dateutil.parser.parse(x).date()
                        except Exception:
                            return x
                return x

            return s.map(to_date_safe)

    fp.PandasData.convert_Date = dvt_convert_Date
except Exception:
    pass


# Monkey-patch BigQueryBackend to load custom DVT methods
try:
    from ibis.backends.bigquery import Backend as BigQueryBackend
    import third_party.ibis.ibis_bigquery as ibq

    BigQueryBackend.do_connect = ibq.Backend.do_connect
    BigQueryBackend._cursor_to_arrow = ibq.Backend._cursor_to_arrow
    BigQueryBackend._parse_project_and_dataset = ibq.Backend._parse_project_and_dataset
    BigQueryBackend.list_primary_key_columns = ibq.Backend.list_primary_key_columns
    BigQueryBackend.dvt_list_tables = ibq.Backend.dvt_list_tables
except Exception:
    pass


# Monkey-patch SqlglotMySQLType._from_sqlglot_DATETIME and TIMESTAMP to accept precision/scale arguments
try:
    from ibis.backends.mysql.datatypes import SqlglotMySQLType
    import ibis.expr.datatypes as dt

    @classmethod
    def _from_sqlglot_datetime_patched(cls, *args, **kwargs) -> dt.Timestamp:
        return dt.Timestamp(nullable=cls.default_nullable)

    @classmethod
    def _from_sqlglot_timestamp_patched(cls, *args, **kwargs) -> dt.Timestamp:
        return dt.Timestamp(timezone="UTC", nullable=cls.default_nullable)

    SqlglotMySQLType._from_sqlglot_DATETIME = _from_sqlglot_datetime_patched
    SqlglotMySQLType._from_sqlglot_TIMESTAMP = _from_sqlglot_timestamp_patched
except Exception:
    pass
