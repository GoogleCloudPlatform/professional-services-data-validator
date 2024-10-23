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
import datetime

import google.cloud.bigquery as bq
import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import pandas as pd
import sqlalchemy as sa
from ibis.backends.base.sql.alchemy.registry import _cast as sa_fixed_cast
from ibis.backends.base.sql.alchemy.registry import fixed_arity as sa_fixed_arity
from ibis.backends.base.sql.alchemy.translator import AlchemyExprTranslator
from ibis.backends.base.sql.compiler.translator import ExprTranslator
from ibis.backends.base.sql.registry import (
    fixed_arity,
    type_to_sql_string as base_type_to_sql_string,
)
from ibis.backends.bigquery.client import (
    _DTYPE_TO_IBIS_TYPE as _BQ_DTYPE_TO_IBIS_TYPE,
    _LEGACY_TO_STANDARD as _BQ_LEGACY_TO_STANDARD,
)
from ibis.backends.bigquery.compiler import BigQueryExprTranslator
from ibis.backends.bigquery.registry import (
    STRFTIME_FORMAT_FUNCTIONS as BQ_STRFTIME_FORMAT_FUNCTIONS,
    bigquery_cast,
)
from ibis.backends.impala.compiler import ImpalaExprTranslator
from ibis.backends.mssql.compiler import MsSqlExprTranslator
from ibis.backends.mysql.compiler import MySQLExprTranslator
from ibis.backends.pandas.dispatch import execute_node
from ibis.backends.pandas.execution.temporal import execute_epoch_seconds
from ibis.backends.postgres.compiler import PostgreSQLExprTranslator
from ibis.expr.operations import (
    Cast,
    Comparison,
    ExtractEpochSeconds,
    HashBytes,
    IfNull,
    RandomScalar,
    Strftime,
    StringJoin,
    Value,
    TableColumn,
)
from ibis.expr.types import BinaryValue, NumericValue, TemporalValue

# Do not remove these lines, they trigger patching of Ibis code.
import third_party.ibis.ibis_mysql.compiler  # noqa
from third_party.ibis.ibis_mssql.registry import mssql_table_column
import third_party.ibis.ibis_postgres.client  # noqa

from third_party.ibis.ibis_cloud_spanner.compiler import SpannerExprTranslator
from third_party.ibis.ibis_redshift.compiler import RedShiftExprTranslator

# DB2 requires ibm_db_dbi
try:
    from third_party.ibis.ibis_db2.compiler import Db2ExprTranslator
except Exception:
    Db2ExprTranslator = None

# Oracle requires cx_Oracle
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


@bigquery_cast.register(str, dt.Binary, dt.String)
def bigquery_cast_from_binary_generate(compiled_arg, from_, to):
    """Cast of binary to string should be hex conversion."""
    return f"TO_HEX({compiled_arg})"


@bigquery_cast.register(str, dt.String, dt.Binary)
def bigquery_cast_to_binary_generate(compiled_arg, from_, to):
    """Cast of binary to string should be hex conversion."""
    return f"FROM_HEX({compiled_arg})"


def format_hash_bigquery(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def format_hashbytes_bigquery(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"TO_HEX(SHA256({arg}))"
    elif op.how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


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


def strftime_bigquery(translator, op):
    """Timestamp formatting."""
    arg = op.arg
    format_str = op.format_str
    arg_type = arg.output_dtype
    strftime_format_func_name = BQ_STRFTIME_FORMAT_FUNCTIONS[type(arg_type)]
    fmt_string = translator.translate(format_str)
    # Deal with issue 1181 due a GoogleSQL bug with dates before 1000 CE affects both date and timestamp types
    if format_str.value.startswith("%Y"):
        fmt_string = fmt_string.replace("%Y", "%E4Y", 1)
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
    arg_type = op.args[0].output_dtype
    if (
        hasattr(arg_type, "timezone") and arg_type.timezone
    ):  # our datetime comparisons do not include timezone, so we need to cast this to Datetime which is timezone naive
        arg = sa.cast(arg, sa.types.DateTime)
    return sa.func.convert(sa.text("VARCHAR"), arg, convert_style)


def strftime_impala(t, op):
    import sqlglot as sg

    hive_dialect = sg.dialects.hive.Hive
    if (time_mapping := getattr(hive_dialect, "TIME_MAPPING", None)) is None:
        time_mapping = hive_dialect.time_mapping
    reverse_hive_mapping = {v: k for k, v in time_mapping.items()}
    format_str = sg.time.format_time(op.format_str.value, reverse_hive_mapping)
    targ = t.translate(ops.Cast(op.arg, to=dt.string))
    return f"from_unixtime(unix_timestamp({targ}, {format_str!r}), {format_str!r})"


def strftime_db2(translator, op):
    """Date, Datetime, Timestamp formatting specific to DB2."""
    # TODO(issue-1296): third_party/ibis/ibis_db2/registry.py:298 - AttributeError: 'Strftime' object has no attribute 'value'
    pass


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
    hash_func = sa.func.hash(compiled_arg, sa.sql.literal_column("2"))
    # OBS: SYSIBM.HEX function accepts a max length of 16336 bytes (https://www.ibm.com/docs/en/db2/11.5?topic=functions-hex)
    hex_func = sa.func.hex(hash_func)
    return sa.func.lower(hex_func)


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


def sa_cast_decimal_when_scale_padded_fmt_fm(t, op):
    """Caters for engines that fully pad scale with 0s when casting decimal to string and support FM format."""
    # Add cast from numeric to string
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    # Specialize going from numeric(p,s>0) to string
    if (
        arg_dtype.is_decimal()
        and arg_dtype.scale
        and arg_dtype.scale > 0
        and typ.is_string()
    ):
        sa_arg = t.translate(arg)
        # When casting a number to string PostgreSQL and Snowflake include the full scale, e.g.:
        #   SELECT CAST(CAST(100 AS DECIMAL(5,2)) AS VARCHAR(10));
        #     100.00
        # This doesn't match most engines which would return "100".
        # Using to_char() function instead of cast to return a more typical value.
        # We've wrapped to_char in rtrim(".") due to whole numbers having a trailing ".".
        # Would have liked to use trim_scale but this is only available in PostgreSQL 13+
        #     return (sa.cast(sa.func.trim_scale(arg), typ))
        precision = arg_dtype.precision or 38
        fmt = (
            "FM"
            + ("9" * (precision - arg_dtype.scale - 1))
            + "0."
            + ("9" * arg_dtype.scale)
        )
        return sa.func.rtrim(sa.func.to_char(sa_arg, fmt), ".")
    return None


def sa_cast_hive(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    arg_formatted = t.translate(arg)

    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return f"lower(hex({arg_formatted}))"
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return f"unhex({arg_formatted})"

    # Cannot use sa_fixed_cast() because of ImpalaExprTranslator ancestry.
    sql_type = base_type_to_sql_string(typ)
    cast_expr = "CAST({} AS {})".format(arg_formatted, sql_type)

    if arg_dtype.is_boolean() and typ.is_string():
        return f"LOWER({cast_expr})"
    else:
        return cast_expr


def sa_cast_postgres(t, op):
    custom_cast = sa_cast_decimal_when_scale_padded_fmt_fm(t, op)
    if custom_cast is not None:
        return custom_cast

    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)
    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.encode(sa_arg, sa.literal("hex"))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.decode(sa_arg, sa.literal("hex"))

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


def sa_cast_mssql(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)
    # Specialize going from a binary float type to a string.
    if (arg_dtype.is_float32() or arg_dtype.is_float64()) and typ.is_string():
        # This prevents output in scientific notation, at least for my tests it did.
        return sa.func.format(sa_arg, "G")
    elif arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.lower(
            sa.func.convert(sa.text("VARCHAR(MAX)"), sa_arg, sa.literal(2))
        )
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.convert(sa.text("VARBINARY(MAX)"), sa_arg, sa.literal(2))

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


def sa_cast_mysql(t, op):
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
    custom_cast = sa_cast_decimal_when_scale_padded_fmt_fm(t, op)
    if custom_cast is not None:
        return custom_cast

    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)
    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.hex_encode(sa_arg, sa.literal(0))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.hex_decode_binary(sa_arg)

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


def _sa_string_join(t, op):
    if (
        len(op.arg) == 1
    ):  # SQL Server CONCAT errs when there is one column being hashed (issue #1202), renaming using type_coerce rather than CONCAT
        return sa.type_coerce(
            t.translate(op.arg[0]),
            sa.types.String,
        )
    else:
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


def string_to_epoch(ts: str):
    "Function to convert string timestamp to epoch seconds"
    from dateutil.parser import isoparse
    from dateutil.tz import UTC
    from datetime import datetime, timezone

    try:
        parsed_ts = isoparse(ts).astimezone(UTC)
        return (parsed_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    except ValueError:
        # Support DATE '0001-01-01' which throws error when converted to UTC
        parsed_ts = isoparse(ts)
        return (parsed_ts - datetime(1970, 1, 1)).total_seconds()


@execute_node.register(ops.ExtractEpochSeconds, (datetime.datetime, pd.Series))
def execute_epoch_seconds_new(op, data, **kwargs):
    import numpy as np

    convert = getattr(data, "view", data.astype)
    try:
        series = convert(np.int64)
        return (series // 1_000_000_000).astype(np.int32)
    except TypeError:
        # Catch 'TypeError' for large timestamps beyond max datetime64[ns] as per Issue #1053
        # Cast to string instead to work around datetime64[ns] limitation
        series = data.astype("string")
        epoch_series = series.map(string_to_epoch)
        return epoch_series


execute_epoch_seconds = execute_epoch_seconds_new

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

ImpalaExprTranslator._registry[Cast] = sa_cast_hive
ImpalaExprTranslator._registry[RawSQL] = format_raw_sql
ImpalaExprTranslator._registry[HashBytes] = format_hashbytes_hive
ImpalaExprTranslator._registry[RandomScalar] = fixed_arity("RAND", 0)
ImpalaExprTranslator._registry[Strftime] = strftime_impala
ImpalaExprTranslator._registry[BinaryLength] = sa_format_binary_length

if OracleExprTranslator:
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
MsSqlExprTranslator._registry[Cast] = sa_cast_mssql
MsSqlExprTranslator._registry[BinaryLength] = sa_format_binary_length_mssql
MsSqlExprTranslator._registry[TableColumn] = mssql_table_column

MySQLExprTranslator._registry[Cast] = sa_cast_mysql
MySQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
MySQLExprTranslator._registry[HashBytes] = sa_format_hashbytes_mysql
MySQLExprTranslator._registry[Strftime] = strftime_mysql
MySQLExprTranslator._registry[BinaryLength] = sa_format_binary_length

RedShiftExprTranslator._registry[HashBytes] = sa_format_hashbytes_redshift
RedShiftExprTranslator._registry[RawSQL] = sa_format_raw_sql
RedShiftExprTranslator._registry[BinaryLength] = sa_format_binary_length

if Db2ExprTranslator:
    Db2ExprTranslator._registry[HashBytes] = sa_format_hashbytes_db2
    Db2ExprTranslator._registry[RawSQL] = sa_format_raw_sql
    Db2ExprTranslator._registry[BinaryLength] = sa_format_binary_length
    Db2ExprTranslator._registry[Strftime] = strftime_db2

SpannerExprTranslator._registry[RawSQL] = format_raw_sql
SpannerExprTranslator._registry[HashBytes] = format_hashbytes_bigquery
SpannerExprTranslator._registry[BinaryLength] = sa_format_binary_length

if TeradataExprTranslator:
    TeradataExprTranslator._registry[RawSQL] = format_raw_sql
    TeradataExprTranslator._registry[HashBytes] = format_hashbytes_teradata
    TeradataExprTranslator._registry[BinaryLength] = sa_format_binary_length

if SnowflakeExprTranslator:
    SnowflakeExprTranslator._registry[Cast] = sa_cast_snowflake
    SnowflakeExprTranslator._registry[HashBytes] = sa_format_hashbytes_snowflake
    SnowflakeExprTranslator._registry[RawSQL] = sa_format_raw_sql
    SnowflakeExprTranslator._registry[IfNull] = sa_fixed_arity(sa.func.ifnull, 2)
    SnowflakeExprTranslator._registry[ExtractEpochSeconds] = sa_epoch_time_snowflake
    SnowflakeExprTranslator._registry[RandomScalar] = sa_format_random
    SnowflakeExprTranslator._registry[BinaryLength] = sa_format_binary_length
