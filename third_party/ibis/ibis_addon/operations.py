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

import ibis
import sqlalchemy as sa

import ibis.expr.api
import ibis.expr.datatypes as dt
import ibis.expr.rules as rlz
import ibis.expr.operations as ops

from data_validation.clients import _raise_missing_client_error
from ibis_bigquery.compiler import (
    reduction as bq_reduction,
    BigQueryExprTranslator,
    STRFTIME_FORMAT_FUNCTIONS as BQ_STRFTIME_FORMAT_FUNCTIONS
)
from ibis.backends.base_sqlalchemy.alchemy import fixed_arity
from ibis.expr.operations import Arg, Cast, Comparison, Reduction, Strftime, ValueOp
from ibis.expr.types import BinaryValue, IntegerColumn, StringValue, NumericValue, TemporalValue
from ibis.backends.impala.compiler import ImpalaExprTranslator
from ibis.backends.pandas import client as _pandas_client
from ibis.backends.base_sqlalchemy.alchemy import AlchemyExprTranslator
from ibis.backends.base_sqlalchemy.compiler import ExprTranslator
from ibis.backends.base_sql.compiler import BaseExprTranslator
from third_party.ibis.ibis_oracle.compiler import OracleExprTranslator
from third_party.ibis.ibis_teradata.compiler import TeradataExprTranslator
from third_party.ibis.ibis_mssql.compiler import MSSQLExprTranslator
from ibis.backends.postgres.compiler import PostgreSQLExprTranslator
from ibis.backends.mysql.compiler import MySQLExprTranslator
from third_party.ibis.ibis_redshift.compiler import RedShiftExprTranslator

# avoid errors if Db2 is not installed and not needed
try:
    from third_party.ibis.ibis_DB2.compiler import DB2ExprTranslator
except Exception:
    DB2ExprTranslator = None


# from third_party.ibis.ibis_snowflake.compiler import SnowflakeExprTranslator
# from third_party.ibis.ibis_oracle.compiler import OracleExprTranslator <<<<<< DB2

class BitXor(Reduction):
    """Aggregate bitwise XOR operation."""

    arg = Arg(rlz.column(rlz.integer))
    where = Arg(rlz.boolean, default=None)
    output_type = rlz.scalar_like("arg")


class Hash(ValueOp):
    arg = Arg(rlz.any)
    how = Arg(rlz.isin({"fnv", "farm_fingerprint"}))
    output_type = rlz.shape_like("arg", dt.int64)


class HashBytes(ValueOp):
    arg = Arg(rlz.one_of([rlz.value(dt.string), rlz.value(dt.binary)]))
    how = Arg(rlz.isin({"sha256", "farm_fingerprint"}))
    output_type = rlz.shape_like("arg", "binary")

class ToChar(ValueOp):
    arg = Arg(rlz.one_of([rlz.value(dt.Decimal), rlz.value(dt.float64), rlz.value(dt.Date), rlz.value(dt.Time), rlz.value(dt.Timestamp)]))
    fmt = Arg(rlz.string)
    output_type = rlz.shape_like("arg", dt.string)

class RawSQL(Comparison):
    pass


def compile_hash(numeric_value, how):
    return Hash(numeric_value, how=how).to_expr()


def compile_hash(binary_value, how):
    return Hash(binary_value, how=how).to_expr()


def format_hash_bigquery(translator, expr):
    op = expr.op()
    arg, how = op.args

    arg_formatted = translator.translate(arg)

    if how == "farm_fingerprint":
        return f"farm_fingerprint({arg_formatted})"
    else:
        raise NotImplementedError(how)


def compile_hashbytes(binary_value, how):
    return HashBytes(binary_value, how=how).to_expr()


def compile_to_char(numeric_value, fmt):
    return ToChar(numeric_value, fmt=fmt).to_expr()


def format_hash_bigquery(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    if how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({compiled_arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")


def format_hashbytes_bigquery(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    if how == "sha256":
        return f"TO_HEX(SHA256({compiled_arg}))"
    elif how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({compiled_arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")


def strftime_bigquery(translator, expr):
    """Timestamp formatting. Copied from bigquery_ibis in order to inject TIMESTAMP cast"""
    arg, format_string = expr.op().args
    arg_type = arg.type()
    strftime_format_func_name = BQ_STRFTIME_FORMAT_FUNCTIONS[type(arg_type)]
    fmt_string = translator.translate(format_string)
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

def strftime_mysql(translator, expr):
    arg, format_string = expr.op().args 
    arg_formatted = translator.translate(arg)
    arg_type = arg.type()
    fmt_string = translator.translate(format_string)
    if isinstance(arg_type, dt.Timestamp):
        fmt_string = "%Y-%m-%d %H:%i:%S"
    return sa.func.date_format(arg_formatted, fmt_string)


def format_hashbytes_teradata(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    if how == "sha256":
        return f"rtrim(hash_sha256({compiled_arg}))"
    elif how == "sha512":
        return f"rtrim(hash_sha512({compiled_arg}))"
    elif how == "md5":
        return f"rtrim(hash_md5({compiled_arg}))"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")

def format_hashbytes_hive(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    if how == "sha256":
        return f"sha2({compiled_arg}, 256)"
    elif how == "md5":
        return f"md5({compiled_arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")

def format_hashbytes_alchemy(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    if how == "sha256":
        return f"sha2({compiled_arg}, 256)"
    elif how == "md5":
        return f"md5({compiled_arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {how}")

def format_hashbytes_base(translator, expr):
    arg, how  = expr.op().args
    compiled_arg = translator.translate(arg)
    return f"sha2({compiled_arg}, 256)"

def compile_raw_sql(table, sql):
    op = RawSQL(table[table.columns[0]].cast(dt.string), ibis.literal(sql))
    return op.to_expr()


def format_raw_sql(translator, expr):
    op = expr.op()
    rand_col, raw_sql = op.args
    return raw_sql.op().args[0]


def sa_format_raw_sql(translator, expr):
    op = expr.op()
    rand_col, raw_sql = op.args
    return sa.text(raw_sql.op().args[0])

def sa_format_hashbytes_mssql(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    hash_func = sa.func.hashbytes(sa.sql.literal_column("'SHA2_256'"), compiled_arg)
    hash_to_string = sa.func.convert(sa.sql.literal_column('CHAR(64)'), hash_func, sa.sql.literal_column('2'))
    return sa.func.lower(hash_to_string)

def sa_format_hashbytes_oracle(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    hash_func = sa.func.standard_hash(compiled_arg, sa.sql.literal_column("'SHA256'"))
    return sa.func.lower(hash_func)

def sa_format_hashbytes_mysql(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    hash_func = sa.func.sha2(compiled_arg, sa.sql.literal_column("'256'"))
    return hash_func

def sa_format_hashbytes_db2(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    hashfunc = sa.func.hash(compiled_arg,sa.sql.literal_column("2"))
    hex = sa.func.hex(hashfunc)
    return sa.func.lower(hex)

def sa_format_hashbytes_redshift(translator, expr):
    arg, how  = expr.op().args
    compiled_arg = translator.translate(arg)
    return sa.sql.literal_column(f"sha2({compiled_arg}, 256)")

def sa_format_hashbytes_postgres(translator, expr):
    arg, how = expr.op().args
    compiled_arg = translator.translate(arg)
    convert = sa.func.convert_to(compiled_arg, sa.sql.literal_column("'UTF8'"))
    hash_func = sa.func.sha256(convert)
    return sa.func.encode(hash_func, sa.sql.literal_column("'hex'"))

def sa_format_to_char(translator, expr):
    arg, fmt = expr.op().args
    compiled_arg = translator.translate(arg)
    compiled_fmt = translator.translate(fmt)
    return sa.func.to_char(compiled_arg, compiled_fmt)

def sa_format_to_stringjoin(translator, expr):
    sep, elements = expr.op().args
    return sa.func.concat_ws(translator.translate(sep), *map(translator.translate, elements))


def sa_cast_postgres(t, expr):
    arg, typ = expr.op().args

    sa_arg = t.translate(arg)
    sa_type = t.get_sqla_type(typ)

    # Specialize going from an integer type to a timestamp
    if isinstance(arg.type(), dt.Integer) and isinstance(sa_type, sa.DateTime):
        return sa.func.timezone('UTC', sa.func.to_timestamp(sa_arg))

    # Specialize going from numeric(p,s>0) to string
    if isinstance(arg.type(), dt.Decimal) and arg.type().scale > 0 and typ.equals(dt.string):
        # When casting a number to string PostgreSQL includes the full scale, e.g.:
        #   SELECT CAST(CAST(100 AS DECIMAL(5,2)) AS VARCHAR(10));
        #     100.00
        # This doesn't match most engines which would return "100".
        # Using to_char() function instead of cast to return a more typical value.
        # We've wrapped to_char in rtrim(".") due to whole numbers having a trailing ".".
        # Would have liked to use trim_scale but this is only available in PostgreSQL 13+
        #     return (sa.cast(sa.func.trim_scale(sa_arg), sa_type))
        precision = arg.type().precision or 38
        fmt = "FM" + ("9" * (precision - arg.type().scale)) + "." + ("9" * arg.type().scale)
        return sa.func.rtrim(sa.func.to_char(sa_arg, fmt), ".")

    if arg.type().equals(dt.binary) and typ.equals(dt.string):
        return sa.func.encode(sa_arg, 'escape')

    if typ.equals(dt.binary):
        # Decode yields a column of memoryview which is annoying to deal with
        # in pandas. CAST(expr AS BYTEA) is correct and returns byte strings.
        return sa.cast(sa_arg, sa.LargeBinary())

    return sa.cast(sa_arg, sa_type)


_pandas_client._inferable_pandas_dtypes["floating"] = _pandas_client.dt.float64
IntegerColumn.bit_xor = ibis.expr.api._agg_function("bit_xor", BitXor, True)
BinaryValue.hash = compile_hash
StringValue.hash = compile_hash
BinaryValue.hashbytes = compile_hashbytes
StringValue.hashbytes = compile_hashbytes
NumericValue.to_char = compile_to_char
TemporalValue.to_char = compile_to_char
BigQueryExprTranslator._registry[BitXor] = bq_reduction("BIT_XOR")
BigQueryExprTranslator._registry[Hash] = format_hash_bigquery
BigQueryExprTranslator._registry[HashBytes] = format_hashbytes_bigquery
BigQueryExprTranslator._registry[RawSQL] = format_raw_sql
BigQueryExprTranslator._registry[Strftime] = strftime_bigquery
MySQLExprTranslator._registry[Strftime] = strftime_mysql
AlchemyExprTranslator._registry[RawSQL] = format_raw_sql
AlchemyExprTranslator._registry[HashBytes] = format_hashbytes_alchemy
MSSQLExprTranslator._registry[HashBytes] = sa_format_hashbytes_mssql
MSSQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
BaseExprTranslator._registry[RawSQL] = format_raw_sql
BaseExprTranslator._registry[HashBytes] = format_hashbytes_base
ImpalaExprTranslator._registry[RawSQL] = format_raw_sql
ImpalaExprTranslator._registry[HashBytes] = format_hashbytes_hive
OracleExprTranslator._registry[RawSQL] = sa_format_raw_sql
OracleExprTranslator._registry[HashBytes] = sa_format_hashbytes_oracle
OracleExprTranslator._registry[ToChar] = sa_format_to_char
TeradataExprTranslator._registry[RawSQL] = format_raw_sql
TeradataExprTranslator._registry[HashBytes] = format_hashbytes_teradata
PostgreSQLExprTranslator._registry[HashBytes] = sa_format_hashbytes_postgres
PostgreSQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
PostgreSQLExprTranslator._registry[ToChar] = sa_format_to_char
PostgreSQLExprTranslator._registry[Cast] = sa_cast_postgres
MySQLExprTranslator._registry[RawSQL] = sa_format_raw_sql
MySQLExprTranslator._registry[HashBytes] = sa_format_hashbytes_mysql
MySQLExprTranslator._registry[ops.IfNull] = fixed_arity(sa.func.ifnull, 2)
MySQLExprTranslator._registry[ops.StringJoin] = sa_format_to_stringjoin
RedShiftExprTranslator._registry[HashBytes] = sa_format_hashbytes_redshift

if DB2ExprTranslator: #check if Db2 driver is loaded
    DB2ExprTranslator._registry[HashBytes] = sa_format_hashbytes_db2
