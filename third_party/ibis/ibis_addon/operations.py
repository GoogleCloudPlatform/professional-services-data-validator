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
extended it's own registry.  Eventually this can potentially be pushed to
Ibis as an override, though it would not apply for Pandas and other
non-textual languages.
"""

import ibis
import sqlalchemy as sa

import ibis.expr.api
import ibis.expr.datatypes as dt
import ibis.expr.rules as rlz

from data_validation.clients import _raise_missing_client_error
from ibis_bigquery.compiler import reduction as bq_reduction, BigQueryExprTranslator
from ibis.expr.operations import Arg, Comparison, Reduction, ValueOp
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
