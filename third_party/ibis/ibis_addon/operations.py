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
import sqlalchemy

import ibis.expr.api
from ibis_bigquery.compiler import reduction as bq_reduction, BigQueryExprTranslator
import ibis.expr.datatypes as dt
from ibis.expr.operations import Arg, Comparison, Reduction, ValueOp
import ibis.expr.rules as rlz
from ibis.expr.types import BinaryValue, IntegerColumn, StringValue
from ibis.backends.impala.compiler import ImpalaExprTranslator
from ibis.backends.pandas import client as _pandas_client
from ibis.backends.base_sqlalchemy.alchemy import AlchemyExprTranslator
from ibis.backends.base_sqlalchemy.compiler import ExprTranslator
from ibis.backends.base_sql.compiler import BaseExprTranslator
from third_party.ibis.ibis_oracle.compiler import OracleExprTranslator
from third_party.ibis.ibis_teradata.compiler import TeradataExprTranslator

# from third_party.ibis.ibis_mssql.compiler import MSSQLExprTranslator # TODO figure how to add RAWSQL
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
        return f"hash_sha256({compiled_arg})"
    elif how == "sha512":
        return f"hash_sha512({compiled_arg})"
    elif how == "md5":
        return f"hash_md5({compiled_arg})"
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
    return sqlalchemy.text(raw_sql.op().args[0])


_pandas_client._inferable_pandas_dtypes["floating"] = _pandas_client.dt.float64
IntegerColumn.bit_xor = ibis.expr.api._agg_function("bit_xor", BitXor, True)
BinaryValue.hash = compile_hash
StringValue.hash = compile_hash
BinaryValue.hashbytes = compile_hashbytes
StringValue.hashbytes = compile_hashbytes
BigQueryExprTranslator._registry[BitXor] = bq_reduction("BIT_XOR")
BigQueryExprTranslator._registry[Hash] = format_hash_bigquery
BigQueryExprTranslator._registry[HashBytes] = format_hashbytes_bigquery
AlchemyExprTranslator._registry[RawSQL] = format_raw_sql
AlchemyExprTranslator._registry[HashBytes] = format_hashbytes_alchemy
BaseExprTranslator._registry[RawSQL] = format_raw_sql
BaseExprTranslator._registry[HashBytes] = format_hashbytes_base
BigQueryExprTranslator._registry[RawSQL] = format_raw_sql
ImpalaExprTranslator._registry[RawSQL] = format_raw_sql
ImpalaExprTranslator._registry[HashBytes] = format_hashbytes_hive
OracleExprTranslator._registry[RawSQL] = sa_format_raw_sql
TeradataExprTranslator._registry[RawSQL] = format_raw_sql
TeradataExprTranslator._registry[HashBytes] = format_hashbytes_teradata
