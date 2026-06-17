# Copyright 2026 Google LLC
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

import importlib.metadata

import ibis
import pytest
from ibis.backends.bigquery.compiler import BigQueryCompiler
from ibis.backends.impala.compiler import ImpalaCompiler
from ibis.backends.mssql.compiler import MsSqlCompiler
from ibis.backends.mysql.compiler import MySQLCompiler
from ibis.backends.postgres.compiler import PostgreSQLCompiler

# Import required in order to register DVT operations.
import third_party.ibis.ibis_addon.operations  # noqa: F401
from third_party.ibis.ibis_cloud_spanner.compiler import SpannerCompiler
from third_party.ibis.ibis_redshift.compiler import RedshiftCompiler

try:
    from ibis.backends.snowflake import SnowflakeCompiler
except (ImportError, importlib.metadata.PackageNotFoundError) as exc:
    SnowflakeCompiler = None
    SNOWFLAKE_SKIP_REASON = f"Snowflake compiler unavailable: {exc}"
else:
    SNOWFLAKE_SKIP_REASON = ""

try:
    from third_party.ibis.ibis_teradata.compiler import TeradataCompiler
except ImportError as exc:
    TeradataCompiler = None
    TERADATA_SKIP_REASON = f"Teradata compiler unavailable: {exc}"
else:
    TERADATA_SKIP_REASON = ""

try:
    from third_party.ibis.ibis_oracle.compiler import OracleCompiler
except ModuleNotFoundError as exc:
    OracleCompiler = None
    ORACLE_SKIP_REASON = f"Oracle compiler unavailable: missing {exc.name}"
else:
    ORACLE_SKIP_REASON = ""

TABLE = ibis.table(
    {
        "s": "string",
        "b": "binary",
        "d": "decimal(10, 2)",
        "ts": "timestamp",
    },
    name="t",
)


def _compile(compiler_class, expr):
    return str(compiler_class().to_sql(expr))


def _assert_fragments(sql, expected_fragments, unexpected_fragments):
    for fragment in expected_fragments:
        assert fragment in sql
    for fragment in unexpected_fragments:
        assert fragment not in sql


HASHBYTES_COMPILER_CASES = [
    pytest.param(
        BigQueryCompiler,
        ["TO_HEX(SHA256(t0.`s`))"],
        [],
        id="bigquery",
    ),
    pytest.param(
        PostgreSQLCompiler,
        ["encode(sha256(convert_to(t0.s, 'UTF8')), 'hex')"],
        [],
        id="postgres",
    ),
    pytest.param(
        MySQLCompiler,
        ["sha2(t0.s, '256')"],
        [],
        id="mysql",
    ),
    pytest.param(
        MsSqlCompiler,
        ["hashbytes('SHA2_256'", "convert(VARCHAR(MAX), t0.s)"],
        [],
        id="mssql",
    ),
    pytest.param(
        ImpalaCompiler,
        ["sha2(t0.`s`, 256)"],
        [],
        id="impala",
    ),
    pytest.param(
        RedshiftCompiler,
        ["sha2(t0.s, 256)"],
        [],
        id="redshift",
    ),
    pytest.param(
        SpannerCompiler,
        ["TO_HEX(SHA256(t0.`s`))"],
        [],
        id="spanner",
    ),
    pytest.param(
        SnowflakeCompiler,
        ['sha2(t0."s")'],
        [],
        id="snowflake",
        marks=pytest.mark.skipif(
            SnowflakeCompiler is None,
            reason=SNOWFLAKE_SKIP_REASON,
        ),
    ),
    pytest.param(
        TeradataCompiler,
        ['rtrim(hash_sha256(TransUnicodeToUTF8(t0."s")))'],
        [],
        id="teradata",
        marks=pytest.mark.skipif(
            TeradataCompiler is None,
            reason=TERADATA_SKIP_REASON,
        ),
    ),
    pytest.param(
        OracleCompiler,
        ["standard_hash(convert(t0.s, 'UTF8'), 'SHA256')"],
        [],
        id="oracle",
        marks=pytest.mark.skipif(
            OracleCompiler is None,
            reason=ORACLE_SKIP_REASON,
        ),
    ),
]


BINARY_LENGTH_COMPILER_CASES = [
    pytest.param(
        BigQueryCompiler,
        ["LENGTH(t0.`b`)"],
        [],
        id="bigquery",
    ),
    pytest.param(
        PostgreSQLCompiler,
        ["length(t0.b)"],
        [],
        id="postgres",
    ),
    pytest.param(
        MySQLCompiler,
        ["length(t0.b)"],
        [],
        id="mysql",
    ),
    pytest.param(
        MsSqlCompiler,
        ["datalength(t0.b)"],
        [],
        id="mssql",
    ),
    pytest.param(
        ImpalaCompiler,
        ["length(t0.`b`)"],
        [":length"],
        id="impala",
    ),
    pytest.param(
        RedshiftCompiler,
        ["length(t0.b)"],
        [],
        id="redshift",
    ),
    pytest.param(
        SpannerCompiler,
        ["length(t0.`b`)"],
        [":length"],
        id="spanner",
    ),
    pytest.param(
        SnowflakeCompiler,
        ['length(t0."b")'],
        [],
        id="snowflake",
        marks=pytest.mark.skipif(
            SnowflakeCompiler is None,
            reason=SNOWFLAKE_SKIP_REASON,
        ),
    ),
    pytest.param(
        TeradataCompiler,
        ['length(t0."b")'],
        [":length"],
        id="teradata",
        marks=pytest.mark.skipif(
            TeradataCompiler is None,
            reason=TERADATA_SKIP_REASON,
        ),
    ),
    pytest.param(
        OracleCompiler,
        ["dbms_lob.getlength(t0.b)"],
        [],
        id="oracle",
        marks=pytest.mark.skipif(
            OracleCompiler is None,
            reason=ORACLE_SKIP_REASON,
        ),
    ),
]


@pytest.mark.parametrize(
    "compiler_class,expected_fragments,unexpected_fragments",
    HASHBYTES_COMPILER_CASES,
)
def test_hashbytes_compiler_matrix(
    compiler_class, expected_fragments, unexpected_fragments
):
    sql = _compile(compiler_class, TABLE.s.hashbytes().name("h"))

    _assert_fragments(sql, expected_fragments, unexpected_fragments)


@pytest.mark.parametrize(
    "compiler_class,expected_fragments,unexpected_fragments",
    BINARY_LENGTH_COMPILER_CASES,
)
def test_binary_length_compiler_matrix(
    compiler_class, expected_fragments, unexpected_fragments
):
    sql = _compile(compiler_class, TABLE.b.byte_length().name("len"))

    _assert_fragments(sql, expected_fragments, unexpected_fragments)


def test_bigquery_epoch_seconds_compiles_timestamp_cast():
    sql = _compile(BigQueryCompiler, TABLE.ts.epoch_seconds().name("epoch"))

    assert "UNIX_SECONDS(CAST(t0.`ts` AS TIMESTAMP))" in sql


def test_postgres_decimal_string_cast_compiles_to_char():
    sql = _compile(PostgreSQLCompiler, TABLE.d.cast("string").name("dstr"))

    assert "rtrim(to_char(t0.d, :to_char_1), :rtrim_1)" in sql
