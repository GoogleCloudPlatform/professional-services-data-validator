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

import ibis
import pytest
from ibis.backends.bigquery.compiler import BigQueryCompiler
from ibis.backends.impala.compiler import ImpalaCompiler
from ibis.backends.mssql.compiler import MsSqlCompiler
from ibis.backends.mysql.compiler import MySQLCompiler
from ibis.backends.postgres.compiler import PostgreSQLCompiler

# Import required in order to register DVT operations.
import third_party.ibis.ibis_addon.operations  # noqa: F401
from third_party.ibis.ibis_redshift.compiler import RedshiftCompiler

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


COMPILER_CASES = [
    pytest.param(
        BigQueryCompiler,
        lambda: TABLE.s.hashbytes().name("h"),
        ["TO_HEX(SHA256(t0.`s`))"],
        [],
        id="bigquery-hashbytes",
    ),
    pytest.param(
        BigQueryCompiler,
        lambda: TABLE.b.byte_length().name("len"),
        ["LENGTH(t0.`b`)"],
        [],
        id="bigquery-binary-length",
    ),
    pytest.param(
        BigQueryCompiler,
        lambda: TABLE.ts.epoch_seconds().name("epoch"),
        ["UNIX_SECONDS(CAST(t0.`ts` AS TIMESTAMP))"],
        [],
        id="bigquery-epoch",
    ),
    pytest.param(
        PostgreSQLCompiler,
        lambda: TABLE.s.hashbytes().name("h"),
        ["encode(sha256(convert_to(t0.s, 'UTF8')), 'hex')"],
        [],
        id="postgres-hashbytes",
    ),
    pytest.param(
        PostgreSQLCompiler,
        lambda: TABLE.d.cast("string").name("dstr"),
        ["rtrim(to_char(t0.d"],
        [],
        id="postgres-decimal-string",
    ),
    pytest.param(
        MySQLCompiler,
        lambda: TABLE.s.hashbytes().name("h"),
        ["sha2(t0.s, '256')"],
        [],
        id="mysql-hashbytes",
    ),
    pytest.param(
        MySQLCompiler,
        lambda: TABLE.b.byte_length().name("len"),
        ["length(t0.b)"],
        [],
        id="mysql-binary-length",
    ),
    pytest.param(
        MsSqlCompiler,
        lambda: TABLE.s.hashbytes().name("h"),
        ["hashbytes('SHA2_256'", "convert(VARCHAR(MAX), t0.s)"],
        [],
        id="mssql-hashbytes",
    ),
    pytest.param(
        MsSqlCompiler,
        lambda: TABLE.b.byte_length().name("len"),
        ["datalength(t0.b)"],
        [],
        id="mssql-binary-length",
    ),
    pytest.param(
        ImpalaCompiler,
        lambda: TABLE.s.hashbytes().name("h"),
        ["sha2(t0.`s`, 256)"],
        [],
        id="impala-hashbytes",
    ),
    pytest.param(
        ImpalaCompiler,
        lambda: TABLE.b.byte_length().name("len"),
        ["length(t0.`b`)"],
        [":length"],
        id="impala-binary-length",
    ),
    pytest.param(
        RedshiftCompiler,
        lambda: TABLE.s.hashbytes().name("h"),
        ["sha2(t0.s, 256)"],
        [],
        id="redshift-hashbytes",
    ),
    pytest.param(
        RedshiftCompiler,
        lambda: TABLE.b.byte_length().name("len"),
        ["length(t0.b)"],
        [],
        id="redshift-binary-length",
    ),
    pytest.param(
        OracleCompiler,
        lambda: TABLE.s.hashbytes().name("h"),
        ["standard_hash(convert(t0.s, 'UTF8'), 'SHA256')"],
        [],
        id="oracle-hashbytes",
        marks=pytest.mark.skipif(
            OracleCompiler is None,
            reason=ORACLE_SKIP_REASON,
        ),
    ),
    pytest.param(
        OracleCompiler,
        lambda: TABLE.b.byte_length().name("len"),
        ["dbms_lob.getlength(t0.b)"],
        [],
        id="oracle-binary-length",
        marks=pytest.mark.skipif(
            OracleCompiler is None,
            reason=ORACLE_SKIP_REASON,
        ),
    ),
]


@pytest.mark.parametrize(
    "compiler_class,expr_factory,expected_fragments,unexpected_fragments",
    COMPILER_CASES,
)
def test_backend_compiler_matrix(
    compiler_class, expr_factory, expected_fragments, unexpected_fragments
):
    sql = _compile(compiler_class, expr_factory())

    for fragment in expected_fragments:
        assert fragment in sql
    for fragment in unexpected_fragments:
        assert fragment not in sql
