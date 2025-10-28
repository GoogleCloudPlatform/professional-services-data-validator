# Copyright 2024 Google Inc.
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

"""
ClickHouse-specific patches for ibis to handle database-specific SQL syntax.

This module patches ibis's ClickHouse compiler to support ClickHouse-specific
SQL features that sqlglot doesn't natively parse, such as:
- ARRAY JOIN (and variants: ARRAY INNER JOIN, ARRAY LEFT JOIN)
- FINAL modifier
- GLOBAL JOIN modifiers

The patches are applied automatically when this module is imported.
"""

import sqlglot as sg
from sqlglot import exp
import ibis.expr.operations as ops
from ibis.backends.clickhouse.compiler import relations

# ClickHouse-specific keywords that sqlglot doesn't support
CLICKHOUSE_SPECIFIC_KEYWORDS = {
    "ARRAY JOIN",
    "ARRAY INNER JOIN",
    "ARRAY LEFT JOIN",
    "FINAL",
    "GLOBAL JOIN",
    "GLOBAL INNER JOIN",
    "GLOBAL LEFT JOIN",
    "GLOBAL RIGHT JOIN",
    "GLOBAL FULL JOIN",
}


@relations.translate_rel.register(ops.SQLQueryResult)
def _query_clickhouse_patched(op: ops.SQLQueryResult, *, aliases, **_):
    """
    Patched version of _query that handles ClickHouse-specific syntax.

    For queries containing ClickHouse-specific keywords that sqlglot cannot parse,
    we wrap them using sqlglot.exp.Command which preserves the raw SQL without
    attempting to parse it. For standard SQL, we use the original parsing approach.

    This allows DVT to validate queries with ClickHouse-specific features like
    ARRAY JOIN, which are commonly used for unnesting arrays in ClickHouse.

    Parameters
    ----------
    op : ops.SQLQueryResult
        The SQL query operation to translate
    aliases : dict
        Mapping of operations to their aliases
    **_
        Additional keyword arguments (unused)

    Returns
    -------
    sqlglot.expressions.Subquery
        A subquery expression wrapping the SQL query
    """
    query_upper = op.query.upper()

    # Check if query contains ClickHouse-specific syntax that sqlglot can't parse
    has_clickhouse_syntax = any(
        kw in query_upper for kw in CLICKHOUSE_SPECIFIC_KEYWORDS
    )

    if has_clickhouse_syntax:
        # Use Command to wrap raw SQL without parsing
        # This preserves ClickHouse-specific syntax that sqlglot doesn't understand
        cmd = exp.Command(this=op.query)
        return exp.Subquery(this=cmd, alias=aliases.get(op, "_"))
    else:
        # Use original parsing for standard SQL
        # This provides sqlglot's validation and optimization for standard queries
        res = sg.parse_one(op.query, read="clickhouse")
        return res.subquery(aliases.get(op, "_"))
