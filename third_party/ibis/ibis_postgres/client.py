# Copyright 2022 Google LLC
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

from __future__ import annotations
import parsy
import re
import ast
import toolz
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from ibis import util
from ibis.backends.postgres.client import PostgreSQLClient


def _get_schema_using_query(self, query: str) -> sch.Schema:
    raw_name = util.guid()
    name = self.con.dialect.identifier_preparer.quote_identifier(raw_name)
    type_info_sql = f"""\
SELECT
attname,
format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = {raw_name!r}::regclass
AND attnum > 0
AND NOT attisdropped
ORDER BY attnum
"""
    with self.con.connect() as con:
        con.execute(f"CREATE TEMPORARY VIEW {name} AS {query}")
        try:
            type_info = con.execute(type_info_sql).fetchall()
        finally:
            con.execute(f"DROP VIEW {name}")
    tuples = [(col, self._get_type(typestr)) for col, typestr in type_info]
    return sch.Schema.from_tuples(tuples)


_BRACKETS = "[]"
_STRING_REGEX = (
    """('[^\n'\\\\]*(?:\\\\.[^\n'\\\\]*)*'|"[^\n"\\\\"]*(?:\\\\.[^\n"\\\\]*)*")"""
)


def spaceless(parser):
    return SPACES.then(parser).skip(SPACES)


def spaceless_string(*strings: str):
    return spaceless(
        parsy.alt(*(parsy.string(s, transform=str.lower) for s in strings))
    )


SPACES = parsy.regex(r"\s*", re.MULTILINE)
RAW_NUMBER = parsy.decimal_digit.at_least(1).concat()
SINGLE_DIGIT = parsy.decimal_digit
PRECISION = SCALE = NUMBER = RAW_NUMBER.map(int)

LPAREN = spaceless_string("(")
RPAREN = spaceless_string(")")

LBRACKET = spaceless_string("[")
RBRACKET = spaceless_string("]")

LANGLE = spaceless_string("<")
RANGLE = spaceless_string(">")

COMMA = spaceless_string(",")
COLON = spaceless_string(":")
SEMICOLON = spaceless_string(";")

RAW_STRING = parsy.regex(_STRING_REGEX).map(ast.literal_eval)
FIELD = parsy.regex("[a-zA-Z_][a-zA-Z_0-9]*")


def _parse_numeric(
    text: str, ddp: tuple[int | None, int | None] = (None, None)
) -> dt.DataType:
    decimal = spaceless_string("decimal", "numeric").then(
        parsy.seq(LPAREN.then(PRECISION.skip(COMMA)), SCALE.skip(RPAREN))
        .optional(ddp)
        .combine(dt.Decimal)
    )

    brackets = spaceless(LBRACKET).then(spaceless(RBRACKET))

    pg_array = parsy.seq(decimal, brackets.at_least(1).map(len)).combine(
        lambda value_type, n: toolz.nth(n, toolz.iterate(dt.Array, value_type))
    )

    ty = pg_array | decimal
    return ty.parse(text)


def _get_type(self, typestr: str) -> dt.DataType:
    is_array = typestr.endswith(_BRACKETS)
    # typ = _type_mapping.get(typestr.replace(_BRACKETS, ""))
    # handle bracket length
    typestr_wob = typestr.replace(_BRACKETS, "")
    if "(" in typestr_wob:
        typestr_wo_length = (
            typestr_wob[: typestr_wob.index("(")]
            + typestr_wob[typestr_wob.index(")") + 1 :]
        )
    else:
        typestr_wo_length = typestr_wob
    typ = _type_mapping.get(typestr_wo_length)
    if typ is not None:
        return dt.Array(typ) if is_array else typ
    return _parse_numeric(typestr)


_type_mapping = {
    "bigint": dt.int64,
    "boolean": dt.boolean,
    "bytea": dt.binary,
    "character varying": dt.string,
    "character": dt.string,
    "character(1)": dt.string,
    "date": dt.date,
    "double precision": dt.float64,
    "geography": dt.geography,
    "geometry": dt.geometry,
    "inet": dt.inet,
    "integer": dt.int32,
    "interval": dt.interval,
    "json": dt.json,
    "jsonb": dt.json,
    "line": dt.linestring,
    "macaddr": dt.macaddr,
    "macaddr8": dt.macaddr,
    "numeric": dt.float64,
    "point": dt.point,
    "polygon": dt.polygon,
    "real": dt.float32,
    "smallint": dt.int16,
    "text": dt.string,
    # NB: this isn't correct because we're losing the "with time zone"
    # information (ibis doesn't have time type that is time-zone aware), but we
    # try to do _something_ here instead of failing
    "time with time zone": dt.time,
    "time without time zone": dt.time,
    "timestamp with time zone": dt.Timestamp("UTC"),
    "timestamp without time zone": dt.timestamp,
    "uuid": dt.uuid,
}

PostgreSQLClient._get_schema_using_query = _get_schema_using_query
PostgreSQLClient._get_type = _get_type
