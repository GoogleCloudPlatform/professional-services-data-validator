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

import sqlalchemy as sa
from ibis.backends.base_sqlalchemy.alchemy import (
    _to_sqla_type,
    AlchemyTable,
    _AlchemyTableSet,
)
import ibis.expr.schema as sch
import ibis.expr.operations as ops

import re
import ibis.expr.datatypes as dt
from ibis.backends.base_sqlalchemy.alchemy import AlchemyClient

from typing import Iterable

_type_codes = {
    0: "DECIMAL",
    1: "TINY",
    2: "SHORT",
    3: "LONG",
    4: "FLOAT",
    5: "DOUBLE",
    6: "NULL",
    7: "TIMESTAMP",
    8: "LONGLONG",
    9: "INT24",
    10: "DATE",
    11: "TIME",
    12: "DATETIME",
    13: "YEAR",
    15: "VARCHAR",
    16: "BIT",
    245: "JSON",
    246: "NEWDECIMAL",
    247: "ENUM",
    248: "SET",
    249: "TINY_BLOB",
    250: "MEDIUM_BLOB",
    251: "LONG_BLOB",
    252: "BLOB",
    253: "VAR_STRING",
    254: "STRING",
    255: "GEOMETRY",
}

_type_mapping = {
    "DECIMAL": dt.Decimal,
    "TINY": dt.Int8,
    "SHORT": dt.Int16,
    "LONG": dt.Int32,
    "FLOAT": dt.Float32,
    "DOUBLE": dt.Float64,
    "NULL": dt.Null,
    "TIMESTAMP": lambda nullable: dt.Timestamp(timezone="UTC", nullable=nullable),
    "LONGLONG": dt.Int64,
    "INT24": dt.Int32,
    "DATE": dt.Date,
    "TIME": dt.Time,
    "DATETIME": dt.Timestamp,
    "YEAR": dt.Int8,
    "VARCHAR": dt.String,
    "JSON": dt.JSON,
    "NEWDECIMAL": dt.Decimal,
    "ENUM": dt.String,
    "SET": lambda nullable: dt.Set(dt.string, nullable=nullable),
    "TINY_BLOB": dt.Binary,
    "MEDIUM_BLOB": dt.Binary,
    "LONG_BLOB": dt.Binary,
    "BLOB": dt.Binary,
    "VAR_STRING": dt.String,
    "STRING": dt.String,
    "GEOMETRY": dt.Geometry,
}
MY_CHARSET_BIN = 63


def _schema_to_sqlalchemy_columns(schema: sch.Schema):
    return [sa.column(n, _to_sqla_type(t)) for n, t in schema.items()]


def _format_table_new(self, expr):
    ctx = self.context
    ref_expr = expr
    op = ref_op = expr.op()

    if isinstance(op, ops.SelfReference):
        ref_expr = op.table
        ref_op = ref_expr.op()

    alias = ctx.get_ref(expr)
    if isinstance(ref_op, AlchemyTable):
        result = ref_op.sqla_table
    elif isinstance(ref_op, ops.SQLQueryResult):
        columns = _schema_to_sqlalchemy_columns(ref_op.schema)
        result = sa.text(ref_op.query).columns(*columns)
    elif isinstance(ref_op, ops.UnboundTable):
        # use SQLAlchemy's TableClause and ColumnClause for unbound tables
        schema = ref_op.schema
        result = sa.table(
            ref_op.name if ref_op.name is not None else ctx.get_ref(expr),
            *(
                sa.column(n, _to_sqla_type(t))
                for n, t in zip(schema.names, schema.types)
            ),
        )
    else:
        # A subquery
        if ctx.is_extracted(ref_expr):
            # Was put elsewhere, e.g. WITH block, we just need to grab
            # its alias
            alias = ctx.get_ref(expr)

            # hack
            if isinstance(op, ops.SelfReference):

                table = ctx.get_table(ref_expr)
                self_ref = table.alias(alias)
                ctx.set_table(expr, self_ref)
                return self_ref
            else:
                return ctx.get_table(expr)

        result = ctx.get_compiled_expr(expr)
        alias = ctx.get_ref(expr)

    result = result.alias(alias)
    ctx.set_table(expr, result)
    return result

class _FieldFlags:
    """Flags used to disambiguate field types.
    Gaps in the flag numbers are because we do not map in flags that are
    of no use in determining the field's type, such as whether the field
    is a primary key or not.
    """

    UNSIGNED = 1 << 5
    SET = 1 << 11
    NUM = 1 << 15

    __slots__ = ("value",)

    def __init__(self, value: int) -> None:
        self.value = value

    @property
    def is_unsigned(self) -> bool:
        return (self.UNSIGNED & self.value) != 0

    @property
    def is_set(self) -> bool:
        return (self.SET & self.value) != 0

    @property
    def is_num(self) -> bool:
        return (self.NUM & self.value) != 0


def _decimal_length_to_precision(*, length: int, scale: int, is_unsigned: bool) -> int:
    return length - (scale > 0) - (not (is_unsigned or not length))


def _type_from_cursor_info(descr, field) -> dt.DataType:
    """Construct an ibis type from MySQL field descr and field result metadata.
    This method is complex because the MySQL protocol is complex.
    Types are not encoded in a self contained way, meaning you need
    multiple pieces of information coming from the result set metadata to
    determine the most precise type for a field. Even then, the decoding is
    not high fidelity in some cases: UUIDs for example are decoded as
    strings, because the protocol does not appear to preserve the logical
    type, only the physical type.
    """
    from pymysql.connections import TEXT_TYPES

    _, type_code, _, _, field_length, scale, _ = descr
    flags = _FieldFlags(field.flags)
    typename = _type_codes.get(type_code)
    if typename is None:
        raise NotImplementedError(f"MySQL type code {type_code:d} is not supported")

    if typename in ("DECIMAL", "NEWDECIMAL"):
        precision = _decimal_length_to_precision(
            length=field_length,
            scale=scale,
            is_unsigned=flags.is_unsigned,
        )
        typ = partial(_type_mapping[typename], precision=precision, scale=scale)
    elif typename == "BIT":
        if field_length <= 8:
            typ = dt.int8
        elif field_length <= 16:
            typ = dt.int16
        elif field_length <= 32:
            typ = dt.int32
        elif field_length <= 64:
            typ = dt.int64
        else:
            raise AssertionError('invalid field length for BIT type')
    elif flags.is_set:
        # sets are limited to strings
        typ = dt.Set(dt.string)
    elif flags.is_unsigned and flags.is_num:
        typ = getattr(dt, f"U{typ.__name__}")
    elif type_code in TEXT_TYPES:
        # binary text
        if field.charsetnr == MY_CHARSET_BIN:
            typ = dt.Binary
        else:
            typ = dt.String
    else:
        typ = _type_mapping[typename]

    # projection columns are always nullable
    return typ(nullable=True)


def _metadata(self, query: str):
    if (
        re.search(r"^\s*SELECT\s", query, flags=re.MULTILINE | re.IGNORECASE)
        is not None
    ):
        query = f"({query})"

    with self.begin() as con:
        result = con.execute(f"SELECT * FROM {query} _ LIMIT 0")
        cursor = result.cursor
        yield from (
            (field.name, _type_from_cursor_info(descr, field))
            for descr, field in zip(cursor.description, cursor._result.fields)
        )

def _get_schema_using_query(self, query: str) -> sch.Schema:
    """Return an ibis Schema from a backend-specific SQL string."""
    return sch.Schema.from_tuples(self._metadata(query))

AlchemyClient._get_schema_using_query = _get_schema_using_query
AlchemyClient._metadata = _metadata
_AlchemyTableSet._format_table = _format_table_new