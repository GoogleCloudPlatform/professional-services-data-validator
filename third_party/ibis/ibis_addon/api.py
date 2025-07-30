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

from typing import Iterable

from ibis.backends.base.sql.alchemy.datatypes import to_sqla_type
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
from ibis.expr.types.generic import Value
import parsy
import sqlalchemy as sa

from data_validation import consts


def _uuid_string_cast(self):
    """Cast UUIDs to a standard format string. For UUIDs stored in binary columns we need to inject hyphens."""
    expr = ops.Cast(self, to="string").to_expr()
    if isinstance(self.type(), dt.Binary):
        # Inject hyphens into the hex string. As far as we know only Oracle UUIDs will follow this path.
        # I tried to use Ibis join() but couldn't get sensible SQL out of it, example:
        #   ibis.literal("-").join([expr.substr(0, 8), expr.substr(8, 4), ...])
        # So I had to resort to the less ideal expression below.
        return (
            expr.substr(0, 8)
            .concat("-")
            .concat(expr.substr(8, 4))
            .concat("-")
            .concat(expr.substr(12, 4))
            .concat("-")
            .concat(expr.substr(16, 4))
            .concat("-")
            .concat(expr.substr(20, 12))
        )
    else:
        return expr.lower()


def cast(self, target_type: dt.DataType) -> Value:
    """Override ibis.expr.api's cast method.
    This allows for Timestamp-typed columns to be cast to Timestamp, since Ibis interprets some similar but non-equivalent types (eg. DateTime) to Timestamp (GitHub issue #451).
    """

    def same_type(from_type, to_type) -> bool:
        # The data type for Non-nullable columns if prefixed with "!", this is causing deviations
        # between nullable and non-nullable columns. The second comparison below is catering for this.
        return bool(
            from_type == to_type
            or str(from_type).lstrip("!") == str(to_type).lstrip("!")
        )

    if target_type == consts.CONFIG_CAST_BOOL_STRING:
        if self.type().is_numeric():
            # Comparing numeric value with boolean.
            op = ops.SimpleCase(self, (0, 1), ("false", "true"), None)
            return op.to_expr()
        elif self.type().is_string():
            # Comparing string value with boolean.
            op = ops.SimpleCase(
                self, ("0", "1", "N", "Y"), ("false", "true", "false", "true"), None
            )
            return op.to_expr()
        else:
            # Allow a standard Cast to kick in.
            target_type = "string"
    elif target_type == "bool" and self.type().is_string():
        # Comparing string value with boolean.
        op = ops.SimpleCase(self, ("0", "1", "N", "Y"), (0, 1, 0, 1), None)
        return op.to_expr()
    elif target_type == consts.CONFIG_CAST_UUID_STRING:
        return _uuid_string_cast(self)

    op = ops.Cast(self, to=target_type)
    if same_type(op.to, self.type()) and not op.to.is_timestamp():
        # noop case if passed type is the same
        return self

    if op.to.is_geospatial():
        from_geotype = self.type().geotype or "geometry"
        to_geotype = op.to.geotype
        if from_geotype == to_geotype:
            return self

    return op.to_expr()


def force_cast(self, target_type: str) -> Value:
    """New method to force cast even if data type is the same.
    Used to cast a value to itself in ComparisonFields for nuances in data types i.e. casting CHAR to VARCHAR which are both Ibis strings
    """
    if target_type == consts.CONFIG_CAST_UUID_STRING:
        return _uuid_string_cast(self)

    # validate target type
    try:
        op = ops.Cast(self, to=target_type)
    except parsy.ParseError:
        raise SyntaxError(
            f"'{target_type}' is an invalid datatype provided to cast a ComparisonField"
        )

    if op.to.is_geospatial():
        from_geotype = self.type().geotype or "geometry"
        to_geotype = op.to.geotype
        if from_geotype == to_geotype:
            return self

    return op.to_expr()


def dvt_handle_failed_column_type_inference(
    self, table: sa.Table, nulltype_cols: Iterable[str]
) -> sa.Table:
    """Handle cases where SQLAlchemy cannot infer the column types of `table`.

    This DVT duplication of Ibis code is so we can prefix the table name with
    a schema name when required. To minimize risk we've not done this globally,
    this function is imported by specific Backends as and when needed."""

    self.inspector.reflect_table(table, table.columns)
    dialect = self.con.dialect
    quoted_name = dialect.identifier_preparer.quote(table.name)
    # Start of custom DVT code
    if table.schema:
        quoted_schema = dialect.identifier_preparer.quote(table.schema)
        quoted_name = quoted_schema + "." + quoted_name
    # End of custom DVT code

    for colname, type in self._metadata(quoted_name):
        if colname.lower() in nulltype_cols:
            # replace null types discovered by sqlalchemy with non null
            # types
            table.append_column(
                sa.Column(
                    colname.lower(),
                    to_sqla_type(dialect, type),
                    nullable=type.nullable,
                    quote=self.compiler.translator_class._quote_column_names,
                ),
                replace_existing=True,
            )
    return table


Value.force_cast = force_cast
