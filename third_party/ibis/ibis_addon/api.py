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

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import parsy

from ibis.expr.types.generic import Value

from data_validation import consts


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


def force_cast(self, target_type: dt.DataType) -> Value:
    """New method to force cast even if data type is the same.
    Used to cast a value to itself in ComparisonFields for nuances in data types i.e. casting CHAR to VARCHAR which are both Ibis strings
    """
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


Value.force_cast = force_cast
