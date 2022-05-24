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
from ibis.expr.types import TimestampColumn


def cast(arg, target_type):
    """Override ibis.expr.api's cast method.
    This allows for Timestamp-typed columns to be cast to Timestamp, since Ibis interprets some similar but non-equivalent types (eg. DateTime) to Timestamp (GitHub issue #451).
    """
    # validate
    op = ops.Cast(arg, to=target_type)

    if op.to.equals(arg.type()) and not isinstance(arg, TimestampColumn):
        # noop case if passed type is the same
        return arg

    if isinstance(op.to, (dt.Geography, dt.Geometry)):
        from_geotype = arg.type().geotype or "geometry"
        to_geotype = op.to.geotype
        if from_geotype == to_geotype:
            return arg

    result = op.to_expr()
    if not arg.has_name():
        return result
    expr_name = "cast({}, {})".format(arg.get_name(), op.to)
    return result.name(expr_name)
