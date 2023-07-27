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

from ibis.expr.types.generic import Value


def cast(self, target_type: dt.DataType) -> Value:
    """Override ibis.expr.api's cast method.
    This allows for Timestamp-typed columns to be cast to Timestamp, since Ibis interprets some similar but non-equivalent types (eg. DateTime) to Timestamp (GitHub issue #451).
    """
    # validate
    op = ops.Cast(self, to=target_type)

    if op.to == self.type() and not op.to.is_timestamp():
        # noop case if passed type is the same
        return self

    if op.to.is_geospatial():
        from_geotype = self.type().geotype or 'geometry'
        to_geotype = op.to.geotype
        if from_geotype == to_geotype:
            return self

    return op.to_expr()
