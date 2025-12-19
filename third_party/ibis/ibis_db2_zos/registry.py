# Copyright 2025 Google Inc.
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

import ibis.expr.operations as ops
import sqlalchemy as sa

from third_party.ibis.ibis_db2.registry import (
    operation_registry as db2_luw_operation_registry,
)

operation_registry = db2_luw_operation_registry.copy()


def _sa_ifnull(t, op):
    sa_arg = t.translate(op.arg)
    return sa.func.coalesce(sa_arg, sa.literal_column(f"'{op.ifnull_expr.value}'"))


def _sa_whitespace_rstrip(t, op):
    # The full string.whitespace second parameter was throwing an error on Db2 z/OS:
    #   the data type, length or value of the argument for the parameter in position "2" of routine "RTRIM" is incorrect
    # Therefore we've removed it below and let Db2 exclude default space character.
    sa_arg = t.translate(op.arg)
    return sa.func.rtrim(sa_arg, sa.literal_column("' \t\r'"))
    # return sa.func.rtrim(sa_arg, " \t\r")


operation_registry[ops.IfNull] = _sa_ifnull
operation_registry[ops.RStrip] = _sa_whitespace_rstrip
