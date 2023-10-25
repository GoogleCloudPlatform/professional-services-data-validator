# Copyright 2023 Google LLC
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

from ibis.backends.postgres.compiler import PostgreSQLExprTranslator
from ibis.backends.base.sql.alchemy import (
    AlchemyCompiler,
    AlchemyExprTranslator,
)
from ibis.expr.operations import StringJoin
import sqlalchemy as sa


def _string_join(t, op):
    sep, elements = op.args
    columns = [str(col.name) for col in map(t.translate, elements)]
    return sa.sql.literal_column(" || ".join(columns))


_operation_registry = PostgreSQLExprTranslator._registry.copy()
_operation_registry.update(
    {
        StringJoin: _string_join,
    }
)


class RedShiftExprTranslator(AlchemyExprTranslator):
    _registry = _operation_registry
    _has_reduction_filter_syntax = True
    _dialect_name = "postgresql"

    # it does support it, but we can't use it because of support for pivot
    supports_unnest_in_select = False


rewrites = PostgreSQLExprTranslator._rewrites


class RedshiftCompiler(AlchemyCompiler):
    translator_class = RedShiftExprTranslator
