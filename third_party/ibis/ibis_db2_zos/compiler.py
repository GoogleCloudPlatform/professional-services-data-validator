# Copyright 2023 Google Inc.
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
import sqlalchemy as sa

from ibis.backends.base.sql.alchemy import AlchemyCompiler, AlchemyExprTranslator
from ibis.backends.base.sql.alchemy.query_builder import AlchemySelect

from third_party.ibis.ibis_db2_zos.registry import operation_registry


class Db2zOSSelect(AlchemySelect):
    def _add_limit(self, fragment):
        """Add LIMIT and OFFSET clauses to the query.

        Db2 z/OS backend does not support parameterization for LIMIT/OFFSET
        (FETCH FIRST n ROWS ONLY) clauses. We convert the integer limits
        into literal SQLAlchemy columns so they render as hardcoded values
        in the final compiled SQL string instead of generating bound parameters.
        """
        if self.limit is None:
            return fragment

        # Use sa.literal_column instead of sa.text for SQLAlchemy .limit() compatibility
        fragment = fragment.limit(sa.literal_column(str(self.limit.n)))
        if offset := self.limit.offset:
            fragment = fragment.offset(sa.literal_column(str(offset)))
        return fragment


class Db2zOSExprTranslator(AlchemyExprTranslator):
    _registry = operation_registry.copy()
    _rewrites = AlchemyExprTranslator._rewrites.copy()
    _dialect_name = "db2"


class Db2zOSCompiler(AlchemyCompiler):
    translator_class = Db2zOSExprTranslator
    select_class = Db2zOSSelect
    supports_indexed_grouping_keys = False
