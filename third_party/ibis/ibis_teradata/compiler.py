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
from ibis.backends.base.sql.compiler import (
    Compiler,
    ExprTranslator,
    TableSetFormatter,
    Select,
)
from ibis.backends.base.sql.registry import identifiers
from third_party.ibis.ibis_teradata.registry import _operation_registry


class TeradataTableSetFormatter(TableSetFormatter):
    @classmethod
    def _quote_identifier(self, name, quotechar='"', force=False):
        if force or name.count(" ") or name in identifiers.base_identifiers:
            return "{0}{1}{0}".format(quotechar, name)
        else:
            return name


class TeradataExprTranslator(ExprTranslator):
    _registry = _operation_registry.copy()

    def name(self, translated, name, force=True):
        quoted_name = TeradataTableSetFormatter._quote_identifier(name, force=force)
        return "{} AS {}".format(translated, quoted_name)


rewrites = TeradataExprTranslator.rewrites


class TeradataSelect(Select):
    def format_limit(self):
        if not self.limit:
            return None

        limit_sql = f"SAMPLE {self.limit.n}"
        return limit_sql


class TeradataCompiler(Compiler):
    translator_class = TeradataExprTranslator
    table_set_formatter_class = TeradataTableSetFormatter
    select_class = TeradataSelect
