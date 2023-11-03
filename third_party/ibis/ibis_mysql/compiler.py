# Copyright 2020 Google LLC
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
from ibis.backends.base.sql.alchemy import AlchemyCompiler
from ibis.backends.mysql import Backend as MySqlBackend
from ibis.backends.mysql.compiler import MySQLExprTranslator

from third_party.ibis.ibis_mysql.base_sql_compiler.select_builder import (
    NewSelectBuilder,
)


# We extend the default SelectBuilder here https://github.com/ibis-project/ibis/blob/5.1.0/ibis/backends/base/sql/compiler/select_builder.py#L34
# to update the code so that we do not use CTEs for MySQL, which are unsupported in MySQL versions < 8.0
class NewAlchemySelectBuilder(NewSelectBuilder):
    def _convert_group_by(self, exprs):
        return exprs


class NewAlchemyCompiler(AlchemyCompiler):
    select_builder_class = NewAlchemySelectBuilder


class MySQLCompilerNew(NewAlchemyCompiler):
    translator_class = MySQLExprTranslator
    support_values_syntax_in_select = False


MySqlBackend.compiler = MySQLCompilerNew
