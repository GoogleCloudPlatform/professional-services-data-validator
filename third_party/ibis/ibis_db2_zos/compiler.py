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
from ibis.backends.base.sql.alchemy import AlchemyExprTranslator

from third_party.ibis.ibis_db2_zos.compiler import Db2Compiler, Db2ExprTranslator
from third_party.ibis.ibis_db2_zos.registry import operation_registry


class Db2zOSExprTranslator(Db2ExprTranslator):
    _registry = operation_registry.copy()
    _rewrites = AlchemyExprTranslator._rewrites.copy()
    _dialect_name = "db2"


class Db2zOSCompiler(Db2Compiler):
    translator_class = Db2zOSExprTranslator
