# Copyright 2024 Google LLC
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

try:
    from ibis.backends.mysql.datatypes import SqlglotMySQLType
    import ibis.expr.datatypes as dt

    @classmethod
    def _from_sqlglot_datetime_patched(cls, *args, **kwargs) -> dt.Timestamp:
        return dt.Timestamp(nullable=cls.default_nullable)

    @classmethod
    def _from_sqlglot_timestamp_patched(cls, *args, **kwargs) -> dt.Timestamp:
        return dt.Timestamp(timezone="UTC", nullable=cls.default_nullable)

    SqlglotMySQLType._from_sqlglot_DATETIME = _from_sqlglot_datetime_patched
    SqlglotMySQLType._from_sqlglot_TIMESTAMP = _from_sqlglot_timestamp_patched
except Exception:
    pass
