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

from ibis.backends.mysql import Backend as MySQLBackend


def _list_primary_key_columns(self, database: str, table: str) -> list:
    """Return a list of primary key column names."""
    # No true binds in MySQL:
    # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html
    list_pk_col_sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND COLUMN_KEY = 'PRI'
        """
    with self.begin() as con:
        result = con.exec_driver_sql(list_pk_col_sql, parameters=(database, table))
        return [_[0] for _ in result.cursor.fetchall()]


MySQLBackend.list_primary_key_columns = _list_primary_key_columns
