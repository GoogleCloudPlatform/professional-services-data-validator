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

from sqlalchemy.engine.row import Row

from data_validation import clients, state_manager


def run_raw_query_against_connection(args) -> list:
    """Return results of raw query for ad hoc usage."""
    mgr = state_manager.StateManager()
    with clients.get_data_client_ctx(mgr.get_connection_config(args.conn)) as client:
        cursor = client.raw_sql(args.query)
        res = cursor.fetchall()
        try:
            cursor.close()
        except Exception:
            pass
        return res


def print_raw_query_output(query_output: list):
    """Print a query resultset avoiding SQLAlchemy "... (nn characters truncated) ..." behaviour.

    Args:
        query_output (list): A set of rows from a SQLAlchemy query.
    """

    def row_to_str(row) -> str:
        """This prevents SQLAlchemy string truncation inside Row() objects by first converting them to a tuple."""
        if isinstance(row, Row):
            return str(tuple(row))
        else:
            return str(row)

    print([row_to_str(_) for _ in query_output or []])
