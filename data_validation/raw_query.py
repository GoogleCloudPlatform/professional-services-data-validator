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

from typing import TYPE_CHECKING

from sqlalchemy.engine.row import Row

from data_validation import clients, consts, state_manager

if TYPE_CHECKING:
    from argparse import Namespace


def run_raw_query_against_connection(args: "Namespace") -> list:
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


def _minimize_raw_query_output(query_output: list) -> list:
    """Reduce single column output from list(tuple(_)) to list(_)"""
    if not query_output:
        return query_output
    if isinstance(query_output[0], (list, tuple)) and len(query_output[0]) == 1:
        return [_[0] for _ in query_output]
    else:
        return query_output


def print_raw_query_output(query_output: list, format: str = consts.FORMAT_TYPE_PYTHON):
    """Print a query resultset avoiding SQLAlchemy "... (nn characters truncated) ..." behaviour.

    Args:
        query_output (list): A set of rows from a SQLAlchemy query.
        format (str): Output format:
            FORMAT_TYPE_PYTHON: Legacy format matching previous behaviour, simple print the Python object.
            FORMAT_TYPE_MINIMAL: Attempt to minimize additional noise for bare minimum output.
                                 This is useful for a specific customer who are using DVT raw queries to
                                 generate other DVT commands and don't want any escaping or extra parentheses
                                 in the output.
    """

    def row_to_tuple(row) -> tuple:
        """This prevents SQLAlchemy string truncation inside Row() objects by first converting them to a tuple."""
        if isinstance(row, (Row, list)):
            return tuple(row)
        else:
            return row

    sanitized_output = [row_to_tuple(_) for _ in query_output or []]

    if format == consts.FORMAT_TYPE_MINIMAL:
        for row in _minimize_raw_query_output(sanitized_output):
            print(row)
    else:
        print(sanitized_output)
