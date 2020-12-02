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

import pandas
import ibis.pandas

from data_validation import clients


TABLE_NAME = "my_table"
DATA = [{"a": 1, "b": 2}]
TABLES_RESULT = {".my_table": {"schema_name": None, "table_name": "my_table"}}


def _get_pandas_client():
    df = pandas.DataFrame(DATA)
    pandas_client = ibis.pandas.connect({TABLE_NAME: df})

    return pandas_client


def test_get_all_tables():
    """Test get all tables util."""
    client = _get_pandas_client()
    all_tables = clients.get_all_tables(client)

    assert all_tables == TABLES_RESULT
