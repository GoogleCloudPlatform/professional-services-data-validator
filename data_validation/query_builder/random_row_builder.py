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

import ibis
from typing import List

from data_validation import clients


""" The QueryBuilder for retreiving random row values to filter against."""


class RandomRowBuilder(object):
    def __init__(self, primary_keys: List[str], batch_size: int):
        """ Build a RandomRowBuilder objct which is ready to build a random row filter query.

    Args:
        primary_keys: A list of primary key field strings used to find random rows.
        batch_size: A max size for the number of random row values to find.
    """
        self.primary_keys = primary_keys
        self.batch_size = batch_size

    def compile(
        self, data_client: ibis.client, schema_name: str, table_name: str
    ) -> ibis.Expr:
        """Return an Ibis query object

    Args:
        data_client (IbisClient): The client used to query random rows.
        schema_name (String): The name of the schema for the given table.
        table_name (String): The name of the table to query.
    """
        table = clients.get_ibis_table(data_client, schema_name, table_name)
        query = table[self.primary_keys].limit(self.batch_size)

        return query
