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
import logging
from typing import List

import ibis

from data_validation import clients
from data_validation.query_builder.query_builder import QueryBuilder


# Adding new data sources should be done by adding the Backend name here
RANDOM_SORT_SUPPORTS = [
    "bigquery",
    "teradata",
    "impala",
    "oracle",
    "postgres",
    "mssql",
    "db2",
    "mysql",
    "spanner",
    "redshift",
    "snowflake",
]


class RandomRowBuilder(object):
    def __init__(self, primary_keys: List[str], batch_size: int):
        """Build a RandomRowBuilder object which is ready to build a random row filter query.

        Args:
            primary_keys: A list of primary key field strings used to find random rows.
            batch_size: A max size for the number of random row values to find.
        """
        self.primary_keys = primary_keys
        self.batch_size = batch_size

    def compile(
        self,
        data_client: ibis.backends.base.BaseBackend,
        schema_name: str,
        table_name: str,
        query_builder: QueryBuilder,
    ) -> ibis.Expr:
        """Return an Ibis query object

        Args:
            data_client (IbisClient): The client used to query random rows.
            schema_name (String): The name of the schema for the given table.
            table_name (String): The name of the table to query.
        """
        table = clients.get_ibis_table(data_client, schema_name, table_name)
        compiled_filters = query_builder.compile_filter_fields(table)
        filtered_table = table.filter(compiled_filters) if compiled_filters else table
        randomly_sorted_table = self.maybe_add_random_sort(data_client, filtered_table)

        return randomly_sorted_table

    def compile_custom_query(
        self, data_client: ibis.backends.base.BaseBackend, query: str
    ) -> ibis.Expr:
        """Return an Ibis query object for a given query.

        Args:
            data_client (IbisClient): The client used to query random rows.
            query (String): Custom query provided by user.
        """
        table = clients.get_ibis_query(data_client, query)
        randomly_sorted_table = self.maybe_add_random_sort(data_client, table)
        query = randomly_sorted_table.limit(self.batch_size)[self.primary_keys]

        return query

    def maybe_add_random_sort(
        self, data_client: ibis.backends.base.BaseBackend, table: ibis.Expr
    ) -> ibis.Expr:
        """Return a randomly sorted query if it is supported for the client."""
        if data_client.name in RANDOM_SORT_SUPPORTS:
            # Teradata 'SAMPLE' and Spanner 'TABLESAMPLE' is random by nature
            # and does not require a sort by
            if data_client.name == "teradata" or data_client.name == "spanner":
                return table[self.primary_keys].limit(self.batch_size)

            return (
                table[self.primary_keys].order_by(ibis.random()).limit(self.batch_size)
            )

        logging.warning(
            "Data Client %s Does Not Enforce Random Sort on Sample",
            data_client.name,
        )
        return table
