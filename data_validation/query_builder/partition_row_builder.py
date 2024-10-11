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
from data_validation import clients
from data_validation.query_builder.query_builder import QueryBuilder


class PartitionRowBuilder(object):
    def __init__(
        self,
        primary_keys: [str],
        data_client: ibis.backends.base.BaseBackend,
        schema_name: str,
        table_name: str,
        custom_query: str,
        query_builder: QueryBuilder,
    ) -> None:
        """Build a PartitionRowBuilder object which is ready to build a partition row filter query.

        Args:
            primary_keys [str]: Keys used to identify a row for validation
            data_client (BaseBackend): The Backend used to query random rows.
            schema_name (String): The name of the schema for the given table.
            table_name (String): The name of the table to query.
            query_builder (QueryBuilder): QueryBuilder object.
        """
        self.primary_keys = primary_keys
        self.query = self._compile_query(
            data_client, schema_name, table_name, custom_query, query_builder
        )

    def _compile_query(
        self,
        data_client: ibis.backends.base.BaseBackend,
        schema_name: str,
        table_name: str,
        custom_query: str,
        query_builder: QueryBuilder,
    ) -> ibis.Expr:
        """Return an Ibis query object

        Args:
            data_client (BaseBackend): The Backend used to query random rows.
            schema_name (String): The name of the schema for the given table.
            table_name (String): The name of the table to query.
            custom_query (String) : Custom query provided instead of a table
            query_builder (QueryBuilder): QueryBuilder object.
        """
        if table_name:
            table = clients.get_ibis_table(data_client, schema_name, table_name)
        else:
            table = clients.get_ibis_query(data_client, custom_query)
        compiled_filters = query_builder.compile_filter_fields(table)
        filtered_table = table.filter(compiled_filters) if compiled_filters else table
        return filtered_table

    def get_count(self) -> int:
        """Return a count of rows of primary keys - they should be all distinct"""
        return self.query[self.primary_keys].count().force_cast("int64").execute()
