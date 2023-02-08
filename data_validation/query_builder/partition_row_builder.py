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
        partition_key: str,
        data_client: ibis.client,
        schema_name: str,
        table_name: str,
        query_builder: QueryBuilder,
    ) -> None:
        """Build a PartitionRowBuilder object which is ready to build a partition row filter query.

        Args:
            partition_key (str): Partition key used to generate Partitions.
            data_client (IbisClient): The client used to query random rows.
            schema_name (String): The name of the schema for the given table.
            table_name (String): The name of the table to query.
            query_builder (QueryBuilder): QueryBuilder object.
        """
        self.partition_key = partition_key
        self.query = self._compile_query(
            data_client, schema_name, table_name, query_builder
        )

    def _compile_query(
        self,
        data_client: ibis.client,
        schema_name: str,
        table_name: str,
        query_builder: QueryBuilder,
    ) -> ibis.Expr:
        """Return an Ibis query object

        Args:
            data_client (IbisClient): The client used to query random rows.
            schema_name (String): The name of the schema for the given table.
            table_name (String): The name of the table to query.
            query_builder (QueryBuilder): QueryBuilder object.
        """
        table = clients.get_ibis_table(data_client, schema_name, table_name)
        compiled_filters = query_builder.compile_filter_fields(table)
        filtered_table = table.filter(compiled_filters) if compiled_filters else table
        return filtered_table

    def get_max_query(self) -> ibis.Expr:
        """Return an Ibis query object to get Max of Primary Key column"""
        return self.query[self.partition_key].max()

    def get_min_query(self) -> ibis.Expr:
        """Return an Ibis query object to get Min of Primary Key column"""
        return self.query[self.partition_key].min()

    def get_count_query(self) -> ibis.Expr:
        """Return an Ibis query object to get count of Primary Key column"""
        return self.query[self.partition_key].count()
