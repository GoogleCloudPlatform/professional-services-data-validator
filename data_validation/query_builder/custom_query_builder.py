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

""" The QueryBuilder for building custom query row|column validation."""


class CustomQueryBuilder(object):
    def __init__(self):
        """Build a CustomQueryBuilder object which is ready to build a custom query row|column validation nested query."""

    def get_aggregation_query(self, agg_type, column_name):
        """Return aggregation query"""
        aggregation_query = ""
        if column_name is None:
            aggregation_query = agg_type + "(*) as " + agg_type + ","
        else:
            aggregation_query = (
                agg_type
                + "("
                + column_name
                + ") as "
                + agg_type
                + "__"
                + column_name
                + ","
            )
        return aggregation_query

    def get_wrapper_aggregation_query(self, aggregate_query, base_query):
        """Return wrapper aggregation query"""

        return (
            aggregate_query[: len(aggregate_query) - 1]
            + " FROM ("
            + base_query
            + ") as base_query"
        )

    def compile_custom_query(self, input_query, client_config):
        """Returns the nested sql query calculated from the input query
            by adding calculated fields.
        Args:
            input_query (InputQuery): User provided sql query
        """
        base_tbl_expr = self.get_table_expression(input_query, client_config)
        base_df = self.get_data_frame(base_tbl_expr, client_config)
        base_df_columns = self.compile_df_fields(base_df)
        calculated_columns = self.get_calculated_columns(base_df_columns)
        cast_query = self.compile_cast_df_fields(
            calculated_columns, input_query, base_df
        )
        ifnull_query = self.compile_ifnull_df_fields(
            calculated_columns, cast_query, client_config
        )
        rstrip_query = self.compile_rstrip_df_fields(calculated_columns, ifnull_query)
        upper_query = self.compile_upper_df_fields(calculated_columns, rstrip_query)
        concat_query = self.compile_concat_df_fields(
            calculated_columns, upper_query, client_config
        )
        sha2_query = self.compile_sha2_df_fields(concat_query, client_config)
        return sha2_query

    def get_table_expression(self, input_query, client_config):
        """Returns the ibis table expression for the input query."""
        return client_config["data_client"].sql(input_query)

    def get_data_frame(self, base_tbl_expr, client_config):
        """Returns the data frame for the table expression."""
        return client_config["data_client"].execute(base_tbl_expr)

    def compile_df_fields(self, data_frame):
        """Returns the list of columns in the dataframe.
        Args:
            data_frame (DataFrame): Pandas Dataframe
        """
        return list(data_frame.columns.values)

    def get_calculated_columns(self, df_columns):
        """Returns the dictionary containing the calculated fields."""

        calculated_columns = {}
        calculated_columns["columns"] = df_columns
        calculated_columns["cast"] = []
        for column in df_columns:
            current_column = "cast__" + column
            calculated_columns["cast"].append(current_column)

        calculated_columns["ifnull"] = []
        for column in calculated_columns["cast"]:
            current_column = "ifnull__" + column
            calculated_columns["ifnull"].append(current_column)

        calculated_columns["rstrip"] = []
        for column in calculated_columns["ifnull"]:
            current_column = "rstrip__" + column
            calculated_columns["rstrip"].append(current_column)

        calculated_columns["upper"] = []
        for column in calculated_columns["rstrip"]:
            current_column = "upper__" + column
            calculated_columns["upper"].append(current_column)

        return calculated_columns

    def compile_cast_df_fields(self, calculated_columns, input_query, data_frame):
        """Returns the wrapper cast query for the input query."""

        query = "SELECT "
        for column in calculated_columns["cast"]:
            df_column = column[len("cast__") :]
            df_column_dtype = data_frame[df_column].dtype.name
            if df_column_dtype != "object" and df_column_dtype != "string":
                query = (
                    query + "CAST(" + df_column + " AS string)" + " AS " + column + ","
                )
            else:
                query += df_column + " AS " + column + ","

        query = query[: len(query) - 1] + " FROM (" + input_query + ") AS base_query"
        return query

    def compile_ifnull_df_fields(self, calculated_columns, cast_query, client_config):
        """Returns the wrapper ifnull query for the input cast_query."""

        client = client_config["data_client"]._source_type
        if client == "Impala":
            operation = "COALESCE"
        elif client == "BigQuery":
            operation = "IFNULL"
        query = "SELECT "
        for column in calculated_columns["ifnull"]:
            query = (
                query
                + operation
                + "("
                + column[len("ifnull__") :]
                + ",'DEFAULT_REPLACEMENT_STRING')"
                + " AS "
                + column
                + ","
            )
        query = query[: len(query) - 1] + " FROM (" + cast_query + ") AS cast_query"
        return query

    def compile_rstrip_df_fields(self, calculated_columns, ifnull_query):
        """Returns the wrapper rstrip query for the input ifnull_query."""

        operation = "RTRIM"
        query = "SELECT "
        for column in calculated_columns["rstrip"]:
            query = (
                query
                + operation
                + "("
                + column[len("rstrip__") :]
                + ")"
                + " AS "
                + column
                + ","
            )
        query = query[: len(query) - 1] + " FROM (" + ifnull_query + ") AS ifnull_query"
        return query

    def compile_upper_df_fields(self, calculated_columns, rstrip_query):
        """Returns the wrapper upper query for the input rstrip_query."""

        query = "SELECT "
        for column in calculated_columns["upper"]:
            query = (
                query
                + "UPPER("
                + column[len("upper__") :]
                + ")"
                + " AS "
                + column
                + ","
            )
        query = query[: len(query) - 1] + " FROM (" + rstrip_query + ") AS rstrip_query"
        return query

    def compile_concat_df_fields(self, calculated_columns, upper_query, client_config):
        """Returns the wrapper concat query for the input upper_query."""

        client = client_config["data_client"]._source_type
        if client == "Impala":
            operation = "CONCAT_WS"
            query = "SELECT  " + operation + "(',',"
            for column in calculated_columns["upper"]:
                query += column + ","
            query = (
                query[: len(query) - 1]
                + ") AS concat__all FROM("
                + upper_query
                + ") AS upper_query"
            )
        elif client == "BigQuery":
            operation = "ARRAY_TO_STRING"
            query = "SELECT  " + operation + "(["
            for column in calculated_columns["upper"]:
                query += column + ","
            query = (
                query[: len(query) - 1]
                + "],',') AS concat__all FROM("
                + upper_query
                + ") AS upper_query"
            )
        return query

    def compile_sha2_df_fields(self, concat_query, client_config):
        """Returns the wrapper sha2 query for the input concat_query."""

        client = client_config["data_client"]._source_type
        if client == "Impala":
            operation = "SHA2"
            query = (
                "SELECT  "
                + operation
                + "(concat__all,256) AS hash__all FROM ("
                + concat_query
                + ") AS concat_query"
            )
        elif client == "BigQuery":
            operation = "TO_HEX"
            query = (
                "SELECT  "
                + operation
                + "(SHA256(concat__all)) AS hash__all FROM ("
                + concat_query
                + ") AS concat_query"
            )
        return query
