# Copyright 2025 Google LLC
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

"""Output validation report to PostgreSQL table"""

import logging
from typing import Iterable, TYPE_CHECKING

import numpy
import sqlalchemy

from data_validation import clients, consts, util
from data_validation.result_handlers.base_backend import (
    BaseBackendResultHandler,
    RESULTS_TABLE_SCHEMA,
    RH_WRITE_MESSAGE,
    RH_NO_WRITE_MESSAGE,
)


if TYPE_CHECKING:
    from pandas import DataFrame
    from ibis.backends.base import BaseBackend


def _psql_insert_copy(table, conn, keys: list, data_iter: Iterable):
    """
    Execute SQL statement inserting data

    Taken from pandas documentation:
         https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#insertion-method

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list[str]: List of column names
    data_iter : Iterable that iterates the values to be inserted
    """
    import csv
    from io import StringIO

    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


class PostgresResultHandler(BaseBackendResultHandler):
    """Write results of data validation to PostgreSQL."""

    def __init__(
        self,
        client: "BaseBackend",
        status_list: list = None,
        table_id: str = "pso_data_validator.results",
        text_format: str = consts.FORMAT_TYPE_TABLE,
    ):
        self._client = client
        self._table_id = table_id
        self._status_list = status_list
        self._text_format = text_format

    @staticmethod
    def get_handler_for_connection(
        connection_config: dict,
        status_list=None,
        table_id: str = "pso_data_validator.results",
        text_format: str = consts.FORMAT_TYPE_TABLE,
    ):
        """Return PostgresResultHandler instance for given connection config.

        Args:
            table_id (str): Table ID used for validation results.
            status_list (list): provided status to filter the results with
            text_format (str, optional):
                This allows the user to influence the text results written via logger.debug.
                See: https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/871
        """
        client = clients.get_data_client(connection_config)
        return PostgresResultHandler(
            client,
            status_list=status_list,
            table_id=table_id,
            text_format=text_format,
        )

    def _set_current_schema(self, schema_name: str):
        """Set the current schema in a PostgreSQL backend.

        We need to do this due to Ibis limitations:
            NotImplementedError: Creating tables from a different database is not yet implemented
            NotImplementedError: Inserting data to a table from a different database is not yet implemented
        """
        with self._client.begin() as con:
            _ = con.exec_driver_sql(f"SET schema '{schema_name}'")

    def _dataframe_to_sql(self, schema_name, table_name, result_df):
        """Inserts Dataframe data into PostgreSQL table using pandas.Dataframe.to_sql() method."""

        def label_to_string(label) -> str:
            if isinstance(label, (list, tuple, numpy.ndarray)) and len(label) == 2:
                # This is the expected format
                return f"'{label[0]}={label[1]}'"
            else:
                # Anything else
                return f"'{label}'"

        def labels_to_array_literal(labels):
            """Convert Pandas labels array into a PostgreSQL array literal."""
            if not labels:
                return "{}"
            return "{" + ",".join(label_to_string(_) for _ in labels) + "}"

        result_df[consts.CONFIG_LABELS] = result_df.labels.apply(
            lambda x: labels_to_array_literal(x)
        )
        result_df.to_sql(
            table_name,
            self._client.con,
            schema=schema_name,
            if_exists="append",
            index=False,
            chunksize=1000,
            method=_psql_insert_copy,
        )

    def _insert_postgres(self, result_df: "DataFrame"):
        """Store the validation results Dataframe to an Ibis Backend."""

        if "." in self._table_id:
            schema_name, table_name = self._table_id.split(".")
        else:
            schema_name, table_name = None, self._table_id

        if schema_name:
            self._set_current_schema(schema_name)
        try:
            _ = clients.get_ibis_table(self._client, schema_name, table_name)
            # Do nothing, the table exists.
        except sqlalchemy.exc.NoSuchTableError:
            self._client.create_table(table_name, schema=RESULTS_TABLE_SCHEMA)

        if not result_df.empty:
            self._dataframe_to_sql(schema_name, table_name, result_df)

        if result_df.empty:
            logging.info(RH_NO_WRITE_MESSAGE)
        else:
            logging.info(
                f"{RH_WRITE_MESSAGE} to {self._table_id}, run id: {result_df.iloc[0][consts.CONFIG_RUN_ID]}"
            )

    def execute(self, result_df: "DataFrame"):
        result_df = self._filter_by_status_list(result_df)

        util.timed_call("Write results to PostgreSQL", self._insert_postgres, result_df)

        self._call_text_handler(result_df)

        return result_df
