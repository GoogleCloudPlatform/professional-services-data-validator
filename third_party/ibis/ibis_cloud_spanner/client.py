# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cloud Spanner ibis client implementation."""

import datetime
from typing import Optional, Tuple

import google.cloud.spanner as cs
from google.cloud import spanner
import pandas as pd
import re
from multipledispatch import Dispatcher

import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
from third_party.ibis.ibis_cloud_spanner import compiler as comp
from third_party.ibis.ibis_cloud_spanner.datatypes import (
    ibis_type_to_cloud_spanner_type,
)
from ibis.client import Database, Query, SQLClient

from third_party.ibis.ibis_cloud_spanner import table

from google.cloud.spanner_v1 import TypeCode
from third_party.ibis.ibis_cloud_spanner.to_pandas import pandas_df


def parse_instance_and_dataset(
    instance: str, dataset: Optional[str] = None
) -> Tuple[str, str, Optional[str]]:

    data_instance = instance
    dataset = dataset

    return data_instance, dataset


class CloudSpannerTable(ops.DatabaseTable):
    pass


def _find_scalar_parameter(expr):
    """Find all :class:`~ibis.expr.types.ScalarParameter` instances.

    Parameters
    ----------
    expr : ibis.expr.types.Expr

    Returns
    -------
    Tuple[bool, object]
        The operation and the parent expresssion's resolved name.

    """
    op = expr.op()

    if isinstance(op, ops.ScalarParameter):
        result = op, expr.get_name()
    else:
        result = None
    return lin.proceed, result


def convert_to_cs_type(dtype):
    if dtype == "FLOAT64":
        return spanner.param_types.FLOAT64
    elif dtype == "INT64":
        return spanner.param_types.INT64
    elif dtype == "DATE":
        return spanner.param_types.DATE
    elif dtype == "TIMESTAMP":
        return spanner.param_types.TIMESTAMP
    elif dtype == "NUMERIC":
        return spanner.param_types.NUMERIC
    elif dtype == "INT64":
        return spanner.param_types.INT64
    else:
        return spanner.param_types.STRING


cloud_spanner_param = Dispatcher("cloud_spanner_param")


@cloud_spanner_param.register(ir.ArrayValue, list)
def cs_param_array(param, value):
    param_type = param.type()
    assert isinstance(param_type, dt.Array), str(param_type)

    try:
        spanner_type = ibis_type_to_cloud_spanner_type(param_type.value_type)
    except NotImplementedError:
        raise com.UnsupportedBackendType(param_type)
    else:
        if isinstance(param_type.value_type, dt.Struct):
            raise TypeError("ARRAY<STRUCT<T>> is not supported in Cloud Spanner")
        elif isinstance(param_type.value_type, dt.Array):
            raise TypeError("ARRAY<ARRAY<T>> is not supported in Cloud Spanner")
        else:
            query_value = value

        params = ({param.get_name(): query_value},)
        param_types = {param.get_name(): convert_to_cs_type(spanner_type)}
        final_dict = {"params": params, "param_types": param_types}

        return final_dict


@cloud_spanner_param.register(
    ir.TimestampScalar, (str, datetime.datetime, datetime.date)
)
def cs_param_timestamp(param, value):
    assert isinstance(param.type(), dt.Timestamp), str(param.type())

    timestamp_value = pd.Timestamp(value, tz="UTC").to_pydatetime()
    params = ({param.get_name(): timestamp_value},)
    param_types = {param.get_name(): spanner.param_types.TIMESTAMP}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


@cloud_spanner_param.register(ir.StringScalar, str)
def cs_param_string(param, value):
    params = ({param.get_name(): value},)
    param_types = {param.get_name(): spanner.param_types.STRING}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


@cloud_spanner_param.register(ir.IntegerScalar, int)
def cs_param_integer(param, value):
    params = ({param.get_name(): value},)
    param_types = {param.get_name(): spanner.param_types.INT64}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


@cloud_spanner_param.register(ir.FloatingScalar, float)
def cs_param_double(param, value):
    params = ({param.get_name(): value},)
    param_types = {param.get_name(): spanner.param_types.FLOAT64}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


@cloud_spanner_param.register(ir.BooleanScalar, bool)
def cs_param_boolean(param, value):
    params = ({param.get_name(): value},)
    param_types = {param.get_name(): spanner.param_types.BOOL}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


@cloud_spanner_param.register(ir.DateScalar, str)
def cs_param_date_string(param, value):
    params = ({param.get_name(): pd.Timestamp(value).to_pydatetime().date()},)
    param_types = {param.get_name(): spanner.param_types.DATE}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


@cloud_spanner_param.register(ir.DateScalar, datetime.datetime)
def cs_param_date_datetime(param, value):
    params = ({param.get_name(): value.date()},)
    param_types = {param.get_name(): spanner.param_types.DATE}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


@cloud_spanner_param.register(ir.DateScalar, datetime.date)
def cs_param_date(param, value):
    params = ({param.get_name(): value},)
    param_types = {param.get_name(): spanner.param_types.DATE}
    final_dict = {"params": params[0], "param_types": param_types}
    return final_dict


class CloudSpannerQuery(Query):
    def __init__(self, client, ddl, query_parameters=None):
        super().__init__(client, ddl)

        # self.expr comes from the parent class
        query_parameter_names = dict(lin.traverse(_find_scalar_parameter, self.expr))

        self.query_parameters = [
            cloud_spanner_param(
                param.to_expr().name(query_parameter_names[param]), value
            )
            for param, value in (query_parameters or {}).items()
        ]

    def execute(self):
        dataframe_output = self.client._execute(
            self.compiled_sql, results=True, query_parameters=self.query_parameters
        )

        return dataframe_output


class SpannerCursor:
    """Spanner cursor.
    This allows the Spanner client to reuse machinery in
    :file:`ibis/client.py`.
    """

    def __init__(self, results):
        self.results = results

    def fetchall(self):
        """Fetch all rows."""
        result = self.results
        return [tuple(row) for row in result]

    @property
    def columns(self):
        """Return the columns of the result set."""
        result = self.results
        return [field.name for field in result.fields]

    @property
    def description(self):
        """Get the fields of the result set's schema."""
        return self.results.metadata.row_type

    def __enter__(self):
        # For compatibility when constructed from Query.execute()
        """No-op for compatibility.
        See Also
        --------
        ibis.client.Query.execute
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """No-op for compatibility.
        See Also
        --------
        ibis.client.Query.execute
        """


class CloudSpannerDatabase(Database):
    """A Cloud spanner dataset."""


class CloudSpannerClient(SQLClient):
    """An ibis CloudSpanner client implementation."""

    query_class = CloudSpannerQuery
    database_class = CloudSpannerDatabase
    table_class = CloudSpannerTable

    def __init__(self, instance_id, database_id, project_id=None, credentials=None):
        """Construct a CloudSpannerClient.

        Parameters
        ----------
        instance_id : str
            A instance name
        database_id : Optional[str]
            A ``<instance_id>.<database_id>`` string or just a dataset name
        project_id  : str (Optional)
            The ID of the project which owns the instances, tables and data.


        """
        self.spanner_client = spanner.Client(project=project_id)
        self.instance = self.spanner_client.instance(instance_id)
        self.database_name = self.instance.database(database_id)
        (self.data_instance, self.dataset,) = parse_instance_and_dataset(
            instance_id, database_id
        )
        self.client = cs.Client()

    def _parse_instance_and_dataset(self, dataset):
        if not dataset and not self.dataset:
            raise ValueError("Unable to determine Cloud Spanner dataset.")
        instance, dataset = parse_instance_and_dataset(
            self.data_instance, (dataset or self.dataset)
        )

        return instance, dataset

    def get_data_using_query(self, query, results=False):
        return self._execute(query, results=results)

    @property
    def instance_id(self):
        return self.data_instance

    @property
    def dataset_id(self):
        return self.dataset

    def table(self, name, database=None):
        t = super().table(name, database=database)
        return t

    def _build_ast(self, expr, context):
        result = comp.build_ast(expr, context)
        return result

    def _get_query(self, dml, **kwargs):
        return self.query_class(self, dml, query_parameters=dml.context.params)

    def _fully_qualified_name(self, name, database):
        return name

    def _get_table_schema(self, qualified_name):
        table = qualified_name
        dataset = self.dataset_id
        assert dataset is not None, "dataset is None"
        return self.get_schema(table, database=dataset)

    @property
    def current_database(self):
        return self.database(self.dataset)

    def list_databases(self, like=None):
        databases = self.instance.list_databases()
        list_db = []
        for row in databases:
            list_db.append((row.name).rsplit("/", 1)[1])
        return list_db

    def list_tables(self, like=None, database=None):
        # TODO:  use list_tables from the Database class when available.

        if database is None:
            db_value = self.dataset_id
        else:
            db_value = database
        db = self.instance.database(db_value)
        tables = []
        with db.snapshot() as snapshot:
            query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where SPANNER_STATE = 'COMMITTED' "
            results = snapshot.execute_sql(query)
            for row in results:
                tables.append(row[0])

        if like:
            tables = [
                table_name
                for table_name in tables
                if re.match(like, table_name) is not None
            ]
        return tables

    def exists_table(self, name, database=None):

        if database is None:
            database = self.dataset_id

        db_value = self.instance.database(database)
        result = table.Table(name, db_value).exists()
        return result

    def get_schema(self, table_id, database=None):
        if database is None:
            database = self.dataset_id
        db_value = self.instance.database(database)
        table_schema = table.Table(table_id, db_value).schema

        t_schema = []
        for item in table_schema:
            field_name = item.name

            if item.type_.code == TypeCode.ARRAY:
                field_type = "array<{}>".format(item.type_.array_element_type.code.name)
            elif item.type_.code == TypeCode.BYTES:
                field_type = "binary"
            elif item.type_.code == TypeCode.NUMERIC:
                field_type = "decimal"
            else:
                field_type = item.type_.code.name

            final_item = (field_name, field_type)

            t_schema.append(final_item)

        return ibis.schema(t_schema)

    def _execute(self, stmt, results=True, query_parameters=None):

        spanner_client = spanner.Client()
        instance_id = self.instance_id
        instance = spanner_client.instance(instance_id)
        database_id = self.dataset_id
        database_1 = instance.database(database_id)

        with database_1.snapshot() as snapshot:
            data_qry = pandas_df.to_pandas(snapshot, stmt, query_parameters)
        return data_qry

    def raw_sql(self, query: str, results=False, params=None):
        query_parameters = [
            cloud_spanner_param(param, value) for param, value in (params or {}).items()
        ]
        spanner_client = spanner.Client()
        instance_id = self.instance_id
        instance = spanner_client.instance(instance_id)
        database_id = self.dataset_id
        database_1 = instance.database(database_id)
        with database_1.snapshot() as snapshot:
            if query_parameters:
                param = {}
                param_type = {}
                for i in query_parameters:
                    param.update(i["params"])
                    param_type.update(i["param_types"])

                results = snapshot.execute_sql(
                    query, params=param, param_types=param_type
                )

            else:
                results = snapshot.execute_sql(query)

        sp = SpannerCursor(results)
        return sp

    def database(self, name=None):
        if name is None and self.dataset is None:
            raise ValueError(
                "Unable to determine Cloud Spanner dataset. Call "
                "client.database('my_dataset') or set_database('my_dataset') "
                "to assign your client a dataset."
            )
        return self.database_class(name or self.dataset, self)

    def set_database(self, name):
        self.data_instance, self.dataset = self._parse_instance_and_dataset(name)

    def dataset(self, database):
        spanner_client = spanner.Client()
        instance = spanner_client.instance(self.data_instance)
        database = instance.database(database)

    def exists_database(self, name):
        return self.instance.database(name).exists()
