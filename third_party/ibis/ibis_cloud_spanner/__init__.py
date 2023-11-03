# Copyright 2023 Google Inc.
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

from typing import Any, Mapping, Optional, Tuple

import google.cloud.spanner as cs
import ibis.expr.schema as sch
import ibis.expr.types as ir
from google.cloud import spanner
from ibis.backends.base.sql import BaseSQLBackend

from third_party.ibis.ibis_cloud_spanner.datatypes import (
    dtype_from_spanner_field,
    schema_from_spanner,
)
from third_party.ibis.ibis_cloud_spanner.compiler import SpannerCompiler
from third_party.ibis.ibis_cloud_spanner.client import SpannerCursor
from third_party.ibis.ibis_cloud_spanner.to_pandas import pandas_df


class Backend(BaseSQLBackend):
    name = "spanner"
    compiler = SpannerCompiler

    def do_connect(
        self,
        instance_id: str,
        database_id: str = None,
        project_id: str = None,
        credentials=None,
    ) -> None:

        self.spanner_client = spanner.Client(
            project=project_id, credentials=credentials
        )
        self.instance = self.spanner_client.instance(instance_id)
        self.database_name = self.instance.database(database_id)
        (
            self.data_instance,
            self.dataset,
        ) = parse_instance_and_dataset(instance_id, database_id)
        self.client = cs.Client()

    def _parse_instance_and_dataset(self, dataset):
        if not dataset and not self.dataset:
            raise ValueError("Unable to determine Cloud Spanner dataset.")
        instance, dataset = parse_instance_and_dataset(
            self.data_instance, (dataset or self.dataset)
        )
        return instance, dataset

    @property
    def instance_id(self):
        return self.data_instance

    @property
    def version(self):
        return spanner.__version__

    @property
    def dataset_id(self):
        return self.dataset

    def table(self, name, database=None):
        t = super().table(name, database=database)
        return t

    def _get_schema_using_query(self, query):
        db = self.instance.database(self.dataset_id)

        with db.snapshot() as snapshot:
            result = snapshot.execute_sql(query)

            # Materialize data for one row so result.fields gets populated
            data = []
            for row in result:
                data.append(row)
                break

            fields = {
                col.name: dtype_from_spanner_field(col.type_) for col in result.fields
            }
        return sch.Schema(fields)

    def _get_table_schema(self, qualified_name):
        table = qualified_name
        database = self.database_name
        assert database is not None, "database is None"
        return self.get_schema(table, database=dataset)

    @property
    def current_database(self) -> str:
        return self.database_name

    def list_databases(self, like=None):
        databases = self.instance.list_databases()
        list_db = []
        for row in databases:
            list_db.append((row.name).rsplit("/", 1)[1])
        return list_db

    def list_tables(self, like=None, database=None):
        if database is None:
            db_value = self.dataset_id
        else:
            db_value = database
        db = self.instance.database(db_value)
        tables = [tbl.table_id for tbl in db.list_tables()]

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
        result = db_value.table(name).exists()
        return result

    def get_schema(self, table_id, database=None):
        if database is None:
            database = self.dataset_id
        db_value = self.instance.database(database)
        spanner_table = db_value.table(table_id)
        return schema_from_spanner(spanner_table.schema)

    def execute(
        self,
        expr: ir.Expr,
        params: Mapping[ir.Scalar, Any] = None,
        limit: str = "default",
        **kwargs: Any,
    ):
        # Overwrite the execute() method to use Spanner to_pandas
        kwargs.pop("timecontext", None)
        query_ast = self.compiler.to_ast_ensure_limit(expr, limit, params=params)
        sql = query_ast.compile()
        self._log(sql)

        schema = self.ast_schema(query_ast, **kwargs)

        self._register_in_memory_tables(expr)
        db = self.instance.database(self.dataset_id)

        with db.snapshot() as snapshot:
            result = pandas_df.to_pandas(snapshot, sql, query_parameters=None)

        return result

    def _execute(self, stmt, results=True, query_parameters=None):
        db = self.instance.database(self.dataset_id)

        with db.snapshot() as snapshot:
            data_qry = pandas_df.to_pandas(snapshot, stmt, query_parameters)
        return data_qry

    def raw_sql(self, query: str, results=False, params=None):
        db = self.instance.database(self.dataset_id)
        with db.snapshot() as snapshot:
            result = snapshot.execute_sql(query)

        return SpannerCursor(result)

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

    def create_table():
        pass

    def create_view():
        pass

    def drop_table():
        pass

    def drop_view():
        pass

    def fetch_from_cursor():
        pass


def parse_instance_and_dataset(
    instance: str, dataset: Optional[str] = None
) -> Tuple[str, str, Optional[str]]:

    data_instance = instance
    dataset = dataset

    return data_instance, dataset
