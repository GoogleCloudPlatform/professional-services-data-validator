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

from ibis.backends.base_sql import fixed_arity
from ibis.backends.impala import connect, udf
from ibis.backends.impala.compiler import rewrites
from ibis.backends.impala.client import ImpalaClient, ImpalaQuery, _column_batches_to_dataframe
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch

_impala_to_ibis_type = udf._impala_to_ibis_type

def impala_connect(
    host=None,
    port=10000,
    database="default",
    auth_mechanism="PLAIN",
    kerberos_service_name="impala",
):
    auth_mechanism = (auth_mechanism, "PLAIN")[auth_mechanism is None]
    database = (database, "default")[database is None]
    port = (port, 10000)[port is None]
    kerberos_service_name = (kerberos_service_name, "impala")[
        kerberos_service_name is None
    ]
    return connect(
        host=host,
        port=int(port),
        database=database,
        auth_mechanism=auth_mechanism,
        kerberos_service_name=kerberos_service_name,
    )


def parse_type(t):
    """Returns the Ibis datatype from source type."""
    t = t.lower()
    if t in _impala_to_ibis_type:
        return _impala_to_ibis_type[t]
    else:
        if "varchar" in t or "char" in t:
            return "string"
        elif "decimal" in t:
            result = dt.dtype(t)
            if result:
                return t
            else:
                return ValueError(t)
        elif "struct" in t or "array" in t or "map" in t:
            return t.replace("int", "int32")
        else:
            raise Exception(t)


def get_schema(self, table_name, database=None):
    """
        Return a Schema object for the indicated table and database

        Parameters
        ----------
        table_name : string
          May be fully qualified
        database : string, default None

        Returns
        -------
        schema : ibis Schema
        """
    qualified_name = self._fully_qualified_name(table_name, database)
    query = "DESCRIBE {}".format(qualified_name)

    # only pull out the first two columns which are names and types
    # pairs = [row[:2] for row in self.con.fetchall(query)]
    pairs = []
    for row in self.con.fetchall(query):
        if row[0] == "":
            break
        pairs.append(row[:2])

    names, types = zip(*pairs)
    ibis_types = [parse_type(type.lower()) for type in types]
    names = [name.lower() for name in names]

    return sch.Schema(names, ibis_types)


def _fetch(self, cursor):
        batches = cursor.fetchall(columnar=True)
        names = []
        for x in cursor.description:
            name = x[0].split('.')[-1]
            names.append(name)
        df = _column_batches_to_dataframe(names, batches)
        return df


@rewrites(ops.IfNull)
def _if_null(expr):
    arg, fill_value = expr.op().args
    return arg.coalesce(fill_value)


udf.parse_type = parse_type
ImpalaClient.get_schema = get_schema
ImpalaQuery._fetch = _fetch
