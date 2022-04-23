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
from ibis.backends.impala.client import ImpalaClient, ImpalaQuery, _HS2_TTypeId_to_dtype
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import numpy as np
import pandas as pd

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
            if "bigint" in t:
                t = t.replace(":bigint", ":int64")
            elif "tinyint" in t:
                t = t.replace(":tinyint", ":int8")
            elif "smallint" in t:
                t = t.replace(":smallint", ":int16")
            else:
                t = t.replace(":int", ":int32")

            if "varchar" in t:
                t = t.replace(":varchar", ":string")
            else:
                t = t.replace(":char",":string")
            return t
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

def _column_batches_to_dataframe(names, batches):
    cols = {}
    for name, chunks in zip(names, zip(*[b.columns for b in batches])):
        cols[name] = _chunks_to_pandas_array(chunks)
    return pd.DataFrame(cols, columns=names)


def _chunks_to_pandas_array(chunks):
    total_length = 0
    have_nulls = False
    for c in chunks:
        total_length += len(c)
        have_nulls = have_nulls or c.nulls.any()

    type_ = chunks[0].data_type
    numpy_type = _HS2_TTypeId_to_dtype[type_]

    def fill_nonnull(target, chunks):
        pos = 0
        for c in chunks:
            target[pos : pos + len(c)] = c.values
            pos += len(c.values)

    def fill(target, chunks, na_rep):
        pos = 0
        for c in chunks:
            nulls = c.nulls.copy()
            nulls.bytereverse()
            bits = np.frombuffer(nulls.tobytes(), dtype='u1')
            mask = np.unpackbits(bits).view(np.bool_)

            k = len(c)

            dest = target[pos : pos + k]
            dest[:] = c.values
            dest[mask[:k]] = na_rep

            pos += k

    if have_nulls:
        if numpy_type in ('bool', 'datetime64[ns]'):
            target = np.empty(total_length, dtype='O')
            na_rep = None
        elif numpy_type.startswith('int'):
            target = np.empty(total_length, dtype='f8')
            na_rep = np.nan
        elif numpy_type in ('object'):
            target = np.empty(total_length, dtype=object)
            na_rep = None
        else:
            target = np.empty(total_length, dtype=numpy_type)
            na_rep = np.nan

        fill(target, chunks, na_rep)
    else:
        target = np.empty(total_length, dtype=numpy_type)
        fill_nonnull(target, chunks)

    return target

@rewrites(ops.IfNull)
def _if_null(expr):
    arg, fill_value = expr.op().args
    return arg.coalesce(fill_value)


udf.parse_type = parse_type
ImpalaClient.get_schema = get_schema
ImpalaQuery._fetch = _fetch
