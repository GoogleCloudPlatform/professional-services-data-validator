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

from functools import lru_cache
import ibis
from ibis.backends.impala import Backend as ImpalaBackend
from ibis.backends.impala.client import ImpalaConnection

from typing import Literal

from ibis.backends.impala import udf
from ibis.backends.impala.compiler import rewrites
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import numpy as np
import fsspec
import re


def do_connect(
    # Override do_connect to add use_http_transport and http_path params
    self,
    host: str = "localhost",
    port: int = 10000,
    database: str = "default",
    timeout: int = 45,
    use_ssl: bool = False,
    ca_cert: str = None,
    user: str = None,
    password: str = None,
    auth_mechanism: Literal["NOSASL", "PLAIN", "GSSAPI", "LDAP"] = "PLAIN",
    kerberos_service_name: str = "impala",
    pool_size: int = 8,
    hdfs_client: fsspec.spec.AbstractFileSystem = None,
    use_http_transport: bool = False,
    http_path: str = "",
):
    self._temp_objects = set()
    self._hdfs = hdfs_client

    params = {
        "host": host,
        "port": port,
        "database": database,
        "timeout": timeout,
        "use_ssl": use_ssl,
        "ca_cert": ca_cert,
        "user": user,
        "password": password,
        "auth_mechanism": auth_mechanism,
        "kerberos_service_name": kerberos_service_name,
        "use_http_transport": use_http_transport,
        "http_path": http_path,
    }
    self.con = ImpalaConnection(pool_size=pool_size, **params)
    self._ensure_temp_db_exists()


def parse_type(t):
    """Returns the Ibis datatype from source type."""
    t = t.lower()
    if t in udf._impala_to_ibis_type:
        return udf._impala_to_ibis_type[t]
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
            # Supports parsing through structs/arrays in schema in #444
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
                t = t.replace(":char", ":string")
            return t
        elif "binary" in t:
            return "binary"
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

    pairs = []
    for row in self.con.fetchall(query):
        # Add check for empty row for Hive partitioned tables in #375
        if row[0] == "":
            break
        pairs.append(row[:2])

    names, types = zip(*pairs)
    ibis_types = [parse_type(type.lower()) for type in types]
    ibis_fields = dict(zip(names, ibis_types))

    return sch.Schema(ibis_fields)


def _chunks_to_pandas_array(chunks):
    total_length = 0
    have_nulls = False
    for c in chunks:
        total_length += len(c)
        have_nulls = have_nulls or c.nulls.any()

    type_ = chunks[0].data_type
    numpy_type = ibis.backends.impala._HS2_TTypeId_to_dtype[type_]

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
            bits = np.frombuffer(nulls.tobytes(), dtype="u1")
            mask = np.unpackbits(bits).view(np.bool_)

            k = len(c)

            dest = target[pos : pos + k]
            dest[:] = c.values
            dest[mask[:k]] = na_rep

            pos += k

    if have_nulls:
        # Updating NaN to None for consistency across DBs as per #406
        if numpy_type in ("bool", "datetime64[ns]"):
            target = np.empty(total_length, dtype="O")
            na_rep = None
        elif numpy_type.startswith("int"):
            target = np.empty(total_length, dtype="f8")
            na_rep = np.nan
        elif numpy_type in ("object"):
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
def _if_null(op):
    return ops.Coalesce((op.arg, op.ifnull_expr))


def update_query_with_limit(query):
    limit_pattern = re.compile(r"LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*;?\s*$", re.IGNORECASE)
    last_limit_match = limit_pattern.search(query)

    if last_limit_match:
        new_query = (
            query[: last_limit_match.start()]
            + "LIMIT 1"
            + query[last_limit_match.end() :]
        )
    else:
        new_query = query + " LIMIT 1"

    return new_query


@lru_cache(maxsize=2)
def _get_schema_using_query(self, query):
    # Removing LIMIT 0 around query since it returns no results in Hive
    updated_query = update_query_with_limit(query)
    cur = self.raw_sql(updated_query)
    cur.fetchone()
    cur.description = [
        (
            description[0].split(".")[-1] if "." in description[0] else description[0],
            *description[1:],
        )
        for description in cur.description
    ]
    ibis_fields = self._adapt_types(cur.description)
    cur.release()

    return sch.Schema(ibis_fields)


def _list_primary_key_columns(self, database: str, table: str) -> list:
    """No primary keys in Hadoop."""
    return None


def _dvt_list_tables(self, like=None, database=None):
    """dvt_list_tables is a pass through on Hadoop due to "show tables" behaviour."""
    return self.list_tables(like=like, database=database)


udf.parse_type = parse_type
ibis.backends.impala._chunks_to_pandas_array = _chunks_to_pandas_array
ImpalaBackend.get_schema = get_schema
ImpalaBackend._get_schema_using_query = _get_schema_using_query
ImpalaBackend.do_connect = do_connect
ImpalaBackend.list_primary_key_columns = _list_primary_key_columns
ImpalaBackend.dvt_list_tables = _dvt_list_tables


def impala_connect(
    host: str = "localhost",
    port: int = 10000,
    database: str = "default",
    timeout: int = 45,
    use_ssl: bool = False,
    ca_cert: str = None,
    user: str = None,
    password: str = None,
    auth_mechanism: Literal["NOSASL", "PLAIN", "GSSAPI", "LDAP"] = "PLAIN",
    kerberos_service_name: str = "impala",
    pool_size: int = 8,
    hdfs_client: fsspec.spec.AbstractFileSystem = None,
    use_http_transport: bool = False,
    http_path: str = "",
):
    backend = ibis.impala.connect(
        host=host,
        port=port,
        database=database,
        timeout=timeout,
        use_ssl=use_ssl,
        ca_cert=ca_cert,
        user=user,
        password=password,
        auth_mechanism=auth_mechanism,
        kerberos_service_name=kerberos_service_name,
        pool_size=pool_size,
        hdfs_client=hdfs_client,
        use_http_transport=use_http_transport,
        http_path=http_path,
    )
    return backend
