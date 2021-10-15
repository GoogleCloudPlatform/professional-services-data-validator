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

from ibis.backends.impala import connect
from ibis.backends.impala import udf
import ibis.expr.datatypes as dt

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


udf.parse_type = parse_type
