# Copyright 2020 Google Inc.
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

from third_party.ibis.ibis_oracle.client import OracleClient
from third_party.ibis.ibis_oracle.compiler import (  # noqa: F401
    dialect,
    rewrites,
)

from ibis.sql.alchemy import to_sqlalchemy


def compile(expr, params=None):
    return to_sqlalchemy(expr, dialect.make_context(params=params))


def connect(
    host=None, port=None, user=None, password=None, database=None, protocol='TCP', url=None, driver='cx_Oracle',
):

    return OracleClient(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        protocol=protocol,
        url=url,
        driver=driver,
    )
