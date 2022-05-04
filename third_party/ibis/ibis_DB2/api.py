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

from ibis.backends.base_sqlalchemy.alchemy import to_sqlalchemy
from third_party.ibis.ibis_DB2.client import DB2Client
from third_party.ibis.ibis_DB2.compiler import dialect, rewrites  # noqa: F401


def compile(expr, params=None):
    return to_sqlalchemy(expr, dialect.make_context(params=params))


def connect(
    host='localhost',
    user=None,
    password=None,
    port=50000,
    database=None,
    url=None,
    driver='ibm_db_sa',
):
    return DB2Client(
        host=host,
        user=user,
        password=password,
        port=port,
        database=database,
        url=url,
        driver=driver,
    )
