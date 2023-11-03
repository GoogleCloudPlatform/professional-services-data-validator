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

# from ibis.backends.base_sqlalchemy.alchemy import to_sqlalchemy
# from third_party.ibis.ibis_db2.compiler import dialect, rewrites  # noqa: F401

from third_party.ibis.ibis_db2 import Backend as DB2Backend
import ibm_db_sa  # NOQA fail early if driver is missing


def db2_connect(
    host: str = "localhost",
    user: str = None,
    password: str = None,
    port: int = 50000,
    database: str = None,
    url: str = None,
    driver: str = "ibm_db_sa",
):
    backend = DB2Backend()
    backend.do_connect(
        host=host,
        user=user,
        password=password,
        port=port,
        database=database,
        url=url,
        driver=driver,
    )
    return backend
