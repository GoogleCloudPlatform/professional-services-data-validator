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
from typing import Literal

from third_party.ibis.ibis_mssql import Backend as MsSqlBackend


def mssql_connect(
    host: str = "localhost",
    user: str = None,
    password: str = None,
    port: int = 1433,
    database: str = None,
    url: str = None,
    driver: Literal["pyodbc"] = "pyodbc",
    odbc_driver: str = "ODBC Driver 17 for SQL Server",
    query: str = None,
):
    backend = MsSqlBackend()
    backend.do_connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        url=url,
        driver=driver,
        odbc_driver=odbc_driver,
        query=query,
    )
    return backend
