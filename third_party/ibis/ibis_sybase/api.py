# Copyright 2025 Google Inc.
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
from typing import Literal, Optional

from third_party.ibis.ibis_sybase import Backend as SybaseBackend


def sybase_connect(
    host: Optional[str] = "localhost",
    user: Optional[str] = None,
    password: Optional[str] = None,
    port: int = 5000,
    database: Optional[str] = None,
    url: Optional[str] = None,
    driver: Literal["pyodbc"] = "pyodbc",
    odbc_driver: Optional[str] = None,
    query: Optional[str] = None,
):
    backend = SybaseBackend()
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
