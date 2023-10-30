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
from third_party.ibis.ibis_redshift import Backend as RedshiftBackend


def redshift_connect(
    host: str = "localhost",
    user: str = None,
    password: str = None,
    port: int = 5439,
    database: str = "public",
    schema: str = None,
    url: str = None,
    driver: Literal["psycopg2"] = "psycopg2",
):
    backend = RedshiftBackend()
    backend.do_connect(
        host=host,
        user=user,
        password=password,
        port=port,
        database=database,
        schema=schema,
        url=url,
        driver=driver,
    )
    return backend
