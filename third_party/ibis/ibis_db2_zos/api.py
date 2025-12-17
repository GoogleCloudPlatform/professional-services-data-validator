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

from typing import Optional

from third_party.ibis.ibis_db2_zos import Backend
import ibm_db_sa  # NOQA fail early if driver is missing

from data_validation.util import dvt_config_string_to_dict


def db2_zos_connect(
    host: str = "localhost",
    user: Optional[str] = None,
    password: Optional[str] = None,
    port: int = 50000,
    database: Optional[str] = None,
    url: Optional[str] = None,
    driver: str = "ibm_db_sa",
    connect_args: Optional[str] = None,
):
    connect_args_dict = dvt_config_string_to_dict(connect_args) if connect_args else {}
    backend = Backend()
    backend.do_connect(
        host=host,
        user=user,
        password=password,
        port=port,
        database=database,
        url=url,
        driver=driver,
        connect_args=connect_args_dict,
    )
    return backend
