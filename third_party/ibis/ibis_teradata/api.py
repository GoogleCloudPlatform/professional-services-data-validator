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
from third_party.ibis.ibis_teradata import Backend as TeradataBackend
import teradatasql  # NOQA fail early if the package is missing


def teradata_connect(
    host: str = "localhost",
    user_name: str = None,
    password: str = None,
    port: int = 1025,
    logmech: str = "TD2",
    use_no_lock_tables: str = "False",
    json_params: str = None,
):
    backend = TeradataBackend()
    backend.do_connect(
        host=host,
        user_name=user_name,
        password=password,
        port=port,
        logmech=logmech,
        use_no_lock_tables=use_no_lock_tables,
        json_params=json_params,
    )
    return backend
