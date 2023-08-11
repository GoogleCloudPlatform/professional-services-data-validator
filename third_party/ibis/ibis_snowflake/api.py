# Copyright(C) 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0(the "License"); you may not
# use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the
# License for the specific language governing permissions and limitations under
# the License.
import logging
from typing import Any, Mapping

import ibis

# Fail fast with imports
import snowflake.connector
import snowflake.sqlalchemy

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

import third_party.ibis.ibis_snowflake.datatypes


def snowflake_connect(
    user: str,
    password: str,
    account: str,
    database: str,
    connect_args: Mapping[str, Any] = None,
):
    return ibis.snowflake.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        connect_args=connect_args,
    )
