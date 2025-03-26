# Copyright 2025 Google LLC
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

from unittest import mock

import pytest

from data_validation import consts, exceptions
from data_validation.result_handlers.bigquery import BigQueryResultHandler
from data_validation.result_handlers.postgres import PostgresResultHandler
from data_validation.result_handlers.text import TextResultHandler


BQ_CONFIG = {
    consts.RH_TYPE: consts.SOURCE_TYPE_BIGQUERY,
    consts.PROJECT_ID: "my_project_id",
    consts.TABLE_ID: "dataset.table",
    consts.API_ENDPOINT: "my_endpoint",
}

PG_CONFIG = {
    consts.RH_TYPE: consts.SOURCE_TYPE_POSTGRES,
    consts.TABLE_ID: "schema.table",
    consts.RH_CONN: {
        "host": "localhost",
        "port": "5432",
        "user": "dvt_u",
        "password": "dvt_p",
        "database": "postgres",
        consts.SOURCE_TYPE: consts.SOURCE_TYPE_POSTGRES,
    },
}

IMPALA_CONFIG = {
    consts.RH_TYPE: consts.SOURCE_TYPE_IMPALA,
    consts.TABLE_ID: "schema.table",
    consts.RH_CONN: {
        "host": "localhost",
        "port": "1234",
        "user": "dvt_u",
        "password": "dvt_p",
        "database": "default",
        consts.SOURCE_TYPE: consts.SOURCE_TYPE_IMPALA,
    },
}


@pytest.fixture
def module_under_test():
    from data_validation.result_handlers import factory

    return factory


def test_build_result_handler_default(module_under_test):
    filter_status = ["fail"]
    handler = module_under_test.build_result_handler(
        None, consts.SCHEMA_VALIDATION, filter_status
    )

    assert isinstance(handler, TextResultHandler)
    assert handler.format == consts.FORMAT_TYPE_TABLE
    assert handler.status_list == filter_status


def test_build_result_handler_bigquery(module_under_test):
    config = BQ_CONFIG
    filter_status = ["fail"]
    handler = module_under_test.build_result_handler(
        config, consts.SCHEMA_VALIDATION, filter_status
    )

    assert isinstance(handler, BigQueryResultHandler)
    assert handler._table_id == "dataset.table"
    assert handler._status_list == filter_status


@mock.patch.dict(
    "data_validation.clients.CLIENT_LOOKUP",
    {consts.SOURCE_TYPE_POSTGRES: mock.Mock()},
)
def test_build_result_handler_postgres(module_under_test):
    config = PG_CONFIG
    filter_status = ["fail"]
    handler = module_under_test.build_result_handler(
        config, consts.SCHEMA_VALIDATION, filter_status
    )

    assert isinstance(handler, PostgresResultHandler)
    assert handler._table_id == "schema.table"
    assert handler._status_list == filter_status


@mock.patch.dict(
    "data_validation.clients.CLIENT_LOOKUP",
    {consts.SOURCE_TYPE_IMPALA: mock.Mock()},
)
def test_build_result_handler_unsupported(module_under_test):
    """Ensure that an unsupported config throws an exception."""
    config = IMPALA_CONFIG
    filter_status = ["fail"]
    with pytest.raises(exceptions.ResultHandlerException):
        _ = module_under_test.build_result_handler(
            config, consts.SCHEMA_VALIDATION, filter_status
        )
