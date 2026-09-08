# Copyright 2026 Google LLC
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

import sqlalchemy as sa
from sqlalchemy.dialects import registry
from sqlalchemy.engine.reflection import ReflectionDefaults

from third_party.ibis import ibis_redshift


def test_redshift_dialect_registered_for_psycopg2():
    assert registry.load("redshift.psycopg2") is ibis_redshift.RedshiftPsycopg2Dialect


def test_sqlalchemy_engine_loads_redshift_dialect():
    engine = sa.create_engine("redshift+psycopg2://user:pass@example.com:5439/dev")

    assert isinstance(engine.dialect, ibis_redshift.RedshiftPsycopg2Dialect)


def test_redshift_dialect_skips_unsupported_postgres_reflection():
    dialect = ibis_redshift.RedshiftPsycopg2Dialect()

    dialect._set_backslash_escapes(None)

    assert dialect._backslash_escapes is False
    assert dialect.supports_native_enum is False
    assert dialect._load_domains(None) == []
    assert dialect.get_multi_pk_constraint(
        None, "analytics", ["orders"], None, None
    ) == [(("analytics", "orders"), ReflectionDefaults.pk_constraint())]
    assert dialect.get_multi_foreign_keys(
        None, "analytics", ["orders"], None, None
    ) == [(("analytics", "orders"), ReflectionDefaults.foreign_keys())]
    assert dialect.get_multi_indexes(None, "analytics", ["orders"], None, None) == [
        (("analytics", "orders"), ReflectionDefaults.indexes())
    ]
    assert dialect.get_multi_unique_constraints(
        None, "analytics", ["orders"], None, None
    ) == [(("analytics", "orders"), ReflectionDefaults.unique_constraints())]
    assert dialect.get_multi_check_constraints(
        None, "analytics", ["orders"], None, None
    ) == [(("analytics", "orders"), ReflectionDefaults.check_constraints())]


@mock.patch("third_party.ibis.ibis_redshift.BaseAlchemyBackend.do_connect")
@mock.patch("third_party.ibis.ibis_redshift.sa.event.listens_for")
@mock.patch("third_party.ibis.ibis_redshift.sa.create_engine")
def test_do_connect_uses_redshift_sqlalchemy_dialect(
    mock_create_engine, mock_listens_for, mock_base_do_connect
):
    engine = mock.Mock()
    mock_create_engine.return_value = engine
    mock_listens_for.side_effect = lambda *args, **kwargs: lambda fn: fn

    backend = ibis_redshift.Backend()
    backend.do_connect(
        host="redshift.example.com",
        port=5439,
        database="dev",
        user="admin",
        password="password",
        schema="analytics",
    )

    alchemy_url = mock_create_engine.call_args.args[0]
    create_engine_kwargs = mock_create_engine.call_args.kwargs

    assert alchemy_url.drivername == "redshift+psycopg2"
    assert alchemy_url.host == "redshift.example.com"
    assert alchemy_url.port == 5439
    assert alchemy_url.database == "dev"
    assert alchemy_url.username == "admin"
    assert create_engine_kwargs["connect_args"] == {
        "options": "-csearch_path=analytics"
    }
    assert create_engine_kwargs["execution_options"] == {
        "isolation_level": "AUTOCOMMIT"
    }
    mock_base_do_connect.assert_called_once_with(engine)
