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

import functools

import pytest

import ibis.expr.datatypes as dt
from third_party.ibis.ibis_oracle.udf.api import OracleUDFError

# mark test module as oracle (for ability to easily exclude,
# e.g. in conda build tests)
#     TODO: update Windows pipeline to exclude oracle_extensions

pytestmark = [
    pytest.mark.oracle,
    pytest.mark.oracle_extensions,
]

# Database setup (tables and UDFs)


@pytest.fixture(scope='session')
def next_serial(con):
    serial_proxy = con.con.execute(
        "select test_sequence.nextval as value from dual"
    )
    return serial_proxy.fetchone()['value']


@pytest.fixture(scope='session')
def test_schema(con, next_serial):
    schema_name = 'udf_test_{}'.format(next_serial)
    # schema_name='"{}"'.format(schema_name)
    print(schema_name)
    con.con.execute("CREATE USER {}".format(schema_name))
    # con.con.execute("CREATE USER "+ schema_name)
    return schema_name


@pytest.fixture(scope='session')
def table_name():
    return 'udf_user_list_11'


@pytest.fixture(scope='session')
def sql_table_setup(test_schema, table_name):
    return """CREATE TABLE {schema}.{table_name} (
            user_id integer,
            user_name varchar(50),
            name_length integer
)""".format(
        schema=test_schema, table_name=table_name
    )


@pytest.fixture(scope='session')
def permission_schema(test_schema):
    return '''GRANT UNLIMITED TABLESPACE TO {schema}'''.format(
        schema=test_schema
    )


@pytest.fixture(scope='session')
def sql_table_Insert(test_schema, table_name):
    return """INSERT INTO {schema}.{table_name}(user_id, user_name, name_length)
    (SELECT 1, 'Raj', 3 FROM dual) UNION ALL
    (SELECT 2, 'Judy', 4 FROM dual) UNION ALL
    (SELECT 3, 'Jonathan', 8 FROM dual)""".format(
        schema=test_schema, table_name=table_name
    )


@pytest.fixture(scope='session')
def sql_define_udf(test_schema):
    return """CREATE OR REPLACE FUNCTION {schema}.addition_func(n1 in number, n2 in number)
RETURN number
is
n3 number(8);
begin
n3 := n1+n2;
return n3;
end;""".format(
        schema=test_schema
    )


@pytest.fixture(scope='session')
def con_for_udf(
    con,
    test_schema,
    table_name,
    sql_table_setup,
    permission_schema,
    sql_table_Insert,
    sql_define_udf,
):
    con.con.execute(sql_table_setup)
    con.con.execute(permission_schema)
    con.con.execute(sql_table_Insert)
    con.con.execute(sql_define_udf)

    try:
        yield con
    finally:
        # teardown
        print("In finally block")
        # con.con.execute("DROP USER "+test_schema +" cascade")
        # con.con.execute("DROP USER schema CASCADE".format(test_schema))


@pytest.fixture
def table(con_for_udf, table_name, test_schema):
    return con_for_udf.table(table_name, schema=test_schema)


# Tests


def pysplit(text, split):
    return text.split(split)


def test_client_udf_decorator_fails(con_for_udf, test_schema):
    """Test that UDF creation fails when creating a UDF based on a Python
    function that has been defined with decorators. Decorators are not
    currently supported, because the decorators end up in the body of the UDF
    but are not defined in the body, therefore causing a NameError."""

    def decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwds):
            return f(*args, **kwds)

        return wrapped

    @decorator
    def multiply(a, b):
        return a * b

    with pytest.raises(OracleUDFError):
        con_for_udf.udf(
            multiply,
            [dt.int32, dt.int32],
            dt.int32,
            schema=test_schema,
            replace=True,
        )
