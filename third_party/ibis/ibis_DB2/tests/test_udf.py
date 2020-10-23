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

import pytest

import ibis.expr.datatypes as dt
from third_party.ibis.ibis_DB2.udf.api import existing_udf

# Add markers for DB2 in the ibis.setup.cfg file
pytestmark = [
    pytest.mark.DB2,
    pytest.mark.DB2_extensions,
]

# Database setup (tables and UDFs)


@pytest.fixture(scope='session')
def next_serial(con):
    # `test_sequence` SEQUENCE is created in database in the
    # CREATE SEQUENCE test_sequence START WITH 1 INCREMENT BY 1
    # NO MAXVALUE NO CYCLE CACHE 24
    # to avoid parallel attempts to create the same sequence (when testing
    # run in parallel
    serial_proxy = con.con.execute(
        "select next value for test_sequence as value from ( values 1 );"
    )
    return serial_proxy.fetchone()['VALUE']


@pytest.fixture(scope='session')
def test_schema(con, next_serial):
    print(next_serial)
    schema_name = 'udf_test_{}'.format(next_serial)
    print(schema_name)
    con.con.execute("CREATE SCHEMA {}".format(schema_name))
    return schema_name


@pytest.fixture(scope='session')
def table_name():
    return 'udf_test_users'


@pytest.fixture(scope='session')
def sql_table_setup(test_schema, table_name):
    return """CREATE TABLE {schema}.{table_name} \
            (user_id integer,user_name varchar(50),name_length integer);
""".format(
        schema=test_schema, table_name=table_name
    )


@pytest.fixture(scope='session')
def sql_table_Insert(test_schema, table_name):
    return """INSERT INTO {schema}.{table_name}(user_id, user_name, name_length) \
            VALUES (1, 'Raj', 3),(2, 'Judy', 4),(3, 'Jonathan', 8)""".format(
        schema=test_schema, table_name=table_name
    )


@pytest.fixture(scope='session')
def sql_define_py_udf(test_schema):
    return """CREATE OR REPLACE FUNCTION {schema}.pylen(x varchar(50)) \
RETURNS table(m integer) \
LANGUAGE PYTHON parameter style \
NPSGENERIC  FENCED  NOT THREADSAFE \
NO FINAL CALL  DISALLOW PARALLEL  NO DBINFO  DETERMINISTIC \
NO EXTERNAL ACTION RETURNS NULL ON NULL INPUT  NO SQL \
external name 'len(x)'""".format(
        schema=test_schema
    )


@pytest.fixture(scope='session')
def sql_define_udf(test_schema):
    return """CREATE OR REPLACE FUNCTION {schema}.custom_len(x varchar(50)) \
RETURNS integer \
LANGUAGE SQL \
DETERMINISTIC \
RETURN LENGTH(X)""".format(
        schema=test_schema
    )


@pytest.fixture(scope='session')
def con_for_udf(
    con,
    test_schema,
    sql_table_setup,
    sql_table_Insert,
    sql_define_udf,
    sql_define_py_udf,
    table_name,
):
    con.con.execute(sql_table_setup)
    con.con.execute(sql_table_Insert)
    con.con.execute(sql_define_udf)
    con.con.execute(sql_define_py_udf)
    try:
        yield con
    finally:
        # teardown
        con.con.execute(
            "DROP TABLE {schema}.{table_name}".format(
                schema=test_schema, table_name=table_name
            )
        )
        con.con.execute(
            "DROP FUNCTION {schema}.custom_len".format(schema=test_schema)
        )
        con.con.execute(
            "DROP FUNCTION {schema}.pylen".format(schema=test_schema)
        )
        con.con.execute(
            "DROP SCHEMA {schema} RESTRICT".format(schema=test_schema)
        )


@pytest.fixture
def table(con_for_udf, table_name, test_schema):
    return con_for_udf.table(table_name, schema=test_schema)


# Tests


def test_existing_sql_udf(test_schema, table):
    """Test creating ibis UDF object based on existing UDF in the database"""
    # Create ibis UDF objects referring to UDFs already created in the database
    custom_length_udf = existing_udf(
        'custom_len',
        input_types=[dt.string],
        output_type=dt.int32,
        schema=test_schema,
    )
    result_obj = table[
        table, custom_length_udf(table['user_name']).name('custom_len')
    ]
    result = result_obj.execute()
    assert result['custom_len'].sum() == result['name_length'].sum()


def mult_a_b(a, b):
    """Test function to be defined in-database as a UDF
    and used via ibis UDF"""
    return a * b


def pysplit(text, split):
    return text.split(split)
