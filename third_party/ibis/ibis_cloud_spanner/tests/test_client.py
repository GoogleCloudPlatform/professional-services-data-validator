# Copyright 2021 Google Inc.
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

import datetime
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest
import pytz

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

from third_party.ibis.ibis_cloud_spanner import api as cs_compile

pytestmark = pytest.mark.cloud_spanner

from third_party.ibis.ibis_cloud_spanner.tests.conftest import connect


def test_table(alltypes):
    assert isinstance(alltypes, ir.TableExpr)


def test_column_execute(alltypes, df):
    col_name = "float_col"
    expr = alltypes[col_name]
    result = expr.execute()
    expected = df[col_name]
    tm.assert_series_equal(
        (result.sort_values(col_name).reset_index(drop=True)).iloc[:, 0],
        expected.sort_values().reset_index(drop=True),
    )


def test_literal_execute(client):
    expected = "1234"
    expr = ibis.literal(expected)
    result = (client.execute(expr)).iloc[0]["tmp"]
    assert result == expected


def test_simple_aggregate_execute(alltypes, df):
    col_name = "float_col"
    expr = alltypes[col_name].sum()
    result = expr.execute()
    expected = df[col_name].sum()
    final_result = result.iloc[0]["sum"]
    assert final_result == expected


def test_list_tables(client):
    tables = client.list_tables(like="functional_alltypes")
    assert set(tables) == {"functional_alltypes"}


def test_current_database(client, database_id):
    assert client.current_database.name == database_id
    assert client.current_database.name == client.dataset_id
    assert client.current_database.tables == client.list_tables()


def test_database(client):
    database = client.database(client.dataset_id)
    assert database.list_tables() == client.list_tables()


def test_compile_toplevel():
    t = ibis.table([("foo", "double")], name="t0")

    expr = t.foo.sum()
    result = cs_compile.compile(expr)

    expected = """\
SELECT sum(`foo`) AS `sum`
FROM t0"""  # noqa
    assert str(result) == expected


def test_count_distinct_with_filter(alltypes):
    expr = alltypes.float_col.nunique(where=alltypes.float_col.cast("int64") > 1)
    result = expr.execute()
    result = result.iloc[:, 0]
    result = result.iloc[0]

    expected = alltypes.float_col.execute()
    expected = expected[expected.astype("int64") > 1].nunique()
    expected = expected.iloc[0]
    assert result == expected


@pytest.mark.parametrize("type", ["date", dt.date])
def test_cast_string_to_date(alltypes, df, type):
    import toolz

    string_col = alltypes.date_string_col
    month, day, year = toolz.take(3, string_col.split("/"))

    expr = ibis.literal("-").join([year, month, day])
    expr = expr.cast(type)

    result = (
        expr.execute()
        .iloc[:, 0]
        .astype("datetime64[ns]")
        .sort_values()
        .reset_index(drop=True)
        .rename("date_string_col")
    )
    expected = (
        pd.to_datetime(df.date_string_col)
        .dt.normalize()
        .sort_values()
        .reset_index(drop=True)
    )
    tm.assert_series_equal(result, expected)


def test_subquery_scalar_params(alltypes):
    t = alltypes
    param = ibis.param("timestamp").name("my_param")
    expr = (
        t[["float_col", "timestamp_col", "int_col", "string_col"]][
            lambda t: t.timestamp_col < param
        ]
        .groupby("string_col")
        .aggregate(foo=lambda t: t.float_col.sum())
        .foo.count()
    )
    result = cs_compile.compile(expr, params={param: "20140101"})
    expected = """\
SELECT count(`foo`) AS `count`
FROM (
  SELECT `string_col`, sum(`float_col`) AS `foo`
  FROM (
    SELECT `float_col`, `timestamp_col`, `int_col`, `string_col`
    FROM functional_alltypes
    WHERE `timestamp_col` < @my_param
  ) t1
  GROUP BY 1
) t0"""
    assert result == expected


def test_scalar_param_string(alltypes, df):
    param = ibis.param("string")
    expr = alltypes[alltypes.string_col == param]

    string_value = "David"
    result = (
        expr.execute(params={param: string_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.string_col == string_value].sort_values("id").reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_scalar_param_int64(alltypes, df):
    param = ibis.param("int64")
    expr = alltypes[alltypes.int_col == param]

    int64_value = 22
    result = (
        expr.execute(params={param: int64_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.int_col == int64_value].sort_values("id").reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_scalar_param_double(alltypes, df):
    param = ibis.param("double")
    expr = alltypes[alltypes.numeric_col == param]

    double_value = 2.5
    result = (
        expr.execute(params={param: double_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.numeric_col == double_value].sort_values("id").reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_scalar_param_boolean(alltypes, df):
    param = ibis.param("boolean")
    expr = alltypes[(alltypes.bool_col == param)]

    bool_value = True
    result = (
        expr.execute(params={param: bool_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.bool_col == bool_value].sort_values("id").reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "timestamp_value", ["2019-04-12 12:09:00+00:00"],
)
def test_scalar_param_timestamp(alltypes, df, timestamp_value):
    param = ibis.param("timestamp")
    expr = (alltypes[alltypes.timestamp_col <= param]).select(["timestamp_col"])

    result = (
        expr.execute(params={param: timestamp_value})
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    value = pd.Timestamp(timestamp_value)
    expected = (
        df.loc[df.timestamp_col <= value, ["timestamp_col"]]
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "date_value",
    ["2009-02-12", datetime.date(2009, 2, 12), datetime.datetime(2009, 2, 12)],
)
def test_scalar_param_date(alltypes, df, date_value):
    param = ibis.param("date")
    expr = alltypes[alltypes.timestamp_col.cast("date") <= param]

    result = (
        expr.execute(params={param: date_value})
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    value = pd.Timestamp(date_value)
    value = pd.to_datetime(value).tz_localize("UTC")
    expected = (
        df.loc[df.timestamp_col.dt.normalize() <= value]
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_raw_sql(client):
    assert (client.raw_sql("SELECT 1")).fetchall()[0][0] == 1


def test_scalar_param_scope(alltypes):
    t = alltypes
    param = ibis.param("timestamp")
    mut = t.mutate(param=param).compile(params={param: "2017-01-01"})
    assert (
        mut
        == """\
SELECT *, @param AS `param`
FROM functional_alltypes"""
    )


def test_column_names(alltypes):
    assert "bigint_col" in alltypes.columns
    assert "string_col" in alltypes.columns


def test_column_names_in_schema(alltypes):
    assert "int_col" in alltypes.schema()


def test_exists_table(client):
    assert client.exists_table("functional_alltypes")
    assert not client.exists_table("footable")


def test_exists_database(client, database_id):
    assert client.exists_database(database_id)
    assert not client.exists_database("foodataset")


def test_set_database(client2, database_id2):
    client2.set_database(database_id2)
    tables = client2.list_tables()
    assert "awards" in tables


def test_exists_table_different_project(client):
    name = "functional_alltypes"
    assert client.exists_table(name)
    assert not client.exists_table("foobar")


def test_repeated_project_name(instance_id, database_id):
    con = connect(instance_id, database_id)
    assert "functional_alltypes" in con.list_tables()


def test_large_timestamp(client):
    huge_timestamp = datetime.datetime(2012, 10, 10, 10, 10, 10, 154117)
    expr = ibis.timestamp("2012-10-10 10:10:10.154117")
    result = client.execute(expr)

    huge_timestamp = (pd.to_datetime(huge_timestamp).tz_localize("UTC")).date()
    result = (result["tmp"][0]).date()
    assert result == huge_timestamp


def test_string_to_timestamp(client):
    timestamp = pd.Timestamp(
        datetime.datetime(year=2017, month=2, day=6), tz=pytz.timezone("UTC")
    )
    expr = ibis.literal("2017-02-06").to_timestamp("%F")
    result = client.execute(expr)
    result = result.iloc[:, 0][0]
    result = result.date()
    timestamp = timestamp.date()
    assert result == timestamp

    timestamp_tz = pd.Timestamp(
        datetime.datetime(year=2017, month=2, day=6, hour=5), tz=pytz.timezone("UTC"),
    )
    expr_tz = ibis.literal("2017-02-06").to_timestamp("%F", "America/New_York")
    result_tz = client.execute(expr_tz)
    result_tz = result_tz.iloc[:, 0][0]
    result_tz = result_tz.date()
    timestamp_tz = timestamp_tz.date()
    assert result_tz == timestamp_tz


def test_client_sql_query(client):
    expr = client.get_data_using_query("select * from functional_alltypes limit 20")
    result = expr
    expected = client.table("functional_alltypes").head(20).execute()
    tm.assert_frame_equal(result, expected)


def test_prevent_rewrite(alltypes):
    t = alltypes
    expr = (
        t.groupby(t.string_col)
        .aggregate(collected_double=t.numeric_col.collect())
        .pipe(ibis.prevent_rewrite)
        .filter(lambda t: t.string_col != "wat")
    )
    result = cs_compile.compile(expr)
    expected = """\
SELECT *
FROM (
  SELECT `string_col`, ARRAY_AGG(`numeric_col`) AS `collected_double`
  FROM functional_alltypes
  GROUP BY 1
) t0
WHERE `string_col` != 'wat'"""
    assert result == expected


@pytest.mark.parametrize(
    ("case", "dtype"),
    [
        (datetime.date(2017, 1, 1), dt.date),
        (pd.Timestamp("2017-01-01"), dt.date),
        ("2017-01-01", dt.date),
        (datetime.datetime(2017, 1, 1, 4, 55, 59), dt.timestamp),
        ("2017-01-01 04:55:59", dt.timestamp),
        (pd.Timestamp("2017-01-01 04:55:59"), dt.timestamp),
    ],
)
def test_day_of_week(client, case, dtype):
    date_var = ibis.literal(case, type=dtype)
    expr_index = date_var.day_of_week.index()
    result = client.execute(expr_index)
    result = result["tmp"][0]
    assert result == 6

    expr_name = date_var.day_of_week.full_name()
    result = client.execute(expr_name)
    result = result["tmp"][0]
    assert result == "Sunday"


def test_boolean_reducers(alltypes):
    b = alltypes.bool_col
    bool_avg = b.mean().execute()
    bool_avg = bool_avg.iloc[:, 0]
    bool_avg = bool_avg[0]
    assert type(bool_avg) == np.float64

    bool_sum = b.sum().execute()
    bool_sum = bool_sum.iloc[:, 0]
    bool_sum = bool_sum[0]
    assert type(bool_sum) == np.int64


def test_students_table_schema(students):
    assert students.schema() == ibis.schema(
        [
            ("id", dt.int64),
            ("name", dt.string),
            ("division", dt.int64),
            ("marks", dt.int64),
            ("exam", dt.string),
            ("overall_pointer", dt.float64),
            ("date_of_exam", dt.timestamp),
        ]
    )


def test_numeric_sum(students):
    t = students
    expr = t.overall_pointer.sum()
    result = expr.execute()
    result = (result.iloc[:, 0])[0]
    assert isinstance(result, np.float64)


def test_boolean_casting(alltypes):
    t = alltypes
    expr = t.groupby(k=t.string_col.nullif("1") == "9").count()
    result = expr.execute().set_index("k")
    count = result["count"]
    assert count.loc[False] == 6


def test_approx_median(alltypes):
    m = alltypes.month
    expected = m.execute().median()
    expected = expected[0]
    assert expected == 7.5


def test_struct_field_access(array_table):
    expr = array_table.string_col
    result = expr.execute()
    result = result.iloc[:, 0]
    expected = pd.Series(
        [["Peter", "David"], ["Raj", "Dev", "Neil"]], name="string_col",
    )

    tm.assert_series_equal(result, expected)


def test_array_index(array_table):
    expr = array_table.string_col[1]
    result = expr.execute()
    result = result.iloc[:, 0]
    expected = pd.Series(["David", "Dev",], name="tmp",)
    tm.assert_series_equal(result, expected)


def test_array_concat(array_table):
    c = array_table.string_col
    expr = c + c
    result = expr.execute()
    result = result.iloc[:, 0]
    expected = pd.Series(
        [
            ["Peter", "David", "Peter", "David"],
            ["Raj", "Dev", "Neil", "Raj", "Dev", "Neil"],
        ],
        name="tmp",
    )
    tm.assert_series_equal(result, expected)


def test_array_length(array_table):
    expr = array_table.string_col.length()
    result = expr.execute()
    result = result.iloc[:, 0]
    expected = pd.Series([2, 3], name="tmp")
    tm.assert_series_equal(result, expected)


def test_scalar_param_array(alltypes, df, client):
    expr = alltypes.sort_by("id").limit(1).numeric_col.collect()
    result = client.get_data_using_query(cs_compile.compile(expr))
    result = result["tmp"][0]
    expected = [df.sort_values("id").numeric_col.iat[0]]
    assert result == expected
