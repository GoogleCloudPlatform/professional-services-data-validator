# Copyright(C) 2020 Google Inc.
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

import operator
import os
import string
import warnings
from datetime import date, datetime

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest
from pytest import param

import ibis
import ibis.expr.datatypes as dt
from ibis import literal as L
import ibis.expr.types as ir
from ibis.expr.window import rows_with_max_lookback

sa = pytest.importorskip('sqlalchemy')
pytest.importorskip('snowflake')

pytestmark = pytest.mark.snowflake


@pytest.fixture
def guid(con):
    name = ibis.util.guid()
    try:
        yield name
    finally:
        con.drop_table(name, force=True)


@pytest.fixture
def guid2(con):
    name = ibis.util.guid()
    try:
        yield name
    finally:
        con.drop_table(name, force=True)


@pytest.mark.parametrize(
    ('left_func', 'right_func'),
    [
        param(
            lambda t: t.double_col.cast('int8'),
            lambda at: sa.cast(at.c.double_col, sa.SMALLINT),
            id='double_to_int8',
        ),
        param(
            lambda t: t.double_col.cast('int16'),
            lambda at: sa.cast(at.c.double_col, sa.SMALLINT),
            id='double_to_int16',
        ),
        param(
            lambda t: t.string_col.cast('double'), # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#double-double-precision-real
            lambda at: sa.cast(
                at.c.string_col, sa.DECIMAL
            ),
            id='string_to_double',
        ),
        param(
            lambda t: t.string_col.cast('float'),
            lambda at: sa.cast(at.c.string_col, sa.FLOAT),
            id='string_to_float',
        ),
        param(
            lambda t: t.string_col.cast('decimal'),
            lambda at: sa.cast(at.c.string_col, sa.NUMERIC(9, 0)),
            id='string_to_decimal_no_params',
        ),
        param(
            lambda t: t.string_col.cast('decimal(9, 3)'),
            lambda at: sa.cast(at.c.string_col, sa.NUMERIC(9, 3)),
            id='string_to_decimal_params',
        ),
    ],
)
def test_cast(alltypes, at, translate, left_func, right_func):
    left = left_func(alltypes)
    right = right_func(at)
    assert str(translate(left).compile()) == str(right.compile())


def test_date_cast(alltypes, at, translate):
    result = alltypes.date_string_col.cast('date')
    expected = sa.cast(at.c.date_string_col, sa.DATE)
    assert str(translate(result)) == str(expected)


@pytest.mark.parametrize(
    'column',
    [
        '`INDEX`',
        'Unnamed: 0',
        'id',
        'bool_col',
        'tinyint_col',
        'smallint_col',
        'int_col',
        'bigint_col',
        'float_col',
        'double_col',
        'date_string_col',
        'string_col',
        'timestamp_col',
        'year',
        'month',
    ],
)
def test_noop_cast(alltypes, at, translate, column):
    col = alltypes[column]
    result = col.cast(col.type())
    expected = at.c[column]
    assert result.equals(col)
    assert str(translate(result)) == str(expected)



def test_timestamp_cast_noop(alltypes, at, translate):
    result1 = alltypes.timestamp_col.cast('timestamp')
    result2 = alltypes.int_col.cast('timestamp')

    assert isinstance(result1, ir.TimestampColumn)
    assert isinstance(result2, ir.TimestampColumn)

    expected1 = at.c.timestamp_col

    assert str(translate(result1)) == "CAST({} AS TIMESTAMP)".format(str(expected1))



@pytest.mark.parametrize(
    ('func', 'expected'),
    [
        param(operator.methodcaller('year'), 2015, id='year'),
        param(operator.methodcaller('month'), 9, id='month'),
        param(operator.methodcaller('day'), 1, id='day'),
        param(operator.methodcaller('hour'), 14, id='hour'),
        param(operator.methodcaller('minute'), 48, id='minute'),
        param(operator.methodcaller('second'), 5, id='second'),
        param(lambda x: x.day_of_week.index(), 1, id='day_of_week_index'),
        param(
            lambda x: x.day_of_week.full_name(),
            'Tue',
            id='day_of_week_full_name',
        ),
    ],
)
def test_simple_datetime_operations(con, func, expected, translate):
    value= L('2015-09-01 14:48:05.359').cast(dt.string).cast(dt.timestamp)
    assert con.execute(func(value)) == expected



@pytest.mark.parametrize(
    ('func', 'left', 'right', 'expected'),
    [
        param(operator.add, L(3), L(4), 7, id='add'),
        param(operator.sub, L(3), L(4), -1, id='sub'),
        param(operator.mul, L(3), L(4), 12, id='mul'),
        param(operator.truediv, L(12), L(4), 3, id='truediv_no_remainder'),
        param(operator.pow, L(12), L(2), 144, id='pow'),
        param(operator.mod, L(12), L(5), 2, id='mod'),
        param(operator.truediv, L(7), L(2), 3.5, id='truediv_remainder'),
        param(operator.floordiv, L(7), L(2), 3, id='floordiv'),
        param(
            lambda x, y: x.floordiv(y), L(7), 2, 3, id='floordiv_no_literal'
        ),
        param(
            lambda x, y: x.rfloordiv(y), L(2), 7, 3, id='rfloordiv_no_literal'
        ),
    ],
)
def test_binary_arithmetic(con, func, left, right, expected):
    expr = func(left, right)
    result = con.execute(expr)
    assert result == expected


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        param(L('foo_bar'), 'VARCHAR', id='text'),
        param(L(5), 'INTEGER', id='integer'),
        param(ibis.NA, None, id='null'),
        # TODO(phillipc): should this really be double?
        param(L(1.2345), 'DECIMAL', id='numeric'),
        param(
            L('2015-09-01 14:48:05.359').cast(dt.string).cast(dt.timestamp),
            'TIMESTAMP_NTZ',
            id='timestamp_without_time_zone',
        )
    ],
)
def test_typeof(con, value, expected):
    assert con.execute(value.typeof()) == expected


@pytest.mark.parametrize(('value', 'expected'), [(0, None), (5.5, 5.5)])
def test_nullifzero(con, value, expected):
    assert con.execute(L(value).nullifzero()) == expected


@pytest.mark.parametrize(('value', 'expected'), [('foo_bar', 7), ('', 0)])
def test_string_length(con, value, expected):
    assert con.execute(L(value).length()) == expected


@pytest.mark.parametrize(
    ('op', 'expected'),
    [
        param(operator.methodcaller('left', 3), 'foo', id='left'),
        param(operator.methodcaller('right', 3), 'bar', id='right'),
        param(operator.methodcaller('substr', 0, 3), 'foo', id='substr_0_3'),
        param(operator.methodcaller('substr', 4, 3), 'bar', id='substr_4, 3'),
        param(operator.methodcaller('substr', 1), 'oo_bar', id='substr_1'),
    ],
)
def test_string_substring(con, op, expected):
    value = L('foo_bar')
    assert con.execute(op(value)) == expected


@pytest.mark.parametrize(
    ('opname', 'expected'),
    [('lstrip', 'foo   '), ('rstrip', '   foo'), ('strip', 'foo')],
)
def test_string_strip(con, opname, expected):
    op = operator.methodcaller(opname)
    value = L('   foo   ')
    assert con.execute(op(value)) == expected


@pytest.mark.parametrize(
    ('opname', 'count', 'char', 'expected'),
    [('lpad', 6, ' ', '   foo'), ('rpad', 6, ' ', 'foo   ')],
)
def test_string_pad(con, opname, count, char, expected):
    op = operator.methodcaller(opname, count, char)
    value = L('foo')
    assert con.execute(op(value)) == expected


def test_string_reverse(con):
    assert con.execute(L('foo').reverse()) == 'oof'


def test_string_upper(con):
    assert con.execute(L('foo').upper()) == 'FOO'


def test_string_lower(con):
    assert con.execute(L('FOO').lower()) == 'foo'


@pytest.mark.parametrize(
    ('haystack', 'needle', 'expected'),
    [
        ('foobar', 'bar', True),
        ('foobar', 'foo', True),
        ('foobar', 'baz', False),
        ('100%', '%', True),
        ('a_b_c', '_', True),
    ],
)
def test_string_contains(con, haystack, needle, expected):
    value = L(haystack)
    expr = value.contains(needle)
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('value', 'expected'),
    [('foo bar foo', 'Foo Bar Foo'), ('foobar Foo', 'Foobar Foo')],
)
def test_capitalize(con, value, expected):
    assert con.execute(L(value).capitalize()) == expected


def test_repeat(con):
    expr = L('bar ').repeat(3)
    assert con.execute(expr) == 'bar bar bar '


def test_re_replace(con):
    expr = L('fudge|||chocolate||candy').re_replace('\\|{2,3}', ', ')
    assert con.execute(expr) == 'fudge, chocolate, candy'


def test_translate(con):
    expr = L('faab').translate('a', 'b')
    assert con.execute(expr) == 'fbbb'


@pytest.mark.parametrize(
    ('raw_value', 'expected'), [('a', 0), ('b', 1), ('d', -1), (None, 3)]
)
def test_find_in_set(demonstration, con, raw_value, expected):
    value = L('a', dt.string)
    haystack = demonstration.array1
    expr = value.find_in_set(haystack)
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('raw_value', 'opname', 'expected'),
    [
        (None, 'isnull', True),
        (1, 'isnull', False),
        (None, 'notnull', False),
        (1, 'notnull', True),
    ],
)
def test_isnull_notnull(con, raw_value, opname, expected):
    lit = L(raw_value)
    op = operator.methodcaller(opname)
    expr = op(lit)
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(L('foobar').find('bar'), 3, id='find_pos'),
        param(L('foobar').find('baz'), -1, id='find_neg'),
        param(L('foobar').like('%bar'), True, id='like_left_pattern'),
        param(L('foobar').like('foo%'), True, id='like_right_pattern'),
        param(L('foobar').like('%baz%'), False, id='like_both_sides_pattern'),
        param(L('foobar').like(['%bar']), True, id='like_list_left_side'),
        param(L('foobar').like(['foo%']), True, id='like_list_right_side'),
        param(L('foobar').like(['%baz%']), False, id='like_list_both_sides'),
        param(
            L('foobar').like(['%bar', 'foo%']), True, id='like_list_multiple'
        ),
        param(L('foobarfoo').replace('foo', 'H'), 'HbarH', id='replace'),
        param(L('a').ascii_str(), ord('a'), id='ascii_str'),
    ],
)
def test_string_functions(con, expr, expected):
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(L('San Francisco').re_search('San* [fF].*'), True, id='re_search_match'),
        param(L('abcd').re_search(r'[\d]+'), False, id='re_search_no_match'),
        param(
            L('1222').re_search(r'[\d]+'), True, id='re_search_match_number'
        ),
    ],
)
def test_regexp(con, expr, expected):
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(
            L('abcd').re_extract('([a-z]+)', 1), 'abcd', id='re_extract_whole'
        ),
        param(
            L('How are you doing today?').re_extract('\\b\\S*o\\S*\\b', 3), 'you', id='re_extract_first'
        ),
        # valid group number but no match => NULL for snowflake
        param(L('abcd').re_extract(r'(\d)', 1), None, id='re_extract_no_match'),
        # match but not a valid group number => NULL
        param(L('abcd').re_extract('abcd', 3), None, id='re_extract_match'),
    ],
)
def test_regexp_extract(con, expr, expected):
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(ibis.NA.fillna(5), 5, id='filled'),
        param(L(5).fillna(10), 5, id='not_filled'),
        param(L(5).nullif(5), None, id='nullif_null'),
        param(L(10).nullif(5), 10, id='nullif_not_null'),
    ],
)
def test_fillna_nullif(con, expr, expected):
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(ibis.coalesce(5, None, 4), 5, id='first'),
        param(ibis.coalesce(ibis.NA, 4, ibis.NA), 4, id='second'),
        param(ibis.coalesce(ibis.NA, ibis.NA, 3.14), 3.14, id='third'),
    ],
)
def test_coalesce(con, expr, expected):
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(ibis.coalesce(ibis.NA, ibis.NA), None, id='all_null'),
        param(
            ibis.coalesce(ibis.NA, ibis.NA, ibis.NA.cast('double')),
            None,
            id='all_nulls_with_one_cast',
        ),
        param(
            ibis.coalesce(
                ibis.NA.cast('int8'),
                ibis.NA.cast('int8'),
                ibis.NA.cast('int8'),
            ),
            None,
            id='all_nulls_with_all_cast',
        ),
    ],
)
def test_coalesce_all_na(con, expr, expected):
    assert con.execute(expr) == expected


def test_numeric_builtins_work(alltypes, df):
    expr = alltypes.double_col.fillna(0)
    result = expr.execute()
    expected = df.double_col.fillna(0)
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    ('op', 'pandas_op'),
    [
        param(
            lambda t: (t.double_col > 20).ifelse(10, -20),
            lambda df: pd.Series(
                np.where(df.double_col > 20, 10, -20), dtype='int8'
            ),
            id='simple',
        ),
        param(
            lambda t: (t.double_col > 20).ifelse(10, -20).abs(),
            lambda df: pd.Series(
                np.where(df.double_col > 20, 10, -20), dtype='int8'
            ).abs(),
            id='abs',
        ),
    ],
)
def test_ifelse(alltypes, df, op, pandas_op):
    expr = op(alltypes)
    result = expr.execute()
    result.name = None
    expected = pandas_op(df)
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    ('distinct1', 'distinct2', 'expected1', 'expected2'),
    [
        (True, True, 'UNION', 'UNION'),
        (True, False, 'UNION', 'UNION ALL'),
        (False, True, 'UNION ALL', 'UNION'),
        (False, False, 'UNION ALL', 'UNION ALL'),
    ],
)
def test_union_cte(alltypes, distinct1, distinct2, expected1, expected2):
    t = alltypes
    expr1 = t.group_by(t.string_col).aggregate(metric=t.double_col.sum())
    expr2 = expr1.view()
    expr3 = expr1.view()
    expr = expr1.union(expr2, distinct=distinct1).union(
        expr3, distinct=distinct2
    )
    result = '\n'.join(
        map(
            lambda line: line.rstrip(),  # strip trailing whitespace
            str(
                expr.compile().compile(compile_kwargs=dict(literal_binds=True))
            ).splitlines(),
        )
    )
    expected = """\
WITH anon_1 AS
(SELECT t0.string_col AS string_col, sum(t0.double_col) AS metric
FROM functional_alltypes AS t0 GROUP BY t0.string_col),
anon_2 AS
(SELECT t0.string_col AS string_col, sum(t0.double_col) AS metric
FROM functional_alltypes AS t0 GROUP BY t0.string_col),
anon_3 AS
(SELECT t0.string_col AS string_col, sum(t0.double_col) AS metric
FROM functional_alltypes AS t0 GROUP BY t0.string_col)
 (SELECT anon_1.string_col, anon_1.metric
FROM anon_1 {} SELECT anon_2.string_col, anon_2.metric
FROM anon_2) {} SELECT anon_3.string_col, anon_3.metric
FROM anon_3""".format(
        expected1, expected2
    )
    assert str(result) == expected


@pytest.mark.parametrize(
    ('func', 'pandas_func'),
    [
        param(
            lambda t, cond: t.bool_col.count(),
            lambda df, cond: df.bool_col.count(),
            id='count',
        ),
        param(
            lambda t, cond: t.double_col.sum(),
            lambda df, cond: df.double_col.sum(),
            id='sum',
        ),
        param(
            lambda t, cond: t.double_col.mean(),
            lambda df, cond: df.double_col.mean(),
            id='mean',
        ),
        param(
            lambda t, cond: t.double_col.min(),
            lambda df, cond: df.double_col.min(),
            id='min',
        ),
        param(
            lambda t, cond: t.double_col.max(),
            lambda df, cond: df.double_col.max(),
            id='max',
        ),
        param(
            lambda t, cond: t.double_col.var(),
            lambda df, cond: df.double_col.var(),
            id='var',
        ),
        param(
            lambda t, cond: t.double_col.std(),
            lambda df, cond: df.double_col.std(),
            id='std',
        ),
        param(
            lambda t, cond: t.double_col.var(how='sample'),
            lambda df, cond: df.double_col.var(ddof=1),
            id='samp_var',
        ),
        param(
            lambda t, cond: t.double_col.std(how='pop'),
            lambda df, cond: df.double_col.std(ddof=0),
            id='pop_std',
        ),
        param(
            lambda t, cond: t.bool_col.count(where=cond),
            lambda df, cond: df.bool_col[cond].count(),
            id='count_where',
        ),
        param(
            lambda t, cond: t.double_col.mean(where=cond),
            lambda df, cond: df.double_col[cond].mean(),
            id='mean_where',
        ),
        param(
            lambda t, cond: t.double_col.min(where=cond),
            lambda df, cond: df.double_col[cond].min(),
            id='min_where',
        ),
        param(
            lambda t, cond: t.double_col.max(where=cond),
            lambda df, cond: df.double_col[cond].max(),
            id='max_where',
        ),
        param(
            lambda t, cond: t.double_col.var(where=cond),
            lambda df, cond: df.double_col[cond].var(),
            id='var_where',
        ),
        param(
            lambda t, cond: t.double_col.std(where=cond),
            lambda df, cond: df.double_col[cond].std(),
            id='std_where',
        ),
        param(
            lambda t, cond: t.double_col.var(where=cond, how='sample'),
            lambda df, cond: df.double_col[cond].var(),
            id='samp_var_where',
        ),
        param(
            lambda t, cond: t.double_col.std(where=cond, how='pop'),
            lambda df, cond: df.double_col[cond].std(ddof=0),
            id='pop_std_where',
        ),
    ],
)
def test_aggregations(alltypes, df, func, pandas_func):
    table = alltypes.limit(100)
    df = df.head(table.count().execute())

    cond = table.string_col.isin(['1', '7'])
    expr = func(table, cond)
    result = expr.execute()
    expected = pandas_func(df, cond.execute())

    np.testing.assert_allclose(result, expected)


def test_not_contains(alltypes, df):
    n = 100
    table = alltypes.limit(n)
    expr = table.string_col.notin(['1', '7'])
    result = expr.execute()
    expected = ~df.head(n).string_col.isin(['1', '7'])
    tm.assert_series_equal(result, expected, check_names=False)


def test_group_concat(alltypes, df):
    expr = alltypes.string_col.group_concat()
    result = expr.execute()
    expected = ','.join(df.string_col.dropna())
    assert result == expected


def test_distinct_aggregates(alltypes, df):
    expr = alltypes.limit(100).double_col.nunique()
    result = expr.execute()
    assert result == df.head(100).double_col.nunique()


def test_not_exists(alltypes, df):
    t = alltypes
    t2 = t.view()

    expr = t[~((t.string_col == t2.string_col).any())]
    result = expr.execute()

    left, right = df, t2.execute()
    expected = left[left.string_col != right.string_col]

    tm.assert_frame_equal(
        result, expected, check_index_type=False, check_dtype=False
    )


def test_subquery(alltypes, df):
    t = alltypes

    expr = (
        t.mutate(d=t.double_col.fillna(0))
        .limit(1000)
        .group_by('string_col')
        .size()
    )
    result = expr.execute().sort_values('string_col').reset_index(drop=True)
    expected = (
        df.assign(d=df.double_col.fillna(0))
        .head(1000)
        .groupby('string_col')
        .string_col.count()
        .reset_index(name='count')
        .sort_values('string_col')
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize('func', ['mean', 'sum'])
def test_rolling_window(alltypes, func, df):
    t = alltypes
    df = (
        df[['double_col', 'timestamp_col']]
        .sort_values('timestamp_col')
        .reset_index(drop=True)
    )
    window = ibis.window(order_by=t.timestamp_col, preceding=6, following=0)
    f = getattr(t.double_col, func)
    df_f = getattr(df.double_col.rolling(7, min_periods=0), func)
    result = (
        t.projection([f().over(window).name('double_col')])
        .execute()
        .double_col
    )
    expected = df_f()
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize('func', ['min', 'max'])
def test_cumulative_ordered_window(alltypes, func, df):
    t = alltypes
    df = df.sort_values('timestamp_col').reset_index(drop=True)
    window = ibis.cumulative_window(order_by=t.timestamp_col)
    f = getattr(t.double_col, func)
    expr = t.projection([(t.double_col - f().over(window)).name('double_col')])
    result = expr.execute().double_col
    expected = df.double_col - getattr(df.double_col, 'cum%s' % func)()
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize('func', ['min', 'max'])
def test_cumulative_partitioned_ordered_window(alltypes, func, df):
    t = alltypes
    df = df.sort_values(['string_col', 'timestamp_col']).reset_index(drop=True)
    window = ibis.cumulative_window(
        order_by=t.timestamp_col, group_by=t.string_col
    )
    f = getattr(t.double_col, func)
    expr = t.projection([(t.double_col - f().over(window)).name('double_col')])
    result = expr.execute().double_col
    method = operator.methodcaller('cum{}'.format(func))
    expected = df.groupby(df.string_col).double_col.transform(
        lambda c: c - method(c)
    )
    tm.assert_series_equal(result, expected)


def test_null_column(alltypes):
    t = alltypes
    nrows = t.count().execute()
    expr = t.mutate(na_column=ibis.NA).na_column
    result = expr.execute()
    tm.assert_series_equal(result, pd.Series([None] * nrows, name='na_column'))


def test_null_column_union(alltypes, df):
    t = alltypes
    s = alltypes[['double_col']].mutate(string_col=ibis.NA.cast('string'))
    expr = t[['double_col', 'string_col']].union(s)
    result = expr.execute()
    nrows = t.count().execute()
    expected = pd.concat(
        [
            df[['double_col', 'string_col']],
            pd.concat(
                [
                    df[['double_col']],
                    pd.DataFrame({'string_col': [None] * nrows}),
                ],
                axis=1,
            ),
        ],
        axis=0,
        ignore_index=True,
    )
    tm.assert_frame_equal(result, expected)


def test_window_with_arithmetic(alltypes, df):
    t = alltypes
    w = ibis.window(order_by=t.timestamp_col)
    expr = t.mutate(new_col=ibis.row_number().over(w) / 2)

    df = (
        df[['timestamp_col']]
        .sort_values('timestamp_col')
        .reset_index(drop=True)
    )
    expected = df.assign(new_col=[x / 2.0 for x in range(len(df))])
    result = expr['timestamp_col', 'new_col'].execute()
    tm.assert_frame_equal(result, expected)


def test_head(con):
    t = con.table('functional_alltypes')
    result = t.head().execute()
    expected = t.limit(5).execute()
    tm.assert_frame_equal(result, expected)


def test_identical_to(con, df):
    # TODO: abstract this testing logic out into parameterized fixtures
    t = con.table('functional_alltypes')
    dt = df[['tinyint_col', 'double_col']]
    expr = t.tinyint_col.identical_to(t.double_col)
    result = expr.execute()
    expected = (dt.tinyint_col.isnull() & dt.double_col.isnull()) | (
        dt.tinyint_col == dt.double_col
    )
    expected.name = result.name
    tm.assert_series_equal(result, expected)


def test_rank(con):
    t = con.table('functional_alltypes')
    expr = t.double_col.rank()
    sqla_expr = expr.compile()
    result = str(sqla_expr.compile(compile_kwargs=dict(literal_binds=True)))
    expected = (
        "SELECT rank() OVER (ORDER BY t0.double_col) - 1 AS tmp \n"
        "FROM functional_alltypes AS t0"
    )
    assert result == expected


def test_percent_rank(con):
    t = con.table('functional_alltypes')
    expr = t.double_col.percent_rank()
    sqla_expr = expr.compile()
    result = str(sqla_expr.compile(compile_kwargs=dict(literal_binds=True)))
    expected = (
        "SELECT percent_rank() OVER (ORDER BY t0.double_col) AS "
        "tmp \nFROM functional_alltypes AS t0"
    )
    assert result == expected


def test_ntile(con):
    t = con.table('functional_alltypes')
    expr = t.double_col.ntile(7)
    sqla_expr = expr.compile()
    result = str(sqla_expr.compile(compile_kwargs=dict(literal_binds=True)))
    expected = (
        "SELECT ntile(7) OVER (ORDER BY t0.double_col) - 1 AS tmp \n"
        "FROM functional_alltypes AS t0"
    )
    assert result == expected


@pytest.mark.parametrize('opname', ['invert', 'neg'])
def test_not_and_negate_bool(con, opname, df):
    op = getattr(operator, opname)
    t = con.table('functional_alltypes').limit(10)
    expr = t.projection([op(t.bool_col).name('bool_col')])
    result = expr.execute().bool_col
    expected = op(df.head(10).bool_col)
    tm.assert_series_equal(result, expected)
#
#
# @pytest.mark.parametrize(
#     'field',
#     [
#         'tinyint_col',
#         'smallint_col',
#         'int_col',
#         'bigint_col',
#         'float_col',
#         'double_col',
#         'year',
#         'month',
#     ],
# )
# def test_negate_non_boolean(con, field, df):
#     t = con.table('functional_alltypes').limit(10)
#     expr = t.projection([(-t[field]).name(field)])
#     result = expr.execute()[field]
#     expected = -df.head(10)[field]
#     tm.assert_series_equal(result, expected)


def test_negate_boolean(con, df):
    t = con.table('functional_alltypes').limit(10)
    expr = t.projection([(-t.bool_col).name('bool_col')])
    result = expr.execute().bool_col
    expected = -df.head(10).bool_col
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    ('opname', 'expected'),
    [
        ('year', {2010,}),
        ('month', {11,}),
        ('day', {1,}),
    ],
)
def test_date_extract_field(db, opname, expected):
    op = operator.methodcaller(opname)
    t = db.functional_alltypes
    expr = op(t.timestamp_col.cast('date')).distinct()
    result = expr.execute().astype(int)
    assert set(result) == expected


@pytest.mark.parametrize('opname', ['sum', 'mean', 'min', 'max', 'std', 'var'])
def test_boolean_reduction(alltypes, opname, df):
    op = operator.methodcaller(opname)
    expr = op(alltypes.bool_col)
    result = expr.execute()
    assert str(result) == str(op(df.bool_col))


@pytest.mark.parametrize(
    ('left', 'right', 'type'),
    [
        param(L('2017-04-01'), date(2017, 4, 2), dt.date, id='ibis_date'),
        param(date(2017, 4, 2), L('2017-04-01'), dt.date, id='python_date'),
        param(
            L('2017-04-01 01:02:33'),
            datetime(2017, 4, 1, 1, 3, 34),
            dt.timestamp,
            id='ibis_timestamp',
        ),
        param(
            datetime(2017, 4, 1, 1, 3, 34),
            L('2017-04-01 01:02:33'),
            dt.timestamp,
            id='python_datetime',
        ),
    ],
)
@pytest.mark.parametrize('opname', ['eq', 'ne', 'lt', 'le', 'gt', 'ge'])
def test_string_temporal_compare(con, opname, left, right, type):
    op = getattr(operator, opname)
    expr = op(left, right)
    result = con.execute(expr)
    left_raw = con.execute(L(left).cast(type))
    right_raw = con.execute(L(right).cast(type))
    expected = op(left_raw, right_raw)
    assert result == expected


def test_scalar_parameter(con):
    start = ibis.param(dt.date)
    end = ibis.param(dt.date)
    t = con.table('functional_alltypes')
    col = t.date_string_col.cast('date')
    expr = col.between(start, end)
    start_string, end_string = '2009-03-01', '2010-07-03'
    result = expr.execute(params={start: start_string, end: end_string})
    expected = col.between(start_string, end_string).execute()
    tm.assert_series_equal(result, expected)
