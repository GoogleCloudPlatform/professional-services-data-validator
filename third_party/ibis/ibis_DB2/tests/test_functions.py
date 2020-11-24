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

import operator
from datetime import datetime

import ibm_db_sa
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest
from pytest import param

import ibis
import ibis.config as config
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis import literal as L

sa = pytest.importorskip('sqlalchemy')
pytest.importorskip('ibm_db_sa')

pytestmark = pytest.mark.DB2


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
            lambda t: t.string_col.cast('double'),
            lambda at: sa.cast(at.c.string_col, ibm_db_sa.DOUBLE),
            id='string_to_double',
        ),
        param(
            lambda t: t.string_col.cast('float'),
            lambda at: sa.cast(at.c.string_col, ibm_db_sa.REAL),
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
        'index',
        'unnamed: 0',
        'id',
        'tinyint_col',
        'smallint_col',
        'int_col',
        'bigint_col',
        'float_col',
        'double_col',
        'date_string_col',
        'string_col',
        'timestamp_col',
        'YEAR',
        'MONTH',
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
    # expected2 = sa.func.timezone('UTC', sa.func.to_timestamp(at.c.int_col))

    assert str(translate(result1)) == str(expected1)
    # assert str(translate(result2)) == str(expected2)


@pytest.mark.parametrize(
    ('func', 'expected'),
    [
        param(operator.methodcaller('year'), 2015, id='year'),
        param(operator.methodcaller('month'), 9, id='month'),
        param(operator.methodcaller('day'), 1, id='day'),
        param(operator.methodcaller('hour'), 14, id='hour'),
        param(operator.methodcaller('minute'), 48, id='minute'),
        param(operator.methodcaller('second'), 5, id='second'),
        param(lambda x: x.day_of_week.index(), 3, id='day_of_week_index'),
        param(
            lambda x: x.day_of_week.full_name(),
            'Tuesday',
            id='day_of_week_full_name',
        ),
    ],
)
def test_simple_datetime_operations(con, func, expected, translate):
    value = L('2015-09-01 14:48:05.359').cast(dt.string)
    final_value = value.cast(dt.timestamp)
    assert con.execute(func(final_value)) == expected


@pytest.mark.parametrize(
    ('func', 'expected'),
    [param(operator.methodcaller('millisecond'), 359, id='millisecond')],
)
def test_simple_datetime_operation_millisecond(con, func, expected, translate):
    value = L('2015-09-01 14:48:05.359').cast(dt.string)
    final_value = value.cast(dt.timestamp)
    assert int(float(con.execute(func(final_value)))) == expected


@pytest.mark.parametrize(
    ('func', 'left', 'right', 'expected'),
    [
        param(operator.truediv, L(12), L(4), 3, id='truediv_no_remainder'),
        param(operator.pow, L(12), L(2), 144, id='pow'),
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
def test_binary_arithmetic_1(con, func, left, right, expected):
    expr = func(left, right)
    result = con.execute(expr)
    assert result == expected


@pytest.mark.parametrize(
    ('func', 'left', 'right', 'expected'),
    [
        param(
            operator.add, L(3).cast(dt.int16), L(4).cast(dt.int16), 7, id='add'
        ),
        param(operator.sub, L(3), L(4), -1, id='sub'),
        param(operator.mul, L(3), L(4), 12, id='mul'),
        param(operator.mod, L(12), L(5), 2, id='mod'),
    ],
)
def test_binary_arithmetic_2(con, func, left, right, expected):
    expr = func(left, right)
    result = round(float(con.execute(expr)))
    assert result == expected


def test_nullifzero(alltypes, df):
    expr = alltypes.limit(100).int_col.nullifzero()
    result = expr.execute()
    expected = df.int_col.replace(to_replace=[0], value=[np.nan])
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


def test_string_length(alltypes, df):
    expr = alltypes.limit(100).string_col.length()
    result = expr.execute()
    expected = df.string_col.str.len().astype('int32')
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    ('op', 'expected'),
    [
        param(operator.methodcaller('left', 3), 'foo', id='left'),
        param(operator.methodcaller('substr', 4), 'bar', id='right'),
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


'''def test_string_reverse(con):
    assert con.execute(L('foo').reverse()) == 'oof' '''


def test_string_upper(con):
    assert con.execute(L('foo').upper()) == 'FOO'


def test_string_lower(con):
    assert con.execute(L('FOO').lower()) == 'foo'


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
    expr = L('HI  HOW  ARE  YOU').re_replace('( ){2,}', ' ')
    assert con.execute(expr) == 'HI HOW ARE YOU'


def test_translate(con):
    expr = L('faab').translate('b', 'a')
    assert con.execute(expr) == 'fbbb'


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
        param(
            L('abcd').re_extract('([a-z]+)', 0), 'abcd', id='re_extract_whole'
        ),
        # valid group number but no match => empty string
        param(
            L('abcd').re_extract(r'(\d)', 0), None, id='re_extract_no_match'
        ),
        # match but not a valid group number => NULL
        param(L('abcd').re_extract('abcd', 3), None, id='re_extract_match'),
    ],
)
def test_regexp_extract(con, expr, expected):
    assert con.execute(expr) == expected


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(L('abcd').re_search('[a-z]'), True, id='re_search_match'),
        param(L('abcd').re_search(r'[\d]+'), False, id='re_search_no_match'),
        param(
            L('1222').re_search(r'[\d]+'), True, id='re_search_match_number'
        ),
    ],
)
def test_regexp(con, expr, expected):
    assert con.execute(expr) == expected


def test_fillna(pointer):
    expr = pointer.limit(100).marks.fillna(5)
    result = (expr.execute()).tolist()
    expected = [500, 460, 480, 390, 410, 410, 500, 460, 480, 390, 5, 5]
    assert result == expected


def test_numeric_builtins_work(alltypes, df):
    expr = alltypes.double_col.fillna(0)
    result = expr.execute()
    expected = df.double_col.fillna(0).astype(object)
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


def test_group_concat(alltypes, df):
    expr = alltypes.string_col.group_concat()
    result = expr.execute()
    expected = ','.join(df.string_col.dropna())
    assert result == expected


def test_distinct_aggregates(alltypes, df):
    expr = alltypes.limit(100).double_col.nunique()
    result = expr.execute()
    assert result == df.head(100).double_col.nunique()


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
FROM "FUNCTIONAL_ALLTYPES" AS t0 GROUP BY t0.string_col),
anon_2 AS
(SELECT t0.string_col AS string_col, sum(t0.double_col) AS metric
FROM "FUNCTIONAL_ALLTYPES" AS t0 GROUP BY t0.string_col),
anon_3 AS
(SELECT t0.string_col AS string_col, sum(t0.double_col) AS metric
FROM "FUNCTIONAL_ALLTYPES" AS t0 GROUP BY t0.string_col)
 (SELECT anon_1.string_col, anon_1.metric
FROM anon_1 {} SELECT anon_2.string_col, anon_2.metric
FROM anon_2) {} SELECT anon_3.string_col, anon_3.metric
FROM anon_3""".format(
        expected1, expected2
    )
    assert str(result) == expected


def test_interactive_repr_shows_error(alltypes):
    expr = alltypes.double_col.approx_median()
    with config.option_context('interactive', True):
        result = repr(expr)
    assert 'no translation rule' in result.lower()


def test_window_with_arithmetic(alltypes, df):
    t = alltypes
    w = ibis.window(order_by=t.timestamp_col)
    expr = t.mutate(new_col=ibis.row_number().over(w) / 2)

    df = (
        df[['timestamp_col']]
        .sort_values('timestamp_col')
        .reset_index(drop=True)
    )

    expected = df.assign(new_col=[float(int(x / 2.0)) for x in range(len(df))])
    result = expr['timestamp_col', 'new_col'].execute()
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize('func', ['sum', 'min', 'max'])
def test_cumulative_ordered_window(alltypes, func, df):
    t = alltypes
    df = df.sort_values('timestamp_col').reset_index(drop=True)
    window = ibis.cumulative_window(order_by=t.timestamp_col)
    f = getattr(t.double_col, func)
    expr = t.projection([(t.double_col - f().over(window)).name('double_col')])
    result = expr.execute().double_col
    result = result.astype('int64')
    expected = df.double_col - getattr(df.double_col, 'cum%s' % func)()
    expected = expected.astype('int64')

    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize('func', ['sum', 'min', 'max'])
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


def test_anonymous_aggregate(alltypes, df):
    t = alltypes
    expr = t[t.double_col > t.double_col.mean()]
    result = expr.execute()
    expected = df[df.double_col > df.double_col.mean()].reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_head(con):
    t = con.table('functional_alltypes')
    result = t.head().execute()
    expected = t.limit(5).execute()
    tm.assert_frame_equal(result, expected)


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
    result = str(sqla_expr.compile(compile_kwargs=dict(literal_binds=False)))
    expected = (
        "SELECT ntile(?) OVER (ORDER BY t0.double_col) - ? AS tmp \n"
        "FROM functional_alltypes AS t0"
    )
    assert result == expected


def test_null_column(alltypes):
    t = alltypes
    nrows = t.execute().shape[0]
    expr = t.mutate(na_column=ibis.NA).na_column
    result = expr.execute()
    tm.assert_series_equal(result, pd.Series([None] * nrows, name='na_column'))


@pytest.mark.parametrize('opname', ['neg'])
def test_not_and_negate_bool(con, opname, df):
    op = getattr(operator, opname)
    t = con.table('functional_alltypes').limit(10)
    expr = t.projection([op(t.MONTH).name('MONTH')])
    result = expr.execute().MONTH
    expected = op(df.head(10).MONTH)
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize('func', ['mean', 'sum', 'min', 'max'])
def test_simple_window(alltypes, func, df):
    t = alltypes
    f = getattr(t.double_col, func)
    df_f = getattr(df.double_col, func)
    result = (
        t.projection([(t.double_col - f()).name('double_col')])
        .execute()
        .double_col
    )
    expected = df.double_col - df_f()
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    'field',
    [
        'id',
        'double_col',
        'tinyint_col',
        'smallint_col',
        'int_col',
        'bigint_col',
        'float_col',
        'double_col',
        'MONTH',
        'YEAR',
    ],
)
def test_negate_non_boolean(con, field, df):
    t = con.table('functional_alltypes').limit(10)
    expr = t.projection([(-t[field]).name(field)])
    result = expr.execute()[field]
    expected = -df.head(10)[field]
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize('opname', ['sum', 'mean', 'min', 'max', 'std'])
def test_boolean_reduction(alltypes, opname, df):
    op = operator.methodcaller(opname)
    expr = op(alltypes.id)
    result = expr.execute()
    assert result == op(df.id)


@pytest.mark.parametrize(
    ('opname', 'expected'),
    [
        ('year', [2011, 2012, 2013, 2014, 2015, 2016]),
        ('month', [3, 4, 5, 6, 7, 8]),
        ('day', [11, 12, 13, 14, 15, 16]),
    ],
)
def test_date_extract_field(db, opname, expected):
    op = operator.methodcaller(opname)
    t = db.functional_alltypes
    expr = op(t.timestamp_col.cast('date')).distinct()
    result = expr.execute().astype(int).to_list()
    assert result == expected


def test_scalar_parameter(con):
    start = ibis.param(dt.date)
    end = ibis.param(dt.date)
    t = con.table('functional_alltypes')
    col = t.date_string_col.cast('date')
    expr = col.between(start, end)
    start_string, end_string = '2010-03-01', '2012-12-31'
    result = expr.execute(params={start: start_string, end: end_string})
    expected = col.between(start_string, end_string).execute()
    tm.assert_series_equal(result, expected)


def test_equals_function(alltypes, df):
    expr = alltypes.tinyint_col.__eq__(114)
    result = expr.execute()
    expected = df.tinyint_col.eq(114)
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


def test_not_equals_function(alltypes, df):
    expr = alltypes.tinyint_col.__ne__(114)
    result = expr.execute()
    expected = df.tinyint_col.ne(114)
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


def test_GreaterEqual_function(alltypes, df):
    expr = alltypes.date_string_col.__ge__('2012-05-05')
    result = expr.execute()
    expected = df.date_string_col.ge('2012-05-05')
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


def test_Greater_function(alltypes, df):
    expr = alltypes.date_string_col.__gt__('2012-05-05')
    result = expr.execute()
    expected = df.date_string_col.gt('2012-05-05')
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


def test_LessEqual_function(alltypes, df):
    expr = alltypes.timestamp_col.__le__('2013-05-13 16:12:14.280')
    result = expr.execute()
    expected = df.timestamp_col.le('2013-05-13 16:12:14.280')
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


def test_Less_function(alltypes, df):
    expr = alltypes.timestamp_col.__lt__('2013-05-13 16:12:14.280')
    result = expr.execute()
    expected = df.timestamp_col.lt('2013-05-13 16:12:14.280')
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


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
            lambda t, cond: t.double_col.sum(where=cond),
            lambda df, cond: df.double_col[cond].sum(),
            id='sum_where',
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
    df = df.head(table.execute().shape[0])
    cond = table.string_col.isin(['abc', 'lmn'])
    expr = func(table, cond)
    result = expr.execute()
    expected = pandas_func(df, cond.execute())
    np.testing.assert_allclose(result, expected)


def test_not_contains(alltypes, df):
    n = 100
    table = alltypes.limit(n)
    expr = table.string_col.notin(['abc', 'lmn'])
    result = expr.execute()
    expected = ~df.head(n).string_col.isin(['abc', 'lmn'])
    tm.assert_series_equal(result, expected, check_names=False)


@pytest.mark.parametrize(
    ('func', 'expected_index'), [('first', 1), ('last', 0)]
)
def test_first_last_value(alltypes, df, func, expected_index):
    col = alltypes.sort_by(ibis.desc(alltypes.string_col)).double_col
    method = getattr(col, func)
    expr = method()
    result = expr.execute().rename('double_col')
    expected = pd.Series(
        df.double_col.iloc[expected_index],
        index=pd.RangeIndex(len(df)),
        name='double_col',
    ).astype('object')
    tm.assert_series_equal(result, expected)


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


def test_Between_function(alltypes, df):
    expr = alltypes.date_string_col.between('2011-10-24', '2014-03-09')
    result = expr.execute()
    expected = df.date_string_col.between('2011-10-24', '2014-03-09')
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    'pattern', ['%Y%m%d', ' %X', ' %d ', ' %I ', ' %H%S ', ' %p ']
)
def test_strftime(con, pattern):
    value = L('2015-09-01 14:48:05.359').cast(dt.string)
    final_value = value.cast(dt.timestamp)
    raw_value = datetime(
        year=2015,
        month=9,
        day=1,
        hour=14,
        minute=48,
        second=5,
        microsecond=359000,
    )
    assert con.execute(final_value.strftime(pattern)) == raw_value.strftime(
        pattern
    )


def test_coalesce(pointer, pdf):
    expr = pointer.marks.coalesce(None, 0)
    result = expr.execute()
    expected = pdf.marks.fillna(0).astype(np.int64)
    expected.name = 'tmp'
    tm.assert_series_equal(result, expected)
