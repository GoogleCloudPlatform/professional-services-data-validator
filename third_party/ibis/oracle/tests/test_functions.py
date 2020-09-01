import operator
from datetime import date, datetime

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
from ibis.expr.window import rows_with_max_lookback

sa = pytest.importorskip('sqlalchemy')
pytest.importorskip('cx_Oracle')

pytestmark = pytest.mark.oracle


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
            lambda at: sa.cast(
                at.c.string_col, sa.dialects.oracle.DOUBLE_PRECISION
            ),
            id='string_to_double',
        ),
        param(
            lambda t: t.string_col.cast('float'),
            lambda at: sa.cast(at.c.string_col, sa.dialects.oracle.FLOAT),
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
    # See GH #592
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
    ],
)
def test_simple_datetime_operations(con, func, expected, translate):
    value = ibis.timestamp('2015-09-01 14:48:05.359')
    assert con.execute(func(value)) == expected


@pytest.mark.parametrize(
    ('query1', 'expected'),
    [
        ("select Extract(hour from TO_TIMESTAMP(:p1,:p2)) from dual", 14),
        ("select Extract(minute from TO_TIMESTAMP(:p1,:p2)) from dual", 10),
        (
            "select Extract(second from TO_TIMESTAMP(:p1,:p2)) from dual",
            10.123,
        ),
        (
            "select to_number(to_char(TO_TIMESTAMP(:p1, :p2),'FF3'))from dual",
            123,
        ),
    ],
)
def test_simple_datetime_operations_1(con, query1, expected):
    params = {'p1': '10-02-02 14:10:10.123000', 'p2': 'DD-MM-RR HH24:MI:SS.FF'}
    ans = 0
    result = con.con.execute(query1, params)
    for i in result:
        ans = i[0]
    assert ans == expected


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
        param(L('foo_bar'), '1 ', id='text'),
        param(L(5), '2 ', id='integer'),
        # TODO(phillipc): should this really be double?
        param(L(1.2345), '2 ', id='numeric'),
        param(
            L(
                datetime(
                    2015,
                    9,
                    1,
                    hour=14,
                    minute=48,
                    second=5,
                    microsecond=359000,
                )
            ),
            '12',
            id='timestamp_without_time_zone',
        ),
        param(L(date(2015, 9, 1)), '12', id='date'),
    ],
)
def test_typeof(con, value, expected):
    ans = con.execute(value.typeof())
    literal_ans = L(ans)
    op = operator.methodcaller('substr', 4, 2)
    result = con.execute(op(literal_ans))
    assert result == expected


@pytest.mark.parametrize(('value', 'expected'), [(0, None), (5.5, 5.5)])
def test_nullifzero(con, value, expected):
    assert con.execute(L(value).nullifzero()) == expected


@pytest.mark.parametrize(('value', 'expected'), [('foo_bar', 7), ('', None)])
def test_string_length(con, value, expected):
    assert con.execute(L(value).length()) == expected


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


def test_string_reverse(con):
    assert con.execute(L('foo').reverse()) == 'oof'


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
    word = 'foo '
    repeat_count = 4
    word_len = len(word)
    word_literal = L(word)
    op = operator.methodcaller('rpad', word_len * repeat_count, word_literal)
    result = con.execute(op(word_literal))
    assert result == 'foo foo foo foo '


def test_re_replace(con):
    expr = L(
        'This linecontains    more  than one   spacing  between words'
    ).re_replace('( ){2,}', ' ')
    assert (
        con.execute(expr)
        == 'This line contains more than one spacing between words'
    )


def test_translate(con):
    expr = L('faab').translate('a', 'b')
    assert con.execute(expr) == 'fbbb'


@pytest.mark.parametrize(
    ('expr', 'expected'),
    [
        param(L('foobar').find('bar'), 4, id='find_pos'),
        param(L('foobar').find('baz'), 0, id='find_neg'),
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
    ('func', 'pandas_func'),
    [
        # tier and histogram
        param(
            lambda d: d.bucket([0, 10, 25, 50, 100]),
            lambda s: pd.cut(
                s, [0, 10, 25, 50, 100], right=False, labels=False
            ),
            id='include_over_false',
        ),
        param(
            lambda d: d.bucket([0, 10, 25, 50], include_over=True),
            lambda s: pd.cut(
                s, [0, 10, 25, 50, np.inf], right=False, labels=False
            ),
            id='include_over_true',
        ),
        param(
            lambda d: d.bucket([0, 10, 25, 50], close_extreme=False),
            lambda s: pd.cut(s, [0, 10, 25, 50], right=False, labels=False),
            id='close_extreme_false',
        ),
        param(
            lambda d: d.bucket(
                [0, 10, 25, 50], closed='right', close_extreme=False
            ),
            lambda s: pd.cut(
                s,
                [0, 10, 25, 50],
                include_lowest=False,
                right=True,
                labels=False,
            ),
            id='closed_right',
        ),
        param(
            lambda d: d.bucket([10, 25, 50, 100], include_under=True),
            lambda s: pd.cut(
                s, [0, 10, 25, 50, 100], right=False, labels=False
            ),
            id='include_under_true',
        ),
    ],
)
def test_bucket(alltypes, df, func, pandas_func):
    expr = func(alltypes.double_col)
    result = expr.execute()
    expected = pandas_func(df.double_col).astype('category')
    tm.assert_series_equal(result, expected, check_names=False)


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
FROM functional_alltypes t0 GROUP BY t0.string_col),
anon_2 AS
(SELECT t0.string_col AS string_col, sum(t0.double_col) AS metric
FROM functional_alltypes t0 GROUP BY t0.string_col),
anon_3 AS
(SELECT t0.string_col AS string_col, sum(t0.double_col) AS metric
FROM functional_alltypes t0 GROUP BY t0.string_col)
 (SELECT anon_1.string_col, anon_1.metric
FROM anon_1 {} SELECT anon_2.string_col, anon_2.metric
FROM anon_2) {} SELECT anon_3.string_col, anon_3.metric
FROM anon_3""".format(
        expected1, expected2
    )
    assert str(result) == expected


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


def test_interactive_repr_shows_error(alltypes):

    expr = alltypes.double_col.approx_median()

    with config.option_context('interactive', True):
        result = repr(expr)

    assert 'no translation rule' in result.lower()


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


@pytest.mark.parametrize('func', ['mean', 'sum', 'min', 'max'])
def test_rolling_window(alltypes, func, df):
    t = alltypes
    df = (
        df[['month', 'timestamp_col']]
        .sort_values('timestamp_col')
        .reset_index(drop=True)
    )
    window = ibis.window(order_by=t.timestamp_col, preceding=6, following=0)
    f = getattr(t.month, func)
    df_f = getattr(df.month.rolling(7, min_periods=0), func)
    result = t.projection([f().over(window).name('month')]).execute().month
    expected = df_f()
    result = result.astype('int64')
    expected = expected.astype('int64')
    tm.assert_series_equal(result, expected)


def test_rolling_window_with_mlb(alltypes):
    t = alltypes
    window = ibis.trailing_window(
        preceding=rows_with_max_lookback(3, ibis.interval(days=5)),
        order_by=t.timestamp_col,
    )
    expr = t['month'].sum().over(window)
    with pytest.raises(NotImplementedError):
        expr.execute()


@pytest.mark.parametrize('func', ['mean', 'sum', 'min', 'max'])
def test_partitioned_window(alltypes, func, df):
    t = alltypes
    window = ibis.window(
        group_by=t.string_col,
        order_by=t.timestamp_col,
        preceding=6,
        following=0,
    )

    def roller(func):
        def rolled(df):
            torder = df.sort_values('timestamp_col')
            rolling = torder.double_col.rolling(7, min_periods=0)
            return getattr(rolling, func)()

        return rolled

    f = getattr(t.double_col, func)
    expr = f().over(window).name('double_col')
    result = t.projection([expr]).execute().double_col
    expected = (
        df.groupby('string_col').apply(roller(func)).reset_index(drop=True)
    )
    result = result.astype('int64')
    expected = expected.astype('int64')
    tm.assert_series_equal(result, expected)


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


def test_null_column(alltypes):
    t = alltypes
    nrows = t.count().execute()
    expr = t.mutate(na_column=ibis.NA).na_column
    result = expr.execute()
    tm.assert_series_equal(result, pd.Series([None] * nrows, name='na_column'))


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
        "FROM functional_alltypes t0"
    )
    assert result == expected


def test_percent_rank(con):
    t = con.table('functional_alltypes')
    expr = t.double_col.percent_rank()
    sqla_expr = expr.compile()
    result = str(sqla_expr.compile(compile_kwargs=dict(literal_binds=True)))
    expected = (
        "SELECT percent_rank() OVER (ORDER BY t0.double_col) AS "
        "tmp \nFROM functional_alltypes t0"
    )
    assert result == expected


def test_ntile(con):
    t = con.table('functional_alltypes')
    expr = t.double_col.ntile(7)
    sqla_expr = expr.compile()
    result = str(sqla_expr.compile(compile_kwargs=dict(literal_binds=True)))
    expected = (
        "SELECT ntile(7) OVER (ORDER BY t0.double_col) - 1 AS tmp \n"
        "FROM functional_alltypes t0"
    )
    assert result == expected


@pytest.mark.parametrize('opname', ['neg'])
def test_not_and_negate_bool(con, opname, df):
    op = getattr(operator, opname)
    t = con.table('functional_alltypes').limit(10)
    expr = t.projection([op(t.month).name('month')])
    result = expr.execute().month
    expected = op(df.head(10).month)
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
        'month',
        'year',
    ],
)
def test_negate_non_boolean(con, field, df):
    t = con.table('functional_alltypes').limit(10)
    expr = t.projection([(-t[field]).name(field)])
    result = expr.execute()[field]
    expected = -df.head(10)[field]
    tm.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    ('opname', 'expected'), [('year', {2020}), ('month', {8}), ('day', {24})]
)
def test_date_extract_field(db, opname, expected):
    op = operator.methodcaller(opname)
    t = db.functional_alltypes
    expr = op(t.timestamp_col.cast('date')).distinct()
    result = expr.execute().astype(int)
    assert set(result) == expected


@pytest.mark.parametrize('opname', ['sum', 'mean', 'min', 'max', 'std'])
def test_boolean_reduction(alltypes, opname, df):
    op = operator.methodcaller(opname)
    expr = op(alltypes.id)
    result = expr.execute()
    assert result == op(df.id)


@pytest.fixture(
    params=[
        None,
        'UTC',
        'America/New_York',
        'America/Los_Angeles',
        'Europe/Paris',
        'Chile/Continental',
        'Asia/Tel_Aviv',
        'Asia/Tokyo',
        'Africa/Nairobi',
        'Australia/Sydney',
    ]
)
def tz(request):
    return request.param


def test_timestamp_type_accepts_all_timezones(con):
    assert all(
        dt.Timestamp(row.tzname).timezone == row.tzname
        for row in con.con.execute('SELECT tzname FROM V$TIMEZONE_NAMES')
    )
