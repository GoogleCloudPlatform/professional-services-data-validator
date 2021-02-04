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


import ibis.backends.base_sqlalchemy.alchemy as alch
import sqlalchemy as sa
import snowflake.sqlalchemy.snowdialect as sf
import snowflake.sqlalchemy as sf_sa

import third_party.ibis.ibis_snowflake.alchemy as al_sf
import ibis.expr.operations as ops
import ibis.expr.datatypes as dt
import ibis.common.geospatial as geo
import ibis.common.exceptions as com
import ibis.expr.types as ir
import locale
import warnings
import re
import itertools
import ibis
import platform
import string
import functools

from ibis.backends.base_sql import fixed_arity, unary


# todo add more operations to registry
_operation_registry = alch._operation_registry.copy()
_operation_registry.update(alch._window_functions)


def _substr(t, expr):
    f = sa.func.substr

    arg, start, length = expr.op().args

    sa_arg = t.translate(arg)
    sa_start = t.translate(start)

    if length is None:
        return f(sa_arg, sa_start + 1)
    else:
        sa_length = t.translate(length)
        return f(sa_arg, sa_start + 1, sa_length)


def _string_find(t, expr):
    arg, substr, start, _ = expr.op().args

    if start is not None:
        raise NotImplementedError

    sa_arg = t.translate(arg)
    sa_substr = t.translate(substr)

    return sa.func.position(sa_substr, sa_arg) - 1


def _table_column(t, expr):
    op = expr.op()
    ctx = t.context
    table = op.table

    sa_table = alch._get_sqla_table(ctx, table)
    out_expr = getattr(sa_table.c, op.name)

    expr_type = expr.type()

    if isinstance(expr_type, dt.Timestamp):
        timezone = expr_type.timezone
        if timezone is not None:
            out_expr = out_expr.op('AT TIME ZONE')(timezone).label(op.name)

    # If the column does not originate from the table set in the current SELECT
    # context, we should format as a subquery
    if t.permit_subquery and ctx.is_foreign_expr(table):
        return sa.select([out_expr])

    return out_expr


def _string_agg(t, expr):
    # we could use sa.func.string_agg since postgres 9.0, but we can cheaply
    # maintain backwards compatibility here, so we don't use it
    arg, sep, where = expr.op().args
    sa_arg = t.translate(arg)
    sa_sep = t.translate(sep)

    if where is not None:
        operand = t.translate(where.ifelse(arg, ibis.NA))
    else:
        operand = sa_arg
    return sa.func.array_to_string(sa.func.array_agg(operand), sa_sep)


def _log(t, expr):
    arg, base = expr.op().args
    sa_arg = t.translate(arg)
    if base is not None:
        sa_base = t.translate(base)
        return sa.cast(
            sa.func.log(
                sa.cast(sa_base, sa.NUMERIC), sa.cast(sa_arg, sa.NUMERIC)
            ),
            t.get_sqla_type(expr.type()),
        )
    return sa.func.ln(sa_arg)


def _round(t, expr):
    arg, digits = expr.op().args
    sa_arg = t.translate(arg)

    if digits is None:
        sa_digits = 0
    else:
        sa_digits = t.translate(digits)

    return sa.func.round(sa_arg, sa_digits)


def _mod(t, expr):
    left, right = map(t.translate, expr.op().args)
    result = left % right
    return result


def _reduction(func_name):
    def reduction_compiler(t, expr):
        arg, where = expr.op().args

        if arg.type().equals(dt.boolean):
            arg = arg.cast('int32')

        func = getattr(sa.func, func_name)

        if where is not None:
            arg = where.ifelse(arg, None)
        return func(t.translate(arg))

    return reduction_compiler


def _random(t, expr):
    return sa.func.random()


def _is_nan(t, expr):
    (arg,) = expr.op().args
    sa_arg = t.translate(arg)
    return sa_arg == float('nan')


def _is_inf(t, expr):
    (arg,) = expr.op().args
    sa_arg = t.translate(arg)
    inf = float('inf')
    return sa.or_(sa_arg == inf, sa_arg == -inf)


def _hll_cardinality(t, expr):
    arg, _ = expr.op().args
    sa_arg = t.translate(arg)
    return sa.func.hll(sa_arg)


def _cardinality(array):
    return sa.case(
        [(array.is_(None), None)],  # noqa: E711
        else_=sa.func.coalesce(sa.func.array_length(array, 1), 0),
    )


def _extract(fmt, output_type=sa.SMALLINT):
    def translator(t, expr, output_type=output_type):
        (arg,) = expr.op().args
        sa_arg = t.translate(arg)
        return sa.cast(sa.extract(fmt, sa_arg), output_type)

    return translator


def _second(t, expr):
    # extracting the second gives us the fractional part as well, so smash that
    # with a cast to SMALLINT
    (sa_arg,) = map(t.translate, expr.op().args)
    return sa.cast(sa.func.FLOOR(sa.extract('second', sa_arg)), sa.SMALLINT)


def _cast(t, expr):
    arg, typ = expr.op().args

    sa_arg = t.translate(arg)
    sa_type = t.get_sqla_type(typ)

    return sa.cast(sa_arg, sa_type)


_strftime_to_snowflake_rules = {
    '%y': 'YY',  # 15
    '%Y': 'YYYY',  # 2015
    '%a': 'DY',
    '%A': 'DY',  # Snowflake does not provide full day name
    '%d': 'DD',  # day of month
    '%b': 'Mon',  # Sep
    '%B': 'MMMM',  # September
    '%m': 'MM',  # 01
    '%H': 'HH24',  # 09
    '%p': 'AM',  # AM or PM
    '%M': 'MI',  # zero padded minute

    '%w': 'D',  # 1-based day of week, see below for how we make this 0-based
    '%-d': 'FMDD',  # - is no leading zero for Python same for FM in postgres

    '%-m': 'FMMM',  # 1

    '%-H': 'FMHH24',  # 9
    '%I': 'HH12',  # 09
    '%-I': 'FMHH12',  # 9
    '%-M': 'FMMI',  # Minute
    '%S': 'SS',  # zero padded second
    '%-S': 'FMSS',  # Second
    '%f': 'US',  # zero padded microsecond
    '%z': 'OF',  # utf offset
    '%Z': 'TZ',  # uppercase timezone name
    '%j': 'DDD',  # zero padded day of year
    '%-j': 'FMDDD',  # day of year
    '%U': 'WW',  # 1-based week of year
    # 'W': ?,  # meh
}

try:
    _strftime_to_snowflake_rules.update(
        {
            '%c': locale.nl_langinfo(locale.D_T_FMT),  # locale date and time
            '%x': locale.nl_langinfo(locale.D_FMT),  # locale date
            '%X': locale.nl_langinfo(locale.T_FMT),  # locale time
        }
    )
except AttributeError:
    warnings.warn(
        'locale specific date formats (%%c, %%x, %%X) are not yet implemented '
        'for %s' % platform.system()
    )

# translate strftime spec into mostly equivalent PostgreSQL spec
_scanner = re.Scanner(
    # double quotes need to be escaped
    [('"', lambda scanner, token: r'\"')]
    + [
        (
            '|'.join(
                map(
                    '(?:{})'.format,
                    itertools.chain(
                        _strftime_to_snowflake_rules.keys(),
                        [
                            # "%e" is in the C standard and Python actually
                            # generates this if your spec contains "%c" but we
                            # don't officially support it as a specifier so we
                            # need to special case it in the scanner
                            '%e',
                            r'\s+',
                            r'[{}]'.format(re.escape(string.punctuation)),
                            r'[^{}\s]+'.format(re.escape(string.punctuation)),
                        ],
                    ),
                )
            ),
            lambda scanner, token: token,
        )
    ]
)

_lexicon_values = frozenset(_strftime_to_snowflake_rules.values())

_strftime_blacklist = frozenset(['%w', '%U', '%c', '%x', '%X', '%e'])


def _reduce_tokens(tokens, arg):
    # current list of tokens
    curtokens = []

    # reduced list of tokens that accounts for blacklisted values
    reduced = []

    non_special_tokens = (
            frozenset(_strftime_to_snowflake_rules) - _strftime_blacklist
    )

    # TODO: how much of a hack is this?
    for token in tokens:
        # we are a non-special token %A, %d, etc.
        if token in non_special_tokens:
            curtokens.append(_strftime_to_snowflake_rules[token])

        # we have a string like DD, to escape this we
        # surround it with double quotes
        elif token in _lexicon_values:
            curtokens.append('"{}"'.format(token))

        # we have a token that needs special treatment
        elif token in _strftime_blacklist:
            if token == '%w':
                value = sa.extract('dow', arg)  # 0 based day of week
            elif token == '%U':
                value = sa.cast(sa.func.to_char(arg, 'WW'), sa.SMALLINT) - 1
            elif token == '%c' or token == '%x' or token == '%X':
                # re scan and tokenize this pattern
                try:
                    new_pattern = _strftime_to_snowflake_rules[token]
                except KeyError:
                    raise ValueError(
                        'locale specific date formats (%%c, %%x, %%X) are '
                        'not yet implemented for %s' % platform.system()
                    )

                new_tokens, _ = _scanner.scan(new_pattern)
                value = functools.reduce(
                    sa.sql.ColumnElement.concat,
                    _reduce_tokens(new_tokens, arg),
                )
            elif token == '%e':
                # pad with spaces instead of zeros
                value = sa.func.replace(sa.func.to_char(arg, 'DD'), '0', ' ')

            reduced += [
                sa.func.to_char(arg, ''.join(curtokens)),
                sa.cast(value, sa.TEXT),
            ]

            # empty current token list in case there are more tokens
            del curtokens[:]

        # uninteresting text
        else:
            curtokens.append(token)
    else:
        # append result to r if we had more tokens or if we have no
        # blacklisted tokens
        if curtokens:
            reduced.append(sa.func.to_char(arg, ''.join(curtokens)))
    return reduced


def _strftime(t, expr):
    arg, pattern = map(t.translate, expr.op().args)
    tokens, _ = _scanner.scan(pattern.value)
    reduced = _reduce_tokens(tokens, arg)
    result = functools.reduce(sa.sql.ColumnElement.concat, reduced)
    return result


def _literal(t, expr):
    dtype = expr.type()
    op = expr.op()
    value = op.value

    if isinstance(dtype, dt.Interval):
        return sa.text("INTERVAL '{} {}'".format(value, dtype.resolution))
    elif isinstance(dtype, dt.Set):
        return list(map(sa.literal, value))
    # geo spatial data type
    elif isinstance(expr, ir.GeoSpatialScalar):
        # inline_metadata ex: 'SRID=4326;POINT( ... )'
        return sa.text(geo.translate_literal(expr, inline_metadata=True))
    else:
        return sa.literal(value)


def _typeof(t, expr):
    (arg,) = expr.op().args
    sa_arg = t.translate(arg)
    return sa.cast(sa.func.typeof(sa.cast(sa_arg, sf_sa.VARIANT)), sa.TEXT)


def _regex_replace(t, expr):
    string, pattern, replacement = map(t.translate, expr.op().args)
    return sa.func.regexp_replace(string, pattern, replacement)


def _string_join(t, expr):
    sep, elements = expr.op().args
    return sa.func.concat_ws(t.translate(sep), *map(t.translate, elements))


_truncate_precisions = {
    'ns': 'nanoseconds',
    'us': 'microseconds',
    'ms': 'milliseconds',
    's': 'second',
    'm': 'minute',
    'h': 'hour',
    'D': 'day',
    'W': 'week',
    'M': 'month',
    'Q': 'quarter',
    'Y': 'year',
}


def _timestamp_truncate(t, expr):
    arg, unit = expr.op().args
    sa_arg = t.translate(arg)
    try:
        precision = _truncate_precisions[unit]
    except KeyError:
        raise com.UnsupportedOperationError(
            'Unsupported truncate unit {!r}'.format(unit)
        )
    return sa.func.date_trunc(precision, sa_arg)


def _interval_from_integer(t, expr):
    arg, unit = expr.op().args
    sa_arg = t.translate(arg)
    interval = sa.text("INTERVAL '1 {}'".format(expr.type().resolution))
    return sa_arg * interval


def _day_of_week_index(t, expr):
    (sa_arg,) = map(t.translate, expr.op().args)
    return sa.cast(
        sa.cast(sa.extract('dow', sa_arg) + 6, sa.SMALLINT) % 7, sa.SMALLINT
    )


def _day_of_week_name(t, expr):
    (sa_arg,) = map(t.translate, expr.op().args)
    return sa.func.dayname(sa_arg)


def _array_slice(t, expr):
    arg, start, stop = expr.op().args
    sa_arg = t.translate(arg)
    sa_start = t.translate(start)
    sa_stop = t.translate(stop)
    return sa.func.array_slice(sa_arg, sa_start, sa_stop)


def _identical_to(t, expr):
    left, right = args = expr.op().args
    if left.equals(right):
        return True
    else:
        left, right = map(t.translate, args)
        return left.op('IS NOT DISTINCT FROM')(right)


def _find_in_set(t, expr):
    needle, haystack = expr.op().args
    return sa.func.array_contains(
        sa.cast(t.translate(needle), sf_sa.VARIANT), t.translate(haystack)
    )


def _regex_search(t, expr):
    arg, expr = expr.op().args

    sa_arg = t.translate(arg)
    sa_expr = t.translate(expr)

    return sa.func.regexp_instr(sa_arg, sa_expr)


def _regex_extract(t, expr):
    arg, expr, group = expr.op().args

    sa_arg = t.translate(arg)
    sa_expr = t.translate(expr)
    sa_group = t.translate(group)

    return sa.func.regexp_substr(sa_arg, sa_expr, sa_group)


_operation_registry.update(
    {
        ops.Literal: _literal,
        # We override this here to support time zones
        ops.TableColumn: _table_column,
        # types
        ops.Cast: _cast,
        ops.TypeOf: _typeof,
        # Floating
        ops.IsNan: _is_nan,
        ops.IsInf: _is_inf,
        # null handling
        ops.IfNull: fixed_arity(sa.func.coalesce, 2),
        # boolean reductions
        ops.Any: unary(sa.func.bool_or),
        ops.All: unary(sa.func.bool_and),
        ops.NotAny: unary(lambda x: sa.not_(sa.func.bool_or(x))),
        ops.NotAll: unary(lambda x: sa.not_(sa.func.bool_and(x))),
        # strings
        ops.Substring: _substr,
        ops.StringFind: _string_find,
        ops.GroupConcat: _string_agg,
        ops.Capitalize: unary(sa.func.initcap),
        ops.RegexSearch: _regex_search,
        ops.RegexReplace: _regex_replace,
        ops.RegexExtract: _regex_extract,
        ops.Translate: fixed_arity('translate', 3),
        ops.StringSplit: fixed_arity(sa.func.string_to_array, 2),
        ops.StringJoin: _string_join,
        ops.FindInSet: _find_in_set,
        # math
        ops.Log: _log,
        ops.Log2: unary(lambda x: sa.func.log(2, x)),
        ops.Log10: unary(sa.func.log),
        ops.Round: _round,
        ops.Modulus: _mod,
        # dates and times
        ops.Date: unary(lambda x: sa.cast(x, sa.Date)),
        ops.DateTruncate: _timestamp_truncate,
        ops.TimestampTruncate: _timestamp_truncate,
        ops.IntervalFromInteger: _interval_from_integer,
        ops.DateAdd: alch.infix_op('+'),
        ops.DateSub: alch.infix_op('-'),
        ops.DateDiff: alch.infix_op('-'),
        ops.TimestampAdd: alch.infix_op('+'),
        ops.TimestampSub: alch.infix_op('-'),
        ops.TimestampDiff: alch.infix_op('-'),
        ops.Strftime: _strftime,
        ops.ExtractYear: _extract('year'),
        ops.ExtractMonth: _extract('month'),
        ops.ExtractDay: _extract('day'),
#        ops.ExtractDayOfYear: _extract('doy'),
#        ops.ExtractQuarter: _extract('quarter'),
#        ops.ExtractEpochSeconds: _extract('epoch', sa.BigInteger),
        ops.ExtractHour: _extract('hour'),
        ops.ExtractMinute: _extract('minute'),
        ops.ExtractSecond: _second,
        ops.DayOfWeekIndex: _day_of_week_index,
        ops.DayOfWeekName: _day_of_week_name,
        ops.Sum: _reduction('sum'),
        ops.Mean: _reduction('avg'),
        ops.Min: _reduction('min'),
        ops.Max: _reduction('max'),
        ops.Variance: alch._variance_reduction('var'),
        ops.StandardDev: alch._variance_reduction('stddev'),
#        ops.RandomScalar: _random,
        # now is in the timezone of the server, but we want UTC
        ops.TimestampNow: lambda *args: sa.func.convert_timezone('UTC', sa.func.now()),
        ops.CumulativeAll: unary(sa.func.bool_and),
        ops.CumulativeAny: unary(sa.func.bool_or),
        # array operations
        ops.ArrayLength: unary(_cardinality),
        ops.ArrayCollect: unary(sa.func.array_agg),
        ops.ArraySlice: _array_slice,
        ops.ArrayIndex: fixed_arity(lambda array, index: array[index + 1], 2),
        ops.ArrayConcat: fixed_arity(
            sa.sql.expression.ColumnElement.concat, 2
        ),
        #         ops.ArrayRepeat: _array_repeat,
        ops.IdenticalTo: _identical_to,
        ops.HLLCardinality: _hll_cardinality,
    }
)


def add_operation(op, translation_func):
    _operation_registry[op] = translation_func


class SnowflakeExprTranslator(al_sf.AlchemyExprTranslator):
    _registry = _operation_registry
    _rewrites = al_sf.AlchemyExprTranslator._rewrites.copy()
    _type_map = al_sf.AlchemyExprTranslator._type_map.copy()
    _type_map.update({dt.Double: sf.DECIMAL, dt.Float: sf.FLOAT #, dt.Timestamp: TIMESTAMP_NTZ
                      })
    # _type_map.update({dt.Timestamp: TIMESTAMP_NTZ,
    #                   'TIMESTAMP_TZ': TIMESTAMP_TZ,
    #                   'TIMESTAMP_LTZ': TIMESTAMP_LTZ,
    #                   'TIMESTAMP_NTZ': TIMESTAMP_NTZ})
    # _type_map.update({dt.Double: pg.DOUBLE_PRECISION, dt.Float: pg.REAL})


rewrites = SnowflakeExprTranslator.rewrites
compiles = SnowflakeExprTranslator.compiles


@rewrites(ops.Any)
@rewrites(ops.All)
@rewrites(ops.NotAny)
@rewrites(ops.NotAll)
def _any_all_no_op(expr):
    return expr


class SnowflakeDialect(al_sf.AlchemyDialect):
    translator = SnowflakeExprTranslator


dialect = SnowflakeDialect
