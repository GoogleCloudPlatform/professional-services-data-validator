import functools
import itertools
import locale
import platform
import re
import string
import warnings

import sqlalchemy as sa
import sqlalchemy.dialects.oracle as ol
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.sql.functions import GenericFunction

import ibis
import ibis.common.exceptions as com
import ibis.common.geospatial as geo
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.sql.alchemy as alch
import ibis.sql.oracle.alchemy as ol_alch

# used for literal translate
from ibis.sql.alchemy import (
    _get_sqla_table,
    _variance_reduction,
    fixed_arity,
    infix_op,
    unary,
)

_operation_registry = alch._operation_registry.copy()
_operation_registry.update(alch._window_functions)


class OracleUDFNode(ops.ValueOp):
    pass


# TODO: substr and find are copied from SQLite, we should really have a
# "base" set of SQL functions that are the most common APIs across the major
# RDBMS
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

    return sa.func.instr(sa_arg, sa_substr)


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


_truncate_precisions = {
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


def _timestamp_add(t, expr):
    sa_args = list(map(t.translate, expr.op().args))
    return sa_args[0] + sa_args[1]


def _is_nan(t, expr):
    (arg,) = expr.op().args
    sa_arg = t.translate(arg)
    return sa_arg == float('None')


def _is_inf(t, expr):
    (arg,) = expr.op().args
    sa_arg = t.translate(arg)
    inf = float('inf')
    return sa.or_(sa_arg == inf, sa_arg == -inf)


def _cast(t, expr):
    arg, typ = expr.op().args

    sa_arg = t.translate(arg)
    sa_type = t.get_sqla_type(typ)

    # specialize going from an integer type to a timestamp
    if isinstance(arg.type(), dt.Integer) and isinstance(sa_type, sa.DateTime):
        return sa.func.timezone('UTC', sa.func.to_timestamp(sa_arg))

    if arg.type().equals(dt.binary) and typ.equals(dt.string):
        return sa.func.encode(sa_arg, 'escape')

    if typ.equals(dt.binary):
        #  decode yields a column of memoryview which is annoying to deal with
        # in pandas. CAST(expr AS BYTEA) is correct and returns byte strings.
        return sa.cast(sa_arg, sa.LargeBinary())

    return sa.cast(sa_arg, sa_type)


def _typeof(t, expr):
    (arg,) = expr.op().args
    sa_arg = t.translate(arg)
    typ = sa.func.dump(sa_arg)
    # typ = sa.cast(sa.func.pg_typeof(sa_arg), sa.TEXT)

    # select pg_typeof('asdf') returns unknown so we have to check the child's
    # type for nullness
    return typ


def _string_agg(t, expr):
    arg, sep, where = expr.op().args
    sa_arg = t.translate(arg)
    sa_sep = t.translate(sep)

    if where is not None:
        operand = t.translate(where.ifelse(arg, ibis.NA))
    else:
        operand = sa_arg
    return sa.func.listagg(operand, sa_sep)


_strftime_to_oracle_rules = {
    '%a': 'TMDy',  # TM does it in a locale dependent way
    '%A': 'TMDay',
    '%b': 'TMMon',  # Sep
    '%B': 'TMMonth',  # September
    '%d': 'DD',  # day of month
    '%H': 'HH24',  # 09
    '%I': 'HH12',  # 09
    '%j': 'DDD',  # zero padded day of year
    '%m': 'MM',  # 01
    '%M': 'MI',  # zero padded minute
    '%p': 'AM',  # AM or PM
    '%S': 'SS',  # zero padded second
    '%U': 'WW',  # 1-based week of year
    '%w': 'D',
    '%y': 'YY',  # 15
    '%Y': 'YYYY',  # 2015
    '%Z': 'TZ',  # uppercase timezone name
}

try:

    _strftime_to_oracle_rules.update(
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


# translate strftime spec into mostly equivalent Oracle spec
_scanner = re.Scanner(
    # double quotes need to be escaped
    [('"', lambda scanner, token: r'\"')]
    + [
        (
            '|'.join(
                map(
                    '(?:{})'.format,
                    itertools.chain(
                        _strftime_to_oracle_rules.keys(),
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


_lexicon_values = frozenset(_strftime_to_oracle_rules.values())

_strftime_blacklist = frozenset(['%w', '%U', '%c', '%x', '%X', '%e'])


def _reduce_tokens(tokens, arg):
    # current list of tokens
    curtokens = []

    # reduced list of tokens that accounts for blacklisted values
    reduced = []

    non_special_tokens = (
        frozenset(_strftime_to_oracle_rules) - _strftime_blacklist
    )

    # TODO: how much of a hack is this?
    for token in tokens:
        # we are a non-special token %A, %d, etc.
        if token in non_special_tokens:
            curtokens.append(_strftime_to_oracle_rules[token])

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
                    new_pattern = _strftime_to_oracle_rules[token]
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


class array_search(expression.FunctionElement):
    type = sa.INTEGER()
    name = 'array_search'


@compiles(array_search)
def oracle_array_search(element, compiler, **kw):
    needle, haystack = element.clauses
    i = sa.func.generate_subscripts(haystack, 1).alias('i')
    c0 = sa.column('i', type_=sa.INTEGER(), _selectable=i)
    result = (
        sa.func.coalesce(
            sa.select([c0])
            .where(haystack[c0].op('IS NOT DISTINCT FROM')(needle))
            .order_by(c0)
            .limit(1)
            .as_scalar(),
            0,
        )
        - 1
    )
    string_result = compiler.process(result, **kw)
    return string_result


def _regex_replace(t, expr):
    string, pattern, replacement = map(t.translate, expr.op().args)

    return sa.func.regexp_replace(string, pattern, replacement)


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


class regex_extract(GenericFunction):
    def __init__(self, string, pattern, index):
        super().__init__(string, pattern, index)
        self.string = string
        self.pattern = pattern
        self.index = index


@compiles(regex_extract, 'oracle')
def compile_regex_extract(element, compiler, **kw):
    result = '(SELECT regexp_substr({}, {}, {}) as TMP FROM dual)'.format(
        compiler.process(element.string, **kw),
        compiler.process(element.pattern, **kw),
        compiler.process(element.index, **kw),
    )
    return result


def _regex_extract(t, expr):
    string, pattern, index = map(t.translate, expr.op().args)
    result = sa.func.regex_extract(string, pattern, index + 1)
    return result


def _cardinality(array):
    return sa.case(
        [(array.is_(None), None)],  # noqa: E711
        else_=sa.func.coalesce(sa.func.array_length(array, 1), 0),
    )


def _table_column(t, expr):
    op = expr.op()
    ctx = t.context
    table = op.table

    sa_table = _get_sqla_table(ctx, table)
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


def _round(t, expr):
    arg, digits = expr.op().args
    sa_arg = t.translate(arg)

    if digits is None:
        return sa.func.round(sa_arg)

    result = sa.func.round(sa.cast(sa_arg, sa.NUMERIC), t.translate(digits))
    if digits is not None and isinstance(arg.type(), dt.Decimal):
        return result
    result = sa.cast(result, sa.dialects.oracle.DOUBLE_PRECISION())
    return result


def _mod(t, expr):
    left, right = map(t.translate, expr.op().args)

    if not isinstance(expr.type(), dt.Integer):
        left = sa.cast(left, sa.NUMERIC)
        right = sa.cast(right, sa.NUMERIC)

    result = left % right
    if expr.type().equals(dt.double):
        return sa.cast(result, sa.dialects.oracle.DOUBLE_PRECISION())
    else:
        return result


def _string_join(t, expr):
    sep, elements = expr.op().args
    return sa.func.concat_ws(t.translate(sep), *map(t.translate, elements))


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


def _random(t, expr):
    return sa.func.random()


def _day_of_week_index(t, expr):
    (sa_arg,) = map(t.translate, expr.op().args)
    return sa.func.to_char(sa_arg, 'd')


def _day_of_week_name(t, expr):
    (sa_arg,) = map(t.translate, expr.op().args)
    return sa.func.to_char(sa_arg, 'Day')


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
        ops.RegexSearch: infix_op('~'),
        ops.RegexReplace: _regex_replace,
        ops.Translate: fixed_arity('translate', 3),
        ops.RegexExtract: _regex_extract,
        ops.StringSplit: fixed_arity(sa.func.string_to_array, 2),
        ops.StringJoin: _string_join,
        # math
        ops.Log: _log,
        ops.Log2: unary(lambda x: sa.func.log(2, x)),
        ops.Log10: unary(sa.func.log),
        ops.Round: _round,
        ops.Modulus: _mod,
        ops.Power: fixed_arity(sa.func.power, 2),
        # dates and times
        ops.Date: unary(lambda x: sa.cast(x, sa.Date)),
        ops.DateTruncate: _timestamp_truncate,
        ops.TimestampTruncate: _timestamp_truncate,
        ops.IntervalFromInteger: _interval_from_integer,
        ops.DateAdd: infix_op('+'),
        ops.DateSub: infix_op('-'),
        ops.DateDiff: infix_op('-'),
        ops.TimestampAdd: infix_op('+'),
        ops.TimestampSub: infix_op('-'),
        ops.TimestampDiff: infix_op('-'),
        ops.Strftime: _strftime,
        ops.ExtractYear: _extract('year'),
        ops.ExtractMonth: _extract('month'),
        ops.ExtractDay: _extract('day'),
        ops.ExtractDayOfYear: _extract('doy'),
        ops.ExtractQuarter: _extract('quarter'),
        ops.ExtractEpochSeconds: _extract('epoch', sa.BigInteger),
        ops.ExtractHour: _extract('hour'),
        ops.ExtractMinute: _extract('minute'),
        ops.ExtractSecond: _second,
        ops.DayOfWeekIndex: _day_of_week_index,
        ops.DayOfWeekName: _day_of_week_name,
        ops.Sum: _reduction('sum'),
        ops.Mean: _reduction('avg'),
        ops.Min: _reduction('min'),
        ops.Max: _reduction('max'),
        ops.Variance: _variance_reduction('var'),
        ops.StandardDev: _variance_reduction('stddev'),
        ops.RandomScalar: _random,
        # now is in the timezone of the server, but we want UTC
        ops.TimestampNow: lambda *args: sa.func.timezone('UTC', sa.func.now()),
        ops.CumulativeAll: unary(sa.func.bool_and),
        ops.CumulativeAny: unary(sa.func.bool_or),
    }
)


def add_operation(op, translation_func):
    _operation_registry[op] = translation_func


class OracleExprTranslator(ol_alch.AlchemyExprTranslator):

    _registry = _operation_registry
    _rewrites = ol_alch.AlchemyExprTranslator._rewrites.copy()
    _type_map = ol_alch.AlchemyExprTranslator._type_map.copy()
    _type_map.update({dt.Double: ol.DOUBLE_PRECISION, dt.Float: ol.FLOAT})


rewrites = OracleExprTranslator.rewrites
compiles = OracleExprTranslator.compiles


@rewrites(ops.Any)
@rewrites(ops.All)
@rewrites(ops.NotAny)
@rewrites(ops.NotAll)
def _any_all_no_op(expr):
    return expr


class OracleDialect(ol_alch.AlchemyDialect):
    translator = OracleExprTranslator


dialect = OracleDialect
