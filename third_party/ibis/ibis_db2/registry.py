# Copyright 2023 Google Inc.
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
import itertools
import locale
import platform
import re
import string
import warnings
import operator

import sqlalchemy as sa

import ibis.common.exceptions as com
import ibis.expr.operations as ops

from ibis.backends.base.sql.alchemy import (
    fixed_arity,
    sqlalchemy_operation_registry,
    sqlalchemy_window_functions_registry,
    unary,
    get_sqla_table,
)
from ibis.backends.base.sql.alchemy.registry import variance_reduction

operation_registry = sqlalchemy_operation_registry.copy()
operation_registry.update(sqlalchemy_window_functions_registry)


def _substr(t, op):
    f = sa.func.substr
    sa_arg = t.translate(op.arg)
    sa_start = t.translate(op.start)

    if op.length is None:
        return f(sa_arg, sa_start + 1)
    else:
        sa_length = t.translate(op.length)
        return f(sa_arg, sa_start + 1, sa_length)


def _string_find(t, op):
    if op.start is not None:
        raise NotImplementedError

    sa_arg = t.translate(op.arg)
    sa_substr = t.translate(op.substr)

    return sa.func.instr(sa_arg, sa_substr)


def _extract(fmt: str):
    def translator(t, op: ops.Node):
        return sa.cast(sa.extract(fmt, t.translate(op.arg)), sa.SMALLINT)

    return translator


def _second(t, op):
    # extracting the second gives us the fractional part as well, so smash that
    # with a cast to SMALLINT
    return sa.cast(
        sa.func.FLOOR(sa.extract("second", t.translate(op.arg))), sa.SMALLINT
    )


def _millisecond(t, op):
    # we get total number of milliseconds including seconds with extract so we
    # mod 1000
    return sa.cast(sa.func.floor(sa.extract("millisecond", op.arg)), sa.SMALLINT) % 1000


_truncate_precisions = {
    "us": "microseconds",
    "ms": "milliseconds",
    "s": "second",
    "m": "minute",
    "h": "hour",
    "D": "day",
    "W": "week",
    "M": "month",
    "Q": "quarter",
    "Y": "year",
}


def _timestamp_truncate(t, op):
    arg = t.translate(op.arg)
    unit = op.unit
    if unit not in _truncate_precisions:
        raise com.UnsupportedOperationError(f"Unsupported truncate unit {op.unit!r}")

    return sa.func.datetrunc(sa.text(_truncate_precisions[unit]), arg)


def _is_nan(t, op):
    sa_arg = t.translate(op.arg)
    return sa_arg == float("nan")


def _is_inf(t, op):
    sa_arg = t.translate(op.arg)
    inf = float("inf")
    return sa.or_(sa_arg == inf, sa_arg == -inf)


def _cast(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)

    # specialize going from an integer type to a timestamp
    if arg_dtype.is_integer() and typ.is_timestamp():
        return t.integer_to_timestamp(sa_arg, tz=typ.timezone)

    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.lower(sa.func.hex(sa_arg))

    if typ.is_binary():
        #  decode yields a column of memoryview which is annoying to deal with
        # in pandas. CAST(expr AS BYTEA) is correct and returns byte strings.
        return sa.cast(sa_arg, sa.LargeBinary())

    if typ.is_json() and not t.native_json_type:
        return sa_arg

    return sa.cast(sa_arg, t.get_sqla_type(typ))


def _string_agg(t, op):
    sa_arg = t.translate(op.arg)
    sa_sep = t.translate(op.sep)

    if (where := op.where) is not None:
        operand = t.translate(where.ifelse(op.arg, ibis.NA))
    else:
        operand = sa_arg
    return sa.func.listagg(operand, sa_sep)


_strftime_to_db2_rules = {
    "%a": "TMDy",  # TM does it in a locale dependent way
    "%A": "TMDay",
    "%w": "D",  # 1-based day of week, see below for how we make this 0-based
    "%d": "DD",  # day of month
    "%-d": "FMDD",
    "%b": "TMMon",  # Sep
    "%B": "TMMonth",  # September
    "%m": "MM",  # 01
    "%-m": "FMMM",  # 1
    "%y": "YY",  # 15
    "%Y": "YYYY",  # 2015
    "%H": "HH24",  # 09
    "%-H": "FMHH24",  # 9
    "%I": "HH12",  # 09
    "%-I": "FMHH12",  # 9
    "%p": "AM",  # AM or PM
    "%M": "MI",  # zero padded minute
    "%-M": "FMMI",  # Minute
    "%S": "SS",  # zero padded second
    "%-S": "FMSS",  # Second
    "%f": "US",  # zero padded microsecond
    "%z": "OF",  # utf offset
    "%Z": "TZ",  # uppercase timezone name
    "%j": "DDD",  # zero padded day of year
    "%-j": "FMDDD",  # day of year
    "%U": "WW",  # 1-based week of year
}

try:
    _strftime_to_db2_rules.update(
        {
            "%c": locale.nl_langinfo(locale.D_T_FMT),  # locale date and time
            "%x": locale.nl_langinfo(locale.D_FMT),  # locale date
            "%X": locale.nl_langinfo(locale.T_FMT),  # locale time
        }
    )
except AttributeError:
    warnings.warn(
        "locale specific date formats (%%c, %%x, %%X) are not yet implemented "
        "for %s" % platform.system()
    )


_scanner = re.Scanner(
    # double quotes need to be escaped
    [('"', lambda scanner, token: r"\"")]
    + [
        (
            "|".join(
                map(
                    "(?:{})".format,
                    itertools.chain(
                        _strftime_to_db2_rules.keys(),
                        [
                            # "%e" is in the C standard and Python actually
                            # generates this if your spec contains "%c" but we
                            # don't officially support it as a specifier so we
                            # need to special case it in the scanner
                            "%e",
                            r"\s+",
                            r"[{}]".format(re.escape(string.punctuation)),
                            r"[^{}\s]+".format(re.escape(string.punctuation)),
                        ],
                    ),
                )
            ),
            lambda scanner, token: token,
        )
    ]
)


_lexicon_values = frozenset(_strftime_to_db2_rules.values())

_strftime_blacklist = frozenset(["%w", "%U", "%c", "%x", "%X", "%e"])


def _reduce_tokens(tokens, arg):
    # current list of tokens
    curtokens = []

    # reduced list of tokens that accounts for blacklisted values
    reduced = []

    non_special_tokens = frozenset(_strftime_to_db2_rules) - _strftime_blacklist

    # TODO: how much of a hack is this?
    for token in tokens:
        # we are a non-special token %A, %d, etc.
        if token in non_special_tokens:
            curtokens.append(_strftime_to_db2_rules[token])

        # we have a string like DD, to escape this we
        # surround it with double quotes
        elif token in _lexicon_values:
            curtokens.append('"{}"'.format(token))

        # we have a token that needs special treatment
        elif token in _strftime_blacklist:
            if token == "%w":
                value = sa.extract("dow", arg)  # 0 based day of week
            elif token == "%U":
                value = sa.cast(sa.func.to_char(arg, "WW"), sa.SMALLINT) - 1
            elif token == "%c" or token == "%x" or token == "%X":
                # re scan and tokenize this pattern
                try:
                    new_pattern = _strftime_to_db2_rules[token]
                except KeyError:
                    raise ValueError(
                        "locale specific date formats (%%c, %%x, %%X) are "
                        "not yet implemented for %s" % platform.system()
                    )

                new_tokens, _ = _scanner.scan(new_pattern)
                value = functools.reduce(
                    sa.sql.ColumnElement.concat,
                    _reduce_tokens(new_tokens, arg),
                )
            elif token == "%e":
                # pad with spaces instead of zeros
                value = sa.func.replace(sa.func.to_char(arg, "DD"), "0", " ")

            reduced += [
                sa.func.to_char(arg, "".join(curtokens)),
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
            reduced.append(sa.func.to_char(arg, "".join(curtokens)))
    return reduced


def _strftime(arg, pattern):
    # TODO(issue-1296): third_party/ibis/ibis_db2/registry.py:298 - AttributeError: 'Strftime' object has no attribute 'value'
    tokens, _ = _scanner.scan(pattern.value)
    reduced = _reduce_tokens(tokens, arg)
    result = functools.reduce(sa.sql.ColumnElement.concat, reduced)
    return result


def _regex_replace(t, op):
    return sa.func.regexp_replace(
        t.translate(op.string), t.translate(op.pattern), t.translate(op.replacement)
    )


def _reduction(func_name):
    def reduction_compiler(t, op):
        arg, where = op.args

        if arg.output_dtype.is_boolean():
            arg = arg.cast("int32")

        func = getattr(sa.func, func_name)

        if where is not None:
            arg = where.ifelse(arg, None)
        return func(t.translate(arg))

    return reduction_compiler


def _count_start(sa_func):
    return sa_func


def _reduction_count(sa_func):
    def formatter(t, op):
        *args, where = op.args

        return _reduction_format(t, sa_func, where, *args)

    return formatter


def _reduction_format(t, sa_func, where, arg, *args):
    if where is not None:
        arg = t.translate(where.ifelse(arg, None))
    else:
        arg = t.translate(arg)

    # Db2 doesn't allow '*' to be parameterized, probably better way to fix this...
    if arg == "*":
        arg = None

    return sa_func(arg, *map(t.translate, args))


def _log(t, op):
    arg, base = op.args
    sa_arg = t.translate(arg)
    if base is not None:
        sa_base = t.translate(base)
        return sa.cast(
            sa.func.log(sa.cast(sa_base, sa.NUMERIC), sa.cast(sa_arg, sa.NUMERIC)),
            t.get_sqla_type(op.output_dtype),
        )
    return sa.func.ln(sa_arg)


def _regex_extract(t, op):
    return sa.func.regex_extract(
        t.translate(op.string), t.translate(op.pattern), t.translate(op.index + 1)
    )


def _identical_to(t, op):
    left, right = args = op.args
    if left.equals(right):
        return True
    else:
        left, right = map(t.translate, args)
        return left.op("IS NOT DISTINCT FROM")(right)


def get_col(sa_table, op: ops.TableColumn):
    """Extract a column from a table."""
    cols = sa_table.exported_columns
    colname = op.name

    if (col := cols.get(colname)) is not None:
        return col

    # `cols` is a SQLAlchemy column collection that contains columns
    # with names that are secretly prefixed by table that contains them
    #
    # for example, in `t0.join(t1).select(t0.a, t1.b)` t0.a will be named `t0_a`
    # and t1.b will be named `t1_b`
    #
    # unfortunately SQLAlchemy doesn't let you select by the *un*prefixed
    # column name despite the uniqueness of `colname`
    #
    # however, in ibis we have already deduplicated column names so we can
    # refer to the name by position
    colindex = op.table.schema._name_locs[colname]
    return cols[colindex]


def _table_column(t, op):
    ctx = t.context
    table = op.table

    sa_table = get_sqla_table(ctx, table)

    out_expr = get_col(sa_table, op)
    out_expr.quote = t._quote_column_names

    # If the column does not originate from the table set in the current SELECT
    # context, we should format as a subquery
    if t.permit_subquery and ctx.is_foreign_expr(table):
        try:
            subq = sa_table.subquery()
        except AttributeError:
            subq = sa_table
        return sa.select(subq.c[out_expr.name])

    return out_expr


def _round(t, op):
    sa_arg = t.translate(op.arg)

    if op.digits is not None:
        return sa.func.round(sa_arg, t.translate(op.digits))
    else:
        return sa.func.round(sa_arg)


def _string_join(t, op):
    sep, elements = op.args
    columns = [str(col.name) for col in map(t.translate, elements)]
    return sa.sql.literal_column(" || ".join(columns))


def _literal(t, op):
    dtype = op.output_dtype
    value = op.value

    if dtype.is_interval():
        return sa.literal_column(f"INTERVAL '{value} {dtype.resolution}'")
    elif dtype.is_set():
        return list(map(sa.literal, value))
    else:
        return sa.literal(value)


def _random(t, expr):
    return sa.func.rand()


def _day_of_week_index(t, op):
    (sa_arg,) = map(t.translate, op.arg)
    return sa.func.dayofweek(sa_arg)


def _day_of_week_name(t, op):
    (sa_arg,) = map(t.translate, op.arg)
    return sa.func.dayname(sa_arg)


operation_registry.update(
    {
        ops.Literal: _literal,
        ops.TableColumn: _table_column,
        # types
        ops.Cast: _cast,
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
        ops.RegexSearch: fixed_arity(sa.func.REGEXP_INSTR, 2),
        ops.RegexReplace: _regex_replace,
        ops.Translate: fixed_arity("translate", 3),
        ops.RegexExtract: _regex_extract,
        ops.StringJoin: _string_join,
        # math
        ops.Log: _log,
        ops.Log2: unary(lambda x: sa.func.log(2, x)),
        ops.Log10: unary(sa.func.log),
        ops.Round: _round,
        ops.Modulus: fixed_arity(operator.mod, 2),
        ops.Power: fixed_arity(sa.func.power, 2),
        # dates and times
        ops.Date: unary(lambda x: sa.cast(x, sa.Date)),
        ops.DateTruncate: _timestamp_truncate,
        ops.TimestampTruncate: _timestamp_truncate,
        ops.DateAdd: fixed_arity(operator.add, 2),
        ops.DateSub: fixed_arity(operator.sub, 2),
        ops.DateDiff: fixed_arity(operator.sub, 2),
        ops.TimestampAdd: fixed_arity(operator.add, 2),
        ops.TimestampSub: fixed_arity(operator.sub, 2),
        ops.TimestampDiff: fixed_arity(operator.sub, 2),
        ops.Strftime: _strftime,
        ops.ExtractYear: _extract("year"),
        ops.ExtractMonth: _extract("month"),
        ops.ExtractDay: _extract("day"),
        ops.ExtractDayOfYear: _extract("doy"),
        ops.ExtractQuarter: _extract("quarter"),
        ops.ExtractEpochSeconds: _extract("epoch"),
        ops.ExtractHour: _extract("hour"),
        ops.ExtractMinute: _extract("minute"),
        ops.ExtractSecond: _second,
        ops.ExtractMillisecond: _millisecond,
        ops.DayOfWeekIndex: _day_of_week_index,
        ops.DayOfWeekName: _day_of_week_name,
        ops.Sum: _reduction("sum"),
        ops.Mean: _reduction("avg"),
        ops.Min: _reduction("min"),
        ops.Max: _reduction("max"),
        ops.Variance: variance_reduction("var", suffix={"sample": "", "pop": "p"}),
        ops.RandomScalar: _random,
        ops.TimestampNow: lambda *args: sa.func.timezone("UTC", sa.func.now()),
        ops.CumulativeAll: unary(sa.func.bool_and),
        ops.CumulativeAny: unary(sa.func.bool_or),
        ops.IdenticalTo: _identical_to,
        # aggregate methods
        ops.Count: _reduction_count(sa.func.count),
    }
)
