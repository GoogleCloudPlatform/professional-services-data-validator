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
import itertools
import locale
import platform
import re
import string
import warnings
import operator

import sqlalchemy as sa

from ibis.backends.base.sql.alchemy.registry import _cast as sa_fixed_cast
import ibis.common.exceptions as com
import ibis.expr.operations as ops

from ibis.backends.base.sql.alchemy import (
    fixed_arity,
    sqlalchemy_operation_registry,
    sqlalchemy_window_functions_registry,
    unary,
    get_sqla_table,
)

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


def _cast(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)
    if (
        arg_dtype.is_decimal()
        and typ.is_string()
        and (arg_dtype.scale is None or arg_dtype.scale > 0)
    ):
        # Specialize going from fractional decimal type to a string.
        # Oracle has an edge case that non-zero values <1 are converted to a string without a leading zero.
        # For example 0.5 becomes ".5", -0.5 becomes "-.5". This does not match most supported engines.
        # When we have precision/scale we can use a TO_CHAR format but Oracle supports NUMBER(*) - without precision/scale.
        # The ugly expression here is the only efficient way I can come up with
        # to get the output we want:
        #   SELECT CASE WHEN c1>0 AND c1<1 THEN '0'||TO_CHAR(0.5)
        #               WHEN c1>-1 AND c1<0 THEN '-0'||TO_CHAR(ABS(0.5))
        #               ELSE TO_CHAR(c1) END
        #   FROM (SELECT 0.5 c1 FROM dual);
        #
        #   CAS
        #   ---
        #   0.5
        return sa.literal_column(
            (
                "CASE "
                f"WHEN {sa_arg}>0 AND {sa_arg}<1 THEN '0'||TO_CHAR({sa_arg}) "
                f"WHEN {sa_arg}>-1 AND {sa_arg}<0 THEN '-0'||TO_CHAR(ABS({sa_arg})) "
                f"ELSE TO_CHAR({sa_arg}) END"
            )
        )
    elif arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return sa.func.lower(sa.func.rawtohex(sa_arg))
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return sa.func.hextoraw(sa_arg)
    elif (arg_dtype.is_float32() or arg_dtype.is_float64()) and typ.is_string():
        # Specialize going from a binary float type to a string.
        # This prevents output in scientific notation but we still have
        # problems with the lossy nature of BINARY_FLOAT/DOUBLE.
        return sa.func.to_char(sa_arg, "TM9")
    elif arg_dtype.is_interval() and typ.is_string():
        return sa.func.to_char(sa_arg)
    elif arg_dtype.is_string() and typ.is_boolean():
        return sa.literal_column(
            (
                "CASE  "
                f"WHEN {sa_arg} IN ('0','N') THEN 0 "
                f"WHEN {sa_arg} IN ('1','Y') THEN 1 "
                "ELSE TO_NUMBER(NULL) END"
            )
        )
    elif (arg_dtype.is_timestamp() or arg_dtype.is_date()) and typ.is_date():
        # If we are casting to Date then simulate what all other engines
        # understand to be a Date, which is without the time element.
        return sa.func.cast(sa.func.TRUNC(sa_arg), sa.types.Date)

    # Follow the original Ibis code path.
    return sa_fixed_cast(t, op)


def _typeof(t, op):
    sa_arg = t.translate(op.arg)
    typ = sa.func.dump(sa_arg)
    return typ


def _string_agg(t, op):
    agg = sa.func.string_agg(t.translate(op.arg), t.translate(op.sep))
    if (where := op.where) is not None:
        return agg.filter(t.translate(where))
    return agg


_strftime_to_oracle_rules = {
    "%a": "TMDy",  # TM does it in a locale dependent way
    "%A": "TMDay",
    "%b": "TMMon",  # Sep
    "%B": "TMMonth",  # September
    "%d": "DD",  # day of month
    "%H": "HH24",  # 09
    "%I": "HH12",  # 09
    "%j": "DDD",  # zero padded day of year
    "%m": "MM",  # 01
    "%M": "MI",  # zero padded minute
    "%p": "AM",  # AM or PM
    "%S": "SS",  # zero padded second
    "%U": "WW",  # 1-based week of year
    "%w": "D",
    "%y": "YY",  # 15
    "%Y": "YYYY",  # 2015
    "%Z": "TZ",  # uppercase timezone name
}

try:
    _strftime_to_oracle_rules.update(
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


# translate strftime spec into mostly equivalent Oracle spec
_scanner = re.Scanner(
    # double quotes need to be escaped
    [('"', lambda scanner, token: r"\"")]
    + [
        (
            "|".join(
                map(
                    "(?:{})".format,
                    itertools.chain(
                        _strftime_to_oracle_rules.keys(),
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


_lexicon_values = frozenset(_strftime_to_oracle_rules.values())

_strftime_blacklist = frozenset(["%w", "%U", "%c", "%x", "%X", "%e"])


def _reduce_tokens(tokens, arg):
    # current list of tokens
    curtokens = []

    # reduced list of tokens that accounts for blacklisted values
    reduced = []

    non_special_tokens = frozenset(_strftime_to_oracle_rules) - _strftime_blacklist

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
            if token == "%w":
                value = sa.extract("dow", arg)  # 0 based day of week
            elif token == "%U":
                value = sa.cast(sa.func.to_char(arg, "WW"), sa.SMALLINT) - 1
            elif token == "%c" or token == "%x" or token == "%X":
                # re scan and tokenize this pattern
                try:
                    new_pattern = _strftime_to_oracle_rules[token]
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


def _strftime(t, op):
    tokens, _ = _scanner.scan(op.format_str.value)
    reduced = _reduce_tokens(tokens, t.translate(op.arg))
    return functools.reduce(sa.sql.ColumnElement.concat, reduced)


def _regex_replace(t, op):
    return sa.func.regexp_replace(
        t.translate(op.string), t.translate(op.pattern), t.translate(op.replacement)
    )


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

    if op.output_dtype.is_timestamp():
        timezone = op.output_dtype.timezone
        if timezone is not None:
            # Using literal_column on Oracle because the time zone string cannot be a bind.
            out_expr = sa.literal_column(
                f"{out_expr.name} AT TIME ZONE '{timezone}'"
            ).label(op.name)

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
    elif dtype.is_array():
        return pg.array(value)
    elif dtype.is_map():
        return pg.hstore(list(value.keys()), list(value.values()))
    else:
        return sa.literal(value)


def _day_of_week_index(t, op):
    return sa.func.to_char(t.translate(op.arg), "d")


def _day_of_week_name(t, op):
    return sa.func.to_char(t.translate(op.arg), "Day")


def _random(t, op):
    return sa.sql.literal_column("DBMS_RANDOM.VALUE")


def _extract_epoch(t, op):
    sub = operator.sub(
        sa.cast(t.translate(op.arg), sa.DATE),
        sa.sql.literal_column("DATE '1970-01-01'"),
    ).self_group()
    mult = sa.cast(
        operator.mul(sub, sa.sql.literal_column("86400")), sa.dialects.oracle.NUMBER
    )
    return mult


operation_registry.update(
    {
        ops.Literal: _literal,
        # We override this here to support time zones
        ops.TableColumn: _table_column,
        # types
        ops.Cast: _cast,
        ops.TypeOf: _typeof,
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
        ops.RegexSearch: fixed_arity(lambda x, y: x.op("~")(y), 2),
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
        ops.IntervalFromInteger: (
            lambda t, op: t.translate(op.arg)
            * sa.text(f"INTERVAL '1 {op.output_dtype.resolution}'")
        ),
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
        ops.ExtractEpochSeconds: _extract_epoch,
        ops.ExtractHour: _extract("hour"),
        ops.ExtractMinute: _extract("minute"),
        ops.ExtractSecond: _second,
        ops.DayOfWeekIndex: _day_of_week_index,
        ops.DayOfWeekName: _day_of_week_name,
        ops.RandomScalar: _random,
        # now is in the timezone of the server, but we want UTC
        ops.TimestampNow: lambda *args: sa.func.timezone("UTC", sa.func.now()),
    }
)
