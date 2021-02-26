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


import math

import pyodbc
import sqlalchemy as sa
import sqlalchemy.dialects.mssql as mssql

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.window as W
import third_party.ibis.ibis_mssql.expr.operations as ms_ops
import ibis.backends.base_sqlalchemy.alchemy as alch

from ibis import literal as L
from ibis.backends.base_sql import _cumulative_to_reduction, fixed_arity, unary


def raise_unsupported_op_error(translator, expr, *args):
    msg = "SQLServer backend doesn't support {} operation!"
    op = expr.op()
    raise com.UnsupportedOperationError(msg.format(type(op)))


def _reduction(func_name, cast_type='int32'):
    def reduction_compiler(t, expr):
        arg, where = expr.op().args

        if arg.type().equals(dt.boolean):
            arg = arg.cast(cast_type)

        func = getattr(sa.func, func_name)

        if where is not None:
            arg = where.ifelse(arg, None)
        return func(t.translate(arg))

    return reduction_compiler


def _reduction_count(sa_func):
    def formatter(t, expr):
        op = expr.op()
        *args, where = op.args

        return _reduction_format(t, sa_func, where, *args)

    return formatter


def _reduction_format(t, sa_func, where, arg, *args):
    if where is not None:
        arg = t.translate(where.ifelse(arg, None))
    else:
        arg = t.translate(arg)

    return sa_func(arg, *map(t.translate, args))


def _variance_reduction(func_name):
    suffix = {'sample': '', 'pop': 'p'}

    def variance_compiler(t, expr):
        arg, how, where = expr.op().args

        if arg.type().equals(dt.boolean):
            arg = arg.cast('int32')

        func = getattr(sa.func, '{}{}'.format(func_name, suffix.get(how, '')))

        if where is not None:
            arg = where.ifelse(arg, None)
        return func(t.translate(arg))

    return variance_compiler


def _substr(t, expr):
    f = sa.func.substring

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

    sa_arg = t.translate(arg)
    sa_substr = t.translate(substr)

    if start is not None:
        sa_start = t.translate(start)
        return sa.func.charindex(sa_substr, sa_arg, sa_start) - 1

    return sa.func.charindex(sa_substr, sa_arg) - 1


def _string_contains(t, expr):
    arg, substr, start, _ = expr.op().args

    sa_arg = t.translate(arg)
    sa_substr = t.translate(substr)

    if start is not None:
        sa_start = t.translate(start)
        return sa.func.charindex(sa_substr, sa_arg, sa_start) - 1

    variable = sa.func.charindex(sa_substr, sa_arg) - 1
    return sa.case([((variable >= 0), 1)], else_=0,)


def _string_like1(t, expr):
    arg, pattern, escape = expr.op().args
    result = t.translate(arg).like(t.translate(pattern), escape=escape)
    return sa.case([(result, 1)], else_=0,)


def _floor_divide(t, expr):
    left, right = map(t.translate, expr.op().args)
    return sa.func.floor(left / right)


def _extract(fmt):
    def translator(t, expr):
        (arg,) = expr.op().args
        sa_arg = t.translate(arg)
        return sa.cast(
            sa.func.datepart(sa.literal_column(fmt), sa_arg), sa.SMALLINT
        )

    return translator


def _round(t, expr):
    op = expr.op()
    arg, digits = op.args
    sa_arg = t.translate(arg)

    f = sa.func.round

    if digits is not None:
        sa_digits = t.translate(digits)
        return f(sa_arg, sa_digits)
    else:
        return f(sa_arg)


def _log(t, expr):
    arg, base = expr.op().args
    sa_arg = t.translate(arg)
    if base is not None:
        sa_base = t.translate(base)
        return sa.cast(
            sa.func.log(
                sa.cast(sa_arg, sa.NUMERIC), sa.cast(sa_base, sa.NUMERIC)
            ),
            t.get_sqla_type(expr.type()),
        )
    return sa.func.ln(sa_arg)


def _cumulative_to_window(translator, expr, window):
    win = W.cumulative_window()
    win = win.group_by(window._group_by).order_by(window._order_by)

    op = expr.op()

    klass = _cumulative_to_reduction[type(op)]
    new_op = klass(*op.args)
    new_expr = expr._factory(new_op, name=expr._name)

    if type(new_op) in translator._rewrites:
        new_expr = translator._rewrites[type(new_op)](new_expr)

    return L.windowize_function(new_expr, win)


def _window(t, expr):
    op = expr.op()

    arg, window = op.args
    reduction = t.translate(arg)

    window_op = arg.op()

    if isinstance(window_op, (ops.Sum, ops.Mean, ops.Min, ops.Max)):
        msg = """SQLServer backend doesn't support {}
         operation with Window Function!"""
        raise com.UnsupportedOperationError(msg.format(type(window_op)))

    _require_order_by = (
        ops.DenseRank,
        ops.MinRank,
        ops.NTile,
        ops.PercentRank,
        ops.Count,
        ops.Mean,
        ops.Min,
        ops.Max,
        ops.Sum,
        ops.FirstValue,
        ops.LastValue,
        ops.Lag,
        ops.Lead,
    )

    if isinstance(window_op, ops.CumulativeOp):
        arg = _cumulative_to_window(t, arg, window)
        return t.translate(arg)

    if window.max_lookback is not None:
        raise NotImplementedError(
            'Rows with max lookback is not implemented '
            'for SQLAlchemy-based backends.'
        )

    if isinstance(window_op, _require_order_by) and not window._order_by:
        order_by = t.translate(window_op.args[0])
    else:
        order_by = list(map(t.translate, window._order_by))

    partition_by = list(map(t.translate, window._group_by))

    frame_clause_not_allowed = (
        ops.Lag,
        ops.Lead,
        ops.DenseRank,
        ops.MinRank,
        ops.NTile,
        ops.PercentRank,
        ops.RowNumber,
    )

    how = {'range': 'range_'}.get(window.how, window.how)
    preceding = window.preceding
    additional_params = (
        {}
        if isinstance(window_op, frame_clause_not_allowed)
        else {
            how: (
                -preceding if preceding is not None else preceding,
                window.following,
            )
        }
    )
    result = reduction.over(
        partition_by=partition_by, order_by=order_by, **additional_params
    )

    if isinstance(
        window_op, (ops.RowNumber, ops.DenseRank, ops.MinRank, ops.NTile)
    ):
        return result - 1
    else:
        return result


def _lag(t, expr):
    arg, offset, default = expr.op().args
    if default is not None:
        raise NotImplementedError()

    sa_arg = t.translate(arg)
    sa_offset = t.translate(offset) if offset is not None else 1
    return sa.func.lag(sa_arg, sa_offset)


def _lead(t, expr):
    arg, offset, default = expr.op().args
    if default is not None:
        raise NotImplementedError()
    sa_arg = t.translate(arg)
    sa_offset = t.translate(offset) if offset is not None else 1
    return sa.func.lead(sa_arg, sa_offset)


def _ntile(t, expr):
    op = expr.op()
    args = op.args
    arg, buckets = map(t.translate, args)
    return sa.func.ntile(buckets)


def _hll_cardinality(t, expr):
    arg, _ = expr.op().args
    sa_arg = t.translate(arg)
    return sa.func.count(sa.distinct(sa_arg))


def _is_null1(t, expr):
    arg = t.translate(expr.op().args[0])
    res = arg.is_(sa.null())
    return sa.case([(res, 1)], else_=0)


def _not_null1(t, expr):
    arg = t.translate(expr.op().args[0])
    res = arg.isnot(sa.null())
    return sa.case([(res, 1)], else_=0)


def _not_any1(t, expr):
    arg = t.translate(expr.op().args[0])
    res = sa.func.max(sa.cast(arg, sa.INTEGER))
    return sa.case([(res > 0, 0)], else_=1)


def _not_all1(t, expr):
    arg = t.translate(expr.op().args[0])
    res = sa.func.min(sa.cast(arg, sa.INTEGER))
    return sa.case([(res > 0, 0)], else_=1)


def _between(t, expr):
    (arg, lower, upper,) = map(t.translate, expr.op().args)
    res = arg.between(lower, upper)
    return sa.case([(res, 1)], else_=0)


def _day_of_week_index(t, expr):
    (sa_arg,) = map(t.translate, expr.op().args)
    return sa.cast(
        sa.cast(sa.extract('dow', sa_arg) + 5, sa.SMALLINT) % 7, sa.SMALLINT
    )


def _day_of_week_name(t, expr):
    (sa_arg,) = map(t.translate, expr.op().args)
    return sa.func.trim(sa.func.format(sa_arg, 'dddd'))


_operation_registry = alch._operation_registry.copy()

_operation_registry.update(
    {
        # aggregate methods
        ops.Count: _reduction_count(sa.func.count),
        ops.Max: _reduction('max'),
        ops.Min: _reduction('min'),
        ops.Sum: _reduction('sum'),
        ops.Mean: _reduction('avg', 'float64'),
        # string methods
        ops.LStrip: unary(sa.func.ltrim),
        ops.Lowercase: unary(sa.func.lower),
        ops.RStrip: unary(sa.func.rtrim),
        ops.Repeat: fixed_arity(sa.func.replicate, 2),
        ops.Reverse: unary(sa.func.reverse),
        ops.StringFind: _string_find,
        ops.StringLength: unary(sa.func.length),
        ops.StringReplace: fixed_arity(sa.func.replace, 3),
        ops.Strip: unary(sa.func.trim),
        ops.Substring: _substr,
        ops.Uppercase: unary(sa.func.upper),
        # math
        ops.Abs: unary(sa.func.abs),
        ops.Acos: unary(sa.func.acos),
        ops.Asin: unary(sa.func.asin),
        ops.Atan2: fixed_arity(sa.func.atn2, 2),
        ops.Atan: unary(sa.func.atan),
        ops.Ceil: unary(sa.func.ceiling),
        ops.Cos: unary(sa.func.cos),
        ops.Floor: unary(sa.func.floor),
        ops.FloorDivide: _floor_divide,
        ops.Power: fixed_arity(sa.func.power, 2),
        ops.Sign: unary(sa.func.sign),
        ops.Sin: unary(sa.func.sin),
        ops.Sqrt: unary(sa.func.sqrt),
        ops.Tan: unary(sa.func.tan),
        # timestamp methods
        ops.TimestampNow: fixed_arity(sa.func.GETDATE, 0),
        ops.ExtractYear: _extract('year'),
        ops.ExtractMonth: _extract('month'),
        ops.ExtractDay: _extract('day'),
        ops.ExtractHour: _extract('hour'),
        ops.ExtractMinute: _extract('minute'),
        ops.ExtractSecond: _extract('second'),
        ops.ExtractMillisecond: _extract('millisecond'),
        # newly added
        ops.Lag: _lag,
        ops.Lead: _lead,
        ops.NTile: _ntile,
        ops.FirstValue: unary(sa.func.first_value),
        ops.LastValue: unary(sa.func.last_value),
        ops.RowNumber: fixed_arity(lambda: sa.func.row_number(), 0),
        ops.WindowOp: _window,
        ops.CumulativeOp: _window,
        ops.DayOfWeekIndex: _day_of_week_index,
        ops.DayOfWeekName: _day_of_week_name,
        ops.Round: _round,
        ops.Log: _log,
        ops.Log2: unary(lambda x: sa.func.log(x, 2)),
        ops.Log10: unary(lambda x: sa.func.log(x, 10)),
        ops.Ln: unary(lambda x: sa.func.log(x, math.e)),
        ops.Exp: unary(sa.func.exp),
        ops.StringAscii: unary(sa.func.ascii),
        ops.Variance: _variance_reduction('var'),
        ops.StandardDev: _variance_reduction('stdev'),
        ms_ops.StringContains: _string_contains,
        ms_ops.StringSQLLike: _string_like1,
        ops.HLLCardinality: _hll_cardinality,
        ms_ops.IsNull: _is_null1,
        ms_ops.NotNull: _not_null1,
        ops.DenseRank: unary(lambda arg: sa.func.dense_rank()),
        ops.MinRank: unary(lambda arg: sa.func.rank()),
        ops.PercentRank: unary(lambda arg: sa.func.percent_rank()),
        ops.NullIf: fixed_arity(sa.func.nullif, 2),
        ops.IfNull: fixed_arity(sa.func.coalesce, 2),
        ms_ops.Between: _between,
        ops.Translate: fixed_arity('translate', 3),
        ms_ops.NotAny: _not_any1,
        ms_ops.NotAll: _not_all1,
    }
)


_unsupported_ops = [
    ops.NotAny,
    ops.Least,
    ops.Greatest,
    ops.LPad,  # not supported by mssql
    ops.RPad,  # not supported by mssql
    ops.Capitalize,  # not supported by mssql
    ops.RegexSearch,  # not supported by mssql
    ops.RegexExtract,  # not supported by mssql
    ops.RegexReplace,  # not supported by mssql
    # aggregate methods
    ops.CumulativeMax,
    ops.CumulativeMin,
    ops.CumulativeMean,
    ops.CumulativeSum,
    # datetime methods
    ops.TimestampTruncate,
]


_unsupported_ops = {k: raise_unsupported_op_error for k in _unsupported_ops}
_operation_registry.update(_unsupported_ops)


class MSSQLExprTranslator(alch.AlchemyExprTranslator):
    _registry = _operation_registry
    _rewrites = alch.AlchemyExprTranslator._rewrites.copy()
    _type_map = alch.AlchemyExprTranslator._type_map.copy()
    _type_map.update(
        {
            dt.Boolean: pyodbc.SQL_BIT,
            dt.Int8: mssql.TINYINT,
            dt.Int32: mssql.INTEGER,
            dt.Int64: mssql.BIGINT,
            dt.Float: mssql.REAL,
            dt.Double: mssql.REAL,
            dt.String: mssql.VARCHAR,
        }
    )


rewrites = MSSQLExprTranslator.rewrites
compiles = MSSQLExprTranslator.compiles


class MSSQLDialect(alch.AlchemyDialect):

    translator = MSSQLExprTranslator


dialect = MSSQLDialect
