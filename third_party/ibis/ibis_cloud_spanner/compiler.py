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
from functools import partial

import numpy as np
import regex as re
import toolz
from multipledispatch import Dispatcher

import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.sql.compiler as comp
from third_party.ibis.ibis_cloud_spanner.datatypes import ibis_type_to_cloud_spanner_type
from ibis.impala import compiler as impala_compiler
from ibis.impala.compiler import (
    ImpalaSelect,
    ImpalaTableSetFormatter,
    _reduction,
    fixed_arity,
    unary,
)


class CloudSpannerSelectBuilder(comp.SelectBuilder):
    @property
    def _select_class(self):
        return CloudSpannerSelect



class CloudSpannerUnion(comp.Union):
    @staticmethod
    def keyword(distinct):
        return 'UNION DISTINCT' if distinct else 'UNION ALL'



class CloudSpannerQueryBuilder(comp.QueryBuilder):

    select_builder = CloudSpannerSelectBuilder
    union_class = CloudSpannerUnion


def build_ast(expr, context):
    builder = CloudSpannerQueryBuilder(expr, context=context)
    return builder.get_result()


def to_sql(expr, context):
    query_ast = build_ast(expr, context)
    compiled = query_ast.compile()
    return compiled


class CloudSpannerContext(comp.QueryContext):
    def _to_sql(self, expr, ctx):
        return to_sql(expr, context=ctx)


def _extract_field(sql_attr):
    def extract_field_formatter(translator, expr):
        op = expr.op()
        arg = translator.translate(op.args[0])
        if sql_attr == 'epochseconds':
            return f'UNIX_SECONDS({arg})'
        else:
            return f'EXTRACT({sql_attr} from {arg})'

    return extract_field_formatter


cloud_spanner_cast = Dispatcher('cloud_spanner_cast')


@cloud_spanner_cast.register(str, dt.Timestamp, dt.Integer)
def cloud_spanner_cast_timestamp_to_integer(compiled_arg, from_, to):
    return 'UNIX_MICROS({})'.format(compiled_arg)


@cloud_spanner_cast.register(str, dt.DataType, dt.DataType)
def cloud_spanner_cast_generate(compiled_arg, from_, to):
    sql_type = ibis_type_to_cloud_spanner_type(to)
    return 'CAST({} AS {})'.format(compiled_arg, sql_type)


def _cast(translator, expr):
    op = expr.op()
    arg, target_type = op.args
    arg_formatted = translator.translate(arg)
    return cloud_spanner_cast(arg_formatted, arg.type(), target_type)


# def _struct_field(translator, expr):
#     arg, field = expr.op().args
#     arg_formatted = translator.translate(arg)
#     return '{}.`{}`'.format(arg_formatted, field)


def _array_concat(translator, expr):
    return 'ARRAY_CONCAT({})'.format(
        ', '.join(map(translator.translate, expr.op().args))
    )


def _array_index(translator, expr):
    # SAFE_OFFSET returns NULL if out of bounds
    return '{}[OFFSET({})]'.format(
        *map(translator.translate, expr.op().args)
    )


def _string_find(translator, expr):
    haystack, needle, start, end = expr.op().args

    if start is not None:
        raise NotImplementedError('start not implemented for string find')
    if end is not None:
        raise NotImplementedError('end not implemented for string find')

    return 'STRPOS({}, {}) - 1'.format(
        translator.translate(haystack), translator.translate(needle)
    )


def _translate_pattern(translator, pattern):
    # add 'r' to string literals to indicate to Cloud Spanner this is a raw string
    return 'r' * isinstance(pattern.op(), ops.Literal) + translator.translate(
        pattern
    )


def _regex_search(translator, expr):
    arg, pattern = expr.op().args
    regex = _translate_pattern(translator, pattern)
    result = 'REGEXP_CONTAINS({}, {})'.format(translator.translate(arg), regex)
    return result


def _regex_extract(translator, expr):
    arg, pattern, index = expr.op().args
    regex = _translate_pattern(translator, pattern)
    result = 'REGEXP_EXTRACT({}, {})'.format(
        translator.translate(arg), regex
    )
    return result


def _regex_replace(translator, expr):
    arg, pattern, replacement = expr.op().args
    regex = _translate_pattern(translator, pattern)
    result = 'REGEXP_REPLACE({}, {}, {})'.format(
        translator.translate(arg), regex, translator.translate(replacement)
    )
    return result


def _string_concat(translator, expr):
    return 'CONCAT({})'.format(
        ', '.join(map(translator.translate, expr.op().arg))
    )


def _string_join(translator, expr):
    sep, args = expr.op().args
    return 'ARRAY_TO_STRING([{}], {})'.format(
        ', '.join(map(translator.translate, args)), translator.translate(sep)
    )


def _string_ascii(translator, expr):
    (arg,) = expr.op().args
    return 'TO_CODE_POINTS({})[SAFE_OFFSET(0)]'.format(
        translator.translate(arg)
    )


def _string_right(translator, expr):
    arg, nchars = map(translator.translate, expr.op().args)
    return 'SUBSTR({arg}, -LEAST(LENGTH({arg}), {nchars}))'.format(
        arg=arg, nchars=nchars
    )


def _array_literal_format(expr):
    return str(list(expr.op().value))


def _log(translator, expr):
    op = expr.op()
    arg, base = op.args
    arg_formatted = translator.translate(arg)

    if base is None:
        return 'ln({})'.format(arg_formatted)

    base_formatted = translator.translate(base)
    return 'log({}, {})'.format(arg_formatted, base_formatted)


def _literal(translator, expr):

    if isinstance(expr, ir.NumericValue):
        value = expr.op().value
        if not np.isfinite(value):
            return 'CAST({!r} AS FLOAT64)'.format(str(value))

    # special case literal timestamp, date, and time scalars
    if isinstance(expr.op(), ops.Literal):
        value = expr.op().value
        if isinstance(expr, ir.DateScalar):
            if isinstance(value, datetime.datetime):
                raw_value = value.date()
            else:
                raw_value = value
            return "DATE '{}'".format(raw_value)
        elif isinstance(expr, ir.TimestampScalar):
            return "TIMESTAMP '{}'".format(value)
        elif isinstance(expr, ir.TimeScalar):
            # TODO: define extractors on TimeValue expressions
            return "TIME '{}'".format(value)

    try:
        return impala_compiler._literal(translator, expr)
    except NotImplementedError:
        if isinstance(expr, ir.ArrayValue):
            return _array_literal_format(expr)
        raise NotImplementedError(type(expr).__name__)


def _arbitrary(translator, expr):
    arg, how, where = expr.op().args

    if where is not None:
        arg = where.ifelse(arg, ibis.NA)

    if how not in (None, 'first'):
        raise com.UnsupportedOperationError(
            '{!r} value not supported for arbitrary in Cloud Spanner'.format(how)
        )

    return 'ANY_VALUE({})'.format(translator.translate(arg))


_date_units = {
    'Y': 'YEAR',
    'Q': 'QUARTER',
    'W': 'WEEK',
    'M': 'MONTH',
    'D': 'DAY',
}


_timestamp_units = {
    'us': 'MICROSECOND',
    'ms': 'MILLISECOND',
    's': 'SECOND',
    'm': 'MINUTE',
    'h': 'HOUR',
}
_time_units = _timestamp_units.copy()
_timestamp_units.update(_date_units)


def _truncate(kind, units):
    def truncator(translator, expr):
        arg, unit = expr.op().args
        trans_arg = translator.translate(arg)
        valid_unit = units.get(unit)
        if valid_unit is None:
            raise com.UnsupportedOperationError(
                'Cloud Spanner does not support truncating {} values to unit '
                '{!r}'.format(arg.type(), unit)
            )
        return '{}_TRUNC({}, {})'.format(kind, trans_arg, valid_unit)

    return truncator


def _timestamp_op(func, units):
    def _formatter(translator, expr):
        op = expr.op()
        arg, offset = op.args

        unit = offset.type().unit
        if unit not in units:
            raise com.UnsupportedOperationError(
                'Cloud Spanner does not allow binary operation '
                '{} with INTERVAL offset {}'.format(func, unit)
            )
        formatted_arg = translator.translate(arg)
        formatted_offset = translator.translate(offset)
        result = '{}({}, {})'.format(func, formatted_arg, formatted_offset)
        return result

    return _formatter


STRFTIME_FORMAT_FUNCTIONS = {
    dt.Date: 'DATE',
    dt.Time: 'TIME',
    dt.Timestamp: 'TIMESTAMP',
}


_operation_registry = impala_compiler._operation_registry.copy()
_operation_registry.update(
    {
        ops.ExtractYear: _extract_field('year'),
        #ops.ExtractQuarter: _extract_field('quarter'),
        ops.ExtractMonth: _extract_field('month'),
        ops.ExtractDay: _extract_field('day'),
        ops.ExtractHour: _extract_field('hour'),
        ops.ExtractMinute: _extract_field('minute'),
        ops.ExtractSecond: _extract_field('second'),
        ops.ExtractMillisecond: _extract_field('millisecond'),
        #ops.ExtractEpochSeconds: _extract_field('epochseconds'),
        ops.StringReplace: fixed_arity('REPLACE', 3),
        ops.StringSplit: fixed_arity('SPLIT', 2),
        ops.StringConcat: _string_concat,
        ops.StringJoin: _string_join,
        ops.StringAscii: _string_ascii,
        ops.StringFind: _string_find,
        ops.StrRight: _string_right,
        ops.Repeat: fixed_arity('REPEAT', 2),
        ops.RegexSearch: _regex_search,
        ops.RegexExtract: _regex_extract,
        ops.RegexReplace: _regex_replace,
        ops.GroupConcat: _reduction('STRING_AGG'),
        ops.IfNull: fixed_arity('IFNULL', 2),
        ops.Cast: _cast,
        #ops.StructField: _struct_field,
        ops.ArrayCollect: unary('ARRAY_AGG'),
        ops.ArrayConcat: _array_concat,
        ops.ArrayIndex: _array_index,
        ops.ArrayLength: unary('ARRAY_LENGTH'),
        ops.HLLCardinality: _reduction('APPROX_COUNT_DISTINCT'),
        ops.Log: _log,
        ops.Sign: unary('SIGN'),
        ops.Modulus: fixed_arity('MOD', 2),
        ops.Date: unary('DATE'),
        # Cloud Spanner doesn't have these operations built in.
        # ops.ArrayRepeat: _array_repeat,
        # ops.ArraySlice: _array_slice,
        ops.Literal: _literal,
        ops.Arbitrary: _arbitrary,
        ops.TimestampTruncate: _truncate('TIMESTAMP', _timestamp_units),
        ops.DateTruncate: _truncate('DATE', _date_units),
        ops.TimeTruncate: _truncate('TIME', _timestamp_units),
        ops.Time: unary('TIME'),
        ops.TimestampAdd: _timestamp_op(
            'TIMESTAMP_ADD', {'h', 'm', 's', 'ms', 'us'}
        ),
        ops.TimestampSub: _timestamp_op(
            'TIMESTAMP_DIFF', {'h', 'm', 's', 'ms', 'us'}
        ),
        ops.DateAdd: _timestamp_op('DATE_ADD', {'D', 'W', 'M', 'Q', 'Y'}),
        ops.DateSub: _timestamp_op('DATE_SUB', {'D', 'W', 'M', 'Q', 'Y'}),
        ops.TimestampNow: fixed_arity('CURRENT_TIMESTAMP', 0),
    }
)

_invalid_operations = {
    ops.Translate,
    ops.FindInSet,
    ops.Capitalize,
    ops.DateDiff,
    ops.TimestampDiff,
}

_operation_registry = {
    k: v
    for k, v in _operation_registry.items()
    if k not in _invalid_operations
}


class CloudSpannerExprTranslator(impala_compiler.ImpalaExprTranslator):
    _registry = _operation_registry
    _rewrites = impala_compiler.ImpalaExprTranslator._rewrites.copy()

    context_class = CloudSpannerContext

    def _trans_param(self, expr):
        op = expr.op()
        if op not in self.context.params:
            raise KeyError(op)
        return '@{}'.format(expr.get_name())


compiles = CloudSpannerExprTranslator.compiles
rewrites = CloudSpannerExprTranslator.rewrites


@compiles(ops.DayOfWeekIndex)
def cloud_spanner_day_of_week_index(t, e):
    arg = e.op().args[0]
    arg_formatted = t.translate(arg)
    return 'MOD(EXTRACT(DAYOFWEEK FROM {}) + 5, 7)'.format(arg_formatted)


@rewrites(ops.DayOfWeekName)
def cloud_spanner_day_of_week_name(e):
    arg = e.op().args[0]
    return arg.strftime('%A')


@compiles(ops.Divide)
def cloud_spanner_compiles_divide(t, e):
    return 'IEEE_DIVIDE({}, {})'.format(*map(t.translate, e.op().args))


@compiles(ops.Strftime)
def compiles_strftime(translator, expr):
    arg, format_string = expr.op().args
    arg_type = arg.type()
    strftime_format_func_name = STRFTIME_FORMAT_FUNCTIONS[type(arg_type)]
    fmt_string = translator.translate(format_string)
    arg_formatted = translator.translate(arg)
    if isinstance(arg_type, dt.Timestamp):
        return 'FORMAT_{}({}, {}, {!r})'.format(
            strftime_format_func_name,
            fmt_string,
            arg_formatted,
            arg_type.timezone if arg_type.timezone is not None else 'UTC',
        )
    return 'FORMAT_{}({}, {})'.format(
        strftime_format_func_name, fmt_string, arg_formatted
    )


@compiles(ops.StringToTimestamp)
def compiles_string_to_timestamp(translator, expr):
    arg, format_string, timezone_arg = expr.op().args
    fmt_string = translator.translate(format_string)
    arg_formatted = translator.translate(arg)
    if timezone_arg is not None:
        timezone_str = translator.translate(timezone_arg)
        return 'PARSE_TIMESTAMP({}, {}, {})'.format(
            fmt_string, arg_formatted, timezone_str
        )
    return 'PARSE_TIMESTAMP({}, {})'.format(fmt_string, arg_formatted)


class CloudSpannerTableSetFormatter(ImpalaTableSetFormatter):
    def _quote_identifier(self, name):
        if re.match(r'^[A-Za-z][A-Za-z_0-9]*$', name):
            return name
        return '`{}`'.format(name)


class CloudSpannerSelect(ImpalaSelect):

    translator = CloudSpannerExprTranslator

    @property
    def table_set_formatter(self):
        return CloudSpannerTableSetFormatter


@rewrites(ops.IdenticalTo)
def identical_to(expr):
    left, right = expr.op().args
    return (left.isnull() & right.isnull()) | (left == right)


@rewrites(ops.Log2)
def log2(expr):
    (arg,) = expr.op().args
    return arg.log(2)


@rewrites(ops.Sum)
def bq_sum(expr):
    arg = expr.op().args[0]
    where = expr.op().args[1]
    if isinstance(arg, ir.BooleanColumn):
        return arg.cast('int64').sum(where=where)
    else:
        return expr


@rewrites(ops.Mean)
def bq_mean(expr):
    arg = expr.op().args[0]
    where = expr.op().args[1]
    if isinstance(arg, ir.BooleanColumn):
        return arg.cast('int64').mean(where=where)
    else:
        return expr


UNIT_FUNCS = {'s': 'SECONDS', 'ms': 'MILLIS', 'us': 'MICROS'}


@compiles(ops.TimestampFromUNIX)
def compiles_timestamp_from_unix(t, e):
    value, unit = e.op().args
    return 'TIMESTAMP_{}({})'.format(UNIT_FUNCS[unit], t.translate(value))


@compiles(ops.Floor)
def compiles_floor(t, e):
    cs_type = ibis_type_to_cloud_spanner_type(e.type())
    arg = e.op().arg
    return 'CAST(FLOOR({}) AS {})'.format(t.translate(arg), cs_type)


@compiles(ops.CMSMedian)
def compiles_approx(translator, expr):
    expr = expr.op()
    arg = expr.arg
    where = expr.where

    if where is not None:
        arg = where.ifelse(arg, ibis.NA)

    return 'APPROX_QUANTILES({}, 2)[OFFSET(1)]'.format(
        translator.translate(arg)
    )


@compiles(ops.Covariance)
def compiles_covar(translator, expr):
    expr = expr.op()
    left = expr.left
    right = expr.right
    where = expr.where

    if expr.how == 'sample':
        how = 'SAMP'
    elif expr.how == 'pop':
        how = 'POP'
    else:
        raise ValueError(
            "Covariance with how={!r} is not supported.".format(how)
        )

    if where is not None:
        left = where.ifelse(left, ibis.NA)
        right = where.ifelse(right, ibis.NA)

    return "COVAR_{}({}, {})".format(how, left, right)


@rewrites(ops.Any)
@rewrites(ops.All)
@rewrites(ops.NotAny)
@rewrites(ops.NotAll)
def cloud_spanner_any_all_no_op(expr):
    return expr


@compiles(ops.Any)
def cloud_spanner_compile_any(translator, expr):
    return "LOGICAL_OR({})".format(*map(translator.translate, expr.op().args))


@compiles(ops.NotAny)
def cloud_spanner_compile_notany(translator, expr):
    return "LOGICAL_AND(NOT ({}))".format(
        *map(translator.translate, expr.op().args)
    )


@compiles(ops.All)
def cloud_spanner_compile_all(translator, expr):
    return "LOGICAL_AND({})".format(*map(translator.translate, expr.op().args))


@compiles(ops.NotAll)
def cloud_spanner_compile_notall(translator, expr):
    return "LOGICAL_OR(NOT ({}))".format(
        *map(translator.translate, expr.op().args)
    )


class CloudSpannerDialect(impala_compiler.ImpalaDialect):
    translator = CloudSpannerExprTranslator


dialect = CloudSpannerDialect



