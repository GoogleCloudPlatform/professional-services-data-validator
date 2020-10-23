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
import operator

import ibis.common.exceptions as com
import ibis.expr.types as ir
import third_party.ibis.ibis_mssql.expr.operations as ms_ops
import ibis.util as util
from ibis.expr.api import _add_methods
from ibis.expr.groupby import GroupedTableExpr  # noqa
from ibis.expr.random import random  # noqa
from ibis.expr.types import (  # noqa
    ArrayColumn,
    ArrayScalar,
    ArrayValue,
    BooleanColumn,
    BooleanScalar,
    BooleanValue,
    CategoryScalar,
    CategoryValue,
    ColumnExpr,
    DateColumn,
    DateScalar,
    DateValue,
    DecimalColumn,
    DecimalScalar,
    DecimalValue,
    Expr,
    FloatingColumn,
    FloatingScalar,
    FloatingValue,
    GeoSpatialColumn,
    GeoSpatialScalar,
    GeoSpatialValue,
    IntegerColumn,
    IntegerScalar,
    IntegerValue,
    IntervalColumn,
    IntervalScalar,
    IntervalValue,
    LineStringColumn,
    LineStringScalar,
    LineStringValue,
    MapColumn,
    MapScalar,
    MapValue,
    MultiLineStringColumn,
    MultiLineStringScalar,
    MultiLineStringValue,
    MultiPointColumn,
    MultiPointScalar,
    MultiPointValue,
    MultiPolygonColumn,
    MultiPolygonScalar,
    MultiPolygonValue,
    NullColumn,
    NullScalar,
    NullValue,
    NumericColumn,
    NumericScalar,
    NumericValue,
    PointColumn,
    PointScalar,
    PointValue,
    PolygonColumn,
    PolygonScalar,
    PolygonValue,
    ScalarExpr,
    StringColumn,
    StringScalar,
    StringValue,
    StructColumn,
    StructScalar,
    StructValue,
    TableExpr,
    TimeColumn,
    TimeScalar,
    TimestampColumn,
    TimestampScalar,
    TimestampValue,
    TimeValue,
    ValueExpr,
    as_value_expr,
    literal,
    null,
    param,
    sequence,
)


def _string_contains(arg, substr):
    return arg.find1(substr)


def _string_find1(self, substr, start=None, end=None):
    if end is not None:
        raise NotImplementedError
    return ms_ops.StringContains(self, substr, start, end).to_expr()


def _string_like1(self, patterns):
    return functools.reduce(
        operator.or_,
        (
            ms_ops.StringSQLLike(self, pattern).to_expr()
            for pattern in util.promote_list(patterns)
        ),
    )


def _unary_op(name, klass, doc=None):
    def f(arg):
        return klass(arg).to_expr()

    f.__name__ = name
    if doc is not None:
        f.__doc__ = doc
    else:
        f.__doc__ = klass.__doc__
    return f


def _between(arg, lower, upper):
    lower = as_value_expr(lower)
    upper = as_value_expr(upper)

    op = ms_ops.Between(arg, lower, upper)
    return op.to_expr()


def _binop_expr(name, klass):
    def f(self, other):
        try:
            other = as_value_expr(other)
            op = klass(self, other)
            return op.to_expr()
        except (com.IbisTypeError, NotImplementedError):
            return NotImplemented

    f.__name__ = name

    return f


_string_value_methods = dict(
    contains=_string_contains, find1=_string_find1, like=_string_like1,
)

_generic_value_methods = dict(
    isnull=_unary_op('isnull', ms_ops.IsNull),
    notnull=_unary_op('notnull', ms_ops.NotNull),
    between=_between,
)

_boolean_column_methods = dict(
    notany=_unary_op('notany', ms_ops.NotAny),
    notall=_unary_op('notany', ms_ops.NotAll),
)

_add_methods(ir.BooleanColumn, _boolean_column_methods)
_add_methods(ir.StringValue, _string_value_methods)
_add_methods(ir.ValueExpr, _generic_value_methods)
