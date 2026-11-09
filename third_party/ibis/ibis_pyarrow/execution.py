# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Monkey patches for Ibis PyArrow execution and precision-preserving schema alignment.

Standard upstream Ibis implementations:
  - BaseSQLBackend.to_pyarrow_batches (ibis/backends/base/sql/__init__.py):
      Calls pa.array(map(tuple, batch), type=array_type) with the statically
      inferred schema. If the database cursor returns Decimals or floats with
      different precision or scale than statically inferred (e.g. Postgres
      STDDEV, AVG, or Decimal conversions), PyArrow throws:
      "Rescaling Decimal128 value would cause data loss" or
      "tried to convert to double".
  - BaseBackend.to_pyarrow & SnowflakeBackend.to_pyarrow:
      Call table.cast(arrow_schema) directly on the entire table. If an aggregate
      returns a decimal with a higher scale or precision than the column's static
      schema (e.g. STDDEV of numeric(10, 2) returning scale 20, or SUM exceeding
      statically inferred precision), casting directly to arrow_schema either
      truncates significant decimal digits or raises ArrowInvalid.
"""

import decimal

import ibis
import pyarrow as pa
import pyarrow.compute as pc
from ibis.backends.base import BaseBackend
from ibis.backends.base.sql import BaseSQLBackend

try:
    from ibis.backends.snowflake import Backend as SnowflakeBackend
except Exception:
    SnowflakeBackend = None


# Largest integer magnitude a float64 can represent exactly.
_MAX_EXACT_FLOAT_INT = 2**53


def _unify_record_batches(batches):
    """Unify schemas across multiple inferred RecordBatches when batch types differ."""
    if len(batches) <= 1 or all(b.schema == batches[0].schema for b in batches[1:]):
        return batches[0].schema, batches

    unified_fields = []
    for col_idx, field0 in enumerate(batches[0].schema):
        col_types = [b.schema.field(col_idx).type for b in batches]
        non_null_types = [t for t in col_types if not pa.types.is_null(t)]
        if not non_null_types:
            unified_type = pa.null()
        elif all(t == non_null_types[0] for t in non_null_types):
            unified_type = non_null_types[0]
        elif all(
            pa.types.is_decimal(t) or pa.types.is_integer(t) for t in non_null_types
        ) and any(pa.types.is_decimal(t) for t in non_null_types):
            int_digits = []
            scales = []
            for t in non_null_types:
                if pa.types.is_decimal(t):
                    int_digits.append(t.precision - t.scale)
                    scales.append(t.scale)
                elif pa.types.is_uint64(t):
                    int_digits.append(20)
                    scales.append(0)
                else:
                    int_digits.append(19)
                    scales.append(0)
            max_scale = max(scales)
            precision = max(1, max(int_digits) + max_scale)
            if precision <= 38 and not any(
                pa.types.is_decimal256(t) for t in non_null_types
            ):
                unified_type = pa.decimal128(precision, max_scale)
            else:
                unified_type = pa.decimal256(min(precision, 76), max_scale)
        else:
            unified_type = (
                pa.unify_schemas(
                    [pa.schema([pa.field(field0.name, t)]) for t in non_null_types],
                    promote_options="permissive",
                )
                .field(0)
                .type
            )
        unified_fields.append(pa.field(field0.name, unified_type))

    out_schema = pa.schema(unified_fields)
    aligned_batches = []
    for b in batches:
        if b.schema == out_schema:
            aligned_batches.append(b)
            continue
        cols = []
        for arr, field in zip(b.columns, out_schema):
            if arr.type == field.type:
                cols.append(arr)
            else:
                try:
                    cols.append(arr.cast(field.type))
                except Exception:
                    if pa.types.is_integer(arr.type) and pa.types.is_decimal(
                        field.type
                    ):
                        cols.append(pc.cast(pc.cast(arr, pa.string()), field.type))
                    else:
                        raise
        aligned_batches.append(pa.RecordBatch.from_arrays(cols, schema=out_schema))
    return out_schema, aligned_batches


def _sql_to_pyarrow_batches(
    self,
    expr,
    *,
    params=None,
    limit=None,
    chunk_size=1_000_000,
):
    """Execute expression and return an iterator of pyarrow record batches.

    Deviation from standard Ibis:
    Instead of forcing each batch into a single StructArray with the statically
    inferred schema type, this converts each column independently using
    `pa.array(col)` to let PyArrow preserve the cursor's actual data types and
    scales (e.g. Postgres DBAPI Decimal values), avoiding rescaling failures.
    """
    pa = self._import_pyarrow()

    schema = expr.as_table().schema()
    names = schema.names

    def _batch_to_record_batch(batch):
        if not batch:
            pa_schema = schema.to_pyarrow()
            return pa.RecordBatch.from_pydict(
                {name: [] for name in names}, schema=pa_schema
            )
        cols = list(zip(*batch))

        def _make_array(col):
            try:
                return pa.array(col)
            except OverflowError:
                return pa.array(
                    [decimal.Decimal(str(x)) if isinstance(x, int) else x for x in col]
                )

        arrays = [_make_array(col) for col in cols]
        pa_schema = pa.schema(
            [pa.field(name, arr.type) for name, arr in zip(names, arrays)]
        )
        return pa.RecordBatch.from_arrays(arrays, schema=pa_schema)

    batches = list(
        _batch_to_record_batch(batch)
        for batch in self._cursor_batches(
            expr, params=params, limit=limit, chunk_size=chunk_size
        )
    )
    if batches:
        out_schema, batches = _unify_record_batches(batches)
    else:
        out_schema = schema.to_pyarrow()
    return pa.ipc.RecordBatchReader.from_batches(out_schema, batches)


BaseSQLBackend.to_pyarrow_batches = _sql_to_pyarrow_batches


def _floats_exactly_fit_integer_decimal(col, target_type) -> bool:
    """Return True when float values can be converted to an integer decimal without loss.

    Some drivers return integer values as floats, e.g. Oracle describes a NUMBER(5) column
    as an unconstrained NUMBER in some grouped queries and python-oracledb fetches 1.0 instead
    of 1. When the expected type is an integer decimal (scale 0) and every value is a whole
    number exactly representable in float64 we can safely use the expected type.

    Note that a PyArrow float to decimal cast rounds (1.5 -> 2) rather than raising, so we
    must check the values ourselves.
    """
    if target_type.scale != 0:
        return False
    values = col.drop_null()
    if len(values) == 0:
        return True
    if not pc.all(pc.equal(values, pc.trunc(values))).as_py():
        return False
    if pc.max(pc.abs(values)).as_py() > _MAX_EXACT_FLOAT_INT:
        return False
    try:
        col.cast(target_type)
    except Exception:
        # e.g. 123456.0 overflows decimal(5, 0).
        return False
    return True


def _cast_column_preserving_precision(col, target_type):
    """Cast an Arrow column to target_type without losing cursor precision or overflowing."""
    # Avoid precision loss when cursor returns higher scale decimal or float for an aggregate,
    # or when cursor returns timestamp for a date column (e.g. Oracle DATE which stores time).
    if (
        (
            pa.types.is_decimal(target_type)
            and pa.types.is_decimal(col.type)
            and col.type.scale > target_type.scale
        )
        or (
            pa.types.is_decimal(target_type)
            and pa.types.is_floating(col.type)
            and not _floats_exactly_fit_integer_decimal(col, target_type)
        )
        or (pa.types.is_date(target_type) and pa.types.is_timestamp(col.type))
    ):
        return col

    try:
        return col.cast(target_type)
    except Exception:
        if (pa.types.is_decimal(target_type) and pa.types.is_decimal(col.type)) or (
            pa.types.is_integer(target_type)
            and (
                pa.types.is_decimal(col.type)
                or pa.types.is_integer(col.type)
                or pa.types.is_floating(col.type)
            )
        ):
            # The cursor value does not fit the statically inferred type,
            # e.g. SUM(NUMBER(38, 2)) exceeding 36 integer digits, PostgreSQL/MySQL
            # SUM(BIGINT) returned as Decimal > int64, or a fractional/overflowing
            # float returned for an integer expression. An unsafe cast would
            # silently wrap, truncate, or corrupt the value so we keep col as-is.
            return col

        if pa.types.is_integer(col.type) and pa.types.is_decimal(target_type):
            try:
                return pc.cast(pc.cast(col, pa.string()), target_type)
            except Exception:
                return col

        return pc.cast(col, target_type, safe=False)


def _align_table_preserving_precision(table, arrow_schema):
    """Rename and cast table columns to arrow_schema while preserving cursor precision."""
    table = table.rename_columns(arrow_schema.names)
    table_arrays = []
    final_fields = []
    for name, field in zip(table.column_names, arrow_schema):
        cast_col = _cast_column_preserving_precision(table[name], field.type)
        table_arrays.append(cast_col)
        final_fields.append(pa.field(name, cast_col.type))
    return pa.Table.from_arrays(table_arrays, schema=pa.schema(final_fields))


def _base_to_pyarrow(self, expr, *, params=None, limit=None, **kwargs):
    """Execute expression and return results as a pyarrow Table.

    Deviation from standard Ibis:
    Standard Ibis does `table.cast(arrow_schema)`. Here, we inspect each column:
    if the column in the batch was returned by the database cursor with a higher
    decimal scale, wider precision, or as a float (e.g. for aggregations like
    STDDEV, AVG, or overflowing SUM), we preserve the cursor's full precision
    rather than truncating or wrapping it to the column's static schema definition.
    """
    pa = self._import_pyarrow()
    self._run_pre_execute_hooks(expr)
    table_expr = expr.as_table()
    arrow_schema = table_expr.schema().to_pyarrow()
    try:
        with self.to_pyarrow_batches(
            table_expr, params=params, limit=limit, **kwargs
        ) as reader:
            batches = list(reader)
            if not batches:
                result = arrow_schema.empty_table()
            else:
                table = pa.Table.from_batches(batches)
                result = _align_table_preserving_precision(table, arrow_schema)
    except pa.lib.ArrowInvalid:
        raise
    except ValueError:
        result = arrow_schema.empty_table()

    if isinstance(expr, ibis.expr.types.Table):
        return result
    return expr.__pyarrow_result__(result)


BaseBackend.to_pyarrow = _base_to_pyarrow


def _snowflake_to_pyarrow(self, expr, *, params=None, limit=None, **_):
    """Execute Snowflake expression and align Arrow columns without precision loss."""
    self._run_pre_execute_hooks(expr)
    table_expr = expr.as_table()
    target_schema = table_expr.schema().to_pyarrow()

    query_ast = self.compiler.to_ast_ensure_limit(expr, limit, params=params)
    sql = query_ast.compile()
    with self.begin() as con:
        res = con.execute(sql).cursor.fetch_arrow_all()

    if res is None or res.num_rows == 0:
        result = target_schema.empty_table()
    else:
        result = _align_table_preserving_precision(res, target_schema)

    if isinstance(expr, ibis.expr.types.Table):
        return result
    return expr.__pyarrow_result__(result)


if SnowflakeBackend:
    SnowflakeBackend.to_pyarrow = _snowflake_to_pyarrow
