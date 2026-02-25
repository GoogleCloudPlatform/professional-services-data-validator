# Copyright 2025 Google Inc.
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

import ibis.expr.operations as ops
import sqlalchemy as sa

from third_party.ibis.ibis_db2.registry import (
    operation_registry as db2_luw_operation_registry,
    db2_luw_cast,
    db2_luw_strftime,
)

operation_registry = db2_luw_operation_registry.copy()


def _sa_ifnull(t, op):
    sa_arg = t.translate(op.arg)
    return sa.func.coalesce(sa_arg, sa.literal_column(f"'{op.ifnull_expr.value}'"))


def _sa_format_hashbytes(translator, op):
    # Use of query parameters is throwing an error on Db2 z/OS.
    # Code below uses sa.sql.literal_column to prevent parameterization.
    # I invested several hours into trying to find a way to utilize literal_binds=True but was unsuccessful.
    compiled_arg = translator.translate(op.arg)
    hash_func = sa.func.hash_sha256(sa.func.unicode_str(compiled_arg))
    hex_func = sa.func.hex(hash_func)
    return sa.func.lower(hex_func)


def _sa_whitespace_rstrip(t, op):
    # The full string.whitespace second parameter was throwing an error on Db2 z/OS:
    #   the data type, length or value of the argument for the parameter in position "2" of routine "RTRIM" is incorrect
    # Therefore we've removed it below and let Db2 exclude default space character.
    sa_arg = t.translate(op.arg)
    return sa.func.rtrim(sa_arg)


def _sa_strftime(t, op):
    return db2_luw_strftime(t, op, allow_query_params=False)


def db2_zos_cast(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)

    if (
        arg_dtype.is_decimal()
        and typ.is_string()
        and arg_dtype.scale is not None
        and arg_dtype.scale > 0
    ):
        # The Db2 LUW regexp_replace technique is not valid for z/OS because regexp_replace is not always available, from the docs:
        #   "Passthrough-only expression: This function is passthrough-only and cannot run on Db2 for z/OS® without acceleration."
        #
        # We use an alternative RTRIM based technique in this z/OS specialization.

        # Db2 always pads fractional part of the number out to length of scale.
        # We need to remove those insignificant digits.
        precision = arg_dtype.precision or 31
        fmt = ("9" * (precision - arg_dtype.scale - 1)) + "0." + ("9" * arg_dtype.scale)
        # Using sa.literal_column below because z/OS does not support parameterized queries.
        return sa.func.ltrim(
            sa.func.rtrim(
                sa.func.rtrim(
                    sa.func.to_char(sa_arg, sa.literal_column(fmt)),
                    sa.literal_column("'0'"),
                ),
                sa.literal_column("'.'"),
            )
        )

    return db2_luw_cast(t, op)


operation_registry[ops.Cast] = db2_zos_cast
operation_registry[ops.HashBytes] = _sa_format_hashbytes
operation_registry[ops.IfNull] = _sa_ifnull
operation_registry[ops.RStrip] = _sa_whitespace_rstrip
operation_registry[ops.Strftime] = _sa_strftime
