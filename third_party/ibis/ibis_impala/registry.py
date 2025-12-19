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

import sqlalchemy as sa
import ibis.expr.operations as ops
from ibis.backends.base.sql.registry import (
    type_to_sql_string as base_type_to_sql_string,
)


def sa_cast(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    arg_formatted = t.translate(arg)

    if arg_dtype.is_binary() and typ.is_string():
        # Binary to string cast is a "to hex" conversion for DVT.
        return f"lower(hex({arg_formatted}))"
    elif arg_dtype.is_string() and typ.is_binary():
        # Binary from string cast is a "from hex" conversion for DVT.
        return f"unhex({arg_formatted})"

    # Cannot use sa_fixed_cast() because of ImpalaExprTranslator ancestry.
    sql_type = base_type_to_sql_string(typ)
    cast_expr = "CAST({} AS {})".format(arg_formatted, sql_type)

    if arg_dtype.is_boolean() and typ.is_string():
        return f"LOWER({cast_expr})"
    else:
        return cast_expr


def sa_ifnull(t, op):
    sa_arg = t.translate(op.arg)
    return sa.func.coalesce(sa_arg, op.ifnull_expr.value)


def sa_format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"sha2({arg}, 256)"
    elif op.how == "md5":
        return f"md5({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def sa_strftime(t, op):
    import sqlglot as sg

    hive_dialect = sg.dialects.hive.Hive
    if (time_mapping := getattr(hive_dialect, "TIME_MAPPING", None)) is None:
        time_mapping = hive_dialect.time_mapping
    reverse_hive_mapping = {v: k for k, v in time_mapping.items()}
    format_str = sg.time.format_time(op.format_str.value, reverse_hive_mapping)
    targ = t.translate(ops.Cast(op.arg, to=dt.string))
    return f"from_unixtime(unix_timestamp({targ}, {format_str!r}), {format_str!r})"
