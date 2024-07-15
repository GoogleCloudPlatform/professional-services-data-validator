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

import ibis.expr.operations as ops
import ibis.expr.datatypes as dt
from ibis.backends.bigquery.registry import OPERATION_REGISTRY

_registry = OPERATION_REGISTRY.copy()

STRFTIME_FORMAT_FUNCTIONS = {
    dt.date: "DATE",
    dt.Timestamp(timezone="UTC"): "TIMESTAMP",
}


def _compiles_strftime(translator, op):
    """Timestamp formatting."""
    arg = op.arg
    format_str = op.format_str
    arg_type = arg.output_dtype
    strftime_format_func_name = STRFTIME_FORMAT_FUNCTIONS[arg_type]
    fmt_string = translator.translate(format_str)
    # Deal with issue 1181 due a GoogleSQL bug with dates before 1000 CE affects both date and timestamp types
    if format_str.value.startswith("%Y"):
        fmt_string = fmt_string.replace("%Y", "%E4Y", 1)
    arg_formatted = translator.translate(arg)
    if isinstance(arg_type, dt.Timestamp) and arg_type.timezone is None:
        # As per issue-767, fixes issue in Spanner where TIMESTAMP() function
        # throws exception if column is already a TIMESTAMP
        arg_formatted = arg_formatted.rstrip(")")
        arg_formatted = arg_formatted.lstrip("TIMESTAMP(")
        return "FORMAT_{}({}, {})".format(
            strftime_format_func_name,
            fmt_string,
            arg_formatted,
        )
    elif isinstance(arg_type, dt.Timestamp):
        # As per issue-767, fixes issue in Spanner where TIMESTAMP() function
        # throws exception if column is already a TIMESTAMP
        arg_formatted = arg_formatted.rstrip(")")
        arg_formatted = arg_formatted.lstrip("TIMESTAMP(")
        return "FORMAT_{}({}, {}, {!r})".format(
            strftime_format_func_name,
            fmt_string,
            arg_formatted,
            arg_type.timezone,
        )
    else:
        return "FORMAT_{}({}, {})".format(
            strftime_format_func_name, fmt_string, arg_formatted
        )


_registry.update(
    {
        ops.Strftime: _compiles_strftime,
    }
)
