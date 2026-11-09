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
_bigquery_cast = OPERATION_REGISTRY[ops.Cast]

STRFTIME_FORMAT_FUNCTIONS = {
    dt.date: "DATE",
    dt.Timestamp(timezone="UTC"): "TIMESTAMP",
}


def _compiles_strftime(translator, op):
    """Timestamp formatting."""
    arg = op.arg
    format_str = op.format_str
    arg_type = arg.dtype
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


def _date_to_utc_timestamp(arg_formatted: str) -> str:
    """Convert a Spanner DATE expression to a TIMESTAMP at midnight UTC.

    A bare CAST(date AS TIMESTAMP) in Spanner uses the default time zone
    (America/Los_Angeles) rather than UTC, therefore we specify UTC explicitly.
    """
    return f"TIMESTAMP({arg_formatted}, 'UTC')"


def _cast(translator, op):
    """Cast with Spanner specific handling for TIMESTAMP targets.

    Spanner GoogleSQL does not have a DATETIME type, therefore a cast to a
    time zone naive timestamp must not be translated to DATETIME as it would
    be for BigQuery.
    """
    arg, target_type = op.arg, op.to
    if target_type.is_timestamp():
        arg_formatted = translator.translate(arg)
        if arg.dtype.is_date():
            return _date_to_utc_timestamp(arg_formatted)
        elif arg.dtype.is_timestamp():
            # All Spanner timestamps are TIMESTAMP, no cast required.
            return arg_formatted
        return f"CAST({arg_formatted} AS TIMESTAMP)"
    return _bigquery_cast(translator, op)


def _extract_epoch_seconds(translator, op):
    """Spanner UNIX_SECONDS only accepts TIMESTAMP, not DATE."""
    arg = op.arg
    arg_formatted = translator.translate(arg)
    if arg.dtype.is_date():
        arg_formatted = _date_to_utc_timestamp(arg_formatted)
    return f"UNIX_SECONDS({arg_formatted})"


_registry.update(
    {
        ops.Cast: _cast,
        ops.ExtractEpochSeconds: _extract_epoch_seconds,
        ops.Strftime: _compiles_strftime,
    }
)
